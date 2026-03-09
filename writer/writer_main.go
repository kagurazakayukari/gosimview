// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gosimview/config"
	"gosimview/udp"

	_ "github.com/go-sql-driver/mysql"
)

// 全局变量
var (
	logger              *log.Logger
	fatalLogger         *log.Logger
	cfg                 *config.Config
	globalDBWriter      *DBWriter
	globalEvent         *Event
	globalSession       *Session
	logDir              string
	latestFile          os.DirEntry
	receivedSessionInfo bool
	receivedVersionInfo bool
	// 新增的全局变量
	globalUsers        map[int]*User
	globalTeams        map[int]*Team
	globalTeamMembers  map[int]*TeamMember
	globalCars         map[int]*Car
	globalTrackConfigs map[int]*TrackConfig
	// 互斥锁
	globalMutex      sync.Mutex
	connectionsMutex sync.Mutex
	entryListMutex   sync.Mutex
	sessionMutex     sync.Mutex
	// 数据收集控制
	dataCollectionEnabled bool = false
	// 全局变量用于暂存车辆信息，带互斥锁保证并发安全
	carLapDataMap = make(map[int][]CarLapData)
	stintLapData  = make(map[int]map[int][]StintLap)
	// 用于跟踪单圈数据的全局变量
	carLapData        = make(map[int][]udp.CarUpdate)
	telemetryData     = make(map[int][]byte)
	globalConnections map[int]*ConnectionInfo
	globalEntryList   map[int]EntryListDriver
	lapDataMutex      sync.Mutex
	// 日志文件处理变量
	lastFileSize    int64
	lastLineIdx     int
	partialLine     string
	lastFileModTime time.Time // 新增：用于跟踪日志文件的最后修改时间
	// 静态会话ID计数器，用于模拟环境
	staticSessionIDCounter int64
	// 维修区边界
	pitBoundary [][3]float32
	// 新增：用于实时发送遥测数据的全局变量
	latestCarUpdates = make(map[udp.CarID]udp.CarUpdate)
	wsMutex          sync.Mutex
)

// 定义结构体
// User 用户表数据结构
type User struct {
	UserID    int    `db:"user_id"`
	Name      string `db:"name"`
	PrevName  string `db:"prev_name"`
	Steam64ID int64  `db:"steam64_id"`
	Country   string `db:"country"`
}

// Team 团队表数据结构
type Team struct {
	ID         int       `db:"team_id"`
	EventID    int       `db:"event_id"`
	Name       string    `db:"name"`
	TeamNo     int       `db:"team_no"`
	CarID      int       `db:"car_id"`
	LiveryName string    `db:"livery_name"`
	Active     int       `db:"active"`
	CreatedAt  time.Time `db:"created_at"`
}

// TeamMember 团队成员表数据结构
type TeamMember struct {
	ID        int       `db:"team_member_id"`
	TeamID    int       `db:"team_id"`
	UserID    int       `db:"user_id"`
	Role      string    `db:"role"`
	Active    int       `db:"active"`
	CreatedAt time.Time `db:"created_at"`
}

// EntryListDriver 表示entry_list.ini中的驱动配置
type EntryListDriver struct {
	Index      int
	Name       string
	Team       string
	Car        string
	Number     string
	Skin       string
	Ballast    int
	Restrictor int
	GUID       string
}

// JSONLap 定义JSON中的圈速信息
type JSONLap struct {
	DriverName string `json:"DriverName"`
	DriverGuid string `json:"DriverGuid"`
	CarId      int    `json:"CarId"`
	CarModel   string `json:"CarModel"`
	Timestamp  int    `json:"Timestamp"`
	StintID    int    `json:"StintID"`
	Sectors    [3]int `json:"Sectors"`
	Time       int    `json:"Time"`
	Tyre       string `json:"Tyre"`
	Cuts       int    `json:"Cuts"`
	LapTime    int    `json:"LapTime"`
	BallastKG  int    `json:"BallastKG"`
	Restrictor int    `json:"Restrictor"`
}

// SessionData 定义完整的JSON文件数据结构
type SessionData struct {
	TrackName    string      `json:"TrackName"`
	TrackConfig  string      `json:"TrackConfig"`
	Type         string      `json:"Type"`
	DurationSecs int         `json:"DurationSecs"`
	RaceLaps     int         `json:"RaceLaps"`
	Cars         []ResultCar `json:"Cars"`
	Result       []Result    `json:"Result"`
	Laps         []JSONLap   `json:"Laps"`
}

// ResultCar 定义JSON中的车辆信息
type ResultCar struct {
	CarId      int    `json:"CarId"`
	Driver     Driver `json:"Driver"`
	Model      string `json:"Model"`
	Skin       string `json:"Skin"`
	BallastKG  int    `json:"BallastKG"`
	Restrictor int    `json:"Restrictor"`
}

// Driver 定义JSON中的驾驶员信息
type Driver struct {
	Name      string   `json:"Name"`
	Team      string   `json:"Team"`
	Nation    string   `json:"Nation"`
	Guid      string   `json:"Guid"`
	GuidsList []string `json:"GuidsList"` // 团队赛事中的多个GUID列表
}

// Result 定义JSON中的结果信息
type Result struct {
	DriverName string `json:"DriverName"`
	DriverGuid string `json:"DriverGuid"`
	CarId      int    `json:"CarId"`
	CarModel   string `json:"CarModel"`
	BestLap    int    `json:"BestLap"`
	TotalTime  int    `json:"TotalTime"`
	BallastKG  int    `json:"BallastKG"`
	Restrictor int    `json:"Restrictor"`
}

// Lap 圈速数据结构
type Lap struct {
	StintID    int    `json:"stint_id"`
	Sectors    [3]int `json:"sectors"`
	Time       int    `json:"time"`
	Tyre       string `json:"tyre"`
	Cuts       int    `json:"cuts"`
	Crashes    int    `json:"crashes"`
	CarCrashes int    `json:"car_crashes"`
}

// CarLapData 车辆单圈数据结构体
type CarLapData struct {
	SessionID  int64     `json:"session_id"`
	CarID      int       `json:"car_id"`
	GameCarID  int       `json:"game_car_id"`
	LapTime    int       `json:"lap_time"`
	Sector1    int       `json:"sector_1"`
	Sector2    int       `json:"sector_2"`
	Sector3    int       `json:"sector_3"`
	Tyre       string    `json:"tyre"`
	Cuts       uint8     `json:"cuts"`
	Crashes    uint8     `json:"crashes"`
	CarCrashes uint8     `json:"car_crashes"`
	Laps       uint16    `json:"laps"`
	Completed  uint16    `json:"completed"`
	Timestamp  time.Time `json:"timestamp"`
}

// Event 事件结构体
type Event struct {
	ID                   int
	Name                 string
	ServerName           string
	TrackConfigID        int
	PracticeDuration     int
	QualiDuration        int
	RaceDuration         int
	RaceDurationType     int
	RaceExtraLaps        int
	ReverseGridPositions int
	TeamEvent            int
	Active               int
	StartGrip            float64
	RaceWaitTime         int
	ReverseGridPos       int
	LiveryPreview        int
	UseNumber            int
}

// Session 会话结构体
type Session struct {
	ID           int64
	EventID      int
	Type         int
	Name         string
	TrackTime    string
	StartTime    time.Time
	DurationMin  int
	ElapsedMs    int
	Laps         int
	Weather      string
	AirTemp      float64
	RoadTemp     float64
	StartGrip    float64
	CurrentGrip  float64
	IsFinished   int
	FinishTime   time.Time
	LastActivity time.Time
	HTTPPort     int
}

// ServerConfig 服务器配置结构体
type ServerConfig struct {
	Name                string
	Slots               int
	Port                int
	HTTP                string
	HTTPPort            int
	AdminPassword       string
	Password            string
	MaxCarSlots         int
	IdleTimeout         int
	VoteTimeout         int
	QualifyStandingType int
	PitWindowStart      int
	PitWindowEnd        int
	ShortFormationLap   int
}

// Car 车辆结构体
type Car struct {
	ID           int    `db:"car_id"`
	DisplayName  string `db:"display_name"`
	Name         string `db:"name"`
	Manufacturer string `db:"manufacturer"`
	CarClass     string `db:"car_class"`
	DriverName   string
	TeamName     string
	CarModel     string
	Number       int
	Ballast      int
	Restrictor   int
}

// SessionStint 会话阶段结构体
type SessionStint struct {
	ID           int       `db:"session_stint_id" json:"id"`
	UserID       int       `db:"user_id" json:"user_id"`
	TeamMemberID int       `db:"team_member_id" json:"team_member_id"`
	SessionID    int       `db:"session_id" json:"session_id"`
	CarID        int       `db:"car_id" json:"car_id"`
	GameCarID    int       `db:"game_car_id" json:"game_car_id"`
	Laps         int       `db:"laps" json:"laps"`
	ValidLaps    int       `db:"valid_laps" json:"valid_laps"`
	BestLapID    int       `db:"best_lap_id" json:"best_lap_id"`
	IsFinished   int       `db:"is_finished" json:"is_finished"`
	StartedAt    time.Time `db:"started_at" json:"started_at"`
	FinishedAt   time.Time `db:"finished_at" json:"finished_at"`
}

// StintLap 阶段圈速结构体
type StintLap struct {
	ID         int       `db:"stint_lap_id"`
	StintID    int       `db:"stint_id"`
	Sector1    int       `db:"sector_1"`
	Sector2    int       `db:"sector_2"`
	Sector3    int       `db:"sector_3"`
	Grip       float64   `db:"grip"`
	Tyre       string    `db:"tyre"`
	Time       int       `db:"time"`
	Cuts       int       `db:"cuts"`
	Crashes    int       `db:"crashes"`
	CarCrashes int       `db:"car_crashes"`
	MaxSpeed   int       `db:"max_speed"`
	AvgSpeed   int       `db:"avg_speed"`
	FinishedAt time.Time `db:"finished_at"`
}

// LapTelemetry 圈速遥测结构体
type LapTelemetry struct {
	ID        int64
	LapID     int64
	Telemetry []byte
}

// SessionFeed 会话feed结构体
type SessionFeed struct {
	ID        int64
	SessionID int64
	Type      int
	Detail    string
	Time      time.Time
}

// Entry 排行榜条目结构体
type Entry struct {
	TeamID        int64
	UserID        int64
	CarID         uint16
	GameCarID     int
	DriverName    string
	TeamName      string
	StatusMask    uint8
	Laps          uint16
	ValidLaps     uint16
	NSP           float32
	PosX          float32
	PosZ          float32
	TelemetryMask uint16
	RPM           uint16
	TyreLength    uint8
	Tyre          string
	BestLapS1     uint32
	BestLapS2     uint32
	BestLapS3     uint32
	BestLapTime   uint32
	CurrentLapS1  uint32
	CurrentLapS2  uint32
	CurrentLapS3  uint32
	SectorMask    uint8
	LastLapTime   int32
	Gap           int32
	Interval      int32
	PosChange     int8
}

// ACSession ACSession结构体
type ACSession struct {
	SessionID   int64
	Type        int
	DurationMin int
	StartGrip   float64
	CurrentGrip float64
	ElapsedMs   int64
}

// TrackConfig 赛道配置结构体
type TrackConfig struct {
	ID          int
	TrackName   string
	ConfigName  string
	DisplayName string
	Country     string
	City        string
	Length      int
}

func handleLapCompletedMessage(lapMsg udp.LapCompleted, lastDataTime *time.Time) {
	*lastDataTime = time.Now()
	collectCarUpdates()
	// 处理单圈完成事件，暂存所有车辆信息
	lapDataMutex.Lock()
	defer lapDataMutex.Unlock()

	// 仅处理当前完成圈数事件的车辆
	car := lapMsg.Cars[lapMsg.CarID]
	lapData := CarLapData{
		SessionID:  globalSession.ID,
		CarID:      int(car.CarID),
		GameCarID:  0,
		LapTime:    int(lapMsg.LapTime),
		Sector1:    0,
		Sector2:    0,
		Sector3:    0,
		Tyre:       "",
		Cuts:       uint8(lapMsg.Cuts),
		Crashes:    0,
		CarCrashes: 0,
		Laps:       car.Laps,
		Completed:  uint16(car.Completed),
		Timestamp:  time.Now(),
	}

	// 从stintLapData获取最新圈数据
	if dataCollectionEnabled {
		sessionMap, sessionOk := stintLapData[int(lapData.SessionID)]
		if sessionOk {
			laps, ok := sessionMap[lapData.CarID]
			if ok && len(laps) > 0 {
				lastLap := laps[len(laps)-1]
				lapData.LapTime = lastLap.Time
				if lastLap.Sector1 >= 0 {
					lapData.Sector1 = lastLap.Sector1
				}
				if lastLap.Sector2 >= 0 {
					lapData.Sector2 = lastLap.Sector2
				}
				if lastLap.Sector3 >= 0 {
					lapData.Sector3 = lastLap.Sector3
				}
				if lastLap.Tyre != "" {
					lapData.Tyre = lastLap.Tyre
				}
				lapData.Cuts = uint8(lastLap.Cuts)
				lapData.Crashes = uint8(lastLap.Crashes + lastLap.CarCrashes)
			}
		}
	}

	// 获取用户连接信息并处理业务逻辑
	connectionsMutex.Lock()
	connInfo, exists := globalConnections[lapData.CarID]
	connectionsMutex.Unlock()

	if exists && connInfo.DriverGUID != "" {
		// 解析GUID并查询用户
		steam64ID, err := strconv.ParseInt(connInfo.DriverGUID, 10, 64)
		if err != nil {
			logger.Printf("解析DriverGUID失败: %v", err)
			// 跳过本次循环剩余逻辑
			return
		}

		// 查询或创建用户
		var userID int
		foundUser := false
		for _, u := range globalUsers {
			if u.Steam64ID == steam64ID {
				userID = u.UserID
				foundUser = true
				break
			}
		}
		if !foundUser {
			user := &User{Steam64ID: steam64ID}
			userID, _, err = globalDBWriter.FindOrCreateUser(user)
			if err != nil {
				logger.Printf("手动添加用户失败: %v", err)
				return
			}
		}

		// 从globalCars缓存获取GameCarID
		var gameCarID int
		for _, car := range globalCars {
			if car.Name == connInfo.CarModel {
				gameCarID = car.ID
				break
			}
		}
		lapData.GameCarID = gameCarID

		// 初始化团队成员ID和团队查找状态
		var teamMemberID int
		var teamFound bool = false

		// 仅团队事件处理团队成员信息
		if globalEvent.TeamEvent == 1 {
			teamFound = false
			// 查询团队成员缓存
			for _, tm := range globalTeamMembers {
				if tm.UserID == userID {
					for _, t := range globalTeams {
						if t.ID == tm.TeamID && t.EventID == globalEvent.ID {
							teamMemberID = tm.ID
							teamFound = true
							break
						}
					}
					if teamFound {
						break
					}
				}
			}
		} // 查找未完成的stint
		if dataCollectionEnabled {
			var existingStintID int
			var existingLaps int
			var existingValidLaps int
			var existingBestLapID int
			queryErr := globalDBWriter.DB.QueryRow(
				"SELECT id, laps, valid_laps, best_lap_id FROM session_stint WHERE user_id = ? AND session_id = ? AND car_id = ? AND is_finished = 0",
				userID, globalSession.ID, lapData.CarID,
			).Scan(&existingStintID, &existingLaps, &existingValidLaps, &existingBestLapID)

			// 创建或更新stint记录
			var stintID int64
			var isCurrentLapValid bool
			if queryErr == nil {
				// 更新现有未完成的stint
				laps := existingLaps + 1
				validLaps := existingValidLaps
				bestLapID := existingBestLapID

				// 先创建stintLap获取lapID
				stintLap := &StintLap{
					StintID:    existingStintID,
					Sector1:    lapData.Sector1,
					Sector2:    lapData.Sector2,
					Sector3:    lapData.Sector3,
					Grip:       globalSession.CurrentGrip,
					Tyre:       lapData.Tyre,
					Time:       lapData.LapTime,
					Cuts:       int(lapData.Cuts),
					Crashes:    int(lapData.Crashes),
					CarCrashes: int(lapData.CarCrashes),
					// 从carLapData计算最大速度
					MaxSpeed:   int(calculateMaxSpeed(carLapData[lapData.CarID])),
					AvgSpeed:   int(float64(globalTrackConfigs[globalEvent.TrackConfigID].Length) * 3.6 / float64(lapData.LapTime)),
					FinishedAt: lapData.Timestamp,
				}
				lapID, err := globalDBWriter.InsertStintLap(stintLap)
				if err != nil {
					logger.Printf("插入圈速记录失败: %v", err)
				}

				result, err := globalDBWriter.DB.Exec("INSERT INTO session_stint (session_id, user_id, team_member_id, car_id, game_car_id, started_at) VALUES (?, ?, ?, ?, ?, ?)",
					globalSession.ID, userID, 0, lapData.CarID, 0, time.Now())
				if err != nil {
					logger.Printf("创建SessionStint失败: %v", err)
					return
				}
				_, err = result.LastInsertId()
				if err != nil {
					logger.Printf("获取SessionStint ID失败: %v", err)
					return
				}

				// 检查当前圈速是否有效并更新valid_laps
				if lapData.Completed > 0 && lapData.Cuts == 0 && lapData.Crashes == 0 {
					validLaps++
					isCurrentLapValid = true
					// 将lapData.LapTime从毫秒转换为秒以匹配数据库单位
					// 检查是否为单阶段最快圈速 (case 4)
					if isCurrentLapValid && (bestLapID == int(lapID)) {
						stintBestDetail := fmt.Sprintf(`{"driver":"%s","lap_time":%d,"sector1":%d,"sector2":%d,"sector3":%d}`, connInfo.DriverName, lapData.LapTime, lapData.Sector1, lapData.Sector2, lapData.Sector3)
						globalDBWriter.InsertSessionFeed(&SessionFeed{
							SessionID: globalSession.ID,
							Type:      4,
							Detail:    stintBestDetail,
							Time:      time.Now(),
						})
					}

					// 检查是否为全场组别最快圈速 (case 5)
					// 查找当前车辆类别
					var carClass string
					for _, car := range globalCars {
						if car.ID == gameCarID {
							carClass = car.CarClass
							break
						}
					}
					if carClass != "" {
						// 查询当前组别最快圈速
						var classBestLapTime int
						err := globalDBWriter.DB.QueryRow(
							`SELECT MIN(sl.time) FROM stint_lap sl
								JOIN session_stint ss ON sl.stint_id = ss.id
								JOIN cars c ON ss.game_car_id = c.id
								WHERE ss.session_id = ? AND c.car_class = ?`,
							globalSession.ID, carClass,
						).Scan(&classBestLapTime)

						// 如果查询失败(无记录)或当前圈速更快，则更新组别最快圈速
						if err != nil || (isCurrentLapValid && lapData.LapTime < classBestLapTime) {
							classBestDetail := fmt.Sprintf(`{"driver":"%s","class":"%s","lap_time":%d}`, connInfo.DriverName, carClass, lapData.LapTime)
							globalDBWriter.InsertSessionFeed(&SessionFeed{
								SessionID: globalSession.ID,
								Type:      5,
								Detail:    classBestDetail,
								Time:      time.Now(),
							})
						}
					}

					// 将lapData.LapTime从毫秒转换为秒以匹配数据库单位
					if bestLapID == 0 || float64(lapData.LapTime)/1000 < getLapTimeByID(bestLapID) {
						bestLapID = int(lapID)
					}
				}
				_, err = globalDBWriter.DB.Exec(
					"UPDATE session_stint SET laps = ?, valid_laps = ?, best_lap_id = ?, finished_at = ? WHERE id = ?",
					laps, validLaps, bestLapID, time.Now(), existingStintID,
				)
				if err != nil {
					logger.Printf("更新stint记录失败: %v", err)
					return
				}
				stintID = int64(existingStintID)
			} else {
				// 创建新的stint记录
				stint := &SessionStint{
					UserID:       userID,
					TeamMemberID: teamMemberID,
					SessionID:    int(globalSession.ID),
					CarID:        lapData.CarID,
					GameCarID:    lapData.GameCarID,
					Laps:         1,
					ValidLaps:    boolToInt(lapData.Cuts == 0 && lapData.Crashes == 0 && lapData.CarCrashes == 0),
					IsFinished:   0,
					StartedAt:    time.Now(),
					FinishedAt:   time.Now(),
				}
				isCurrentLapValid = false
				var sqlStr string
				var args []interface{}
				if globalEvent.TeamEvent == 1 {
					sqlStr = "INSERT INTO session_stint (user_id, team_member_id, session_id, car_id, game_car_id, laps, valid_laps, is_finished, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
					args = []interface{}{stint.UserID, stint.TeamMemberID, stint.SessionID, stint.CarID, stint.GameCarID, stint.Laps, stint.ValidLaps, stint.IsFinished, stint.StartedAt, stint.FinishedAt}
				} else {
					sqlStr = "INSERT INTO session_stint (user_id, session_id, car_id, game_car_id, laps, valid_laps, is_finished, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
					args = []interface{}{stint.UserID, stint.SessionID, stint.CarID, stint.GameCarID, stint.Laps, stint.ValidLaps, stint.IsFinished, stint.StartedAt, stint.FinishedAt}
				}
				result, err := globalDBWriter.DB.Exec(sqlStr, args...)
				if err == nil {
					stintID, _ = result.LastInsertId()
				}
			}
			if err == nil {

				stintLap := &StintLap{
					StintID:    int(stintID),
					Sector1:    lapData.Sector1,
					Sector2:    lapData.Sector2,
					Sector3:    lapData.Sector3,
					Grip:       globalSession.CurrentGrip,
					Tyre:       lapData.Tyre,
					Time:       lapData.LapTime,
					Cuts:       int(lapData.Cuts),
					Crashes:    int(lapData.Crashes),
					CarCrashes: int(lapData.CarCrashes),
					MaxSpeed:   int(calculateMaxSpeed(carLapData[lapData.CarID])),
					AvgSpeed:   int(float64(globalTrackConfigs[globalEvent.TrackConfigID].Length) * 3.6 / float64(lapData.LapTime)),
					FinishedAt: lapData.Timestamp,
				}
				lapID, err := globalDBWriter.InsertStintLap(stintLap)
				if err != nil {
					logger.Printf("插入圈速记录失败: %v", err)
					return
				}
				logger.Printf("创建stint %d 及圈速记录 for 车辆 %d", stintID, lapData.CarID)

				// 仅有效圈才插入遥测数据
				if isCurrentLapValid {
					// 插入遥测数据
					if telemetry, ok := telemetryData[lapData.CarID]; ok && len(telemetry) > 0 {
						if err := globalDBWriter.InsertLapTelemetry(int(lapID), telemetry); err != nil {
							logger.Printf("插入遥测数据失败: %v", err)
						} else {
							logger.Printf("遥测数据已插入，lapID: %d, 数据大小: %d 字节", lapID, len(telemetry))
						}
					}
					delete(telemetryData, lapData.CarID) // 清除已完成圈数的遥测数据
				}
			}
		}
		// 数据收集未启用时跳过数据库操作
	} else {
		//logger.Printf("数据收集未启用，跳过stint和圈速记录处理")
	}
	// 添加到对应车辆的暂存列表
	carLapDataMap[lapData.CarID] = append(carLapDataMap[lapData.CarID], lapData)
	logger.Printf("暂存车辆 %d 圈速数据，累计 %d 条", lapData.CarID, len(carLapDataMap[lapData.CarID]))
}

func handleCarInfoMessage(carInfoMsg udp.CarInfo, lastDataTime *time.Time) {
	*lastDataTime = time.Now()
}

func handleClientLoadedMessage(clientMsg udp.ClientLoaded, lastDataTime *time.Time) {
	*lastDataTime = time.Now()
	connInfo := globalConnections[int(clientMsg.Event())]
	if connInfo != nil {
		connInfo.ConnectionStatus = 0 // 设为已连接状态
	}
}

type PitVisit struct {
	EntryTime time.Time
	ExitTime  time.Time
}

type ConnectionInfo struct {
	CarID                      int
	DriverName                 string
	DriverGUID                 string
	CarModel                   string
	CarSkin                    string
	DriverInitials             string
	CarName                    string
	ConnectionStatus           uint8      // 连接状态掩码（0=已连接, 1=加载中, 2=已断开连接）
	TrackStatus                uint8      // 赛道状态掩码（0=正常赛道, 1=偏离赛道, 2=维修区入口, 3=维修区, 4=维修区出口）
	TrackStatusLastChangedTime time.Time  // 赛道状态最后变更时间
	LastConnectionLine         int        // 最后连接日志行号
	PitEntryTime               time.Time  // 进入维修区的时间
	LastMoveTime               time.Time  // 最后移动的时间
	PitVisits                  []PitVisit // 维修区访问历史记录
}

func getLapTimeByID(lapID int) float64 {
	var lapTime float64
	err := globalDBWriter.DB.QueryRow("SELECT time FROM stint_lap WHERE id = ?", lapID).Scan(&lapTime)
	if err != nil {
		return math.MaxFloat64 // 返回最大浮点数确保当前圈速成为最佳圈
	}
	return lapTime
}

func handleNewConnectionMessage(connMsg udp.SessionCarInfo, lastDataTime *time.Time) {
	carID := int(connMsg.CarID)
	// 存储连接信息到全局变量
	connectionsMutex.Lock()
	globalConnections[carID] = &ConnectionInfo{
		CarID:                      carID,
		DriverName:                 connMsg.DriverName,
		DriverGUID:                 string(connMsg.DriverGUID),
		CarModel:                   connMsg.CarModel,
		CarSkin:                    connMsg.CarSkin,
		DriverInitials:             connMsg.DriverInitials,
		CarName:                    connMsg.CarName,
		ConnectionStatus:           1,
		TrackStatus:                3,
		TrackStatusLastChangedTime: time.Now(),
	}
	connectionsMutex.Unlock()
	// 处理用户GUID并查询/创建用户
	steam64ID, err := strconv.ParseInt(string(connMsg.DriverGUID), 10, 64)
	if err != nil {
		logger.Printf("解析DriverGUID失败: %v\n", err)
	} else {
		user := &User{
			Name:      connMsg.DriverName,
			Steam64ID: steam64ID,
		}

		userID, newUser, err := globalDBWriter.FindOrCreateUser(user)
		if err != nil {
			logger.Printf("查询/创建用户 %s 失败: %v\n", connMsg.DriverName, err)
		} else {
			if newUser {
				logger.Printf("新用户 (CarID: %d): %d, %s\n", int(connMsg.CarID), userID, connMsg.DriverName)
			} else {
				logger.Printf("已知用户 (CarID: %d): %d, %s\n", int(connMsg.CarID), userID, connMsg.DriverName)
			}
		}
	}

	// 插入用户连接事件SessionFeed (case2)
	userID := getUserIDByCarID(carID)
	teamID := getTeamIDByCarID(carID)
	detail := map[string]interface{}{
		"user_id": userID,
		"team_id": teamID,
		"car_id":  carID,
	}
	detailJSON, _ := json.Marshal(detail)
	globalDBWriter.InsertSessionFeed(&SessionFeed{
		SessionID: globalSession.ID,
		Type:      2,
		Detail:    string(detailJSON),
		Time:      time.Now(),
	})

	// 处理车队车手变更事件 (case6)
	if teamID > 0 && connMsg.DriverName != "" {
		teamName := getTeamNameByID(int(teamID))
		if teamName != "" {
			detail := map[string]interface{}{
				"team":       teamName,
				"new_driver": connMsg.DriverName,
			}
			detailJSON, _ := json.Marshal(detail)
			globalDBWriter.InsertSessionFeed(&SessionFeed{
				SessionID: globalSession.ID,
				Type:      6,
				Detail:    string(detailJSON),
				Time:      time.Now(),
			})
		}
	}

	*lastDataTime = time.Now()
}

func handleChatMessage(chatMsg udp.Chat, lastDataTime *time.Time) {
	*lastDataTime = time.Now()

	// 获取发送者信息
	userID := getUserIDByCarID(int(chatMsg.CarID))
	username := getUserNameByID(int(userID))

	// 构造聊天消息详情
	detail := map[string]interface{}{
		"user":    username,
		"message": chatMsg.Message,
	}
	detailJSON, _ := json.Marshal(detail)

	// 插入聊天消息事件到SessionFeed
	globalDBWriter.InsertSessionFeed(&SessionFeed{
		SessionID: globalSession.ID,
		Type:      7,
		Detail:    string(detailJSON),
		Time:      time.Now(),
	})
}

// 补全lap数据实现
func supplementStintLapData(stintID int, sessionData *SessionData) error {
	logger.Printf("补全stint %d的lap数据", stintID)
	// 从sessionData提取lap数据
	for _, lap := range sessionData.Laps {
		// 检查lap是否已存在于数据库并获取当前数据
		var existingLap StintLap
		err := globalDBWriter.DB.QueryRow(`
			SELECT id, sector1, sector2, sector3, tyre, cuts 
			FROM stint_lap 
			WHERE stint_id = ? AND time = ?`, stintID, lap.LapTime).Scan(
			&existingLap.ID, &existingLap.Sector1, &existingLap.Sector2, &existingLap.Sector3, &existingLap.Tyre, &existingLap.Cuts)

		if err != nil && err != sql.ErrNoRows {
			logger.Printf("检查lap存在性失败: %v", err)
			return err
		}

		lapExists := err != sql.ErrNoRows
		needUpdate := false

		if lapExists {
			// 检查是否需要更新字段
			if existingLap.Sector1 == 0 && lap.Sectors[0] > 0 {
				existingLap.Sector1 = lap.Sectors[0]
				needUpdate = true
			}
			if existingLap.Sector2 == 0 && lap.Sectors[1] > 0 {
				existingLap.Sector2 = lap.Sectors[1]
				needUpdate = true
			}
			if existingLap.Sector3 == 0 && lap.Sectors[2] > 0 {
				existingLap.Sector3 = lap.Sectors[2]
				needUpdate = true
			}
			if existingLap.Tyre == "" && lap.Tyre != "" {
				existingLap.Tyre = lap.Tyre
				needUpdate = true
			}

			if needUpdate {
				// 执行更新
				_, err := globalDBWriter.DB.Exec(`
					UPDATE stint_lap SET sector1=?, sector2=?, sector3=?, tyre=?, cuts=? WHERE id=?`,
					existingLap.Sector1, existingLap.Sector2, existingLap.Sector3,
					existingLap.Tyre, existingLap.Cuts, existingLap.ID)
				if err != nil {
					logger.Printf("更新lap数据失败: %v", err)
					return err
				}
				logger.Printf("已更新stint %d的lap %d数据", stintID, lap.LapTime)
			}
		} else {
			// 插入新lap数据
			newLap := &StintLap{
				StintID: stintID,
				Sector1: lap.Sectors[0],
				Sector2: lap.Sectors[1],
				Sector3: lap.Sectors[2],
				Time:    lap.LapTime,
				Tyre:    lap.Tyre,
				Cuts:    0,
			}
			_, err := globalDBWriter.InsertStintLap(newLap)
			if err != nil {
				logger.Printf("插入lap数据失败: %v", err)
				return err
			}
			logger.Printf("已补充stint %d的lap %d数据", stintID, lap.LapTime)
		}
	}
	return nil
}

func handleEndSessionMessage(endMsg udp.EndSession, lastDataTime *time.Time) {
	*lastDataTime = time.Now()

	// 查找当前事件的最新会话
	existingSession, err := globalDBWriter.FindLatestSessionByEventID(globalEvent.ID)
	if err != nil {
		logger.Printf("查找session失败: %v", err)
		return
	}
	if existingSession == nil {
		logger.Printf("未找到当前会话信息")
		return
	}

	// 读取JSON会话数据(移至循环外，仅读取一次)
	sessionData, err := readSessionDataFromJSON(filepath.Join(cfg.Game.Path, string(endMsg)))
	if err != nil {
		logger.Printf("读取会话数据失败: %v", err)
		return
	}

	// 查询当前会话的所有stint
	stints, err := globalDBWriter.FindSessionStintsBySessionID(int64(existingSession.ID))
	if err != nil {
		logger.Printf("查询session_stints失败: %v", err)
		return
	}
	if len(stints) == 0 {
		logger.Printf("未找到任何stint信息")
		return
	}

	// 按ID排序stints确保顺序正确
	sort.Slice(stints, func(i, j int) bool {
		return stints[i].ID < stints[j].ID
	})

	// 按carID分组处理stint
	carStints := make(map[int][]*SessionStint)
	for _, stint := range stints {
		carStints[stint.CarID] = append(carStints[stint.CarID], stint)
	}

	// 处理每个车辆的stint数据
	for carID, stints := range carStints {
		// 过滤当前车辆的lap数据
		var carStintLaps []StintLap
		if sessionData != nil {
			for _, lap := range sessionData.Laps {
				// 转换Lap为StintLap并使用当前stint的ID
				carStintLaps = append(carStintLaps, StintLap{
					StintID:    lap.StintID,
					Sector1:    lap.Sectors[0],
					Sector2:    lap.Sectors[1],
					Sector3:    lap.Sectors[2],
					Grip:       globalEvent.StartGrip,
					Tyre:       lap.Tyre,
					Time:       lap.LapTime,
					Cuts:       lap.Cuts,
					Crashes:    0,
					CarCrashes: 0,
					MaxSpeed:   0,
					AvgSpeed:   globalTrackConfigs[globalEvent.TrackConfigID].Length / lap.LapTime,
					FinishedAt: existingSession.StartTime.Add(time.Duration(lap.Timestamp) * time.Millisecond),
				})
			}
		}

		// 创建当前车辆的sessionData副本
		currentSessionData := *sessionData
		// Convert StintLap to JSONLap
		currentSessionData.Laps = make([]JSONLap, len(carStintLaps))
		for i, sl := range carStintLaps {
			currentSessionData.Laps[i] = JSONLap{
				StintID: sl.StintID,
				Sectors: [3]int{sl.Sector1, sl.Sector2, sl.Sector3},
				LapTime: sl.Time,
				Tyre:    sl.Tyre,
				Cuts:    sl.Cuts,
			}
		}

		// 处理该车辆的所有stint
		for _, stint := range stints {
			logger.Printf("处理车辆 %d 的stint %d", carID, stint.ID)

			// 补全lap数据
			if err := supplementStintLapData(stint.ID, &currentSessionData); err != nil {
				logger.Printf("补全stint %d 的lap数据失败: %v", stint.ID, err)
			}
		}

		// 检查并补全缺失的lap
		// Convert StintLap to Lap
		var laps []Lap
		for _, sl := range carStintLaps {
			laps = append(laps, Lap{
				StintID:    sl.StintID,
				Sectors:    [3]int{sl.Sector1, sl.Sector2, sl.Sector3},
				Time:       sl.Time,
				Tyre:       sl.Tyre,
				Cuts:       sl.Cuts,
				Crashes:    sl.Crashes,
				CarCrashes: sl.CarCrashes,
			})
		}
		if err := completeMissingLaps(carID, stints[len(stints)-1].ID, laps); err != nil {
			logger.Printf("补全车辆 %d 的lap数据失败: %v", carID, err)
		}
	}
	logger.Printf("所有车辆的stint处理完成")
	// 更新会话状态为已结束
	if existingSession.IsFinished == 0 {
		existingSession.IsFinished = 1
		existingSession.LastActivity = time.Now()
		if err := globalDBWriter.UpdateSession(existingSession); err != nil {
			logger.Printf("更新session状态失败: %v", err)
		}
		logger.Printf("session %d 结束", existingSession.ID)
	}
}

// 补全缺失的lap到最后一个stint
func completeMissingLaps(carID int, lastStintID int, jsonLaps []Lap) error {
	// 查询数据库中已有的lap数
	dbLaps, err := globalDBWriter.FindStintLapsByStintID(lastStintID)
	if err != nil {
		return fmt.Errorf("查询lap数失败: %v", err)
	}

	// 比较JSON和数据库中的lap数
	if len(dbLaps) >= len(jsonLaps) {
		return nil // 无需补全
	}

	// 计算需要补全的lap数
	missing := len(jsonLaps) - len(dbLaps)
	logger.Printf("车辆 %d 需要补全 %d 个lap到stint %d", carID, missing, lastStintID)

	// 补全缺失的lap
	added := 0
	for _, lap := range jsonLaps {
		if added >= missing {
			break
		}

		// 检查lap是否已存在
		exists, err := globalDBWriter.FindStintLapByStintIDAndTime(lastStintID, lap.Time)
		if err != nil {
			logger.Printf("检查lap存在性失败: %v", err)
			continue
		}

		if !exists {
			// 插入新lap
			_, err := globalDBWriter.InsertStintLap(&StintLap{
				StintID: lastStintID,
				Sector1: lap.Sectors[0],
				Sector2: lap.Sectors[1],
				Sector3: lap.Sectors[2],
				Time:    lap.Time,
				Tyre:    lap.Tyre,
				Cuts:    lap.Cuts,
				Crashes: 0,
			})
			if err != nil {
				logger.Printf("插入lap失败: %v", err)
			} else {
				added++
				logger.Printf("已补全lap %d", lap.Time)
			}
		}
	}

	return nil
}

func handleNewSessionMessage(sessionMsg udp.SessionInfo, lastDataTime *time.Time) {
	*lastDataTime = time.Now()
	lapDataMutex.Lock()
	carLapDataMap = make(map[int][]CarLapData)
	stintLapData = make(map[int]map[int][]StintLap) // 清空stint_lap数据
	lapDataMutex.Unlock()
	dataCollectionEnabled = true
	collectCarUpdates()
	// 重置文件读取位置，忽略之前的日志内容
	lastFileSize = 0
	lastLineIdx = 0
	partialLine = ""
	// 检查event表中是否存在匹配的server_name记录
	trackConfig := &TrackConfig{
		TrackName:  sessionMsg.Track,
		ConfigName: sessionMsg.TrackConfig,
	}
	// 获取track_config_id
	trackConfigID, err := globalDBWriter.FindOrCreateTrackConfigID(trackConfig)
	if err != nil {
		logger.Printf("获取track_config_id失败: %v", err)
		return
	}
	// 确定server name，优先使用配置中的event name
	Name := sessionMsg.ServerName
	if cfg.ACServer.Event.Name != "" {
		Name = cfg.ACServer.Event.Name
		logger.Printf("使用配置的event name: %s", Name)
	}
	foundEvent, err := globalDBWriter.FindEventByName(Name)
	if err != nil {
		if err != sql.ErrNoRows {
			logger.Printf("查询event失败: %v", err)
			return
		}
		// 不存在匹配的记录，创建新的event
		event := &Event{
			ServerName:       sessionMsg.ServerName,
			TrackConfigID:    trackConfigID,
			Name:             Name,
			TeamEvent:        boolToInt(cfg.ACServer.Event.Team.Enable),
			Active:           1, // 默认为激活状态
			LiveryPreview:    boolToInt(cfg.ACServer.Event.Team.LiveryPreviewEnable),
			UseNumber:        boolToInt(cfg.ACServer.Event.Team.UseNumber),
			PracticeDuration: globalEvent.PracticeDuration,
			QualiDuration:    globalEvent.QualiDuration,
			RaceDuration:     globalEvent.RaceDuration,
			RaceDurationType: globalEvent.RaceDurationType,
			RaceWaitTime:     globalEvent.RaceWaitTime,
			RaceExtraLaps:    globalEvent.RaceExtraLaps,
			ReverseGridPos:   globalEvent.ReverseGridPos,
		}
		if err := globalDBWriter.InsertEvent(event); err != nil {
			logger.Printf("插入event记录失败: %v", err)
			return
		}
		globalEvent = event
		logger.Printf("创建event %d, 名称:%s", globalEvent.ID, globalEvent.Name)
	} else {
		// 更新现有event
		event := &Event{
			ID:               foundEvent.ID,
			ServerName:       sessionMsg.ServerName,
			TrackConfigID:    trackConfigID,
			Name:             Name,
			TeamEvent:        boolToInt(cfg.ACServer.Event.Team.Enable),
			Active:           1,
			LiveryPreview:    boolToInt(cfg.ACServer.Event.Team.LiveryPreviewEnable),
			UseNumber:        boolToInt(cfg.ACServer.Event.Team.UseNumber),
			PracticeDuration: globalEvent.PracticeDuration,
			QualiDuration:    globalEvent.QualiDuration,
			RaceDuration:     globalEvent.RaceDuration,
			RaceDurationType: globalEvent.RaceDurationType,
			RaceWaitTime:     globalEvent.RaceWaitTime,
			RaceExtraLaps:    globalEvent.RaceExtraLaps,
			ReverseGridPos:   globalEvent.ReverseGridPos,
		}
		if err := globalDBWriter.UpdateEvent(event); err != nil {
			logger.Printf("更新event记录失败: %v", err)
			return
		}
		globalEvent = event
		logger.Printf("当前event %d, 名称:%s", globalEvent.ID, globalEvent.Name)
	}
	// 直接更新原有session为已结束
	existingSession, err := globalDBWriter.FindLatestSessionByEventID(globalEvent.ID)
	if err != nil {
		logger.Printf("查找现有session失败: %v", err)
	}
	if existingSession != nil && existingSession.IsFinished == 0 {
		existingSession.IsFinished = 1
		existingSession.FinishTime = time.Now()
		existingSession.LastActivity = time.Now()
		if err := globalDBWriter.UpdateSession(existingSession); err != nil {
			logger.Printf("更新session状态失败: %v", err)
		}
	}

	// 创建新会话
	session := &Session{
		ID:           0,
		EventID:      globalEvent.ID,
		Type:         int(sessionMsg.Type),
		TrackTime:    "0",
		Name:         sessionMsg.Name,
		StartTime:    time.Now().Add(-time.Duration(sessionMsg.ElapsedMilliseconds) * time.Millisecond),
		ElapsedMs:    int(sessionMsg.ElapsedMilliseconds),
		DurationMin:  int(sessionMsg.Time),
		Laps:         0,
		Weather:      sessionMsg.WeatherGraphics,
		AirTemp:      float64(sessionMsg.AmbientTemp),
		RoadTemp:     float64(sessionMsg.RoadTemp),
		StartGrip:    globalEvent.StartGrip,
		CurrentGrip:  globalEvent.StartGrip,
		IsFinished:   0,
		LastActivity: time.Now(),
		HTTPPort:     cfg.ACServer.UDP.LocalPort,
	}
	sessionID, err := globalDBWriter.InsertSession(session)
	if err != nil {
		logger.Printf("插入session记录失败: %v", err)
		return
	}
	// 处理模拟环境下返回ID为0的情况
	if sessionID == 0 && globalDBWriter.SimulateSQL {
		staticSessionIDCounter++
		sessionID = staticSessionIDCounter
	}
	session.ID = sessionID
	logger.Printf("创建session %d, 名称:%s", session.ID, session.Name)
	sessionMutex.Lock()
	globalSession = session
	sessionMutex.Unlock()
	// 将所有在线车辆状态设为维修区
	connectionsMutex.Lock()
	for _, conn := range globalConnections {
		if conn.ConnectionStatus == 0 {
			conn.TrackStatus = 3
		}
	}
	connectionsMutex.Unlock()
}

func handleConnectionClosedMessage(connMsg udp.SessionCarInfo, lastDataTime *time.Time) {
	*lastDataTime = time.Now()
	// 插入用户断开连接事件SessionFeed (case3)
	carID := int(connMsg.CarID)
	userID := getUserIDByCarID(carID)
	teamID := getTeamIDByCarID(carID)
	detail := map[string]interface{}{
		"user_id": userID,
		"team_id": teamID,
		"car_id":  carID,
	}
	detailJSON, _ := json.Marshal(detail)
	globalDBWriter.InsertSessionFeed(&SessionFeed{
		SessionID: globalSession.ID,
		Type:      3,
		Detail:    string(detailJSON),
		Time:      time.Now(),
	})

	// 更新连接状态为离线
	connectionsMutex.Lock()
	defer connectionsMutex.Unlock()
	if conn, exists := globalConnections[int(connMsg.CarID)]; exists {
		conn.ConnectionStatus = 0 // 设为已断开连接状态
	}
}

// 碰撞事件处理函数 (case 0)
func handleCollisionWithCarMessage(collisionMsg udp.CollisionWithCar, lastDataTime *time.Time) {
	*lastDataTime = time.Now()
	carID1 := int(collisionMsg.CarID)
	carID2 := int(collisionMsg.OtherCarID)
	// 创建两车碰撞事件的session feed
	sf := &SessionFeed{
		SessionID: globalSession.ID,
		Type:      0,
		Detail: fmt.Sprintf(`{"user_id_1": %d, "team_id_1": %d, "car_id_1": %d, "user_id_2": %d, "team_id_2": %d, "car_id_2": %d, "nsp": %.2f}`,
			getUserIDByCarID(carID1), getTeamIDByCarID(carID1), carID1,
			getUserIDByCarID(carID2), getTeamIDByCarID(carID2), carID2,
			latestCarUpdates[udp.CarID(carID1)].NormalisedSplinePos),
		Time: time.Now(),
	}
	if err := globalDBWriter.InsertSessionFeed(sf); err != nil {
		logger.Printf("插入碰撞事件session feed失败: %v", err)
	}
}

func handleCollisionWithEnvMessage(collisionMsg udp.CollisionWithEnvironment, lastDataTime *time.Time) {
	*lastDataTime = time.Now()
	carID := int(collisionMsg.CarID)
	// 创建车辆与环境碰撞事件的session feed
	sf := &SessionFeed{
		SessionID: globalSession.ID,
		Type:      1,
		Detail: fmt.Sprintf(`{"user_id": %d, "team_id": %d, "speed": %.2f, "nsp": %.2f}`,
			getUserIDByCarID(carID), getTeamIDByCarID(carID), collisionMsg.ImpactSpeed,
			latestCarUpdates[udp.CarID(carID)].NormalisedSplinePos),
		Time: time.Now(),
	}
	if err := globalDBWriter.InsertSessionFeed(sf); err != nil {
		logger.Printf("插入环境碰撞事件session feed失败: %v", err)
	}
}

// 车辆更新事件处理函数
func handleCarUpdateMessage(updateMsg udp.CarUpdate, lastDataTime *time.Time) {
	*lastDataTime = time.Now()
	carID := int(updateMsg.CarID)
	currentSplinePos := updateMsg.NormalisedSplinePos
	// 检查是否完成一圈 (从接近1变为接近0)
	lastUpdates, exists := carLapData[carID]

	var lastPos float32
	lapCompleted := false
	if exists && len(lastUpdates) > 0 {
		lastPos = lastUpdates[len(lastUpdates)-1].NormalisedSplinePos
		lapCompleted = lastPos > 0.9 && currentSplinePos < 0.1
	}

	if lapCompleted {
		// 生成遥测数据
		if len(lastUpdates) > 0 {
			telemetry, err := generateLapTelemetry(carID, int32(globalTrackConfigs[globalEvent.TrackConfigID].Length), lastUpdates)
			if err != nil {
				logger.Printf("生成遥测数据失败: %v", err)
			} else {
				telemetryData[carID] = telemetry
				logger.Printf("遥测数据已暂存: 车辆 %d，数据大小 %d 字节", carID, len(telemetry))
			}
		}
	}
	// 添加新数据点到圈跟踪
	carLapData[carID] = append(carLapData[carID], updateMsg)

	// 判断是否在维修区
	if len(pitBoundary) > 0 {
		vehiclePos := [2]float32{updateMsg.Pos.X, updateMsg.Pos.Z}
		inPit := pointInPolygon(vehiclePos, pitBoundary)

		connectionsMutex.Lock()
		defer connectionsMutex.Unlock()

		if connInfo, exists := globalConnections[carID]; exists {
			oldStatus := connInfo.TrackStatus
			if inPit {
				// 进入维修区状态逻辑
				if (oldStatus == 0 || oldStatus == 1) && connInfo.TrackStatus != 2 && connInfo.TrackStatus != 3 {
					// 从赛道状态(1)进入维修区入口(2)
					connInfo.TrackStatus = 2
					connInfo.TrackStatusLastChangedTime = time.Now()

					// 计算当前速度 (m/s)
					velocityMagnitude := math.Sqrt(float64(updateMsg.Velocity.X*updateMsg.Velocity.X +
						updateMsg.Velocity.Y*updateMsg.Velocity.Y +
						updateMsg.Velocity.Z*updateMsg.Velocity.Z))

					// 判断是否为teleport回维修区 (>400km/h = 111.11m/s)
					isTeleport := velocityMagnitude > 111.11
					sfType := 11
					if isTeleport {
						sfType = 15
					}

					// 记录新的维修区访问
					currentVisit := PitVisit{EntryTime: time.Now()}
					connInfo.PitVisits = append(connInfo.PitVisits, currentVisit)
					// 插入维修区进入事件到session feed
					sf := &SessionFeed{
						SessionID: globalSession.ID,
						Type:      sfType,
						Detail:    fmt.Sprintf(`{"user_id": %d, "team_id": %d}`, getUserIDByCarID(connInfo.CarID), getTeamIDByCarID(connInfo.CarID)),
						Time:      time.Now(),
					}
					if err := globalDBWriter.InsertSessionFeed(sf); err != nil {
						logger.Printf("Failed to insert pit entry session feed: %v", err)
					}

					connInfo.PitEntryTime = time.Now()
				} else if connInfo.TrackStatus == 2 && time.Since(connInfo.TrackStatusLastChangedTime) >= 10*time.Second {
					// 入口状态保持10秒后切换到维修区状态(3)
					connInfo.TrackStatus = 3
					connInfo.TrackStatusLastChangedTime = time.Now()
				}
			} else {
				// 离开维修区状态逻辑
				if oldStatus == 3 && connInfo.TrackStatus != 4 && connInfo.TrackStatus != 0 && connInfo.TrackStatus != 1 {
					// 从维修区状态(3)进入出口状态(4)
					connInfo.TrackStatus = 4
					connInfo.TrackStatusLastChangedTime = time.Now()

					// 记录维修区离开时间并更新session feed
					for i := len(connInfo.PitVisits) - 1; i >= 0; i-- {
						if connInfo.PitVisits[i].ExitTime.IsZero() {
							connInfo.PitVisits[i].ExitTime = time.Now()
							// 计算维修区停留时间
							entryTime := connInfo.PitVisits[i].EntryTime
							pitDuration := time.Since(entryTime)
							// 插入维修区离开事件到session feed (Type=12)
							sf := &SessionFeed{
								SessionID: globalSession.ID,
								Type:      12,
								Detail:    fmt.Sprintf(`{"user_id": %d, "team_id": %d, "pit_time": %d}`, getUserIDByCarID(connInfo.CarID), getTeamIDByCarID(connInfo.CarID), pitDuration.Milliseconds()),
								Time:      time.Now(),
							}
							if err := globalDBWriter.InsertSessionFeed(sf); err != nil {
								logger.Printf("Failed to insert pit exit session feed: %v", err)
							}
							break
						}
					}

					connInfo.PitEntryTime = time.Time{}
				} else if connInfo.TrackStatus == 4 && time.Since(connInfo.TrackStatusLastChangedTime) >= 10*time.Second {
					// 出口状态保持10秒后切换到正常赛道状态(0)
					connInfo.TrackStatus = 0
					connInfo.TrackStatusLastChangedTime = time.Now()
					// 重置维修区相关时间
					connInfo.PitEntryTime = time.Time{}
					connInfo.LastMoveTime = time.Time{}
				}
			}
		}
	}

	// 实时发送遥测数据
	wsMutex.Lock()
	// 更新全局遥测数据变量，覆盖对应CarID的最新数据
	latestCarUpdates[updateMsg.CarID] = updateMsg
	wsMutex.Unlock()
}

var currentLogFile string = ""
var globalNextSessionLineIdx int = -1
var userTyres = make(map[string]string)
var userStintIDs = make(map[string]int)
var tyreRegex = regexp.MustCompile(`(\w+) \[[^\]]*\] changed tyres to (\w+)`)
var connectionRegex = regexp.MustCompile(`(?s)NEW PICKUP CONNECTION from\s+(.*?)\s*\nVERSION .*?\n(.*?)\nREQUESTED CAR: (.*?)\n.*?GUID (\d+) .*?DRIVER ACCEPTED FOR CAR (\d+)`)
var disconnectRegex = regexp.MustCompile(`.*driver disconnected: (\w+) \[[^\]]*\]`)

func handleSessionInfoMessage(sessionMsg udp.SessionInfo, lastDataTime *time.Time) {
	var err error
	latestFile, err = findLatestSessionLogFile(logDir)
	if err != nil {
		logger.Printf("查找最新会话日志文件失败: %v", err)
		return
	}
	// 搜索日志中所有已在服务器上的车辆相关信息
	if latestFile != nil {
		filePath := filepath.Join(logDir, latestFile.Name())
		content, err := os.ReadFile(filePath)
		if err != nil {
			logger.Printf("无法读取日志文件: %v", err)
			return
		}
		// 解析玩家车辆信息
		playerLineRegex := regexp.MustCompile(`^(\d+)\) (.*?) BEST: (.*?) TOTAL: (.*?) Laps:(\d+) SesID:(\d+) HasFinished:(\w+)$`)
		lines := strings.Split(string(content), "\n")

		for lineIdx, line := range lines {
			match := playerLineRegex.FindStringSubmatch(line)
			if len(match) >= 8 {
				userName := strings.TrimSpace(match[2])
				sesID := match[6]
				if userName == "" {
					continue
				}

				// 查找匹配的连接数据
				matches := connectionRegex.FindAllStringSubmatch(string(content), -1)
				for _, m := range matches {
					// m[1]=IP, m[2]=用户名, m[3]=车型, m[4]=GUID, m[5]=CarID
					if len(m) >= 6 && strings.TrimSpace(m[2]) == userName {
						// 转换sesID为整数
						carID, err := strconv.Atoi(m[5])
						if err != nil {
							logger.Printf("无效的CarID: %s, 错误: %v", sesID, err)
							continue
						}
						// 更新全局连接和用户信息
						lineNumber := lineIdx + 1

						connInfo := ConnectionInfo{
							CarID:              carID,
							DriverName:         userName,
							DriverGUID:         m[4],
							CarModel:           m[3],
							CarSkin:            globalEntryList[carID].Skin,
							DriverInitials:     "",
							CarName:            globalEntryList[carID].Name,
							ConnectionStatus:   0, // 设为在线状态
							TrackStatus:        3, // 设为在线状态
							LastConnectionLine: lineNumber,
						}
						user := &User{
							Name: connInfo.DriverName,
							Steam64ID: func() int64 {
								id, err := strconv.ParseInt(m[4], 10, 64)
								if err != nil {
									logger.Printf("解析Steam64ID失败: %v", err)
									return 0
								}
								return id
							}(),
						}
						userID, _, err := globalDBWriter.FindOrCreateUser(user)
						if err != nil {
							logger.Printf("获取用户信息失败: %v", err)
							globalMutex.Unlock()
							return
						}
						globalMutex.Lock()
						globalConnections[carID] = &connInfo

						globalUsers[userID] = user
						globalMutex.Unlock()
						//logger.Printf("已添加全局连接信息: %+v", connInfo)
						break
					}
				}
			}
		}
	} else {
		logger.Println("未找到日志文件")
	}

	*lastDataTime = time.Now()
	// 检查event表中是否存在匹配的server_name记录
	trackConfig := &TrackConfig{
		TrackName:  sessionMsg.Track,
		ConfigName: sessionMsg.TrackConfig,
	}
	// 获取track_config_id
	trackConfigID, err := globalDBWriter.FindOrCreateTrackConfigID(trackConfig)
	if err != nil {
		logger.Printf("获取track_config_id失败: %v", err)
		return
	}
	// 确定server name，优先使用配置中的event name
	Name := sessionMsg.ServerName
	if cfg.ACServer.Event.Name != "" {
		Name = cfg.ACServer.Event.Name
		logger.Printf("使用配置的event name: %s", Name)
	}
	foundEvent, err := globalDBWriter.FindEventByName(Name)
	if err != nil {
		if err != sql.ErrNoRows {
			logger.Printf("查询event失败: %v", err)
			return
		}
		// 不存在匹配的记录，创建新的event
		event := &Event{
			ServerName:       sessionMsg.ServerName,
			TrackConfigID:    trackConfigID,
			Name:             Name,
			TeamEvent:        boolToInt(cfg.ACServer.Event.Team.Enable),
			Active:           1, // 默认为激活状态
			LiveryPreview:    boolToInt(cfg.ACServer.Event.Team.LiveryPreviewEnable),
			UseNumber:        boolToInt(cfg.ACServer.Event.Team.UseNumber),
			PracticeDuration: globalEvent.PracticeDuration,
			QualiDuration:    globalEvent.QualiDuration,
			RaceDuration:     globalEvent.RaceDuration,
			RaceDurationType: globalEvent.RaceDurationType,
			RaceWaitTime:     globalEvent.RaceWaitTime,
			RaceExtraLaps:    globalEvent.RaceExtraLaps,
			ReverseGridPos:   globalEvent.ReverseGridPos,
		}
		if err := globalDBWriter.InsertEvent(event); err != nil {
			logger.Printf("插入event记录失败: %v", err)
			return
		}
		globalEvent = event
		logger.Printf("创建event %d, 名称:%s", globalEvent.ID, globalEvent.Name)
	} else {
		// 更新现有event
		event := &Event{
			ID:               foundEvent.ID,
			ServerName:       sessionMsg.ServerName,
			TrackConfigID:    trackConfigID,
			Name:             Name,
			TeamEvent:        boolToInt(cfg.ACServer.Event.Team.Enable),
			Active:           1,
			LiveryPreview:    boolToInt(cfg.ACServer.Event.Team.LiveryPreviewEnable),
			UseNumber:        boolToInt(cfg.ACServer.Event.Team.UseNumber),
			PracticeDuration: globalEvent.PracticeDuration,
			QualiDuration:    globalEvent.QualiDuration,
			RaceDuration:     globalEvent.RaceDuration,
			RaceDurationType: globalEvent.RaceDurationType,
			RaceWaitTime:     globalEvent.RaceWaitTime,
			RaceExtraLaps:    globalEvent.RaceExtraLaps,
			ReverseGridPos:   globalEvent.ReverseGridPos,
		}
		if err := globalDBWriter.UpdateEvent(event); err != nil {
			logger.Printf("更新event记录失败: %v", err)
			return
		}
		globalEvent = event
		logger.Printf("更新event %d, 名称:%s", globalEvent.ID, globalEvent.Name)
	}
}

// 错误事件处理函数
func handleErrorMessage(errorMsg udp.ServerError, lastDataTime *time.Time) {
	*lastDataTime = time.Now()
}

// 版本消息处理函数
func handleVersionMessage(versionMsg udp.Version, lastDataTime *time.Time) {
	*lastDataTime = time.Now()
	// 处理版本信息
	logger.Printf("AC服务器版本信息: %+v", versionMsg)
}

// 从JSON文件读取完整的session数据
func readSessionDataFromJSON(filePath string) (*SessionData, error) {
	// 读取JSON文件
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("读取JSON文件失败: %v", err)
	}

	// 解析完整JSON数据
	var sessionData SessionData
	if err := json.Unmarshal(data, &sessionData); err != nil {
		return nil, fmt.Errorf("解析JSON数据失败: %v", err)
	}

	return &sessionData, nil
}

// 创建UDP客户端
func createUDPClient(host string, localPort, serverPort int, lastDataTime *time.Time, sessionInfoChan chan<- struct{}, versionInfoChan chan<- struct{}) (*udp.AssettoServerUDP, error) {
	// 使用正确的函数名和参数创建UDP客户端
	client, err := udp.NewServerClient("", localPort, serverPort, false, "", 0, func(msg udp.Message) {
		// 处理接收到的消息
		switch msg.Event() {
		case udp.EventVersion:
			if versionMsg, ok := msg.(udp.Version); ok {
				logger.Printf("AC服务器版本信息: %v", versionMsg)
				handleVersionMessage(versionMsg, lastDataTime)
				if !receivedVersionInfo {
					select {
					case versionInfoChan <- struct{}{}:
					default:
					}
				}
			}
		case udp.EventNewSession:
			if sessionMsg, ok := msg.(udp.SessionInfo); ok {
				logger.Printf("新比赛节开始: %+v", sessionMsg)
				handleNewSessionMessage(sessionMsg, lastDataTime)
				if !receivedSessionInfo {
					select {
					case sessionInfoChan <- struct{}{}:
					default:
					}
				}
			}
		case udp.EventEndSession:
			if endMsg, ok := msg.(udp.EndSession); ok {
				logger.Printf("比赛节结束: %v", endMsg)
				handleEndSessionMessage(endMsg, lastDataTime)
			}
		case udp.EventChat:
			if chatMsg, ok := msg.(udp.Chat); ok {
				logger.Printf("聊天消息 [车辆%d]: %s", chatMsg.CarID, chatMsg.Message)
				handleChatMessage(chatMsg, lastDataTime)
			}
		case udp.EventNewConnection:
			if connMsg, ok := msg.(udp.SessionCarInfo); ok {
				logger.Printf("新客户端连接: %+v", connMsg)
				handleNewConnectionMessage(connMsg, lastDataTime)
			}
		case udp.EventConnectionClosed:
			if connMsg, ok := msg.(udp.SessionCarInfo); ok {
				logger.Printf("客户端连接断开: 车辆%d, 驾驶员%s", connMsg.CarID, connMsg.DriverName)
				handleConnectionClosedMessage(connMsg, lastDataTime)
			}
		case udp.EventClientLoaded:
			if clientMsg, ok := msg.(udp.ClientLoaded); ok {
				logger.Printf("已进入服务器: CarID %+v", clientMsg)
				handleClientLoadedMessage(clientMsg, lastDataTime)
			}
		case udp.EventLapCompleted:
			if lapMsg, ok := msg.(udp.LapCompleted); ok {
				logger.Printf("完成圈数事件: %+v", lapMsg)
				handleLapCompletedMessage(lapMsg, lastDataTime)
			}
		case udp.EventCarInfo:
			if carInfoMsg, ok := msg.(udp.CarInfo); ok {
				logger.Printf("车辆信息事件: %+v", carInfoMsg)
				handleCarInfoMessage(carInfoMsg, lastDataTime)
			}
		case udp.EventClientEvent:
			if carCollision, ok := msg.(udp.CollisionWithCar); ok {
				logger.Printf("车辆碰撞事件: %+v", carCollision)
				handleCollisionWithCarMessage(carCollision, lastDataTime)
			} else if envCollision, ok := msg.(udp.CollisionWithEnvironment); ok {
				logger.Printf("环境碰撞事件: %+v", envCollision)
				handleCollisionWithEnvMessage(envCollision, lastDataTime)
			}
		case udp.EventCarUpdate:
			if updateMsg, ok := msg.(udp.CarUpdate); ok {
				//logger.Printf("车辆更新事件: %+v", updateMsg)
				handleCarUpdateMessage(updateMsg, lastDataTime)
			}
		case udp.EventSessionInfo:
			if sessionMsg, ok := msg.(udp.SessionInfo); ok {
				//logger.Printf("比赛节信息事件: %+v", sessionMsg)
				handleSessionInfoMessage(sessionMsg, lastDataTime)
				if !receivedSessionInfo {
					select {
					case sessionInfoChan <- struct{}{}:
					default:
					}
				}
			}
		case udp.EventError:
			if errorMsg, ok := msg.(udp.ServerError); ok {
				logger.Printf("服务器错误事件: %+v", errorMsg)
				handleErrorMessage(errorMsg, lastDataTime)
			}
		default:
			logger.Printf("收到未处理的消息类型: %v", msg)
		}
	})
	if err != nil {
		return nil, err
	}

	return client, nil
}

func collectCarUpdates() {
	if !dataCollectionEnabled {
		return
	}
	lapDataMutex.Lock()
	defer lapDataMutex.Unlock()

	if latestFile != nil {
		//logger.Printf("最新日志文件: %s", latestFile.Name())
		// 读取日志文件内容
		filePath := filepath.Join(logDir, latestFile.Name())
		file, err := os.Open(filePath)
		if err != nil {
			logger.Printf("无法打开日志文件: %v", err)
			return
		}
		defer file.Close()

		fileInfo, err := file.Stat()
		if err != nil {
			logger.Printf("无法获取文件信息: %v", err)
			return
		}

		// 检查文件是否已轮换（大小变小表示可能轮换或截断）
		currentFileSize := fileInfo.Size()
		currentFileModTime := fileInfo.ModTime()
		if currentFileSize < lastFileSize {
			// 文件已轮换重置读取位置
			lastFileSize = 0
			lastLineIdx = 0
			partialLine = ""
		}

		// 定位到上次读取位置
		if _, err := file.Seek(lastFileSize, io.SeekStart); err != nil {
			logger.Printf("无法定位文件: %v", err)
			return
		}

		// 读取新增内容
		newContent := make([]byte, currentFileSize-lastFileSize)
		if _, err := io.ReadFull(file, newContent); err != nil && err != io.EOF {
			logger.Printf("无法读取文件内容: %v", err)
			return
		}

		lastFileSize = currentFileSize
		lastFileModTime = currentFileModTime

		// 处理新增内容
		fullContent := partialLine + string(newContent)
		lines := strings.Split(fullContent, "\n")
		partialLine = lines[len(lines)-1]
		lines = lines[:len(lines)-1] // 移除最后一个元素（可能不完整的行）

		// 从上次处理的行开始
		if len(lines) > lastLineIdx {
			lines = lines[lastLineIdx:]
			lastLineIdx += len(lines)
		} else {
			lines = []string{}
		}

		// 查找最后一个"Session"行
		for i, line := range lines {
			if strings.Contains(line, "SESSION:") {
				globalNextSessionLineIdx = lastLineIdx + i
			}
		}

		// 初始化新连接用户的stintID为1
		for _, conn := range globalConnections {
			if _, exists := userStintIDs[conn.DriverName]; !exists {
				userStintIDs[conn.DriverName] = 1
			}
		}
		// 按用户存储最后轮胎类型（使用全局变量）
		processLines := lines
		if globalNextSessionLineIdx != -1 && globalNextSessionLineIdx >= lastLineIdx {
			currentIdx := globalNextSessionLineIdx - lastLineIdx
			if currentIdx < len(lines) {
				processLines = lines[:currentIdx+1]
			}
		}
		for i, line := range processLines {
			currentLineIdx := lastLineIdx + i
			if tyreMatch := tyreRegex.FindStringSubmatch(line); len(tyreMatch) == 3 {
				username := tyreMatch[1]
				tyreType := tyreMatch[2]
				userTyres[username] = tyreType
				userStintIDs[username]++

				// 获取车辆ID
				carID := -1
				connectionsMutex.Lock()
				for _, conn := range globalConnections {
					if conn.DriverName == username {
						carID = conn.CarID
						break
					}
				}
				connectionsMutex.Unlock()

				if carID != -1 {
					// 获取用户ID和团队ID
					userID := getUserIDByCarID(carID)
					teamID := getTeamIDByCarID(carID)
					teamName := getTeamNameByID(int(teamID))
					userName := getUserNameByID(int(userID))

					// 构造轮胎更换事件详情
					detail := fmt.Sprintf(`{
						"car_id": %d,
						"driver": "%s",
						"team": "%s",
						"tire_compound": "%s",
						"change_time": "%s"
					}`,
						carID,
						userName,
						teamName,
						tyreType,
						time.Now().Format(time.RFC3339),
					)

					// 插入轮胎更换事件
					globalDBWriter.InsertSessionFeed(&SessionFeed{
						SessionID: globalSession.ID,
						Type:      16,
						Detail:    detail,
						Time:      time.Now(),
					})
				}
			} else if connectMatch := connectionRegex.FindStringSubmatch(line); len(connectMatch) == 2 {
				// m[1]=IP, m[2]=用户名, m[3]=车型, m[4]=GUID, m[5]=CarID
				username := connectMatch[2]
				connectionsMutex.Lock()
				for _, conn := range globalConnections {
					if conn.DriverName == username {
						conn.LastConnectionLine = currentLineIdx
					}
				}
				connectionsMutex.Unlock()
			} else if disconnectMatch := disconnectRegex.FindStringSubmatch(line); len(disconnectMatch) == 2 {
				username := disconnectMatch[1]
				connectionsMutex.Lock()
				for _, conn := range globalConnections {
					if conn.DriverName == username && currentLineIdx > conn.LastConnectionLine {
						conn.ConnectionStatus = 2 // 离线状态
						break
					}
				}
				connectionsMutex.Unlock()
			}
		}

		// 计算当前增量中的NextSession位置
		currentNextSessionIdx := -1
		if globalNextSessionLineIdx != -1 && globalNextSessionLineIdx >= lastLineIdx {
			currentNextSessionIdx = globalNextSessionLineIdx - lastLineIdx
			if currentNextSessionIdx >= len(lines) {
				currentNextSessionIdx = -1
			}
		}

		if currentNextSessionIdx != -1 {
			// 初始化当前session的stintLapData结构
			currentSessionID := globalSession.ID
			if stintLapData[int(currentSessionID)] == nil {
				stintLapData[int(currentSessionID)] = make(map[int][]StintLap)
			}
			// 编译正则表达式
			splitRegex := regexp.MustCompile(`Car\.onSplitCompleted (\d+) (\d+) (\d+)`)
			envCollisionRegex := regexp.MustCompile(`COLLISION BETWEEN: (\w+) \[\] AND ENVIRONMENT`)
			carCollisionRegex := regexp.MustCompile(`COLLISION BETWEEN: (\w+) \[\] AND (\w+) \[\]`)
			cutsRegex := regexp.MustCompile(`LAP WITH CUTS: (\w+), (\d+) cuts`)
			lapRegex := regexp.MustCompile(`LAP (\w+) (\d+):(\d+):(\d+)`)

			// 处理NextSession之后的所有行
			for i := currentNextSessionIdx + 1; i < len(lines); i++ {
				line := lines[i]

				// 检查实时轮胎更换
				if tyreMatch := tyreRegex.FindStringSubmatch(line); len(tyreMatch) == 3 {
					username := tyreMatch[1]
					tyreType := tyreMatch[2]
					userTyres[username] = tyreType
				}

				// 处理Split完成事件
				if splitMatch := splitRegex.FindStringSubmatch(line); len(splitMatch) == 4 {
					carID, _ := strconv.Atoi(splitMatch[1])
					split, _ := strconv.Atoi(splitMatch[2])
					time, _ := strconv.ParseInt(splitMatch[3], 10, 64)

					// 根据split值区分处理逻辑
					switch split {
					case 0:
						// 通过CarID获取用户名
						var username string
						if connInfo, exists := globalConnections[carID]; exists {
							username = connInfo.DriverName
						}
						newLap := StintLap{
							StintID:    userStintIDs[username],
							Tyre:       userTyres[username],
							Sector1:    int(time),
							Sector2:    0,
							Sector3:    0,
							Time:       0,
							Cuts:       0,
							Crashes:    0,
							CarCrashes: 0,
						}
						stintLapData[int(currentSessionID)][carID] = append(stintLapData[int(currentSessionID)][carID], newLap)
						// 仅初始化Sector1，Sector3和Time将在Lap完成时计算
					case 1:
						// split为1时更新最新StintLap的Sector2
						if laps, ok := stintLapData[int(currentSessionID)][carID]; ok && len(laps) > 0 {
							lastIdx := len(laps) - 1
							stintLapData[int(currentSessionID)][carID][lastIdx].Sector2 = int(time)
						}
					}
				}
				// 处理Lap完成事件
				if lapMatch := lapRegex.FindStringSubmatch(line); len(lapMatch) == 4 {
					// 解析用户名和分:秒:毫秒格式 (例如 LAP PlayerName 7:25:669)
					username := lapMatch[1]
					minutes, _ := strconv.Atoi(lapMatch[2])
					seconds, _ := strconv.Atoi(lapMatch[3])
					millis, _ := strconv.Atoi(lapMatch[4])

					// 通过用户名查找CarID
					carID := -1
					for _, entry := range globalConnections {
						if entry.DriverName == username {
							carID = entry.CarID
							break
						}
					}

					if carID == -1 {
						logger.Printf("警告: 未找到用户 '%s' 的CarID，无法处理轮胎数据", username)
						continue
					}

					if carID != -1 {
						// 计算Sector3和总Time
						totalTime := int64(minutes*60*1000 + seconds*1000 + millis)
						if laps, ok := stintLapData[int(currentSessionID)][carID]; ok && len(laps) > 0 {
							lastIdx := len(laps) - 1
							laps[lastIdx].Time = int(totalTime)
							laps[lastIdx].Sector3 = int(totalTime) - (laps[lastIdx].Sector1 + laps[lastIdx].Sector2)

							// 创建新的一圈
							newLap := StintLap{
								StintID:    userStintIDs[username],
								Sector1:    0,
								Sector2:    0,
								Sector3:    0,
								Tyre:       userTyres[username],
								Time:       0,
								Cuts:       0,
								Crashes:    0,
								CarCrashes: 0,
							}
							stintLapData[int(currentSessionID)][carID] = append(stintLapData[int(currentSessionID)][carID], newLap)
						}
					}
				}

				// 处理环境碰撞事件
				if envCollisionMatch := envCollisionRegex.FindStringSubmatch(line); len(envCollisionMatch) == 2 {
					username := envCollisionMatch[1]
					carID := -1
					for _, entry := range globalConnections {
						if entry.DriverName == username {
							carID = entry.CarID
							break
						}
					}
					if carID != -1 {
						if _, ok := stintLapData[int(currentSessionID)]; !ok {
							stintLapData[int(currentSessionID)] = make(map[int][]StintLap)
						}
						if laps, ok := stintLapData[int(currentSessionID)][carID]; !ok || len(laps) == 0 {
							stintLapData[int(currentSessionID)][carID] = []StintLap{{
								StintID:    userStintIDs[username],
								Sector1:    0,
								Sector2:    0,
								Sector3:    0,
								Tyre:       userTyres[username],
								Time:       0,
								Cuts:       0,
								Crashes:    1,
								CarCrashes: 0,
							}}
						} else {
							lastIdx := len(laps) - 1
							laps[lastIdx].Crashes++
							stintLapData[int(currentSessionID)][carID] = laps
						}
					}
				}
				// 处理车辆碰撞事件
				if carCollisionMatch := carCollisionRegex.FindStringSubmatch(line); len(carCollisionMatch) == 3 {
					username := carCollisionMatch[1]
					carID := -1
					for _, entry := range globalConnections {
						if entry.DriverName == username {
							carID = entry.CarID
							break
						}
					}
					if carID != -1 {
						if _, ok := stintLapData[int(currentSessionID)]; !ok {
							stintLapData[int(currentSessionID)] = make(map[int][]StintLap)
						}
						if laps, ok := stintLapData[int(currentSessionID)][carID]; !ok || len(laps) == 0 {
							stintLapData[int(currentSessionID)][carID] = []StintLap{{
								StintID:    0,
								Sector1:    0,
								Sector2:    0,
								Sector3:    0,
								Tyre:       userTyres[username],
								Time:       0,
								Cuts:       0,
								Crashes:    0,
								CarCrashes: 1,
							}}
						} else {
							lastIdx := len(laps) - 1
							laps[lastIdx].CarCrashes++
							stintLapData[int(currentSessionID)][carID] = laps
						}
					}
				}
				// 处理切弯事件
				if cutsMatch := cutsRegex.FindStringSubmatch(line); len(cutsMatch) == 3 {
					username := cutsMatch[1]
					cuts, _ := strconv.Atoi(cutsMatch[2])
					carID := -1
					for _, entry := range globalConnections {
						if entry.DriverName == username {
							carID = entry.CarID
							break
						}
					}
					if carID != -1 {
						if _, ok := stintLapData[int(currentSessionID)]; !ok {
							stintLapData[int(currentSessionID)] = make(map[int][]StintLap)
						}
						if laps, ok := stintLapData[int(currentSessionID)][carID]; !ok || len(laps) == 0 {
							stintLapData[int(currentSessionID)][carID] = []StintLap{{
								StintID:    0,
								Sector1:    0,
								Sector2:    0,
								Sector3:    0,
								Tyre:       userTyres[username],
								Time:       0,
								Cuts:       cuts,
								Crashes:    0,
								CarCrashes: 0,
							}}
						} else {
							lastIdx := len(laps) - 1
							laps[lastIdx].Cuts = cuts
							stintLapData[int(currentSessionID)][carID] = laps
						}
					}
				}
			}
		}
	}
}

func main() {
	initLogger()
	logger.Println("Writer应用程序启动")

	data, err := os.ReadFile("config/config.toml")
	if err != nil {
		fatalLogger.Fatalf("读取配置文件失败: %v", err)
	}

	// 调用config包中的ExtractConfigFromTOML函数
	config, err := config.ExtractConfigFromTOML(data)
	if err != nil {
		fatalLogger.Fatalf("解析配置失败: %v", err)
	}
	cfg = &config
	//logger.Printf("配置加载成功: %+v", cfg)
	logDir = filepath.Join(cfg.Game.Path, "logs", "session")
	port := config.Database.Port
	if port == 0 {
		port = 3306
	}
	// 初始化数据库连接
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		cfg.Database.User,
		cfg.Database.Password,
		cfg.Database.Host,
		port,
		cfg.Database.Schema)

	dbWriter, err := NewDBWriter(dsn)
	if err != nil {
		fatalLogger.Fatalf("Failed to create DBWriter: %v", err)
	}
	globalDBWriter = dbWriter // 设置全局DBWriter实例
	defer dbWriter.Close()

	// 加载并验证server_cfg.ini配置
	if err := dbWriter.LoadServerConfig(cfg.Game.Path); err != nil {
		fatalLogger.Fatalf("server_cfg.ini验证失败: %v", err)
	}
	// 加载并验证entry_list配置
	if err := dbWriter.LoadEntryList(cfg.Game.Path); err != nil {
		fatalLogger.Fatalf("entry_list验证失败: %v", err)
	}
	latestFile, err = findLatestSessionLogFile(logDir)
	if err != nil {
		logger.Printf("查找最新服务器日志文件失败: %v", err)
	}
	logger.Println("Writer应用程序启动成功")

	// 启动writer主循环
	var lastDataTime time.Time = time.Now() // 记录最后一次数据接收时间

	// 创建UDP客户端连接
	acHost := cfg.ACServer.Host
	acPort := cfg.ACServer.UDP.Port
	acLocalPort := cfg.ACServer.UDP.LocalPort

	var acClient *udp.AssettoServerUDP
	var reconnectInterval = 3 * time.Second
	receivedSessionInfo = false
	receivedVersionInfo = false

	// 连接循环 - 支持重连
outerLoop:
	for {
		var err error
		// 创建用于SessionInfo通知的通道
		sessionInfoChan := make(chan struct{}, 1)
		versionInfoChan := make(chan struct{}, 1)
		acClient, err = createUDPClient(acHost, acLocalPort, acPort, &lastDataTime, sessionInfoChan, versionInfoChan)
		if err != nil {
			logger.Printf("创建UDP连接失败: %v, 将在%d秒后重试", err, reconnectInterval/time.Second)
			time.Sleep(reconnectInterval)
			continue
		}
		// 移除defer，改为显式关闭

		// 根据日志文件存在性决定是否等待version包
		if latestFile == nil {
			logger.Println("等待AC服务器发送version包...")
			<-versionInfoChan
			logger.Println("成功接收version包")
			receivedVersionInfo = true
		} else {
			logger.Println("发现日志文件，跳过version包等待")
		}

		// 发送GetSessionInfo请求并实现超时重试逻辑
		retryInterval := 3 * time.Second
		retryCount := 0

		for {
			// 发送GetSessionInfo请求
			//acClient.SendGetSessionInfo()

			// 等待SessionInfo响应或超时
			select {
			case <-sessionInfoChan:
				logger.Println("成功接收SessionInfo")
				goto sessionInfoReceived
			case <-time.After(10 * time.Second):
				retryCount++
				if retryCount >= 3 {
					logger.Println("获取SessionInfo超时，放弃重试")
					goto sessionInfoTimeout
				}
				logger.Printf("获取SessionInfo超时，%d秒后重试...", retryInterval/time.Second)
				time.Sleep(retryInterval)
			}
		}

	sessionInfoTimeout:
		acClient.Close() // 显式关闭UDP连接
		continue outerLoop

	sessionInfoReceived:
		logger.Println("已获取SessionInfo，开始主循环")

		// 初始化WebSocket连接
		wsPort := cfg.App.Server.Port
		if err := waitForWebSocketServer(wsPort); err != nil {
			logger.Printf("等待WebSocket服务器失败: %v", err)
		}

		if err := initWebSocket(); err != nil {
			logger.Printf("WebSocket初始连接失败: %v", err)
		}
		startWebSocketReconnect()

		// 主循环
		for {
			// 构建并发送WebSocket消息
			buildAndSendWebSocketMessage()

			// 定期检查连接状态
			time.Sleep(time.Duration(cfg.Writer.HTTP.Leaderboard.Broadcast.Interval.MS) * time.Millisecond)
		}
	}
}
