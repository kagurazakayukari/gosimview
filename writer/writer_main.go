// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"gosimview/config"
	"gosimview/kn5conv"
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
	// 新增的全局变量（预初始化，避免向 nil map 写入导致 panic）
	globalUsers        = make(map[int]*User)
	globalTeams        = make(map[int]*Team)
	globalTeamMembers  = make(map[int]*TeamMember)
	globalCars         = make(map[int]*Car)
	globalTrackConfigs = make(map[int]*TrackConfig)
	// 互斥锁
	// UDP 状态由单个写锁保护，广播只读快照。
	stateMutex                sync.RWMutex
	activeStints              = make(map[int]*SessionStint)
	entryListLoaded           bool
	lastLogPoll               time.Time
	bestLapTimes              = make(map[int]int)
	classBestTimes            = make(map[string]int)
	sessionStints             = make([]*SessionStint, 0)
	completedTelemetrySamples = make(map[int][]udp.CarUpdate)
	globalMutex               sync.Mutex
	connectionsMutex          sync.Mutex
	entryListMutex            sync.Mutex
	sessionMutex              sync.Mutex
	// 数据收集控制
	dataCollectionEnabled bool = false
	// 全局变量用于暂存车辆信息，带互斥锁保证并发安全
	carLapDataMap = make(map[int][]CarLapData)
	stintLapData  = make(map[int]map[int][]StintLap)
	// 用于跟踪单圈数据的全局变量
	carLapData        = make(map[int][]udp.CarUpdate)
	telemetryData     = make(map[int][]byte)
	globalConnections = make(map[int]*ConnectionInfo)
	globalEntryList   = make(map[int]EntryListDriver)
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
	// 已加载边界的赛道标识（track|config），避免重复生成
	pitBoundaryLoadedFor string
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
	Name                 string
	Slots                int
	Port                 int
	HTTP                 string
	HTTPPort             int
	AdminPassword        string
	Password             string
	MaxCarSlots          int
	IdleTimeout          int
	VoteTimeout          int
	QualifyStandingType  int
	PitWindowStart       int
	PitWindowEnd         int
	ShortFormationLap    int
	PracticeDuration     int
	QualiDuration        int
	RaceDuration         int
	RaceDurationType     int
	RaceWaitTime         int
	RaceExtraLaps        int
	ReverseGridPositions int
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
	Gear          uint8
	Speed         uint16
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

// ACSession ACSession结构体（json tag 必须与 HTTP 端 WebSocketMessage.Session 对齐）
type ACSession struct {
	SessionID    int64   `json:"session_id"`
	EventID      int     `json:"event_id"`
	Type         int     `json:"type"`
	Name         string  `json:"name"`
	TrackTime    string  `json:"track_time"`
	Laps         int     `json:"laps"`
	Weather      string  `json:"weather"`
	DurationMin  int     `json:"duration_min"`
	ElapsedMs    int64   `json:"elapsed_ms"`
	StartGrip    float64 `json:"start_grip"`
	CurrentGrip  float64 `json:"current_grip"`
	HttpPort     int     `json:"http_port"`
	StartTime    int64   `json:"start_time"`
	FinishTime   int64   `json:"finish_time"`
	LastActivity int64   `json:"last_activity"`
	AirTemp      float64 `json:"air_temp"`
	RoadTemp     float64 `json:"road_temp"`
	IsFinished   int     `json:"is_finished"`
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
	if globalSession == nil || globalSession.ID == 0 {
		return
	}
	collectCarUpdates()
	var completedCar *udp.LapCompletedCar
	for _, car := range lapMsg.Cars {
		if car != nil && car.CarID == lapMsg.CarID {
			completedCar = car
			break
		}
	}
	if completedCar == nil {
		logger.Printf("完成圈消息缺少车辆 %d", lapMsg.CarID)
		return
	}
	slot := int(lapMsg.CarID)
	conn := globalConnections[slot]
	if conn != nil && conn.UserID == 0 {
		initializeConnectionIdentity(conn)
	}
	if conn == nil || conn.UserID == 0 || conn.CarDBID == 0 {
		logger.Printf("车辆 %d 身份尚未初始化，跳过写圈", slot)
		return
	}
	lap := CarLapData{SessionID: globalSession.ID, CarID: slot, GameCarID: conn.CarDBID,
		LapTime: int(lapMsg.LapTime), Cuts: lapMsg.Cuts, Laps: completedCar.Laps,
		Completed: uint16(completedCar.Completed), Timestamp: time.Now()}
	// 日志只补充完整圈的分段，保留 UDP 圈时这一权威值。
	for i := len(stintLapData[int(globalSession.ID)][slot]) - 1; i >= 0; i-- {
		v := stintLapData[int(globalSession.ID)][slot][i]
		if v.Time == lap.LapTime {
			lap.Sector1, lap.Sector2, lap.Sector3, lap.Tyre = v.Sector1, v.Sector2, v.Sector3, v.Tyre
			lap.Crashes, lap.CarCrashes = uint8(v.Crashes), uint8(v.CarCrashes)
			break
		}
	}
	if lap.Tyre == "" {
		lap.Tyre = userTyres[conn.DriverName]
	}
	previous := carLapDataMap[slot]
	if len(previous) > 0 && previous[len(previous)-1].Laps >= lap.Laps {
		return
	}
	stint := activeStints[slot]
	if stint == nil || stint.UserID != conn.UserID || stint.IsFinished != 0 {
		stint = &SessionStint{UserID: conn.UserID, SessionID: int(globalSession.ID), CarID: conn.CarDBID, GameCarID: slot, StartedAt: lap.Timestamp}
		if globalEvent.TeamEvent == 1 {
			for _, member := range globalTeamMembers {
				if member.UserID == conn.UserID && member.TeamID == conn.TeamID {
					stint.TeamMemberID = member.ID
					break
				}
			}
		}
		id, err := globalDBWriter.CreateSessionStint(stint)
		if err != nil {
			logger.Printf("创建 stint 失败: %v", err)
			return
		}
		stint.ID = int(id)
		activeStints[slot] = stint
		sessionStints = append(sessionStints, stint)
	}
	valid := lap.Cuts == 0 && lap.Crashes == 0 && lap.CarCrashes == 0 && lap.LapTime > 0
	record := &StintLap{StintID: stint.ID, Sector1: lap.Sector1, Sector2: lap.Sector2, Sector3: lap.Sector3,
		Grip: globalSession.CurrentGrip, Tyre: lap.Tyre, Time: lap.LapTime, Cuts: int(lap.Cuts), Crashes: int(lap.Crashes),
		CarCrashes: int(lap.CarCrashes), MaxSpeed: int(calculateMaxSpeed(completedTelemetrySamples[slot])), FinishedAt: lap.Timestamp}
	if lap.LapTime > 0 {
		record.AvgSpeed = int(float64(getTrackLength()) * 3600 / float64(lap.LapTime))
	}
	lapID, err := globalDBWriter.InsertStintLap(record)
	if err != nil {
		logger.Printf("写入单圈失败: %v", err)
		return
	}
	stint.Laps++
	if valid {
		stint.ValidLaps++
		if stint.BestLapID == 0 || lap.LapTime < bestLapTimes[stint.ID] {
			bestLapTimes[stint.ID] = lap.LapTime
			stint.BestLapID = lapID
			globalDBWriter.InsertSessionFeed(&SessionFeed{SessionID: globalSession.ID, Type: 4,
				Detail: feedDetail(slot, map[string]interface{}{"lap_time": lap.LapTime}), Time: lap.Timestamp})
		}
		carClass := "N/A"
		if car := globalCars[conn.CarDBID]; car != nil {
			carClass = car.CarClass
		}
		if classBestTimes[carClass] == 0 || lap.LapTime < classBestTimes[carClass] {
			classBestTimes[carClass] = lap.LapTime
			globalDBWriter.InsertSessionFeed(&SessionFeed{SessionID: globalSession.ID, Type: 5, Detail: feedDetail(slot, map[string]interface{}{"lap_time": lap.LapTime}), Time: lap.Timestamp})
		}
		if cfg.ACServer.Event.LapTelemetryEnable && len(telemetryData[slot]) == 0 && len(carLapData[slot]) > 0 {
			data, err := generateLapTelemetry(slot, int32(getTrackLength()), carLapData[slot])
			if err == nil {
				telemetryData[slot] = data
			}
		}
		if cfg.ACServer.Event.LapTelemetryEnable && len(telemetryData[slot]) > 0 {
			if err := globalDBWriter.InsertLapTelemetry(lapID, telemetryData[slot]); err != nil {
				logger.Printf("写遥测失败: %v", err)
			}
		}
	}
	_, err = globalDBWriter.executeSQL("UPDATE session_stint SET laps=?, valid_laps=?, best_lap_id=?, finished_at=? WHERE session_stint_id=?",
		stint.Laps, stint.ValidLaps, nullableLapID(stint.BestLapID), lap.Timestamp, stint.ID)
	if err != nil {
		logger.Printf("更新 stint 失败: %v", err)
	}
	delete(telemetryData, slot)
	delete(completedTelemetrySamples, slot)
	carLapDataMap[slot] = append(previous, lap)
	globalSession.LastActivity = lap.Timestamp
	globalDBWriter.UpdateSessionGrip(globalSession.ID, globalSession.CurrentGrip)
}

func nullableLapID(id int) interface{} {
	if id == 0 {
		return nil
	}
	return id
}

// UDP 类型保持原协议，数据库和原版网页使用 0/1/2。
func databaseSessionType(msg udp.SessionInfo) int {
	switch strings.ToLower(msg.Name) {
	case "practice":
		return 0
	case "qualify", "qualifying", "qualification":
		return 1
	case "race":
		return 2
	}
	if msg.Type >= udp.SessionTypePractice && msg.Type <= udp.SessionTypeRace {
		return int(msg.Type) - 1
	}
	return 0
}

func handleCarInfoMessage(msg udp.CarInfo, last *time.Time) {
	*last = time.Now()
	if !msg.IsConnected {
		return
	}
	slot := int(msg.CarID)
	conn := globalConnections[slot]
	if conn == nil {
		conn = &ConnectionInfo{CarID: slot, TrackStatus: 3}
		globalConnections[slot] = conn
	}
	conn.DriverName, conn.DriverGUID, conn.CarModel, conn.CarSkin = msg.DriverName, string(msg.DriverGUID), msg.CarModel, msg.CarSkin
	conn.ConnectionStatus = 0
	initializeConnectionIdentity(conn)
}

func handleClientLoadedMessage(msg udp.ClientLoaded, last *time.Time) {
	*last = time.Now()
	if conn := globalConnections[int(msg)]; conn != nil {
		conn.ConnectionStatus = 0
	}
}

type PitVisit struct {
	EntryTime time.Time
	ExitTime  time.Time
}

type ConnectionInfo struct {
	UserID                     int
	CarDBID                    int
	TeamID                     int
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
	if globalDBWriter.SimulateSQL {
		for _, values := range carLapDataMap {
			for _, v := range values {
				if v.LapTime > 0 {
					return float64(v.LapTime)
				}
			}
		}
		return math.MaxFloat64
	}
	var lapTime sql.NullFloat64
	err := globalDBWriter.DB.QueryRow("SELECT time FROM stint_lap WHERE stint_lap_id = ?", lapID).Scan(&lapTime)
	if err != nil {
		return math.MaxFloat64 // 返回最大浮点数确保当前圈速成为最佳圈
	}
	if !lapTime.Valid {
		return math.MaxFloat64
	}
	return lapTime.Float64
}

// 身份查询只在车辆接入时执行；广播只使用已解析的数据库 ID。
func initializeConnectionIdentity(conn *ConnectionInfo) {
	guid, err := strconv.ParseInt(conn.DriverGUID, 10, 64)
	if err != nil || guid <= 0 {
		logger.Printf("车辆 %d 的 GUID 无效", conn.CarID)
		return
	}
	user := &User{Steam64ID: guid, Name: conn.DriverName}
	uid, _, err := globalDBWriter.FindOrCreateUser(user)
	if err != nil {
		logger.Printf("解析车手失败: %v", err)
		return
	}
	conn.UserID = uid
	globalUsers[uid] = user
	for id, car := range globalCars {
		if car.Name == conn.CarModel {
			conn.CarDBID = id
			break
		}
	}
	if conn.CarDBID == 0 {
		car := &Car{Name: conn.CarModel, DisplayName: conn.CarModel, Manufacturer: "N/A", CarClass: "N/A"}
		id, err := globalDBWriter.FindOrCreateCarID(car)
		if err != nil {
			logger.Printf("解析车辆失败: %v", err)
			return
		}
		car.ID = id
		globalCars[id] = car
		conn.CarDBID = id
	}
	conn.TeamID = 0
	conn.TeamID = int(getTeamIDByCarID(conn.CarID))
}

func handleNewConnectionMessage(msg udp.SessionCarInfo, last *time.Time) {
	*last = time.Now()
	slot := int(msg.CarID)
	previous := globalConnections[slot]
	finishStint(slot)
	conn := &ConnectionInfo{CarID: slot, DriverName: msg.DriverName, DriverGUID: string(msg.DriverGUID), CarModel: msg.CarModel, CarSkin: msg.CarSkin, DriverInitials: msg.DriverInitials, CarName: msg.CarName, ConnectionStatus: 1, TrackStatus: 3, TrackStatusLastChangedTime: time.Now()}
	globalConnections[slot] = conn
	initializeConnectionIdentity(conn)
	if globalSession != nil && globalSession.ID > 0 && conn.UserID > 0 {
		globalDBWriter.InsertSessionFeed(&SessionFeed{SessionID: globalSession.ID, Type: 2, Detail: feedDetail(slot, nil), Time: time.Now()})
		if previous != nil && conn.TeamID > 0 && previous.UserID > 0 && previous.UserID != conn.UserID {
			data, _ := json.Marshal(map[string]interface{}{"team_id_1": conn.TeamID, "user_id_1": previous.UserID, "user_id_2": conn.UserID})
			globalDBWriter.InsertSessionFeed(&SessionFeed{SessionID: globalSession.ID, Type: 6, Detail: string(data), Time: time.Now()})
		}
	}
}

func handleChatMessage(msg udp.Chat, last *time.Time) {
	*last = time.Now()
	if !cfg.App.Live.SessionFeedShowChat || globalSession == nil || globalSession.ID == 0 {
		return
	}
	conn := globalConnections[int(msg.CarID)]
	if conn == nil || conn.UserID == 0 {
		return
	}
	globalDBWriter.InsertSessionFeed(&SessionFeed{SessionID: globalSession.ID, Type: 7, Detail: feedDetail(int(msg.CarID), map[string]interface{}{"msg": msg.Message}), Time: time.Now()})
}

func handleEndSessionMessage(msg udp.EndSession, last *time.Time) {
	*last = time.Now()
	if globalSession == nil || globalSession.ID == 0 {
		return
	}
	// 成绩 JSON 按槽位和 GUID 归属补充，绝不把全场圈写入每辆车。
	data, err := readSessionDataFromJSON(filepath.Join(cfg.Game.Path, string(msg)))
	if err != nil {
		logger.Printf("读取成绩文件失败: %v", err)
	} else {
		importResultLaps(data)
	}
	finishCurrentSession()
}

func importResultLaps(data *SessionData) {
	seen := make(map[[3]int]int)
	for _, lap := range data.Laps {
		slot := lap.CarId
		conn := globalConnections[slot]
		if conn == nil {
			continue
		}
		userID := conn.UserID
		if lap.DriverGuid != "" {
			guid, err := strconv.ParseInt(lap.DriverGuid, 10, 64)
			if err != nil {
				continue
			}
			userID = 0
			for id, user := range globalUsers {
				if user.Steam64ID == guid {
					userID = id
					break
				}
			}
			if userID == 0 {
				continue
			}
		}
		lapTime := lap.Time
		if lapTime <= 0 {
			lapTime = lap.LapTime
		}
		if lapTime <= 0 {
			continue
		}
		stint := activeStints[slot]
		if stint != nil && stint.UserID != userID {
			stint = nil
		}
		if stint == nil {
			for i := len(sessionStints) - 1; i >= 0; i-- {
				v := sessionStints[i]
				if v.GameCarID == slot && v.UserID == userID {
					stint = v
					break
				}
			}
		}
		if stint == nil {
			continue
		}
		key := [3]int{slot, userID, lapTime}
		seen[key]++
		exists := false
		if !globalDBWriter.SimulateSQL {
			var count int
			if err := globalDBWriter.DB.QueryRow("SELECT COUNT(*) FROM stint_lap WHERE stint_id=? AND time=?", stint.ID, lapTime).Scan(&count); err != nil {
				logger.Printf("查询成绩圈失败: %v", err)
				continue
			}
			exists = count >= seen[key]
		} else {
			count := 0
			for _, v := range carLapDataMap[slot] {
				if v.LapTime == lapTime {
					count++
				}
			}
			exists = count >= seen[key]
		}
		if exists {
			continue
		}
		record := &StintLap{StintID: stint.ID, Sector1: lap.Sectors[0], Sector2: lap.Sectors[1], Sector3: lap.Sectors[2], Grip: globalSession.CurrentGrip, Tyre: lap.Tyre, Time: lapTime, Cuts: lap.Cuts, AvgSpeed: int(float64(getTrackLength()) * 3600 / float64(lapTime)), FinishedAt: globalSession.StartTime.Add(time.Duration(lap.Timestamp) * time.Millisecond)}
		id, err := globalDBWriter.InsertStintLap(record)
		if err != nil {
			logger.Printf("补成绩圈失败: %v", err)
			continue
		}
		stint.Laps++
		if lap.Cuts == 0 {
			stint.ValidLaps++
			if stint.BestLapID == 0 || lapTime < bestLapTimes[stint.ID] {
				stint.BestLapID = id
				bestLapTimes[stint.ID] = lapTime
			}
		}
		globalDBWriter.executeSQL("UPDATE session_stint SET laps=?,valid_laps=?,best_lap_id=? WHERE session_stint_id=?", stint.Laps, stint.ValidLaps, nullableLapID(stint.BestLapID), stint.ID)
	}
}

// 初始化会话使用同一条路径，启动接入已有比赛与换节都可采集。
func handleNewSessionMessage(msg udp.SessionInfo, last *time.Time) {
	*last = time.Now()
	if err := initializeSession(msg, true); err != nil {
		logger.Printf("新会话初始化失败: %v", err)
	}
}

func initializeSession(msg udp.SessionInfo, newSession bool) error {
	track := &TrackConfig{TrackName: msg.Track, ConfigName: msg.TrackConfig}
	trackID, err := globalDBWriter.FindOrCreateTrackConfigID(track)
	if err != nil {
		return err
	}
	name := cfg.ACServer.Event.Name
	if name == "" {
		name = msg.ServerName
	}
	event, err := globalDBWriter.FindEventByName(name)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if event == nil || err == sql.ErrNoRows {
		event = &Event{}
	}
	event.Name, event.ServerName, event.TrackConfigID = name, msg.ServerName, trackID
	event.Active, event.TeamEvent, event.LiveryPreview, event.UseNumber = 1, boolToInt(cfg.ACServer.Event.Team.Enable), boolToInt(cfg.ACServer.Event.Team.LiveryPreviewEnable), boolToInt(cfg.ACServer.Event.Team.UseNumber)
	event.StartGrip = -1
	if sc := globalDBWriter.ServerCfg; sc != nil {
		event.PracticeDuration, event.QualiDuration, event.RaceDuration, event.RaceDurationType = sc.PracticeDuration, sc.QualiDuration, sc.RaceDuration, sc.RaceDurationType
		event.RaceWaitTime, event.RaceExtraLaps, event.ReverseGridPositions = sc.RaceWaitTime, sc.RaceExtraLaps, sc.ReverseGridPositions
	}
	if event.ID == 0 {
		err = globalDBWriter.InsertEvent(event)
	} else {
		err = globalDBWriter.UpdateEvent(event)
	}
	if err != nil {
		return err
	}
	changedEvent := globalEvent == nil || globalEvent.ID != event.ID
	globalEvent = event
	if !entryListLoaded || changedEvent {
		if err := globalDBWriter.LoadEntryList(cfg.Game.Path); err != nil {
			return err
		}
		entryListLoaded = true
	}
	// SESSION_INFO 表示当前会话；重复响应不能再次创建会话。
	if !newSession && globalSession != nil && globalSession.ID > 0 && globalSession.EventID == event.ID {
		return nil
	}
	if newSession {
		finishCurrentSession()
	}
	var session *Session
	if !newSession && !globalDBWriter.SimulateSQL {
		session, err = globalDBWriter.FindLatestSessionByEventID(event.ID)
		if err != nil {
			return err
		}
		if session != nil && session.IsFinished != 0 {
			session = nil
		}
	}
	if session == nil {
		port := cfg.Writer.HTTP.Port
		if port == 0 {
			port = cfg.ACServer.UDP.LocalPort
		}
		session = &Session{EventID: event.ID, Type: databaseSessionType(msg), Name: msg.Name, TrackTime: "0", StartTime: time.Now().Add(-time.Duration(msg.ElapsedMilliseconds) * time.Millisecond), DurationMin: int(msg.Time), ElapsedMs: int(msg.ElapsedMilliseconds), Laps: int(msg.Laps), Weather: msg.WeatherGraphics, AirTemp: float64(msg.AmbientTemp), RoadTemp: float64(msg.RoadTemp), StartGrip: -1, CurrentGrip: -1, LastActivity: time.Now(), HTTPPort: port}
		if _, err = globalDBWriter.InsertSession(session); err != nil {
			return err
		}
	}
	globalSession = session
	dataCollectionEnabled = true
	carLapDataMap = make(map[int][]CarLapData)
	stintLapData = make(map[int]map[int][]StintLap)
	carLapData = make(map[int][]udp.CarUpdate)
	telemetryData = make(map[int][]byte)
	activeStints = make(map[int]*SessionStint)
	latestCarUpdates = make(map[udp.CarID]udp.CarUpdate)
	bestLapTimes = make(map[int]int)
	classBestTimes = make(map[string]int)
	sessionStints = make([]*SessionStint, 0)
	completedTelemetrySamples = make(map[int][]udp.CarUpdate)
	lastFileSize, lastLineIdx, partialLine, globalNextSessionLineIdx = 0, 0, "", -1
	latestFile, _ = findLatestSessionLogFile(logDir)
	// 历史日志不属于本次采集；从接入时的文件末尾开始。
	if latestFile != nil {
		if info, err := latestFile.Info(); err == nil {
			lastFileSize = info.Size()
		}
	}
	for _, conn := range globalConnections {
		conn.TrackStatus = 3
		initializeConnectionIdentity(conn)
	}
	loadPitBoundary(msg.Track, msg.TrackConfig)
	logger.Printf("当前事件 %d，会话 %d: %s", event.ID, session.ID, session.Name)
	return nil
}

func finishStint(slot int) {
	stint := activeStints[slot]
	if stint == nil || stint.IsFinished != 0 {
		return
	}
	stint.IsFinished = 1
	stint.FinishedAt = time.Now()
	if _, err := globalDBWriter.executeSQL("UPDATE session_stint SET is_finished=1,finished_at=? WHERE session_stint_id=?", stint.FinishedAt, stint.ID); err != nil {
		logger.Printf("结束 stint 失败: %v", err)
	}
	delete(activeStints, slot)
}

func finishCurrentSession() {
	if globalSession == nil || globalSession.ID == 0 || globalSession.IsFinished != 0 {
		return
	}
	for slot := range activeStints {
		finishStint(slot)
	}
	globalSession.IsFinished = 1
	globalSession.FinishTime = time.Now()
	globalSession.LastActivity = time.Now()
	globalSession.ElapsedMs = int(time.Since(globalSession.StartTime).Milliseconds())
	if err := globalDBWriter.UpdateSession(globalSession); err != nil {
		logger.Printf("结束会话失败: %v", err)
	}
	dataCollectionEnabled = false
}

// feed 的 car_id 是数据库车辆 ID，槽位只用于寻找当前身份。
func feedDetail(slot int, detail map[string]interface{}) string {
	if detail == nil {
		detail = make(map[string]interface{})
	}
	if conn := globalConnections[slot]; conn != nil {
		detail["user_id"] = conn.UserID
		detail["car_id"] = conn.CarDBID
		if conn.TeamID > 0 {
			detail["team_id"] = conn.TeamID
		}
	}
	data, err := json.Marshal(detail)
	if err != nil {
		logger.Printf("序列化feed失败: %v", err)
		return "{}"
	}
	return string(data)
}

func handleConnectionClosedMessage(msg udp.SessionCarInfo, last *time.Time) {
	*last = time.Now()
	slot := int(msg.CarID)
	if globalSession != nil && globalSession.ID > 0 {
		globalDBWriter.InsertSessionFeed(&SessionFeed{SessionID: globalSession.ID, Type: 3, Detail: feedDetail(slot, nil), Time: time.Now()})
	}
	finishStint(slot)
	if conn := globalConnections[slot]; conn != nil {
		conn.ConnectionStatus = 2
	}
}

func handleCollisionWithCarMessage(msg udp.CollisionWithCar, last *time.Time) {
	*last = time.Now()
	if globalSession == nil || globalSession.ID == 0 {
		return
	}
	a, b := globalConnections[int(msg.CarID)], globalConnections[int(msg.OtherCarID)]
	if a == nil || b == nil {
		return
	}
	detail := map[string]interface{}{"user_id_1": a.UserID, "user_id_2": b.UserID, "car_id_1": a.CarDBID, "car_id_2": b.CarDBID, "speed": int(msg.ImpactSpeed), "nsp": latestCarUpdates[msg.CarID].NormalisedSplinePos}
	if a.TeamID > 0 {
		detail["team_id_1"] = a.TeamID
	}
	if b.TeamID > 0 {
		detail["team_id_2"] = b.TeamID
	}
	data, _ := json.Marshal(detail)
	globalDBWriter.InsertSessionFeed(&SessionFeed{SessionID: globalSession.ID, Type: 0, Detail: string(data), Time: time.Now()})
}

func handleCollisionWithEnvMessage(msg udp.CollisionWithEnvironment, last *time.Time) {
	*last = time.Now()
	if globalSession == nil || globalSession.ID == 0 {
		return
	}
	globalDBWriter.InsertSessionFeed(&SessionFeed{SessionID: globalSession.ID, Type: 1, Detail: feedDetail(int(msg.CarID), map[string]interface{}{"speed": int(msg.ImpactSpeed), "nsp": latestCarUpdates[msg.CarID].NormalisedSplinePos}), Time: time.Now()})
}

// 车辆更新事件处理函数
func handleCarUpdateMessage(updateMsg udp.CarUpdate, lastDataTime *time.Time) {
	*lastDataTime = time.Now()
	if globalSession == nil || globalSession.ID == 0 {
		return
	}
	if time.Since(lastLogPoll) >= time.Second {
		collectCarUpdates()
		lastLogPoll = time.Now()
	}
	carID := int(updateMsg.CarID)
	currentSplinePos := updateMsg.NormalisedSplinePos

	// 实时更新最新车辆数据（wsMutex 保护，供主循环构建排行榜使用）
	latestCarUpdates[updateMsg.CarID] = updateMsg

	// 圈跟踪数据用 lapDataMutex 保护
	lapDataMutex.Lock()
	lastUpdates, exists := carLapData[carID]

	var lastPos float32
	lapCompleted := false
	if exists && len(lastUpdates) > 0 {
		lastPos = lastUpdates[len(lastUpdates)-1].NormalisedSplinePos
		lapCompleted = lastPos > 0.9 && currentSplinePos < 0.1
	}

	if lapCompleted {
		completedTelemetrySamples[carID] = append([]udp.CarUpdate(nil), lastUpdates...)
		// 生成遥测数据
		if len(lastUpdates) > 0 {
			telemetry, err := generateLapTelemetry(carID, int32(getTrackLength()), lastUpdates)
			if err != nil {
				logger.Printf("生成遥测数据失败: %v", err)
			} else {
				telemetryData[carID] = telemetry
				logger.Printf("遥测数据已暂存: 车辆 %d，数据大小 %d 字节", carID, len(telemetry))
			}
		}
	}
	// 添加新数据点到圈跟踪
	if lapCompleted {
		carLapData[carID] = nil
	}
	if len(carLapData[carID]) >= 100000 {
		carLapData[carID] = nil
	}
	carLapData[carID] = append(carLapData[carID], updateMsg)
	lapDataMutex.Unlock()

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
						Detail:    feedDetail(connInfo.CarID, nil),
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
								Detail:    feedDetail(connInfo.CarID, map[string]interface{}{"pit_time": pitDuration.Milliseconds()}),
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
}

// getTrackLength 安全获取当前事件赛道长度，缺失时返回 0（避免 nil 解引用）
func getTrackLength() int {
	id := globalEvent.TrackConfigID
	if tc, ok := globalTrackConfigs[id]; ok && tc != nil {
		return tc.Length
	}
	tc, err := globalDBWriter.GetTrackConfigByID(id)
	if err != nil || tc == nil {
		return 0
	}
	globalTrackConfigs[id] = tc
	return tc.Length
}

// loadPitBoundary 调用 kn5conv 生成/读取当前赛道的维修区边界（同一赛道只处理一次）。
// 生成的 SVG 同时写入 cache.path/live-track-maps，供 HTTP 端赛道地图使用。
func loadPitBoundary(trackName, configName string) {
	if trackName == "" || cfg.App.Server.CachePath == "" {
		return
	}
	key := trackName + "|" + configName
	if pitBoundaryLoadedFor == key {
		return
	}
	pitBoundary = nil
	boundary, err := kn5conv.BatchProcess(filepath.Dir(cfg.Game.Path), cfg.App.Server.CachePath, trackName, configName)
	if err != nil {
		logger.Printf("生成/读取维修区边界失败(%s): %v", key, err)
		return
	}
	pitBoundary = boundary
	pitBoundaryLoadedFor = key
	logger.Printf("维修区边界已加载: %s, 顶点数: %d", key, len(boundary))
}

var currentLogFile string = ""
var globalNextSessionLineIdx int = -1
var userTyres = make(map[string]string)
var userStintIDs = make(map[string]int)
var tyreRegex = regexp.MustCompile(`^\s*(.*?) \[[^\]]*\] changed tyres to (\S+)`)
var connectionRegex = regexp.MustCompile(`(?s)NEW PICKUP CONNECTION from\s+(.*?)\s*\nVERSION .*?\n(.*?)\nREQUESTED CAR: (.*?)\n.*?GUID (\d+) .*?DRIVER ACCEPTED FOR CAR (\d+)`)
var disconnectRegex = regexp.MustCompile(`.*driver disconnected: (.*?) \[[^\]]*\]`)

func handleSessionInfoMessage(msg udp.SessionInfo, last *time.Time) {
	*last = time.Now()
	if err := initializeSession(msg, false); err != nil {
		logger.Printf("会话初始化失败: %v", err)
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
	client, err := udp.NewServerClient(host, localPort, serverPort, false, "", 0, func(msg udp.Message) {
		stateMutex.Lock()
		defer stateMutex.Unlock()
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
		case udp.EventClientEvent, udp.EventCollisionWithCar, udp.EventCollisionWithEnv:
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

		// 文件已经按字节增量读取，不再次用累计行号切片。
		chunkStart := lastLineIdx
		lastLineIdx += len(lines)

		// 查找最后一个"Session"行
		for i, line := range lines {
			if strings.Contains(line, "SESSION:") {
				globalNextSessionLineIdx = chunkStart + i
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
			currentLineIdx := chunkStart + i
			if tyreMatch := tyreRegex.FindStringSubmatch(line); len(tyreMatch) == 3 {
				username := tyreMatch[1]
				tyreType := tyreMatch[2]
				oldTyre := userTyres[username]
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
					if oldTyre != "" && oldTyre != tyreType {
						finishStint(carID)
					}

					// 插入轮胎更换事件（原版格式: {"user_id","car_id","old_t","new_t"}）
					globalDBWriter.InsertSessionFeed(&SessionFeed{
						SessionID: globalSession.ID,
						Type:      16,
						Detail:    feedDetail(carID, map[string]interface{}{"old_t": oldTyre, "new_t": tyreType}),
						Time:      time.Now(),
					})
				}
			} else if connectMatch := connectionRegex.FindStringSubmatch(line); len(connectMatch) == 6 {
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

		// 每个增量都处理分段与单圈，SESSION 标记不必与后续数据同批到达。
		currentNextSessionIdx := -1
		if globalSession != nil && globalSession.ID > 0 {
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
			lapRegex := regexp.MustCompile(`LAP (.*?) (\d+):(\d+):(\d+)`)

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
				if lapMatch := lapRegex.FindStringSubmatch(line); len(lapMatch) == 5 {
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

	// 默认真实采集；只有显式指定 -simulate=true 才隔离数据库。
	simulateMode := flag.Bool("simulate", false, "显式指定 -simulate=true 时模拟运行，不连接或写入数据库；默认真实采集")
	flag.Parse()

	configPath := firstExistingPath("config/config.toml", "simview/config/config.toml", "config.toml")
	if configPath == "" {
		fatalLogger.Fatalf("未找到 config.toml（已尝试 config/config.toml、simview/config/config.toml）")
	}
	data, err := os.ReadFile(configPath)
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
	// 初始化全局对象，避免首个消息到达前对 nil 解引用
	globalEvent = &Event{}
	globalSession = &Session{}
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

	dbWriter, err := NewDBWriter(dsn, *simulateMode)
	if err != nil {
		fatalLogger.Fatalf("Failed to create DBWriter: %v", err)
	}
	globalDBWriter = dbWriter // 设置全局DBWriter实例
	defer dbWriter.Close()
	if *simulateMode {
		logger.Println("SQL模拟模式已开启：本次运行不会向数据库写入任何数据")
	}

	// 加载并验证server_cfg.ini配置
	if err := dbWriter.LoadServerConfig(cfg.Game.Path); err != nil {
		fatalLogger.Fatalf("server_cfg.ini验证失败: %v", err)
	}
	// 用 server_cfg.ini 的 session 时长初始化 event 默认值（模仿原版写入 event 表）
	if sc := dbWriter.ServerCfg; sc != nil {
		globalEvent.PracticeDuration = sc.PracticeDuration
		globalEvent.QualiDuration = sc.QualiDuration
		globalEvent.RaceDuration = sc.RaceDuration
		globalEvent.RaceDurationType = sc.RaceDurationType
		globalEvent.RaceWaitTime = sc.RaceWaitTime
		globalEvent.RaceExtraLaps = sc.RaceExtraLaps
		globalEvent.ReverseGridPos = sc.ReverseGridPositions
	}
	// entry_list 在收到会话并建立 event 后加载，团队外键才能正确关联。
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
			select {
			case <-versionInfoChan:
				logger.Println("成功接收version包")
				receivedVersionInfo = true
			case <-time.After(3 * time.Second):
				logger.Println("未收到version包，主动请求当前会话")
			}
		} else {
			logger.Println("发现日志文件，跳过version包等待")
		}

		// 发送GetSessionInfo请求并实现超时重试逻辑
		retryInterval := 3 * time.Second
		retryCount := 0

		for {
			// 发送GetSessionInfo请求
			if err := acClient.SendMessage(udp.GetSessionInfo{}); err != nil {
				logger.Printf("请求当前会话失败: %v", err)
			}

			// 等待SessionInfo响应或超时
			select {
			case <-sessionInfoChan:
				stateMutex.RLock()
				ready := globalSession != nil && globalSession.ID > 0
				stateMutex.RUnlock()
				if !ready {
					retryCount++
					if retryCount >= 3 {
						goto sessionInfoTimeout
					}
					continue
				}
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

		// 启动时主动请求既有在线车辆，不要求车手重新连接。
		if cfg.ACServer.UDP.Realtime.Update.Interval.MS > 0 {
			udp.RealtimePosIntervalMs = cfg.ACServer.UDP.Realtime.Update.Interval.MS
		}
		if err := acClient.SendMessage(udp.NewEnableRealtimePosInterval(udp.RealtimePosIntervalMs)); err != nil {
			logger.Printf("开启车辆更新失败: %v", err)
		}
		stateMutex.RLock()
		slots := make([]int, 0, len(globalEntryList))
		for slot := range globalEntryList {
			slots = append(slots, slot)
		}
		stateMutex.RUnlock()
		for _, slot := range slots {
			if err := acClient.SendGetCarInfo(udp.CarID(slot)); err != nil {
				logger.Printf("请求车辆信息失败: %v", err)
			}
		}
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
			interval := cfg.Writer.HTTP.Leaderboard.Broadcast.Interval.MS
			if interval <= 0 {
				interval = 2000
			}
			time.Sleep(time.Duration(interval) * time.Millisecond)
		}
	}
}
