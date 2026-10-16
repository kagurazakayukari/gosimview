// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"gosimview/config"
	"gosimview/kn5conv"
	"gosimview/udp"

	_ "github.com/go-sql-driver/mysql"
)

// 全局变量
var (
	logger         *log.Logger
	fatalLogger    *log.Logger
	cfg            *config.Config
	globalDBWriter *DBWriter
	globalEvent    *Event
	globalSession  *Session
	logDir         string
	latestFile     os.DirEntry
	// 新增的全局变量（预初始化，避免向 nil map 写入导致 panic）
	globalUsers        = make(map[int]*User)
	globalTeams        = make(map[int]*Team)
	globalTeamMembers  = make(map[int]*TeamMember)
	globalCars         = make(map[int]*Car)
	globalTrackConfigs = make(map[int]*TrackConfig)
	// 互斥锁
	// UDP 状态由单个写锁保护，广播只读快照。
	stateMutex       sync.RWMutex
	activeStints     = make(map[int]*SessionStint)
	entryListLoaded  bool
	lastLogPoll      time.Time
	bestLapTimes     = make(map[int]int)
	classBestTimes   = make(map[string]int)
	sessionStints    = make([]*SessionStint, 0)
	globalMutex      sync.Mutex
	connectionsMutex sync.Mutex
	entryListMutex   sync.Mutex
	sessionMutex     sync.Mutex
	// 数据收集控制
	dataCollectionEnabled bool = false
	// 周期 UDP 在文件恢复完成后才进入当前采集状态。
	recoveryReady bool
	// 新节初始化失败时保留真实通知，重试时不能把旧节当作新节继续采集。
	pendingNewSession *udp.SessionInfo
	pendingSessionEnd bool
	pendingEndMessage *udp.EndSession
	// 新节首次到达时固定的起点基准；重试沿用同一基准，避免 now-旧 elapsed 漂移。
	pendingNewSessionStartTime time.Time
	// 当前采集会话的轻量签名，用于判定探活响应是否仍是同一节。
	activeSessionSignature string
	// 全局变量用于暂存车辆信息，带互斥锁保证并发安全
	carLapDataMap = make(map[int][]CarLapData)
	stintLapData  = make(map[int]map[int][]StintLap)
	// 用于跟踪单圈数据的全局变量
	carLapData = make(map[int][]udp.CarUpdate)
	// 车辆至少经历一次完圈事件后，才收集下一整圈的持久化样本。
	lapTelemetryReady = make(map[int]bool)
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
	if !sessionCollecting() {
		return
	}
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
	if obs := pitObservations[int(lapMsg.CarID)]; obs != nil && obs.resumePending {
		return
	}
	if !acceptLapCompletion(lapMsg, *completedCar, time.Now()) {
		return
	}
	// 日志暂时读不到时本圈不丢：先按 UDP 观测入队，日志恢复后再补分段提交。
	logPending := false
	if err := collectCarUpdates(); err != nil {
		logPending = true
		logger.Printf("完圈前读取日志失败，本圈入队等待日志恢复: %v", err)
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
	// 当前抓地力来自完成圈报文，不反推接入前的初始抓地力。
	grip := float64(lapMsg.GripLevel)
	if !math.IsNaN(grip) && !math.IsInf(grip, 0) && grip >= 0 && grip <= 1 {
		globalSession.CurrentGrip = grip
	} else {
		logger.Printf("完成圈抓地力数值无效: %v", grip)
	}
	lap := CarLapData{SessionID: globalSession.ID, CarID: slot, GameCarID: conn.CarDBID,
		LapTime: int(lapMsg.LapTime), Cuts: lapMsg.Cuts, Laps: completedCar.Laps,
		Completed: uint16(completedCar.Completed), Timestamp: time.Now()}
	// 日志只补充完整圈的分段，保留 UDP 圈时这一权威值。
	// 日志读取失败时先留空，待日志恢复后由 supplementPendingLapFromLog 补齐。
	if !logPending {
		for i := len(stintLapData[int(globalSession.ID)][slot]) - 1; i >= 0; i-- {
			v := stintLapData[int(globalSession.ID)][slot][i]
			if v.Time == lap.LapTime {
				lap.Sector1, lap.Sector2, lap.Sector3, lap.Tyre = v.Sector1, v.Sector2, v.Sector3, v.Tyre
				lap.Crashes, lap.CarCrashes = uint8(v.Crashes), uint8(v.CarCrashes)
				break
			}
		}
	}
	if lap.Tyre == "" {
		lap.Tyre = userTyres[conn.DriverName]
	}
	stint, err := ensureCurrentStint(slot, lap.Timestamp)
	if stint == nil {
		logger.Printf("建立当前 stint 失败: %v", err)
		return
	}
	// 身份与段已绑定后才消费本圈样本；建段写库失败仍保留占位对象供队列重试。
	samples := completeLapSamples(slot)
	// 冻结本次观测：身份、时间、整圈样本与绑定的段对象；重试只用这份观测，不再切样本。
	carClass := "N/A"
	if car := globalCars[conn.CarDBID]; car != nil {
		carClass = car.CarClass
	}
	record := StintLap{StintID: stint.ID, Sector1: lap.Sector1, Sector2: lap.Sector2, Sector3: lap.Sector3,
		Grip: globalSession.CurrentGrip, Tyre: lap.Tyre, Time: lap.LapTime, Cuts: int(lap.Cuts), Crashes: int(lap.Crashes),
		CarCrashes: int(lap.CarCrashes), MaxSpeed: int(calculateMaxSpeed(samples)), FinishedAt: lap.Timestamp}
	if lap.LapTime > 0 {
		record.AvgSpeed = int(float64(getTrackLength()) * 3600 / float64(lap.LapTime))
	}
	entry := &pendingLap{
		stint:       stint,
		sessionID:   globalSession.ID,
		slot:        slot,
		record:      record,
		carLap:      lap,
		samples:     samples,
		telemetryOn: cfg.ACServer.Event.LapTelemetryEnable,
		trackLength: int32(getTrackLength()),
		carClass:    carClass,
		feedDetail:  feedDetail(slot, map[string]interface{}{"lap_time": lap.LapTime}),
		timestamp:   lap.Timestamp,
		needsLog:    logPending,
	}
	enqueuePendingLap(entry)
	// 观测去重与持久化分别记录；只有已有待处理实体时才屏蔽重复报文。
	lastLapCompletions[slot] = lapCompletion{lapMsg.LapTime, lapMsg.Cuts, completedCar.Laps, completedCar.Completed, lap.Timestamp}
	if err := flushPendingLaps(); err != nil {
		logger.Printf("提交单圈失败，已入队等待重试(slot=%d): %v", slot, err)
	}
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

	// ConnectedAt 记录本段连接的建立时刻；stint 的 started_at 取它，而不是第一次完圈时间。
	// 由 handleNewConnectionMessage 写入；日志恢复出的连接没有该时刻，为空。
	ConnectedAt time.Time
	// 新段从进站停车后的再次起步开始，不沿用旧连接的起点。
	StintStartedAt time.Time
}

func getLapTimeByID(lapID int) (int, error) {
	var lapTime sql.NullInt64
	if err := globalDBWriter.DB.QueryRow("SELECT time FROM stint_lap WHERE stint_lap_id = ?", lapID).Scan(&lapTime); err != nil {
		return 0, err
	}
	if !lapTime.Valid || lapTime.Int64 <= 0 {
		return 0, fmt.Errorf("最快圈 %d 缺少有效圈时", lapID)
	}
	return int(lapTime.Int64), nil
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
	// 旧段未成功结束前不覆盖连接与 stint，避免同槽位出现两条未收尾的 stint。
	if err := finishStint(slot); err != nil {
		// 旧车手不能继续贡献遥测；保留旧连接/stint等待收尾，但清掉跨车手样本。
		resetLapSamples(slot)
		logger.Printf("新连接前结束旧 stint 失败，保留旧连接等待重试(slot=%d): %v", slot, err)
		return
	}
	previous := globalConnections[slot]
	resetLapSamples(slot)
	delete(pitObservations, slot)
	delete(latestCarUpdates, msg.CarID)
	conn := &ConnectionInfo{CarID: slot, DriverName: msg.DriverName, DriverGUID: string(msg.DriverGUID), CarModel: msg.CarModel, CarSkin: msg.CarSkin, DriverInitials: msg.DriverInitials, CarName: msg.CarName, ConnectedAt: time.Now(), ConnectionStatus: 1, TrackStatus: 3, TrackStatusLastChangedTime: time.Now()}
	globalConnections[slot] = conn
	initializeConnectionIdentity(conn)
	if sessionCollecting() && conn.UserID > 0 {
		globalDBWriter.InsertSessionFeed(&SessionFeed{SessionID: globalSession.ID, Type: 2, Detail: feedDetail(slot, nil), Time: time.Now()})
		if previous != nil && conn.TeamID > 0 && previous.UserID > 0 && previous.UserID != conn.UserID {
			data, _ := json.Marshal(map[string]interface{}{"team_id_1": conn.TeamID, "user_id_1": previous.UserID, "user_id_2": conn.UserID})
			globalDBWriter.InsertSessionFeed(&SessionFeed{SessionID: globalSession.ID, Type: 6, Detail: string(data), Time: time.Now()})
		}
	}
}

func handleChatMessage(msg udp.Chat, last *time.Time) {
	*last = time.Now()
	if !cfg.App.Live.SessionFeedShowChat || !sessionCollecting() {
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
	// 结束通知无论采集门是否已开都先记录：恢复阶段收到的 END 不能被丢弃，
	// 否则随后的恢复会把已结束的节当成新节继续采集。
	pendingSessionEnd = true
	pendingEndMessage = &msg
	if globalSession == nil || globalSession.ID == 0 {
		logger.Printf("收到结束通知但当前尚无有效会话，等待探活对齐: %v", msg)
		return
	}
	if err := completePendingSessionEnd(); err != nil {
		logger.Printf("结束会话失败，保留成绩文件与待处理圈重试: %v", err)
	}
}

// completePendingSessionEnd 重试同一次真实结束通知，先消化在线圈再按原文件回填。
// 回填按已保存圈去重，任一写入失败都不提前清掉结束通知。
func completePendingSessionEnd() error {
	if err := flushPendingLaps(); err != nil {
		return err
	}
	if pendingEndMessage != nil {
		data, err := readSessionDataFromJSON(filepath.Join(cfg.Game.Path, string(*pendingEndMessage)))
		if err != nil {
			// 官方结束通知足以收尾；辅助结果文件缺失或损坏不能永久阻塞下一节。
			logger.Printf("结束成绩文件不可用，已观测圈保留并继续收尾: %v", err)
		} else if err := importResultLaps(data); err != nil {
			return err
		}
	}
	return finishCurrentSession()
}

func importResultLaps(data *SessionData) error {
	var firstErr error
	seen := make(map[[4]int]int)
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
		if lap.Timestamp <= 0 {
			logger.Printf("成绩圈缺少 Timestamp，无法归属 stint，跳过(slot=%d, lap=%d)", slot, lapTime)
			continue
		}
		when := globalSession.StartTime.Add(time.Duration(lap.Timestamp) * time.Millisecond)
		// 按 session_stint 的 started_at/finished_at 归属，不用无法证实的 JSON StintID。
		stint := stintForResultLap(slot, userID, when)
		if stint == nil {
			logger.Printf("成绩圈无法唯一归属 stint，跳过(slot=%d, user=%d, when=%s, lap=%d)", slot, userID, when.Format(time.RFC3339Nano), lapTime)
			continue
		}
		key := [4]int{slot, userID, lapTime, stint.ID}
		seen[key]++
		exists := false
		if !globalDBWriter.SimulateSQL {
			var count int
			if err := globalDBWriter.DB.QueryRow("SELECT COUNT(*) FROM stint_lap WHERE stint_id=? AND time=?", stint.ID, lapTime).Scan(&count); err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("查询成绩圈失败: %w", err)
				}
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
		record := &StintLap{StintID: stint.ID, Sector1: lap.Sectors[0], Sector2: lap.Sectors[1], Sector3: lap.Sectors[2], Grip: globalSession.CurrentGrip, Tyre: lap.Tyre, Time: lapTime, Cuts: lap.Cuts, AvgSpeed: int(float64(getTrackLength()) * 3600 / float64(lapTime)), FinishedAt: when}
		updated := *stint
		updated.Laps++
		newBest := lap.Cuts == 0 && (stint.BestLapID == 0 || lapTime < bestLapTimes[stint.ID])
		if lap.Cuts == 0 {
			updated.ValidLaps++
			if newBest {
				updated.BestLapID = -1
			}
		}
		if _, err := globalDBWriter.PersistLap(record, &updated, false); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("补成绩圈失败: %w", err)
			}
			continue
		}
		*stint = updated
		if newBest {
			bestLapTimes[stint.ID] = lapTime
		}
	}
	return firstErr
}

// 初始化会话使用同一条路径，启动接入已有比赛与换节都可采集。
func handleNewSessionMessage(msg udp.SessionInfo, last *time.Time) {
	*last = time.Now()
	// 新到达的真实通知取代过时待办；内部重试不重新计算时间基准。
	pendingNewSession = &msg
	pendingNewSessionStartTime = last.Add(-time.Duration(msg.ElapsedMilliseconds) * time.Millisecond)
	retryPendingNewSession()
}

func retryPendingNewSession() {
	recoveryReady = false
	pending := *pendingNewSession
	if err := initializeSession(pending, true, pendingNewSessionStartTime); err != nil {
		logger.Printf("新会话初始化失败: %v", err)
		return
	}
	pendingNewSession = nil
	pendingNewSessionStartTime = time.Time{}
	// 新节由服务器主动通知；ini 和会话初始化成功后进入新节采集。
	recoveryReady = true
	activeSessionSignature = sessionInfoSignature(pending)
}

func updateEventFromSessionInfo(msg udp.SessionInfo) error {
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
	return nil
}

func initializeSession(msg udp.SessionInfo, newSession bool, startTime time.Time) error {
	// 旧节先收尾，再更换 event/赛道缓存；失败时仍保留原节的归属与长度。
	if newSession {
		if pendingSessionEnd {
			if err := completePendingSessionEnd(); err != nil {
				return err
			}
		} else if err := finishCurrentSession(); err != nil {
			return err
		}
	}
	if err := updateEventFromSessionInfo(msg); err != nil {
		return err
	}
	event := globalEvent
	// SESSION_INFO 表示当前会话；重复响应不能再次创建会话。
	if !newSession && globalSession != nil && globalSession.ID > 0 && globalSession.EventID == event.ID {
		return nil
	}
	var session *Session
	var err error
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
		session = &Session{EventID: event.ID, Type: databaseSessionType(msg), Name: msg.Name, TrackTime: "0", StartTime: startTime, DurationMin: int(msg.Time), ElapsedMs: int(msg.ElapsedMilliseconds), Laps: int(msg.Laps), Weather: msg.WeatherGraphics, AirTemp: float64(msg.AmbientTemp), RoadTemp: float64(msg.RoadTemp), StartGrip: -1, CurrentGrip: -1, LastActivity: time.Now(), HTTPPort: port}
		if _, err = globalDBWriter.InsertSession(session); err != nil {
			return err
		}
	}
	globalSession = session
	dataCollectionEnabled = true
	carLapDataMap = make(map[int][]CarLapData)
	stintLapData = make(map[int]map[int][]StintLap)
	recoveredScores = make(map[int]*recoveredScore)
	restoredStintIDs = make(map[int]bool)
	recoveredLogSessionID = 0
	userTyres = make(map[string]string)
	userStintIDs = make(map[string]int)
	carLapData = make(map[int][]udp.CarUpdate)
	lapTelemetryReady = make(map[int]bool)
	lastLapCompletions = make(map[int]lapCompletion)
	pitObservations = make(map[int]*pitObservation)
	activeStints = make(map[int]*SessionStint)
	latestCarUpdates = make(map[udp.CarID]udp.CarUpdate)
	bestLapTimes = make(map[int]int)
	classBestTimes = make(map[string]int)
	sessionStints = make([]*SessionStint, 0)
	lastFileSize, lastLineIdx, partialLine, globalNextSessionLineIdx = 0, 0, "", -1
	latestFile, _ = findLatestSessionLogFile(logDir)
	// 历史日志不属于本次采集；从接入时的文件末尾开始。
	if latestFile != nil {
		if info, err := latestFile.Info(); err == nil {
			lastFileSize = info.Size()
		}
		// 新节由服务器通知，旧日志快照不得在随后探活时再次回放。
		restoredSessionLogPath = filepath.Join(logDir, latestFile.Name())
		recoveredLogSessionID = session.ID
	}
	for _, conn := range globalConnections {
		conn.TrackStatus = 3
		conn.StintStartedAt = time.Time{}
		initializeConnectionIdentity(conn)
	}
	loadPitBoundary(msg.Track, msg.TrackConfig)
	logger.Printf("当前事件 %d，会话 %d: %s", event.ID, session.ID, session.Name)
	return nil
}

// sessionCollecting 只约束持久化；实时车辆状态可在结束后继续刷新。
// 调用者持有 stateMutex。
func sessionCollecting() bool {
	return recoveryReady && dataCollectionEnabled && !pendingSessionEnd && globalSession != nil && globalSession.ID > 0 && globalSession.IsFinished == 0
}

func finishCurrentSession() error {
	// 先提交待处理圈；失败则保留会话与队列，下次探活/后台重试继续，避免跨节重试。
	if err := flushPendingLaps(); err != nil {
		return fmt.Errorf("结束会话前提交待处理圈失败: %w", err)
	}
	if globalSession == nil || globalSession.ID == 0 || globalSession.IsFinished != 0 {
		pendingSessionEnd = false
		pendingEndMessage = nil
		return nil
	}
	for slot := range activeStints {
		if err := finishStint(slot); err != nil {
			return err
		}
	}
	finished := *globalSession
	finished.IsFinished = 1
	finished.FinishTime = time.Now()
	finished.LastActivity = finished.FinishTime
	finished.ElapsedMs = int(time.Since(finished.StartTime).Milliseconds())
	if err := globalDBWriter.UpdateSession(&finished); err != nil {
		logger.Printf("结束会话失败: %v", err)
		return err
	}
	*globalSession = finished
	pendingSessionEnd = false
	pendingEndMessage = nil
	dataCollectionEnabled = false
	lapDataMutex.Lock()
	carLapData = make(map[int][]udp.CarUpdate)
	lapTelemetryReady = make(map[int]bool)
	lapDataMutex.Unlock()
	return nil
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
	// 断开必须先把旧 stint 写库收尾；失败则保留原状态，不做“先删内存/先覆盖连接”。
	if err := finishStint(slot); err != nil {
		logger.Printf("断开时结束 stint 失败，保留连接状态等待重试(slot=%d): %v", slot, err)
		return
	}
	resetLapSamples(slot)
	delete(pitObservations, slot)
	if sessionCollecting() {
		globalDBWriter.InsertSessionFeed(&SessionFeed{SessionID: globalSession.ID, Type: 3, Detail: feedDetail(slot, nil), Time: time.Now()})
	}
	if conn := globalConnections[slot]; conn != nil {
		conn.ConnectionStatus = 2
	}
}

func handleCollisionWithCarMessage(msg udp.CollisionWithCar, last *time.Time) {
	*last = time.Now()
	if !sessionCollecting() {
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
	if !sessionCollecting() {
		return
	}
	globalDBWriter.InsertSessionFeed(&SessionFeed{SessionID: globalSession.ID, Type: 1, Detail: feedDetail(int(msg.CarID), map[string]interface{}{"speed": int(msg.ImpactSpeed), "nsp": latestCarUpdates[msg.CarID].NormalisedSplinePos}), Time: time.Now()})
}

// 车辆更新事件处理函数
func handleCarUpdateMessage(updateMsg udp.CarUpdate, lastDataTime *time.Time) {
	*lastDataTime = time.Now()
	if !recoveryReady || globalSession == nil || globalSession.ID == 0 {
		return
	}
	// 实时状态由外层 stateMutex 保护，和持久化遥测闸门无关。
	latestCarUpdates[updateMsg.CarID] = updateMsg
	if !sessionCollecting() {
		return
	}
	if time.Since(lastLogPoll) >= time.Second {
		lastLogPoll = time.Now()
		if err := collectCarUpdates(); err != nil {
			logger.Printf("读取增量日志失败，保留游标待重试: %v", err)
			return
		}
	}
	carID := int(updateMsg.CarID)
	collectLapSample(updateMsg)

	if len(pitBoundary) >= 3 {
		inPit := pointInPolygon([2]float32{updateMsg.Pos.X, updateMsg.Pos.Z}, pitBoundary)
		observePitVisit(carID, inPit, updateMsg, time.Now())
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
		pitBoundary = nil
		pitBoundaryLoadedFor = ""
		return
	}
	key := trackName + "|" + configName
	if pitBoundaryLoadedFor == key && len(pitBoundary) >= 3 {
		return
	}
	pitBoundary = nil
	pitBoundaryLoadedFor = ""
	boundary, err := kn5conv.BatchProcess(cfg.Game.Path, cfg.App.Server.CachePath, trackName, configName)
	if err != nil {
		logger.Printf("生成/读取维修区边界失败(%s)，沿用原换胎分段方式: %v", key, err)
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
	if pendingNewSession != nil {
		retryPendingNewSession()
		return
	}
	if pendingSessionEnd && globalSession != nil && globalSession.ID > 0 {
		// 结束通知尚未落地：先提交待处理圈再收尾；失败保留状态等下次探活。
		if err := completePendingSessionEnd(); err != nil {
			logger.Printf("重试结束会话失败: %v", err)
		}
		return
	}
	// 已采集且仍是同一节的探活响应：只刷新时间，不重跑恢复；
	// 增量日志的临时错误不能把整个 recoveryReady 关掉。
	if recoveryReady && globalSession != nil && globalSession.ID > 0 && sessionInfoSignature(msg) == activeSessionSignature {
		return
	}
	recoveryReady = false
	// 原 SESSION_INFO 只更新 event、恢复已有车辆，不创建 session 或清空采集状态。
	if err := updateEventFromSessionInfo(msg); err != nil {
		logger.Printf("更新会话事件失败: %v", err)
		return
	}
	// 沿用原查询路径接入已有会话，只启用解析，不 INSERT 新 session。
	if err := adoptCurrentSession(msg); err != nil {
		logger.Printf("接入已有会话失败: %v", err)
		return
	}
	if err := restoreConnectionsFromSessionLog(); err != nil {
		logger.Printf("恢复会话日志失败: %v", err)
		return
	}
	// 中途接入与新节一样需要地图/维修区边界，不能只在 NEW_SESSION 时加载。
	loadPitBoundary(msg.Track, msg.TrackConfig)
	activeSessionSignature = sessionInfoSignature(msg)
	recoveryReady = true
	// 恢复阶段就收到过结束通知：接入后立即按真实结束状态收尾，不进入新节采集。
	if pendingSessionEnd {
		if err := completePendingSessionEnd(); err != nil {
			logger.Printf("恢复阶段结束会话失败: %v", err)
		}
	}
}

// sessionInfoSignature 是会话的轻量标识（赛道|配置|名称|数据库类型），
// 用来判定探活响应是否仍是当前采集中的同一节。
func sessionInfoSignature(msg udp.SessionInfo) string {
	return strings.Join([]string{msg.Track, msg.TrackConfig, msg.Name, strconv.Itoa(databaseSessionType(msg))}, "|")
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
		// 探活/新节/结束通知是初始化与收尾入口；文件恢复前不处理车辆事件，
		// 但结束通知必须送达，否则恢复阶段收到的 END 会被丢弃。
		if !recoveryReady {
			switch msg.Event() {
			case udp.EventVersion, udp.EventSessionInfo, udp.EventNewSession, udp.EventEndSession, udp.EventError:
			default:
				return
			}
		}
		// 处理接收到的消息
		switch msg.Event() {
		case udp.EventVersion:
			if versionMsg, ok := msg.(udp.Version); ok {
				logger.Printf("AC服务器版本信息: %v", versionMsg)
				handleVersionMessage(versionMsg, lastDataTime)
				select {
				case versionInfoChan <- struct{}{}:
				default:
				}
			}
		case udp.EventNewSession:
			if sessionMsg, ok := msg.(udp.SessionInfo); ok {
				logger.Printf("新比赛节开始: %+v", sessionMsg)
				handleNewSessionMessage(sessionMsg, lastDataTime)
				select {
				case sessionInfoChan <- struct{}{}:
				default:
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
				select {
				case sessionInfoChan <- struct{}{}:
				default:
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

func collectCarUpdates() error {
	if !dataCollectionEnabled {
		return nil
	}
	lapDataMutex.Lock()
	defer lapDataMutex.Unlock()

	if latestFile != nil {
		//logger.Printf("最新日志文件: %s", latestFile.Name())
		// 读取日志文件内容
		filePath := filepath.Join(logDir, latestFile.Name())
		file, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("打开日志文件: %w", err)
		}
		defer file.Close()

		fileInfo, err := file.Stat()
		if err != nil {
			return fmt.Errorf("获取日志文件信息: %w", err)
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
			return fmt.Errorf("定位日志文件: %w", err)
		}

		// 读取新增内容
		newContent := make([]byte, currentFileSize-lastFileSize)
		if _, err := io.ReadFull(file, newContent); err != nil {
			return fmt.Errorf("读取日志新增内容: %w", err)
		}

		// partialLine 已包含上次读取但尚未消费的字节，失败时从该行真实起点重读。
		chunkOffset := lastFileSize - int64(len(partialLine))
		fullContent := partialLine + string(newContent)
		lines := strings.Split(fullContent, "\n")
		tail := lines[len(lines)-1]
		lines = lines[:len(lines)-1]
		consumed, err := processSessionLogLinesAt(lines, false, lastLineIdx)
		lastLineIdx += consumed
		if err != nil {
			lastFileSize = chunkOffset
			for _, line := range lines[:consumed] {
				lastFileSize += int64(len(line) + 1)
			}
			partialLine = ""
			return err
		}
		lastFileSize = currentFileSize
		lastFileModTime = currentFileModTime
		partialLine = tail
	}
	return nil
}

func waitWriterContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func main() {
	initLogger()
	defer writerLogFile.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
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
		logger.Printf("server_cfg.ini验证失败: %v", err)
		return
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

	// UDP 配置只在创建 goroutine 前设置，运行期状态由客户端实例管理。
	if cfg.ACServer.UDP.Realtime.Update.Interval.MS > 0 {
		udp.RealtimePosIntervalMs = cfg.ACServer.UDP.Realtime.Update.Interval.MS
	}
	var acClient *udp.AssettoServerUDP
	var wsDone <-chan struct{}
	var retryDone <-chan struct{}
	defer func() {
		stop()
		if acClient != nil {
			acClient.Close()
		}
		closeWebSocket()
		if retryDone != nil {
			<-retryDone
		}
		stateMutex.Lock()
		if pendingSessionEnd && globalSession != nil && globalSession.ID > 0 {
			if err := completePendingSessionEnd(); err != nil {
				logger.Printf("退出前会话收尾尚未完成: %v", err)
			}
		} else if err := flushPendingLaps(); err != nil {
			logger.Printf("退出前仍有 %d 个完圈待提交: %v", len(pendingLaps), err)
		}
		stateMutex.Unlock()
		if wsDone != nil {
			<-wsDone
		}
	}()
	var reconnectInterval = 3 * time.Second

	// 待处理圈的后台重试独立于 UDP 连接与探活节奏。
	retryDone = startPendingLapRetry(ctx)

	// 连接循环 - 支持重连
outerLoop:
	for {
		if ctx.Err() != nil {
			return
		}
		stateMutex.Lock()
		recoveryReady = false
		lastDataTime = time.Now()
		stateMutex.Unlock()
		stateMutex.RLock()
		hasSessionLog := latestFile != nil
		stateMutex.RUnlock()
		var err error
		// 创建用于SessionInfo通知的通道
		sessionInfoChan := make(chan struct{}, 1)
		versionInfoChan := make(chan struct{}, 1)
		acClient, err = createUDPClient(acHost, acLocalPort, acPort, &lastDataTime, sessionInfoChan, versionInfoChan)
		if err != nil {
			logger.Printf("创建UDP连接失败: %v, 将在%d秒后重试", err, reconnectInterval/time.Second)
			if !waitWriterContext(ctx, reconnectInterval) {
				return
			}
			continue
		}

		// 根据日志文件存在性决定是否等待version包
		if !hasSessionLog {
			logger.Println("等待AC服务器发送version包...")
			select {
			case <-ctx.Done():
				return
			case <-versionInfoChan:
				logger.Println("成功接收version包")
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
			if err := acClient.SendMessage(udp.NewGetSessionInfo()); err != nil {
				logger.Printf("请求当前会话失败: %v", err)
			}

			// 等待SessionInfo响应或超时
			select {
			case <-ctx.Done():
				return
			case <-sessionInfoChan:
				stateMutex.RLock()
				ready := recoveryReady && globalEvent != nil && globalEvent.ID > 0
				stateMutex.RUnlock()
				if !ready {
					retryCount++
					if retryCount >= 3 {
						goto sessionInfoTimeout
					}
					if !waitWriterContext(ctx, retryInterval) {
						return
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
				if !waitWriterContext(ctx, retryInterval) {
					return
				}
			}
		}

	sessionInfoTimeout:
		acClient.Close() // 显式关闭UDP连接
		continue outerLoop

	sessionInfoReceived:
		logger.Println("已获取SessionInfo，开始主循环")

		if err := acClient.SendMessage(udp.NewEnableRealtimePosInterval(udp.RealtimePosIntervalMs)); err != nil {
			logger.Printf("开启车辆更新失败: %v", err)
		}
		// WebSocket 生命周期独立于 UDP 重连，只启动一个读取/重连协程。
		if wsDone == nil {
			wsDone = startWebSocketReconnect(ctx)
		}
		interval := cfg.Writer.HTTP.Leaderboard.Broadcast.Interval.MS
		if interval <= 0 {
			interval = 2000
		}
		broadcast := time.NewTicker(time.Duration(interval) * time.Millisecond)
		heartbeat := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ctx.Done():
				broadcast.Stop()
				heartbeat.Stop()
				return
			case <-broadcast.C:
				buildAndSendWebSocketMessage()
			case <-heartbeat.C:
				stateMutex.Lock()
				stale := time.Since(lastDataTime) > 30*time.Second
				if stale {
					recoveryReady = false
					// 失联间隔不构成完整圈，重连后重新等一次完圈。
					for slot := range globalConnections {
						resetLapSamples(slot)
					}
				}
				stateMutex.Unlock()
				if stale {
					logger.Println("UDP超过30秒未收报，重新连接并恢复当前会话")
					broadcast.Stop()
					heartbeat.Stop()
					acClient.Close()
					continue outerLoop
				}
				// 同一恢复入口也重试临时数据库/日志失败，不跳过恢复步骤。
				if err := acClient.SendMessage(udp.NewGetSessionInfo()); err != nil {
					logger.Printf("会话探活请求失败: %v", err)
				}
			}
		}
	}
}
