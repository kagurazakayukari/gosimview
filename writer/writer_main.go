// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
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
	latestCarUpdates    map[int64]udp.CarUpdate
)

// 定义结构体
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
}

// Session 会话结构体
type Session struct {
	ID          int64
	EventID     int
	Type        int
	Name        string
	TrackTime   string
	StartTime   time.Time
	DurationMin int
	Weather     string
	AirTemp     int
	RoadTemp    int
	StartGrip   float64
	CurrentGrip float64
	HTTPPort    int
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
	DriverName string
	TeamName   string
	CarModel   string
	Number     int
	Ballast    int
	Restrictor int
}

// SessionStint 会话阶段结构体
type SessionStint struct {
	SessionStintID int64
	SessionID      int64
	TeamMemberID   int64
	UserID         int64
	CarID          int64
	StartTime      time.Time
	EndTime        time.Time
}

// StintLap 阶段圈速结构体
type StintLap struct {
	StintLapID int64
	StintID    int64
	Time       int64
	Sector1    int64
	Sector2    int64
	Sector3    int64
	Grip       int64
	Tyre       string
	AvgSpeed   int64
	MaxSpeed   int64
	Cuts       int64
	Crashes    int64
	CarCrashes int64
	FinishedAt time.Time
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

// 创建UDP客户端
func createUDPClient(host string, localPort, serverPort int, lastDataTime *time.Time, sessionInfoChan chan<- struct{}, versionInfoChan chan<- struct{}) (*udp.AssettoServerUDP, error) {
	// 使用正确的函数名和参数创建UDP客户端
	client, err := udp.NewServerClient("", localPort, serverPort, false, "", 0, func(msg udp.Message) {
		// 处理接收到的消息
		switch msg := msg.(type) {
		case udp.SessionInfo:
			logger.Printf("接收到SessionInfo: %+v", msg)
			receivedSessionInfo = true
			// 处理会话信息...
			sessionInfoChan <- struct{}{}
		case udp.Version:
			logger.Printf("接收到Version: %+v", msg)
			receivedVersionInfo = true
			// 处理版本信息...
			versionInfoChan <- struct{}{}
		}
	})
	if err != nil {
		return nil, err
	}

	return client, nil
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
		defer acClient.Close()

		// 根据日志文件存在性决定是否等待version包
		if latestFile == nil {
			logger.Println("等待AC服务器发送version包...")
			select {
			case <-versionInfoChan:
				logger.Println("成功接收version包")
				receivedVersionInfo = true
			}
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
