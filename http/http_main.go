// This file is part of SimView Extension
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gosimview/config"
	"gosimview/udp"

	"github.com/gorilla/websocket"

	_ "github.com/go-sql-driver/mysql"
)

type TimeWithTimezone int64

func (t TimeWithTimezone) MarshalJSON() ([]byte, error) {
	// 获取本地时区偏移（秒）
	_, offset := time.Now().Zone()
	// 转换为微秒并添加到时间戳
	adjustedTimestamp := int64(t) + int64(offset)*1000000
	return json.Marshal(adjustedTimestamp)
}

var fs http.Handler // 静态文件服务器实例

func (s ACSession) MarshalJSON() ([]byte, error) {
	type Alias ACSession
	var typeStr string
	switch s.Type {
	case 0:
		typeStr = "Practice"
	case 1:
		typeStr = "Qualifying"
	case 2:
		typeStr = "Race"
	default:
		typeStr = "Unknown"
	}
	return json.Marshal(&struct {
		StartTime    TimeWithTimezone `json:"start_time"`
		FinishTime   TimeWithTimezone `json:"finish_time"`
		LastActivity TimeWithTimezone `json:"last_activity"`
		Type         string           `json:"type"`
		*Alias
	}{
		StartTime:    s.StartTime,
		FinishTime:   s.FinishTime,
		LastActivity: s.LastActivity,
		Type:         typeStr,
		Alias:        (*Alias)(&s),
	})
}

// 统一错误响应工具函数
func sendErrorResponse(w http.ResponseWriter, statusCode int, message string, dataField string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	response := map[string]interface{}{
		"status":  "error",
		"message": message,
	}
	if dataField != "" {
		response[dataField] = []interface{}{}
	}
	jsonData, err := json.Marshal(response)
	if err != nil {
		logger.Printf("编码错误响应失败: %v", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(jsonData); err != nil {
		logger.Printf("写入错误响应失败: %v", err)
	}
}

// 统一成功响应工具函数
func sendSuccessResponse(w http.ResponseWriter, message string, dataField string, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{
		"status":  "success",
		"message": message,
	}
	if dataField != "" {
		if data == nil {
			response[dataField] = []interface{}{}
		} else {
			response[dataField] = data
		}
	}
	// 日志响应数据用于调试
	jsonData, err := json.Marshal(response)
	if err != nil {
		logger.Printf("编码响应失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "数据序列化失败", "")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(jsonData); err != nil {
		logger.Printf("编码成功响应失败: %v", err)
	}
}

// 方法验证中间件(集成日志功能)
func methodMiddleware(next http.HandlerFunc, allowedMethods ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 初始化响应记录器和计时器
		rec := &responseRecorder{w, http.StatusOK}

		// 方法验证逻辑
		allowed := false
		for _, method := range allowedMethods {
			if r.Method == method {
				allowed = true
				break
			}
		}

		// 处理不允许的请求
		if !allowed {
			sendErrorResponse(rec, http.StatusMethodNotAllowed, "方法不允许", "")
		} else {
			// 处理正常请求
			next.ServeHTTP(rec, r)
		}
	}
}

// 响应记录器用于捕获状态码
type responseRecorder struct {
	http.ResponseWriter
	statusCode int
}

var db *sql.DB
var (
	logger      *log.Logger
	fatalLogger *log.Logger
)

func (rec *responseRecorder) WriteHeader(code int) {
	rec.statusCode = code
	rec.ResponseWriter.WriteHeader(code)
}

// 全局配置变量
var cfg *config.Config // 全局配置实例

func main() {
	// 初始化日志系统
	initLogger()
	logger.Println("应用程序启动")

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
	// 连接数据库
	if err := connectDB(cfg); err != nil {
		fatalLogger.Fatalf("数据库连接失败: %v", err)
	}
	defer db.Close()

	// 设置HTTP路由
	setupRoutes()

	// 启动HTTP服务器
	// 从配置读取HTTP端口
	port := cfg.App.Server.Port
	logger.Printf("HTTP服务器启动在: %d", port)

	// 从配置读取WebSocket端口
	wsPort := cfg.ACServer.UDP.LocalPort

	wsMux := http.NewServeMux()
	wsMux.HandleFunc("/live", handleACEventWebSocket)
	go func() {
		logger.Printf("WebSocket服务器启动在: %d", wsPort)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", wsPort), wsMux); err != nil {
			logger.Printf("WebSocket服务器启动失败: %v", err)
		}
	}()

	// 启动主HTTP服务器
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
		fatalLogger.Fatalf("服务器启动失败: %v", err)
	}
}

// 初始化日志系统
func initLogger() {
	logDir := "logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		fatalLogger.Fatalf("创建日志目录失败: %v", err)
	}

	logFile, err := os.OpenFile(
		filepath.Join(logDir, fmt.Sprintf("simview-http-%s.log", time.Now().Format("2006-01-02-15-04-05"))),
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		0644,
	)
	if err != nil {
		fatalLogger.Fatalf("打开日志文件失败: %v", err)
	}

	// 同时输出日志到文件和终端
	logger = log.New(io.MultiWriter(logFile, os.Stdout), "[SIMVIEW] ", log.Ldate|log.Ltime)
	fatalLogger = log.New(io.MultiWriter(logFile, os.Stdout), "[SIMVIEW] ", log.Ldate|log.Ltime|log.Lshortfile)
}

// 连接到MySQL数据库
func connectDB(config *config.Config) error {
	port := config.Database.Port
	if port == 0 {
		port = 3306
	}
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?parseTime=true&timeout=30s",
		config.Database.User,
		config.Database.Password,
		config.Database.Host,
		port,
		func() string {
			if config.Database.Schema == "" {
				return "simview"
			}
			return config.Database.Schema
		}(),
	)

	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("无法打开数据库连接: %v", err)
	}

	// 验证连接
	if err := db.Ping(); err != nil {
		return fmt.Errorf("无法 ping 数据库: %v", err)
	}

	logger.Println("数据库连接成功")
	return nil
}

// 设置HTTP路由
// serveFileWithGzip 统一处理静态文件，支持自动处理.gz压缩文件
func serveFileWithGzip(w http.ResponseWriter, _ *http.Request, fileName string) {
	// 构建文件路径
	basePath := filepath.Join("html", fileName)
	gzPath := basePath + ".gz"
	var filePath string
	var isGzip bool

	// 优先检查gzip版本
	if _, err := os.Stat(gzPath); err == nil {
		filePath = gzPath
		isGzip = true
	} else if _, err := os.Stat(basePath); err == nil {
		filePath = basePath
		isGzip = false
	} else {
		// 文件不存在
		sendErrorResponse(w, http.StatusNotFound, "File not found", "staticFile")
		return
	}

	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		sendErrorResponse(w, http.StatusNotFound, "File not found", "staticFile")
		return
	}
	defer file.Close()

	// 确定原始文件扩展名以设置正确的Content-Type
	var originalExt string
	if isGzip {
		originalExt = filepath.Ext(strings.TrimSuffix(fileName, ".gz"))
	} else {
		originalExt = filepath.Ext(fileName)
	}

	// 设置适当的Content-Type
	switch originalExt {
	case ".html":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	case ".js":
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	case ".css":
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
	case ".png":
		w.Header().Set("Content-Type", "image/png")
	case ".jpg", ".jpeg":
		w.Header().Set("Content-Type", "image/jpeg")
	case ".svg":
		w.Header().Set("Content-Type", "image/svg+xml")
	}

	// 处理gzip压缩
	if isGzip {
		w.Header().Set("Content-Encoding", "gzip")
	}

	io.Copy(w, file)
}

// 根路径处理器
func rootHandler(w http.ResponseWriter, r *http.Request) {
	// 处理根路径和支持的文件类型
	isRootPath := r.URL.Path == "/"
	ext := strings.ToLower(filepath.Ext(r.URL.Path))
	supportedExts := map[string]bool{
		".html": true,
		".js":   true,
		".css":  true,
		".png":  true,
		".jpg":  true,
		".jpeg": true,
		".svg":  true,
	}

	if r.URL.Path == "/events" {
		serveFileWithGzip(w, r, "index.html")
		return
	}

	if r.URL.Path == "/driver" {
		serveFileWithGzip(w, r, "driver.html")
		return
	}

	if r.URL.Path == "/result" {
		// 获取eventId cookie
		cookie, err := r.Cookie("eventId")
		if err != nil {
			// 重定向到事件列表页要求选择事件
			http.Redirect(w, r, "/events", http.StatusSeeOther)
			return
		}
		// 重定向到带eventId的URL路径
		http.Redirect(w, r, "/ac/event/"+cookie.Value+"/result", http.StatusSeeOther)
		return
	}

	if r.URL.Path == "/bestlap" {
		serveFileWithGzip(w, r, "bestlap.html")
		return
	}

	if regexp.MustCompile(`^/ac/event/([0-9]+)/live$`).MatchString(r.URL.Path) {
		serveFileWithGzip(w, r, "live.html")
		return
	}

	if isRootPath {
		// 根路径指向index.html
		serveFileWithGzip(w, r, "index.html")
		return
	} else if ext == ".html" {
		// HTML文件使用统一处理
		relativePath := strings.TrimPrefix(r.URL.Path, "/")
		serveFileWithGzip(w, r, relativePath)
		return
	} else if ext == ".js" || ext == ".css" {
		// JavaScript和CSS文件使用serveFileWithGzip处理，确保正确设置Content-Type和处理压缩
		relativePath := strings.TrimPrefix(r.URL.Path, "/")
		serveFileWithGzip(w, r, relativePath)
		return
	} else if supportedExts[ext] || ext == "" {
		// 其他静态文件由文件服务器处理
		fs.ServeHTTP(w, r)
		return
	}

	// 非HTML文件由文件服务器处理
	fs.ServeHTTP(w, r)
}

// 分析页面处理器
func analysisHandler(w http.ResponseWriter, r *http.Request) {
	// 解析URL路径
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")

	// 处理 /analysis/lap/{id} 格式
	if len(parts) >= 2 && parts[1] == "lap" && len(parts) >= 3 && parts[2] != "" {
		lapID := parts[2]
		if _, err := strconv.Atoi(lapID); err == nil {
			http.SetCookie(w, &http.Cookie{
				Name:  "lapId",
				Value: lapID,
				Path:  "/",
			})
			serveFileWithGzip(w, r, "analysis.html")
			return
		}
		logger.Printf("无效的lap ID: %s", lapID)
		sendErrorResponse(w, http.StatusBadRequest, "无效的lap ID", "lapID")
		return
	}

	// 处理 /analysis/compare/lap1/{id1}/lap2/{id2} 格式
	if len(parts) >= 6 && parts[1] == "compare" && parts[2] == "lap1" && parts[4] == "lap2" && parts[3] != "" && parts[5] != "" {
		lap1ID := parts[3]
		lap2ID := parts[5]
		if _, err1 := strconv.Atoi(lap1ID); err1 == nil {
			if _, err2 := strconv.Atoi(lap2ID); err2 == nil {
				http.SetCookie(w, &http.Cookie{
					Name:  "lap1Id",
					Value: lap1ID,
					Path:  "/",
				})
				http.SetCookie(w, &http.Cookie{
					Name:  "lap2Id",
					Value: lap2ID,
					Path:  "/",
				})
			}
			serveFileWithGzip(w, r, "analysis.html")
			return
		}
		logger.Printf("无效的lap ID: lap1=%s, lap2=%s", lap1ID, lap2ID)
		sendErrorResponse(w, http.StatusBadRequest, "无效的lap ID", "lapID")
		return
	}

	if r.URL.Path == "/analysis/" {
		serveFileWithGzip(w, r, "analysis.html")
		return
	}
	// 未找到匹配的路径
	http.NotFound(w, r)
}

// AC事件页面处理器
func acEventHandler(w http.ResponseWriter, r *http.Request) {
	// 解析URL路径获取事件ID
	parts := strings.Split(r.URL.Path, "/")
	// 路径格式应为: ac/event/{event_id}/result
	if len(parts) >= 5 && parts[4] == "result" && len(parts[3]) > 0 {
		eventID := parts[3]
		if _, err := strconv.Atoi(eventID); err == nil {
			http.SetCookie(w, &http.Cookie{
				Name:  "eventId",
				Value: eventID,
				Path:  "/",
			})
			serveFileWithGzip(w, r, "result.html")
			return
		}
		logger.Printf("无效的事件ID: %s", eventID)
		sendErrorResponse(w, http.StatusBadRequest, "无效的事件ID", "eventID")
		return
	}

	if len(parts) >= 5 && parts[4] == "live" && len(parts[3]) > 0 {
		eventId := parts[3]
		// 验证eventId是数字
		if _, err := strconv.Atoi(eventId); err == nil {
			http.SetCookie(w, &http.Cookie{
				Name:  "eventId",
				Value: eventId,
				Path:  "/",
			})
			serveFileWithGzip(w, r, "live.html")
			return
		}
		logger.Printf("无效的事件ID: %s", eventId)
		sendErrorResponse(w, http.StatusBadRequest, "无效的事件ID", "eventID")
		return
	}
	// 未找到匹配的路径
	http.NotFound(w, r)
}

func setupRoutes() {
	// 静态文件服务
	fs = http.FileServer(http.Dir("html"))
	// 创建自定义处理器处理所有HTML文件和静态资源
	http.HandleFunc("/", methodMiddleware(rootHandler, "GET"))
	http.HandleFunc("/analysis/", methodMiddleware(analysisHandler, "GET"))
	http.HandleFunc("/ac/event/", methodMiddleware(acEventHandler, "GET"))
	http.HandleFunc("/api/ac/events", methodMiddleware(handleACEvents, "GET"))
	http.HandleFunc("/api/ac/events/", methodMiddleware(handleACEvents, "GET"))
	http.HandleFunc("/api/ac/events/live", methodMiddleware(handleACEventsLive, "GET"))
	http.HandleFunc("/api/ac/teams/", methodMiddleware(handleACTeams, "GET"))
	http.HandleFunc("/api/ac/car/", methodMiddleware(handleACCars, "GET"))
	http.HandleFunc("/api/ac/cars", methodMiddleware(handleACCars, "GET"))
	http.HandleFunc("/api/ac/cars/", methodMiddleware(handleACCars, "GET"))
	http.HandleFunc("/api/ac/users", methodMiddleware(handleACUsers, "GET"))
	http.HandleFunc("/api/ac/users/", methodMiddleware(handleACUsers, "GET"))
	http.HandleFunc("/api/ac/user", methodMiddleware(handleACUserDetails, "GET"))
	http.HandleFunc("/api/ac/user/", methodMiddleware(handleACUserDetails, "GET"))
	http.HandleFunc("/api/ac/team/", methodMiddleware(handleACTeamMembers, "GET"))
	http.HandleFunc("/api/ac/track/", methodMiddleware(handleACTrack, "GET"))
	http.HandleFunc("/api/ac/lap/summary/", methodMiddleware(handleACLapSummary, "GET"))
	http.HandleFunc("/api/ac/lap/telemetry/", methodMiddleware(handleACLapTelemetry, "GET"))
	http.HandleFunc("/api/ac/session/", methodMiddleware(handleACSession, "GET"))
	http.HandleFunc("/api/ac/event", methodMiddleware(handleACEventDetails, "GET"))
	http.HandleFunc("/api/ac/event/", methodMiddleware(handleACEventDetails, "GET"))
	http.HandleFunc("/api/ac/tracks", methodMiddleware(handleACTracks, "GET"))
	http.HandleFunc("/api/ac/bestlap/", methodMiddleware(handleACBestLap, "GET"))
	http.HandleFunc("/images/ac/track/", handleTrackResource)
	http.HandleFunc("/images/ac/car/", handleCarResource)
	http.HandleFunc("/live", handleACEventWebSocket)

	logger.Println("路由配置完成")
}

// 最佳圈速数据结构
type ACBestLap struct {
	LapID            int              `json:"lap_id"`
	User_id          int              `json:"user_id"`
	Car_id           int              `json:"car_id"`
	Time             float64          `json:"time"`
	Gap              int              `json:"gap,omitempty"`
	GapPer           float64          `json:"gap_per,omitempty"`
	Sector1          int              `json:"sector_1"`
	Sector2          int              `json:"sector_2"`
	Sector3          int              `json:"sector_3"`
	Grip             int              `json:"grip"`
	Tyre             string           `json:"tyre"`
	MaxSpeed         int              `json:"max_speed"`
	CarBest          int              `json:"car_best,omitempty"`
	ClassBest        int              `json:"class_best,omitempty"`
	Sector1CarBest   int              `json:"sector_1_car_best,omitempty"`
	Sector2CarBest   int              `json:"sector_2_car_best,omitempty"`
	Sector3CarBest   int              `json:"sector_3_car_best,omitempty"`
	Sector1ClassBest int              `json:"sector_1_class_best,omitempty"`
	Sector2ClassBest int              `json:"sector_2_class_best,omitempty"`
	Sector3ClassBest int              `json:"sector_3_class_best,omitempty"`
	FinishedAt       TimeWithTimezone `json:"finished_at"`
	EventID          int              `json:"event_id,omitempty"`
}

// 数据库表对应的数据结构定义
// 赛道配置
type ACTrackConfig struct {
	TrackConfigID int    `json:"track_config_id"`
	TrackName     string `json:"track_name"`
	ConfigName    string `json:"config_name"`
	DisplayName   string `json:"display_name"`
	Country       string `json:"country"`
	City          string `json:"city"`
	Length        int    `json:"length"`
}

// 赛事事件
type ACEvent struct {
	EventID              int    `json:"event_id"`
	ServerName           string `json:"server_name"`
	TrackConfigID        int    `json:"track_config_id"`
	Name                 string `json:"name"`
	TeamEvent            int    `json:"team_event"`
	Active               int    `json:"active"`
	PracticeDuration     int    `json:"practice_duration"`
	QualiDuration        int    `json:"quali_duration"`
	RaceDuration         int    `json:"race_duration"`
	RaceDurationType     int    `json:"race_duration_type"`
	RaceExtraLaps        int    `json:"race_extra_laps"`
	ReverseGridPositions int    `json:"reverse_grid_positions"`
}

// 团队成员
type ACTeamMember struct {
	TeamMemberID int    `json:"team_member_id"`
	TeamID       int    `json:"team_id"`
	UserID       int    `json:"user_id"`
	Role         string `json:"role"`
}

// 团队数据结构定义
type ACTeam struct {
	TeamID     int    `json:"team_id"`
	Name       string `json:"name"`
	Country    string `json:"country,omitempty"`
	CarID      int    `json:"car_id,omitempty"`
	TeamNo     int    `json:"team_no,omitempty"`
	LiveryName string `json:"livery_name,omitempty"`
}

// 车辆数据结构定义
type ACCar struct {
	CarID        int    `json:"car_id"`
	Name         string `json:"name"`
	DisplayName  string `json:"display_name"`
	Manufacturer string `json:"manufacturer"`
	CarClass     string `json:"car_class"`
	Model        string `json:"model"`
	TeamID       int    `json:"team_id,omitempty"`
}

// 用户数据结构定义
type ACUser struct {
	UserID   int    `json:"user_id"`
	Name     string `json:"name"`
	PrevName string `json:"prev_name"`
	Country  string `json:"country"`
}

// 会话数据结构定义
type ACSession struct {
	SessionID    int64            `json:"session_id"`
	EventID      int              `json:"event_id"`
	Type         int              `json:"type"`
	TrackTime    string           `json:"track_time"`
	Name         string           `json:"name"`
	StartTime    TimeWithTimezone `json:"start_time"`
	DurationMin  int              `json:"duration_min"`
	ElapsedMs    int64            `json:"elapsed_ms"`
	Laps         int              `json:"laps"`
	Weather      string           `json:"weather"`
	AirTemp      int              `json:"air_temp"`
	RoadTemp     int              `json:"road_temp"`
	StartGrip    float64          `json:"start_grip"`
	CurrentGrip  float64          `json:"current_grip"`
	IsFinished   int              `json:"is_finished"`
	FinishTime   TimeWithTimezone `json:"finish_time"`
	LastActivity TimeWithTimezone `json:"last_activity"`
	HttpPort     int              `json:"http_port"`
}

type ACLiveData struct {
	SessionID string `json:"session_id"`
	Timestamp string `json:"timestamp"`
	Status    string `json:"status"`
}

type ACLivePosition struct {
	DriverID   string  `json:"driver_id"`
	DriverName string  `json:"driver_name"`
	Position   int     `json:"position"`
	X          float64 `json:"x"`
	Y          float64 `json:"y"`
	Z          float64 `json:"z"`
}

type ACResult struct {
	SessionID     string `json:"session_id"`
	WinnerDriver  string `json:"winner_driver"`
	WinnerTeam    string `json:"winner_team"`
	LapsCompleted int    `json:"laps_completed"`
	FastestLap    string `json:"fastest_lap"`
}

type ACStint struct {
	ID        string `json:"id"`
	DriverID  string `json:"driver_id"`
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time"`
	Duration  string `json:"duration"`
}

type ACDriverStint struct {
	ID         string `json:"id"`
	DriverName string `json:"driver_name"`
	StartTime  string `json:"start_time"`
	EndTime    string `json:"end_time"`
	Duration   string `json:"duration"`
}

type Lap struct {
	LapID      int              `json:"lap_id"`
	LapTime    int64            `json:"lap_time"`
	Sector1    int64            `json:"sector_1"`
	Sector2    int64            `json:"sector_2"`
	Sector3    int64            `json:"sector_3"`
	Grip       int              `json:"grip"`
	Tyre       string           `json:"tyre"`
	AvgSpeed   int              `json:"avg_speed"`
	MaxSpeed   int              `json:"max_speed"`
	Cuts       int              `json:"cuts"`
	Crashes    int              `json:"crashes"`
	CarCrashes int              `json:"car_crashes"`
	FinishAt   TimeWithTimezone `json:"finish_at"`
	BestLap    int              `json:"best_lap,omitempty"`
}

type ACTeamStint struct {
	UserID      int   `json:"user_id"`
	TotalLaps   int   `json:"total_laps"`
	ValidLaps   int   `json:"valid_laps"`
	BestLapTime int64 `json:"best_lap_time"`
	AvgLapTime  int64 `json:"avg_lap_time"`
	AvgLapGap   int64 `json:"avg_lap_gap"`
	Laps        []Lap `json:"laps"`
}

type ACUserCarStint struct {
	ID        string `json:"id"`
	UserName  string `json:"user_name"`
	CarModel  string `json:"car_model"`
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time"`
	Duration  string `json:"duration"`
}

type ACStanding struct {
	SessionStintID int `json:"session_stint_id,omitempty"`
	TeamID         int `json:"team_id,omitempty"`
	UserID         int `json:"user_id,omitempty"`
	CarID          int `json:"car_id"`
	LapID          int `json:"lap_id,omitempty"`
	Laps           int `json:"laps,omitempty"`
	ValidLaps      int `json:"valid_laps"`
	BestLapTime    int `json:"best_lap_time,omitempty"`
	Sector1        int `json:"sector_1,omitempty"`
	Sector2        int `json:"sector_2,omitempty"`
	Sector3        int `json:"sector_3,omitempty"`
	TotalTime      int `json:"total_time,omitempty"`
	Gap            int `json:"gap,omitempty"`
	Interval       int `json:"interval,omitempty"`
}

// 从数据库获取赛事数据
type Entry struct {
	SessionStintID uint32
	TeamID         uint32
	UserID         uint32
	CarID          uint16
	GameCarID      int
	DriverName     string
	TeamName       string
	StatusMask     uint8
	Laps           uint16
	ValidLaps      uint16
	NSP            float32
	PosX           float32
	PosZ           float32
	TelemetryMask  uint16
	RPM            uint16
	TyreLength     uint8
	Tyre           string
	BestLapS1      uint32
	BestLapS2      uint32
	BestLapS3      uint32
	BestLapTime    uint32
	CurrentLapS1   uint32
	CurrentLapS2   uint32
	CurrentLapS3   uint32
	SectorMask     uint8
	LastLapTime    int32
	Gap            int32
	Interval       int32
	PosChange      int8
}

type LapInfo struct {
	StintLapID int64            `json:"stint_lap_id"`
	StintID    int64            `json:"stint_id"`
	Sector1    int64            `json:"sector_1"`
	Sector2    int64            `json:"sector_2"`
	Sector3    int64            `json:"sector_3"`
	Grip       int64            `json:"grip"`
	Tyre       string           `json:"tyre"`
	Time       int64            `json:"time"`
	Cuts       int64            `json:"cuts"`
	Crashes    int64            `json:"crashes"`
	CarCrashes int64            `json:"car_crashes"`
	MaxSpeed   int64            `json:"max_speed"`
	AvgSpeed   int64            `json:"avg_speed"`
	FinishedAt TimeWithTimezone `json:"finished_at"`
}

type Summary struct {
	EventID       int64   `json:"event_id"`
	EventName     string  `json:"event_name"`
	TrackName     string  `json:"track_name"`
	TrackLength   int64   `json:"track_length"`
	TrackConfigID int64   `json:"track_config_id"`
	UserID        int64   `json:"user_id"`
	CarID         int64   `json:"car_id"`
	SessionType   string  `json:"session_type"`
	AirTemp       string  `json:"air_temp"`
	RoadTemp      string  `json:"road_temp"`
	Weather       string  `json:"weather"`
	Lap           LapInfo `json:"lap"`
}

// 事件详情响应结构体
type ACEventResponse struct {
	EventID              int    `json:"event_id"`
	Name                 string `json:"name"`
	ServerName           string `json:"server_name"`
	TrackConfigID        int    `json:"track_config_id"`
	TeamEvent            int    `json:"team_event"`
	Active               int    `json:"active"`
	LiveryPreview        int    `json:"livery_preview"`
	UseNumber            int    `json:"use_number"`
	PracticeDuration     int    `json:"practice_duration"`
	QualiDuration        int    `json:"quali_duration"`
	RaceDuration         int    `json:"race_duration"`
	RaceDurationType     int    `json:"race_duration_type"`
	RaceWaitTime         int    `json:"race_wait_time"`
	RaceExtraLaps        int    `json:"race_extra_laps"`
	ReverseGridPositions int    `json:"reverse_grid_positions"`
}

type SectorResult struct {
	UserID         int  `json:"user_id"`
	CarID          int  `json:"car_id"`
	BestSectorTime int  `json:"best_sector_time"`
	Gap            *int `json:"gap,omitempty"`
	Interval       *int `json:"interval,omitempty"`
}

type SectorResponse struct {
	Sector1 []SectorResult `json:"sector1"`
	Sector2 []SectorResult `json:"sector2"`
	Sector3 []SectorResult `json:"sector3"`
}

type StintInfo struct {
	UserID     int64
	CarID      int64
	TeamID     sql.NullInt64
	StartedAt  TimeWithTimezone
	FinishedAt TimeWithTimezone
}

type DriverEvent struct {
	EventID          int              `json:"event_id"`
	EventName        string           `json:"event_name"`
	TrackName        string           `json:"track_name"`
	TeamName         string           `json:"team_name"`
	DistanceDrivenKm float64          `json:"distance_driven_km"`
	TotalLaps        int              `json:"total_laps"`
	TotalValidLaps   int              `json:"total_valid_laps"`
	TimeAgoSec       int64            `json:"time_ago_sec"`
	EventTime        TimeWithTimezone `json:"event_time"` // 事件时间戳(秒级)
}

type TrackSummary struct {
	TrackName      string  `json:"track_name"`
	DistanceDriven float64 `json:"distance_driven"`
}

type CarSummary struct {
	CarName        string  `json:"car_name"`
	DistanceDriven float64 `json:"distance_driven"`
}

type DriverSummary struct {
	UserID                int            `json:"user_id"`
	TotalEvents           int            `json:"total_events"`
	TotalDistanceDrivenKm float64        `json:"total_distance_driven_km"`
	TotalLaps             int            `json:"total_laps"`
	TotalValidLaps        int            `json:"total_valid_laps"`
	Events                []DriverEvent  `json:"events"`
	TopTracks             []TrackSummary `json:"top_tracks"`
	TopCars               []CarSummary   `json:"top_cars"`
}

type TeamLapTimes struct {
	TeamID   int   `json:"team_id"`
	CarID    int   `json:"car_id"`
	LapTimes []int `json:"lap_times"`
}
type UserLapTimes struct {
	UserID   int   `json:"user_id"`
	CarID    int   `json:"car_id"`
	LapTimes []int `json:"lap_times"`
}

type rankedTeamLapData struct {
	TeamLapTimes
	Stability   float64
	bestLapTime int
}

type rankedLapData struct {
	UserLapTimes
	Stability   float64
	bestLapTime int
}

type Pitstop struct {
	UserID  *int64 `json:"user_id,omitempty"`
	TeamID  *int64 `json:"team_id,omitempty"`
	CarID   int64  `json:"car_id"`
	PitTime int64  `json:"pit_time"`
}

// WebSocket处理器，发送符合前端LeaderBoardDeserialiser v10格式的二进制数据
var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	wsConnectionCount int32
	carUpdates        = make(map[int64]udp.CarUpdate)
	carUpdatesMutex   sync.Mutex
)

// 写入二进制数据并统一处理错误
func writeBinary(buf *bytes.Buffer, order binary.ByteOrder, data interface{}, field string) error {
	if err := binary.Write(buf, order, data); err != nil {
		return fmt.Errorf("写入%s失败: %v", field, err)
	}
	return nil
}

// 写入条目数据到缓冲区
func writeEntriesToBuffer(entries []Entry, buf *bytes.Buffer) error {
	for _, e := range entries {
		// 逐个字段写入基础数据（每个writeBinary单独成行）
		if err := writeBinary(buf, binary.LittleEndian, e.TeamID, "TeamID"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.UserID, "UserID"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.CarID, "CarID"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.StatusMask, "StatusMask"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.Laps, "Laps"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.ValidLaps, "ValidLaps"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.NSP, "NSP"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.PosX, "PosX"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.PosZ, "PosZ"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.TelemetryMask, "TelemetryMask"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.RPM, "RPM"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.TyreLength, "TyreLength"); err != nil {
			return err
		}

		// 写入Tyre字符串
		buf.Write([]byte(e.Tyre))

		// 逐个字段写入圈速数据（每个writeBinary单独成行）
		if err := writeBinary(buf, binary.LittleEndian, e.BestLapS1, "BestLapS1"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.BestLapS2, "BestLapS2"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.BestLapS3, "BestLapS3"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.CurrentLapS1, "CurrentLapS1"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.CurrentLapS2, "CurrentLapS2"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.CurrentLapS3, "CurrentLapS3"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.SectorMask, "SectorMask"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.LastLapTime, "LastLapTime"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.Gap, "Gap"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.Interval, "Interval"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.PosChange, "PosChange"); err != nil {
			return err
		}
	}
	return nil
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// 构建WebSocket消息
func buildWebSocketMessage(data WebSocketMessage, buf *bytes.Buffer) error {
	buf.Reset()

	if err := writeBinary(buf, binary.LittleEndian, data.Version, "版本号"); err != nil {
		return err
	}
	if err := writeBinary(buf, binary.LittleEndian, data.BroadcastInterval, "广播间隔"); err != nil {
		return err
	}
	if err := writeBinary(buf, binary.LittleEndian, data.ViewerCount, "观众数量"); err != nil {
		return err
	}
	if err := writeBinary(buf, binary.LittleEndian, int64(data.Session.SessionID), "会话ID"); err != nil {
		return err
	}
	if err := writeBinary(buf, binary.LittleEndian, int32(data.Session.ElapsedMs), "已用时间"); err != nil {
		return err
	}
	if err := writeBinary(buf, binary.LittleEndian, uint16(data.Session.DurationMin), "持续时间"); err != nil {
		return err
	}

	// 写入会话类型
	sessionType := uint8(0)
	if data.Session.Type == 2 {
		sessionType = 1
	}
	if err := writeBinary(buf, binary.LittleEndian, sessionType, "会话类型"); err != nil {
		return err
	}

	// 写入抓地力值和赛道状况（每个writeBinary单独成行）
	if err := writeBinary(buf, binary.LittleEndian, float32(math.Min(math.Max(float64(data.Session.StartGrip), 0), 100)), "抓地力值"); err != nil {
		return err
	}
	if err := writeBinary(buf, binary.LittleEndian, float32(math.Min(math.Max(float64(data.Session.CurrentGrip), 0), 100)), "赛道状况"); err != nil {
		return err
	}
	return nil
}

func handleACEventWebSocket(w http.ResponseWriter, r *http.Request) {
	// 升级HTTP连接为WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Printf("WebSocket升级失败: %v", err)
		return
	}
	defer conn.Close()

	// 从cookie获取eventId
	cookie, err := r.Cookie("eventId")
	if err != nil || cookie.Value == "" {
		logger.Printf("eventId cookie缺失")
		return
	}
	eventId := cookie.Value

	atomic.AddInt32(&wsConnectionCount, 1)
	defer func() {
		atomic.AddInt32(&wsConnectionCount, -1)
		conn.Close()
	}()

	// 读取WebSocket消息
	go func() {
		defer conn.Close()
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				logger.Printf("WebSocket读取错误: %v", err)
				break
			}
			// 解析WebSocket消息
			var msg WebSocketMessage
			if err := json.Unmarshal(message, &msg); err == nil {
				// 验证消息中的eventId是否与cookie匹配
				if msg.EventID > 0 && strconv.Itoa(msg.EventID) == eventId {
					// 更新活跃会话和条目数据
					liveEventsMutex.Lock()
					liveEventsData.Session = msg.Session
					liveEventsData.Entries = msg.Entries
					liveEventsMutex.Unlock()
				} else {
					logger.Printf("消息eventId不匹配: 消息=%d, Cookie=%s", msg.Session.EventID, eventId)
				}
			}
		}
	}()

	// 获取最新会话ID
	latestSessionID := liveEventsData.Session.SessionID

	// 获取会话详情
	session := liveEventsData.Session

	// 查询session feed数据并记录最后一个feed_id
	var lastFeedID int64
	feedRows, err := db.Query(`SELECT session_feed_id, type, detail, CAST(UNIX_TIMESTAMP(time) * 1000000 AS SIGNED) AS time FROM session_feed WHERE session_id = ? ORDER BY session_feed_id ASC`, session.SessionID)
	if err != nil {
		logger.Printf("查询feed数据失败: %v", err)
	} else {
		defer feedRows.Close()
		for feedRows.Next() {
			var feedID int64
			var typ int
			var detail string
			var t TimeWithTimezone
			if err := feedRows.Scan(&feedID, &typ, &detail, &t); err != nil {
				logger.Printf("解析feed数据失败: %v", err)
				continue
			} else {
				lastFeedID = feedID
			}
		}
	}

	// 数据更新和心跳检测循环
	pingTicker := time.NewTicker(15 * time.Second) // 缩短ping间隔至15秒
	// Ensure non-negative interval with default fallback
	interval := liveEventsData.BroadcastInterval

	updateTicker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	defer func() {
		pingTicker.Stop()
		updateTicker.Stop()
	}()

	for {
		select {
		case <-pingTicker.C:
			// 发送ping帧并等待pong响应
			if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(5*time.Second)); err != nil {
				logger.Printf("Ping错误: %v，连接已停止，正在关闭连接", err)
				conn.Close()
				return
			}
			// 设置pong处理
			conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			conn.SetPongHandler(func(string) error {
				conn.SetReadDeadline(time.Now().Add(30 * time.Second))
				return nil
			})
		case <-updateTicker.C:
			// 使用goroutine异步处理数据查询和发送，避免阻塞ticker
			go func() {
				// 增量查询新的session feed数据
				var newFeeds []struct {
					feedType  uint8
					detail    string
					timestamp TimeWithTimezone
				}
				if lastFeedID > 0 {
					feedRows, err := db.Query(`SELECT type, detail, CAST(UNIX_TIMESTAMP(time) * 1000000 AS SIGNED) AS time FROM session_feed WHERE session_id = ? AND session_feed_id > ? ORDER BY session_feed_id ASC`, latestSessionID, lastFeedID)
					if err == nil {
						defer feedRows.Close()
						for feedRows.Next() {
							var typ int
							var detail string
							var t TimeWithTimezone
							if err := feedRows.Scan(&typ, &detail, &t); err == nil {
								newFeeds = append(newFeeds, struct {
									feedType  uint8
									detail    string
									timestamp TimeWithTimezone
								}{uint8(typ), detail, TimeWithTimezone(t)})
							}
						}
						// 更新最后一个feed_id
						if len(newFeeds) > 0 {
							lastFeedIDQuery := db.QueryRow(`SELECT MAX(session_feed_id) FROM session_feed WHERE session_id = ?`, latestSessionID)
							lastFeedIDQuery.Scan(&lastFeedID)
						}
					}
				}
				if err != nil {
					logger.Printf("查询feed更新数据失败: %v", err)
					return
				}

				// 解析排行榜数据
				entries := liveEventsData.Entries

				// 从池获取缓冲区
				buf := bufferPool.Get().(*bytes.Buffer)
				defer bufferPool.Put(buf)

				// 构建WebSocket消息
				if err := buildWebSocketMessage(liveEventsData, buf); err != nil {
					logger.Printf("构建消息失败: %v", err)
					return
				}

				// 写入条目数量
				binary.Write(buf, binary.LittleEndian, uint8(len(entries)))

				// 复制carUpdates数据
				carUpdatesMutex.Lock()
				copiedCarUpdates := make(map[int64]udp.CarUpdate)
				for k, v := range carUpdates {
					copiedCarUpdates[k] = v
				}
				carUpdatesMutex.Unlock()

				if err := writeEntriesToBuffer(entries, buf); err != nil {
					logger.Printf("写入条目数据失败: %v", err)
					return
				}

				// 写入feed数量
				binary.Write(buf, binary.LittleEndian, uint8(len(newFeeds)))
				// 写入每个feed
				for _, f := range newFeeds {
					binary.Write(buf, binary.LittleEndian, f.feedType)
					binary.Write(buf, binary.LittleEndian, f.timestamp)
					binary.Write(buf, binary.LittleEndian, uint16(len(f.detail)))
					buf.Write([]byte(f.detail))
				}

				// 写入结束标识
				binary.Write(buf, binary.LittleEndian, uint8(0))

				// 发送消息
				if err := conn.WriteMessage(websocket.BinaryMessage, buf.Bytes()); err != nil {
					logger.Printf("发送更新数据失败: %v", err)
				}
			}()
		}
	}
}

// 处理AC团队列表请求
func handleACTeams(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 4 || pathParts[2] != "ac" || pathParts[3] != "teams" {
		sendErrorResponse(w, http.StatusBadRequest, "无效的URL格式", "teams")
		return
	}

	// 检查是否提供了ID参数
	var query string
	var args []interface{}
	if len(pathParts) >= 5 && pathParts[4] != "" {
		idStr := pathParts[4]
		ids := strings.Split(idStr, ",")

		placeholders := make([]string, len(ids))
		args = make([]interface{}, len(ids))
		for i, id := range ids {
			// Convert string ID to integer
			idInt, err := strconv.Atoi(id)
			if err != nil {
				sendErrorResponse(w, http.StatusBadRequest, "Invalid team ID format: "+id, "teams")
				return
			}
			placeholders[i] = "?"
			args[i] = idInt
		}

		query = fmt.Sprintf("SELECT team_id, name, car_id, team_no, livery_name FROM team WHERE team_id IN (%s)", strings.Join(placeholders, ","))
	} else {
		// 如果没有提供ID参数，返回所有团队
		query = "SELECT team_id, name, car_id, team_no, livery_name FROM team"
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		logger.Printf("数据库查询失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "数据库查询失败", "teams")
		return
	}
	defer rows.Close()

	var teams []ACTeam
	for rows.Next() {
		var t ACTeam = ACTeam{}
		if err := rows.Scan(&t.TeamID, &t.Name, &t.CarID, &t.TeamNo, &t.LiveryName); err != nil {
			logger.Printf("数据库行扫描失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "数据解析失败", "teams")
			return
		}
		teams = append(teams, t)
	}

	if err = rows.Err(); err != nil {
		logger.Printf("行迭代错误: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "数据处理失败", "teams")
		return
	}

	sendSuccessResponse(w, "团队数据获取成功", "teams", teams)
}

// 处理AC车辆列表请求
func handleACCars(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 4 || pathParts[2] != "ac" || (pathParts[3] != "cars" && pathParts[3] != "car") {
		sendErrorResponse(w, http.StatusBadRequest, "无效的URL格式", "cars")
		return
	}

	// 检查是否提供了ID参数
	var query string
	var args []interface{}
	if len(pathParts) >= 5 && pathParts[4] != "" {
		idStr := pathParts[4]
		ids := strings.Split(idStr, ",")

		placeholders := make([]string, len(ids))
		args = make([]interface{}, len(ids))
		for i, id := range ids {
			placeholders[i] = "?"
			args[i] = id
		}

		query = fmt.Sprintf("SELECT car_id, name, display_name, manufacturer, car_class FROM car WHERE car_id IN (%s)", strings.Join(placeholders, ","))
		// Validate single ID format for /api/ac/car/
		if pathParts[3] == "car" && len(ids) > 1 {
			sendErrorResponse(w, http.StatusBadRequest, "单个车辆查询不支持多个ID", "car")
			return
		}
	} else {
		// 如果没有提供ID参数，返回所有车辆
		query = "SELECT car_id, name, display_name, manufacturer, car_class FROM car"
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		logger.Printf("数据库查询失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "数据库查询失败", "cars")
		return
	}
	defer rows.Close()

	var cars []ACCar
	for rows.Next() {
		var c ACCar = ACCar{}
		if err := rows.Scan(&c.CarID, &c.Name, &c.DisplayName, &c.Manufacturer, &c.CarClass); err != nil {
			logger.Printf("数据库行扫描失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "数据解析失败", "cars")
			return
		}
		cars = append(cars, c)
	}

	if err = rows.Err(); err != nil {
		logger.Printf("行迭代错误: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "数据处理失败", "cars")
		return
	}

	// Return single car object for /api/ac/car/ endpoint
	if pathParts[3] == "car" {
		if len(cars) > 0 {
			sendSuccessResponse(w, "车辆数据获取成功", "car", cars[0])
		} else {
			sendErrorResponse(w, http.StatusNotFound, "车辆数据未找到", "car")
		}
	} else {
		sendSuccessResponse(w, "车辆数据获取成功", "cars", cars)
	}
}

// 处理AC用户列表
func handleACUsers(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 4 || pathParts[2] != "ac" {
		sendErrorResponse(w, http.StatusBadRequest, "无效的URL格式", "users")
		return
	}

	// 处理单个用户详情请求
	if pathParts[3] == "user" && len(pathParts) >= 5 && pathParts[4] != "" {
		handleACUserDetails(w, r)
		return
	}

	// 处理用户列表请求
	if pathParts[3] != "users" {
		sendErrorResponse(w, http.StatusBadRequest, "无效的URL格式", "users")
		return
	}

	// 检查是否提供了ID参数
	var query string
	var args []interface{}
	if len(pathParts) >= 5 && pathParts[4] != "" {
		idStr := pathParts[4]
		ids := strings.Split(idStr, ",")

		placeholders := make([]string, len(ids))
		args = make([]interface{}, len(ids))
		for i, id := range ids {
			placeholders[i] = "?"
			args[i] = id
		}

		query = fmt.Sprintf("SELECT user_id, name FROM user WHERE user_id IN (%s) ORDER BY user_id ASC", strings.Join(placeholders, ","))
	} else {
		// 如果没有提供ID参数，返回所有用户
		query = "SELECT user_id, name FROM user ORDER BY user_id ASC"
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		logger.Printf("数据库查询失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "数据库查询失败", "users")
		return
	}
	defer rows.Close()

	var users []ACUser
	for rows.Next() {
		u := ACUser{}
		if err := rows.Scan(&u.UserID, &u.Name); err != nil {
			logger.Printf("数据库行扫描失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "数据解析失败", "users")
			return
		}
		users = append(users, u)
	}

	if err = rows.Err(); err != nil {
		logger.Printf("行迭代错误: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "数据处理失败", "users")
		return
	}

	sendSuccessResponse(w, "用户数据获取成功", "users", users)
}

// 处理单个用户详情请求
func handleACUserDetails(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")
	driverId := pathParts[4]
	hasSummary := false
	if len(pathParts) > 5 && strings.ToLower(pathParts[5]) == "summary" {
		hasSummary = true
	}

	// 基础用户信息
	user := ACUser{}
	err := db.QueryRow("SELECT user_id, name, prev_name, country FROM user WHERE user_id = ?", driverId).Scan(
		&user.UserID, &user.Name, &user.PrevName, &user.Country)

	if err != nil {
		if err == sql.ErrNoRows {
			sendErrorResponse(w, http.StatusNotFound, "用户数据未找到", "user")
		} else {
			logger.Printf("数据库查询失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "数据库查询失败", "user")
		}
		return
	}

	if !hasSummary {
		sendSuccessResponse(w, "用户详情数据获取成功", "user", user)
		return
	}

	// 定义所需结构体

	// 查询用户参与的事件列表及统计
	eventsQuery := `
		SELECT 
			e.event_id, e.name as event_name, COALESCE(tc.track_name, 'Unknown') as track_name, 
 			COALESCE(MAX(t.name), 'N/A') as team_name, 
			COALESCE(SUM(l.time * l.avg_speed / 3600000), 0) as distance_driven_km,
			COALESCE(COUNT(l.stint_lap_id), 0) as total_laps,
			COALESCE(SUM(CASE WHEN l.cuts = 0 AND l.crashes = 0 THEN 1 ELSE 0 END), 0) as total_valid_laps, 
			CAST(UNIX_TIMESTAMP(COALESCE(MAX(s.last_activity), FROM_UNIXTIME(0))) * 1000000 AS UNSIGNED) as event_time
		FROM event e
		LEFT JOIN session s ON e.event_id = s.event_id
		LEFT JOIN session_stint st ON s.session_id = st.session_id AND st.user_id = ?
		LEFT JOIN stint_lap l ON st.session_stint_id = l.stint_id
		LEFT JOIN track_config tc ON e.track_config_id = tc.track_config_id
		LEFT JOIN team_member tm ON st.team_member_id = tm.team_member_id
		LEFT JOIN team t ON tm.team_id = t.team_id
		GROUP BY e.event_id, e.name, tc.track_name
		ORDER BY event_time DESC
		`

	rows, err := db.Query(eventsQuery, driverId)
	if err != nil && err != sql.ErrNoRows {
		logger.Printf("事件数据查询失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "事件数据查询失败", "userDetails")
		return
	}

	defer rows.Close()

	var eventMap = make(map[int]*DriverEvent)
	var totalDistance float64
	var totalLaps int
	var totalValidLaps int
	now := time.Now().Unix()

	for rows.Next() {
		var event DriverEvent = DriverEvent{}
		var eventTime int64
		var distance float64
		var laps, validLaps int

		err := rows.Scan(
			&event.EventID, &event.EventName, &event.TrackName, &event.TeamName,
			&distance, &laps, &validLaps, &eventTime,
		)
		if err != nil {
			logger.Printf("事件数据扫描失败: %v", err)
			continue
		}

		event.DistanceDrivenKm = distance
		event.TotalLaps = laps
		event.TotalValidLaps = validLaps
		eventTimeSec := eventTime / 1000
		event.EventTime = TimeWithTimezone(eventTimeSec)
		event.TimeAgoSec = now - int64(eventTimeSec)
		if event.TimeAgoSec < 0 {
			event.TimeAgoSec = 0 // 防止未来时间导致的负数值
		}

		// 合并相同event_id的记录
		if existingEvent, exists := eventMap[event.EventID]; exists {
			existingEvent.DistanceDrivenKm += event.DistanceDrivenKm
			existingEvent.TotalLaps += event.TotalLaps
			existingEvent.TotalValidLaps += event.TotalValidLaps
			// 保留最新的事件时间
			if event.EventTime > existingEvent.EventTime {
				existingEvent.EventTime = event.EventTime
				existingEvent.TimeAgoSec = now - int64(event.EventTime)
			}
		} else {
			eventMap[event.EventID] = &event
		}
	}

	// 转换map为切片并过滤公里数为0的事件
	events := []DriverEvent{}
	totalEvents := 0
	totalDistance = 0
	totalLaps = 0
	totalValidLaps = 0
	for _, e := range eventMap {
		if e.DistanceDrivenKm > 0 {
			e.DistanceDrivenKm = math.Floor(e.DistanceDrivenKm)
			events = append(events, *e)
			totalEvents++
			totalDistance += e.DistanceDrivenKm
			totalLaps += e.TotalLaps
			totalValidLaps += e.TotalValidLaps
		}
	}

	// 按事件时间排序（最新的在前）
	sort.Slice(events, func(i, j int) bool {
		return events[i].TimeAgoSec < events[j].TimeAgoSec
	})

	totalDistance = math.Floor(totalDistance)

	// 查询顶级赛道
	topTracksQuery := `
		SELECT tc.track_name, SUM(l.time * l.avg_speed / 3600000) as distance_driven
		FROM session s
		JOIN session_stint st ON s.session_id = st.session_id
		JOIN stint_lap l ON st.session_stint_id = l.stint_id
		JOIN event ev ON s.event_id = ev.event_id
		JOIN track_config tc ON ev.track_config_id = tc.track_config_id
		WHERE st.user_id = ?
		GROUP BY tc.track_name
		ORDER BY distance_driven DESC
		`

	rows, err = db.Query(topTracksQuery, driverId)
	if err != nil && err != sql.ErrNoRows {
		logger.Printf("赛道数据查询失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "赛道数据查询失败", "userDetails")
		return
	}

	defer rows.Close()

	topTracks := []TrackSummary{}
	for rows.Next() {
		var track TrackSummary = TrackSummary{}
		err := rows.Scan(&track.TrackName, &track.DistanceDriven)
		if err != nil {
			logger.Printf("赛道数据扫描失败: %v", err)
			continue
		}
		track.DistanceDriven = math.Floor(track.DistanceDriven)
		topTracks = append(topTracks, track)
	}

	// 查询顶级车辆
	topCarsQuery := `
		SELECT c.name as car_name, SUM(l.time * l.avg_speed / 3600000) as distance_driven
		FROM session s
		JOIN session_stint st ON s.session_id = st.session_id
		JOIN stint_lap l ON st.session_stint_id = l.stint_id
		JOIN car c ON st.car_id = c.car_id
		WHERE st.user_id = ?
		GROUP BY c.name
		ORDER BY distance_driven DESC
		`

	rows, err = db.Query(topCarsQuery, driverId)
	if err != nil && err != sql.ErrNoRows {
		logger.Printf("车辆数据查询失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "车辆数据查询失败", "userDetails")
		return
	}

	defer rows.Close()

	topCars := []CarSummary{}
	for rows.Next() {
		car := CarSummary{}
		err := rows.Scan(&car.CarName, &car.DistanceDriven)
		if err != nil {
			logger.Printf("车辆数据扫描失败: %v", err)
			continue
		}
		car.DistanceDriven = math.Floor(car.DistanceDriven)
		topCars = append(topCars, car)
	}

	// 构建响应结果
	driver := DriverSummary{
		UserID:                user.UserID,
		TotalEvents:           totalEvents,
		TotalDistanceDrivenKm: totalDistance,
		TotalLaps:             totalLaps,
		TotalValidLaps:        totalValidLaps,
		Events:                events,
		TopTracks:             topTracks,
		TopCars:               topCars,
	}
	sendSuccessResponse(w, "用户详情数据获取成功", "driver", driver)
}

// 处理车辆徽章图片请求
func handleCarResource(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// 路径格式应为: images/ac/car/{car_id}/badge
	if len(pathParts) != 5 || pathParts[2] != "car" || pathParts[4] != "badge" {
		logger.Printf("无效的车辆徽章请求路径: %s, 路径部分: %v", r.URL.Path, pathParts)
		http.NotFound(w, r)
		return
	}

	// 提取car_id
	carID := pathParts[3]

	// 查询车辆名称
	var carName string
	query := `SELECT name FROM car WHERE car_id = ?`
	err := db.QueryRow(query, carID).Scan(&carName)
	if err != nil {
		logger.Printf("查询车辆名称失败: %v", err)
		http.NotFound(w, r)
		return
	}

	// 清理文件名并构建路径
	sanitizedName := strings.ToLower(strings.ReplaceAll(carName, " ", "_"))
	fileName := fmt.Sprintf("badge_%s.png", sanitizedName)
	filePath := filepath.Join(cfg.App.Server.CachePath, "cars", fileName)

	// 读取并返回文件
	file, err := os.Open(filePath)
	if err != nil {
		// 构建GameConfig.Path父目录下的备选路径
		gameConfigParentDir := filepath.Dir(cfg.Game.Path)
		fallbackPath := filepath.Join(gameConfigParentDir, "content", "cars", carName, "ui", "badge.png")
		file, err = os.Open(fallbackPath)
		if err != nil {
			logger.Printf("车标加载失败: %v", err)
			http.NotFound(w, r)
			return
		}

		// 确保缓存目录存在
		cacheDir := filepath.Dir(filePath)
		if err := os.MkdirAll(cacheDir, 0755); err != nil {
			logger.Printf("创建缓存目录失败: %v", err)
		}

		// 从备选路径复制文件到缓存目录
		fallbackContent, err := io.ReadAll(file)
		if err != nil {
			logger.Printf("读取备选文件失败: %v", err)
		} else {
			if err := os.WriteFile(filePath, fallbackContent, 0644); err != nil {
				logger.Printf("复制文件到缓存目录失败: %v", err)
			} else {
				logger.Printf("成功将车标复制到缓存目录: %s", filePath)
			}
			// 重新打开文件以读取
			file, _ = os.Open(filePath)
		}
	}
	defer file.Close()

	fi, _ := file.Stat()
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Content-Length", strconv.FormatInt(fi.Size(), 10))
	io.Copy(w, file)
}

// 处理赛道图片请求
func handleTrackResource(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// 路径格式应为: images/ac/track/{track_config_id}/{resource_type}
	if len(pathParts) != 5 || pathParts[2] != "track" {
		logger.Printf("无效的请求路径: %s, 路径部分: %v", r.URL.Path, pathParts)
		http.NotFound(w, r)
		return
	}

	// 提取track_config_id和资源类型
	trackConfigID := pathParts[3]
	resourceType := pathParts[4]

	// 设置URL参数以便后续处理函数获取
	q := r.URL.Query()
	q.Set("track_config_id", trackConfigID)
	r.URL.RawQuery = q.Encode()

	// 根据资源类型分发请求
	switch resourceType {
	case "map":
		handleTrackSVG(w, r)
	case "preview":
		handleTrackImage(w, r)
	default:
		logger.Printf("不支持的资源类型: %s", resourceType)
		http.NotFound(w, r)
	}
}

func handleTrackSVG(w http.ResponseWriter, r *http.Request) {
	// 从查询参数获取track_config_id
	trackConfigID := r.URL.Query().Get("track_config_id")
	if trackConfigID == "" {
		sendErrorResponse(w, http.StatusBadRequest, "缺少赛道ID参数", "trackID")
		return
	}

	// 查询赛道名称
	var trackName, configName string
	query := `SELECT track_name, config_name FROM track_config
		WHERE track_config_id = ?`
	err := db.QueryRow(query, trackConfigID).Scan(&trackName, &configName)
	if err != nil {
		logger.Printf("查询赛道失败: %v", err)
		sendErrorResponse(w, http.StatusNotFound, "未找到赛道", "track")
		return
	}

	// 清理文件名并构建路径
	// 将赛道名称转换为小写并替换空格为下划线
	sanitizedTrack := strings.ToLower(strings.ReplaceAll(trackName, " ", "_"))
	sanitizedConfig := strings.ToLower(strings.ReplaceAll(configName, " ", "_"))
	fileName := fmt.Sprintf("map_%s_%s.svg", sanitizedTrack, sanitizedConfig)
	filePath := filepath.Join(cfg.App.Server.CachePath, "live-track-maps", fileName)

	// 读取并返回SVG文件
	file, err := os.Open(filePath)
	if err != nil {
		logger.Printf("打开地图文件失败: %v", err)
		w.WriteHeader(http.StatusNotFound)
		// 返回默认SVG以避免前端错误
		w.Header().Set("Content-Type", "image/svg+xml")
		w.Write([]byte(`<svg xmlns="http://www.w3.org/2000/svg" width="200" height="100"><text x="100" y="50" text-anchor="middle">地图未找到</text></svg>`))
		return
	}
	defer file.Close()

	fi, _ := file.Stat()
	w.Header().Set("Content-Type", "image/svg+xml")
	w.Header().Set("Content-Length", strconv.FormatInt(fi.Size(), 10))
	io.Copy(w, file)
}

func handleTrackImage(w http.ResponseWriter, r *http.Request) {
	// 从查询参数获取track_config_id
	trackConfigID := r.URL.Query().Get("track_config_id")
	if trackConfigID == "" {
		sendErrorResponse(w, http.StatusBadRequest, "缺少赛道ID参数", "trackID")
		return
	}

	// 查询数据库获取赛道名称和配置名称
	var trackName, configName string
	err := db.QueryRow("SELECT track_name, config_name FROM track_config WHERE track_config_id = ?", trackConfigID).Scan(&trackName, &configName)
	if err != nil {
		if err == sql.ErrNoRows {
			http.NotFound(w, r)
		} else {
			logger.Printf("数据库查询失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "数据库错误", "database")
		}
		return
	}

	// 清理名称以创建有效的文件名
	sanitizedTrack := sanitize(trackName)
	sanitizedConfig := sanitize(configName)
	filename := fmt.Sprintf("preview_%s_%s.png", sanitizedTrack, sanitizedConfig)

	// 构建完整文件路径
	filePath := filepath.Join(cfg.App.Server.CachePath, "tracks", filename)

	// 提供文件，先尝试从缓存路径加载
	file, err := os.Open(filePath)
	if err != nil {
		logger.Printf("缓存赛道图片不存在: %s，尝试备选路径", filePath)
		// 构建GameConfig.Path父目录下的备选路径
		gameConfigParentDir := filepath.Dir(cfg.Game.Path)
		fallbackPath := filepath.Join(gameConfigParentDir, "content", "tracks", trackName, configName, "ui", "outline.png")
		file, err = os.Open(fallbackPath)
		if err != nil {
			// 尝试新的路径组合: trackName/ui/configName
			fallbackPath2 := filepath.Join(gameConfigParentDir, "content", "tracks", trackName, "ui", configName, "outline.png")
			file, err = os.Open(fallbackPath2)
			if err != nil {
				// 尝试新的路径组合: trackName/ui
				fallbackPath2 := filepath.Join(gameConfigParentDir, "content", "tracks", trackName, "ui", "outline.png")
				file, err = os.Open(fallbackPath2)
				if err != nil {
					logger.Printf("赛道图片路径加载失败: %v", err)
					http.NotFound(w, r)
					return
				}
			}
		}

		// 确保缓存目录存在
		cacheDir := filepath.Dir(filePath)
		if err := os.MkdirAll(cacheDir, 0755); err != nil {
			logger.Printf("创建缓存目录失败: %v", err)
		}

		// 从备选路径复制文件到缓存目录
		fallbackContent, err := io.ReadAll(file)
		if err != nil {
			logger.Printf("读取备选文件失败: %v", err)
		} else {
			if err := os.WriteFile(filePath, fallbackContent, 0644); err != nil {
				logger.Printf("复制文件到缓存目录失败: %v", err)
			} else {
				logger.Printf("成功将赛道图片复制到缓存目录: %s", filePath)
			}
			// 重新打开文件以读取
			file, _ = os.Open(filePath)
		}
	}
	defer file.Close()

	fi, _ := file.Stat()
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Content-Length", strconv.FormatInt(fi.Size(), 10))
	io.Copy(w, file)
}

// 清理字符串以用于文件名
func sanitize(name string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, name)
}

func handleACTeamMembers(w http.ResponseWriter, r *http.Request) {
	teamID := strings.TrimPrefix(r.URL.Path, "/api/ac/team/")
	teamID = strings.TrimSuffix(teamID, "/members")

	rows, err := db.Query("SELECT user_id FROM team_member WHERE team_id = ?", teamID)
	if err != nil {
		logger.Printf("数据库查询失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "数据库查询失败", "teamMembers")
		return
	}
	defer rows.Close()

	var members []ACTeamMember

	for rows.Next() {
		m := ACTeamMember{}
		if err := rows.Scan(&m.UserID); err != nil {
			logger.Printf("数据解析失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "数据解析失败", "teamMembers")
			return
		}
		members = append(members, m)
	}

	if err := rows.Err(); err != nil {
		logger.Printf("行迭代错误: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "行迭代错误", "teamMembers")
		return
	}

	sendSuccessResponse(w, "团队成员数据获取成功", "teamMembers", members)
}

func handleACTrack(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(pathParts) < 4 || pathParts[2] != "track" {
		http.NotFound(w, r)
		return
	}

	// 提取track_config_id和资源类型
	trackID := pathParts[3]
	var resourceType string
	if len(pathParts) >= 5 {
		resourceType = pathParts[4]
	}

	// 根据资源类型分发到不同的处理函数
	switch resourceType {
	case "map":
		handleTrackSVG(w, r)
	case "preview":
		handleTrackImage(w, r)
	default:
		// 原始赛道信息查询逻辑
		track := ACTrackConfig{}
		err := db.QueryRow("SELECT track_config_id, track_name, config_name, display_name, country, city, length FROM track_config WHERE track_config_id = ?", trackID).Scan(
			&track.TrackConfigID, &track.TrackName, &track.ConfigName, &track.DisplayName, &track.Country, &track.City, &track.Length)
		if err != nil {
			if err == sql.ErrNoRows {
				logger.Printf("赛道未找到: %s", trackID)
				sendErrorResponse(w, http.StatusNotFound, "赛道未找到", "track")
			} else {
				logger.Printf("数据库查询失败: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, "数据库查询失败", "track")
			}
			return
		}
		sendSuccessResponse(w, "赛道信息获取成功", "track", track)
	}
}

func handleACLapSummary(w http.ResponseWriter, r *http.Request) {
	lapID := strings.TrimPrefix(r.URL.Path, "/api/ac/lap/summary/")

	type Response struct {
		Status  string  `json:"status"`
		Summary Summary `json:"summary"`
	}

	response := Response{}

	err := db.QueryRow(`
		SELECT 
			l.stint_lap_id, l.stint_id, l.sector_1, l.sector_2, l.sector_3, l.grip, l.tyre, 
			l.time, l.cuts, l.crashes, l.car_crashes, l.max_speed, l.avg_speed, 
			CAST(UNIX_TIMESTAMP(l.finished_at) * 1000000 AS SIGNED) AS finished_at, ss.user_id, ss.car_id, 
			e.event_id, e.name, t.track_name, t.length, t.track_config_id, 
			s.type, s.air_temp, s.road_temp, s.weather
		FROM stint_lap l
		JOIN session_stint ss ON l.stint_id = ss.session_stint_id
		JOIN session s ON ss.session_id = s.session_id
		JOIN event e ON s.event_id = e.event_id
		JOIN user u ON ss.user_id = u.user_id
		JOIN car c ON ss.car_id = c.car_id
		JOIN track_config t ON e.track_config_id = t.track_config_id
		WHERE l.stint_lap_id = ?`, lapID).Scan(
		&response.Summary.Lap.StintLapID, &response.Summary.Lap.StintID,
		&response.Summary.Lap.Sector1, &response.Summary.Lap.Sector2, &response.Summary.Lap.Sector3,
		&response.Summary.Lap.Grip, &response.Summary.Lap.Tyre, &response.Summary.Lap.Time,
		&response.Summary.Lap.Cuts, &response.Summary.Lap.Crashes, &response.Summary.Lap.CarCrashes,
		&response.Summary.Lap.MaxSpeed, &response.Summary.Lap.AvgSpeed, &response.Summary.Lap.FinishedAt,
		&response.Summary.UserID, &response.Summary.CarID, &response.Summary.EventID,
		&response.Summary.EventName, &response.Summary.TrackName, &response.Summary.TrackLength,
		&response.Summary.TrackConfigID, &response.Summary.SessionType, &response.Summary.AirTemp,
		&response.Summary.RoadTemp, &response.Summary.Weather)
	if err != nil {
		if err == sql.ErrNoRows {
			logger.Printf("圈速摘要未找到")
			sendErrorResponse(w, http.StatusNotFound, "圈速摘要未找到", "summary")
		} else {
			logger.Printf("数据库查询失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "数据库查询失败", "summary")
		}
		return
	}
	sendSuccessResponse(w, "圈速摘要获取成功", "summary", response.Summary)
}

// 处理AC单圈遥测数据请求
func handleACLapTelemetry(w http.ResponseWriter, r *http.Request) {
	// 解析URL路径获取lap ID
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(pathParts) < 5 || pathParts[1] != "ac" || pathParts[2] != "lap" || pathParts[3] != "telemetry" {
		logger.Printf("无效的URL格式: %s", r.URL.Path)
		sendErrorResponse(w, http.StatusBadRequest, "无效的URL格式", "telemetry")
		return
	}

	if pathParts[4] == "" {
		logger.Printf("缺少lap_id参数")
		sendErrorResponse(w, http.StatusBadRequest, "缺少lap_id参数", "telemetry")
		return
	}
	lapID := pathParts[4]

	// 转换lapID为整数
	id, err := strconv.Atoi(lapID)
	if err != nil {
		logger.Printf("无效的lap ID: %v", err)
		sendErrorResponse(w, http.StatusBadRequest, "无效的lap ID格式", "telemetry")
		return
	}

	// 从lap_telemetry表查询telemetry二进制数据
	var telemetryData []byte
	// 查询telemetry数据
	err = db.QueryRow("SELECT telemetry FROM lap_telemetry WHERE lap_id = ?", id).Scan(&telemetryData)
	if err != nil {
		if err == sql.ErrNoRows {
			logger.Printf("遥测数据未找到: %d", id)
			sendErrorResponse(w, http.StatusNotFound, "遥测数据未找到", "telemetry")
		} else {
			logger.Printf("查询遥测数据失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "数据库查询失败", "telemetry")
		}
		return
	}

	// 创建包含版本和赛道长度的二进制数据
	buffer := new(bytes.Buffer)
	buffer.Write(telemetryData)

	// 设置响应头为二进制数据
	w.Header().Set("Content-Type", "application/octet-stream")

	// 写入响应数据
	_, err = w.Write(buffer.Bytes())
	if err != nil {
		logger.Printf("写入响应数据失败: %v", err)
	}
}

// 通用最佳圈速处理函数
func handleACBestLap(w http.ResponseWriter, r *http.Request) {
	// 解析URL路径参数
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	var allLaps []ACBestLap
	var page, entries int = 1, 10
	var err error

	// 判断是按事件还是按赛道查询
	switch pathParts[3] {
	case "event":
		// 按事件查询逻辑
		// 验证URL格式
		if len(pathParts) < 6 || pathParts[3] != "event" || pathParts[5] != "cars" {
			logger.Printf("无效的URL路径格式")
			sendErrorResponse(w, http.StatusBadRequest, "无效的URL路径格式", "bestlap")
			return
		}

		// 提取参数
		eventIDStr := pathParts[4]
		carIDsStr := pathParts[6]

		// 解析分页参数
		if len(pathParts) >= 8 && pathParts[7] == "page" && len(pathParts) >= 9 {
			pageStr := pathParts[8]
			page, err = strconv.Atoi(pageStr)
			if err != nil || page < 1 {
				logger.Printf("无效的页码: %s", pageStr)
				sendErrorResponse(w, http.StatusBadRequest, "无效的页码参数", "bestlap")
				return
			}

			if len(pathParts) >= 10 && pathParts[9] == "entries" && len(pathParts) >= 11 {
				entriesStr := pathParts[10]
				entries, err = strconv.Atoi(entriesStr)
				if err != nil || entries < 1 || entries > 100 {
					logger.Printf("无效的条目数: %s", entriesStr)
					sendErrorResponse(w, http.StatusBadRequest, "条目数必须为1-100之间的整数", "bestlap")
					return
				}
			}
		}

		// 验证参数
		if eventIDStr == "" {
			logger.Printf("缺少必要参数:eventID")
			sendErrorResponse(w, http.StatusBadRequest, "缺少必要参数:eventID", "bestlap")
			return
		}

		// 转换为整数
		eventID, err := strconv.Atoi(eventIDStr)
		if err != nil || eventID <= 0 {
			logger.Printf("无效的event_id参数: %v", err)
			sendErrorResponse(w, http.StatusBadRequest, "无效的event_id参数", "bestlap")
			return
		}

		// 处理车辆ID（可选）
		var carFilter string
		var args []interface{}
		carIDs := strings.Split(carIDsStr, ",")
		if len(carIDs) > 0 && carIDs[0] != "" {
			placeholders := make([]string, len(carIDs))
			for i := range carIDs {
				placeholders[i] = "?"
			}
			carFilter = fmt.Sprintf("AND ss.car_id IN (%s)", strings.Join(placeholders, ","))
		}

		// 验证并处理车辆ID
		if carIDsStr != "" {
			carIDs = strings.Split(carIDsStr, ",")
			for _, id := range carIDs {
				if _, err := strconv.Atoi(id); err != nil {
					logger.Printf("无效的车辆ID: %s", id)
					sendErrorResponse(w, http.StatusBadRequest, "无效的车辆ID格式", "bestlap")
					return
				}
			}
		}
		placeholders := make([]string, len(carIDs))
		args = make([]interface{}, 0, len(carIDs)+1)
		for i, id := range carIDs {
			placeholders[i] = "?"
			args = append(args, id)
		}

		// 查询最佳圈速数据
		query := fmt.Sprintf(`
            WITH all_laps AS (
                SELECT l.stint_lap_id AS lap_id, ss.user_id, ss.car_id, l.time, l.sector_1, l.sector_2, l.sector_3, l.grip, l.tyre, l.max_speed, CAST(UNIX_TIMESTAMP(l.finished_at) * 1000000 AS SIGNED) AS finished_at, e.event_id
                FROM stint_lap l
                JOIN session_stint ss ON l.stint_id = ss.session_stint_id
                JOIN user u ON ss.user_id = u.user_id
                JOIN car c ON ss.car_id = c.car_id
                JOIN session s ON ss.session_id = s.session_id
                JOIN event e ON s.event_id = e.event_id
                WHERE e.event_id = ? AND l.cuts = 0 AND l.crashes = 0 %s
            ),
            ranked_laps AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id, car_id ORDER BY time ASC) as rn
                FROM all_laps
            )
            SELECT lap_id, user_id, car_id, time, sector_1, sector_2, sector_3, grip, tyre, max_speed, finished_at, event_id
            FROM ranked_laps
            WHERE rn = 1
            ORDER BY time ASC
       `, carFilter)

		// 插入eventID作为第一个参数
		args = append([]interface{}{eventID}, args...)
		rows, err := db.Query(query, args...)
		if err != nil {
			logger.Printf("查询最佳圈速数据失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "查询数据失败", "bestlap")
			return
		}
		defer rows.Close()

		// 收集所有数据
		allLaps = []ACBestLap{}
		for rows.Next() {
			lap := ACBestLap{}
			if err := rows.Scan(&lap.LapID, &lap.User_id, &lap.Car_id, &lap.Time, &lap.Sector1, &lap.Sector2, &lap.Sector3, &lap.Grip, &lap.Tyre, &lap.MaxSpeed, &lap.FinishedAt, &lap.EventID); err != nil {
				logger.Printf("数据解析失败: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, "数据解析失败", "bestlap")
				return
			}
			allLaps = append(allLaps, lap)
		}

		if err = rows.Err(); err != nil {
			logger.Printf("行迭代错误: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "行迭代错误", "bestlap")
			return
		}

	case "track":
		// 按赛道查询逻辑
		// 验证URL格式
		if len(pathParts) < 11 {
			logger.Printf("无效的URL路径格式")
			sendErrorResponse(w, http.StatusBadRequest, "无效的URL路径格式", "bestlap")
			return
		}

		// 提取参数
		trackIDStr := pathParts[4]
		carIDStr := pathParts[6]
		pageStr := pathParts[8]
		entriesStr := pathParts[10]

		// 转换为整数
		trackID, err := strconv.Atoi(trackIDStr)
		if err != nil || trackID <= 0 {
			logger.Printf("无效的track_id参数: %v", err)
			sendErrorResponse(w, http.StatusBadRequest, "无效的track_id参数", "bestlap")
			return
		}

		carID, err := strconv.Atoi(carIDStr)
		if err != nil || carID <= 0 {
			logger.Printf("无效的car_id参数: %v", err)
			sendErrorResponse(w, http.StatusBadRequest, "无效的car_id参数", "bestlap")
			return
		}

		page, err = strconv.Atoi(pageStr)
		if err != nil || page <= 0 {
			logger.Printf("无效的page参数: %v", err)
			sendErrorResponse(w, http.StatusBadRequest, "无效的page参数", "bestlap")
			return
		}

		entries, err = strconv.Atoi(entriesStr)
		if err != nil || entries <= 0 || entries > 100 {
			logger.Printf("无效的entries参数: %v", err)
			sendErrorResponse(w, http.StatusBadRequest, "无效的entries参数", "bestlap")
			return
		}

		// 查询所有数据（不使用LIMIT/OFFSET，后续在应用层分页）
		query := `
            WITH ranked_laps AS (
                SELECT 
                    l.stint_lap_id AS lap_id, 
                    ss.user_id, 
                    ss.car_id, 
                    l.time, 
                    l.sector_1, 
                    l.sector_2, 
                    l.sector_3, 
                    l.grip, 
                    l.tyre, 
                    l.max_speed, 
                    CAST(UNIX_TIMESTAMP(l.finished_at) * 1000000 AS SIGNED) AS finished_at,
                    ROW_NUMBER() OVER (PARTITION BY ss.user_id, ss.car_id ORDER BY l.time ASC) AS rn
                FROM stint_lap l
                JOIN session_stint ss ON l.stint_id = ss.session_stint_id
                JOIN session s ON ss.session_id = s.session_id
                JOIN event e ON s.event_id = e.event_id
                JOIN user u ON ss.user_id = u.user_id
                JOIN car c ON ss.car_id = c.car_id
                WHERE e.track_config_id = ? AND ss.car_id = ? AND l.cuts = 0 AND l.crashes = 0 
            )
            SELECT lap_id, user_id, car_id, time, sector_1, sector_2, sector_3, grip, tyre, max_speed, CAST(UNIX_TIMESTAMP(finished_at) * 1000000 AS SIGNED) AS finished_at 
            FROM ranked_laps 
            WHERE rn = 1
            ORDER BY time ASC
        `

		rows, err := db.Query(query, trackID, carID)
		if err != nil {
			logger.Printf("查询最佳圈速数据失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "查询数据失败", "bestlap")
			return
		}
		defer rows.Close()

		// 收集所有数据
		allLaps = []ACBestLap{}
		for rows.Next() {
			lap := ACBestLap{}
			if err := rows.Scan(&lap.LapID, &lap.User_id, &lap.Car_id, &lap.Time, &lap.Sector1, &lap.Sector2, &lap.Sector3, &lap.Grip, &lap.Tyre, &lap.MaxSpeed, &lap.FinishedAt); err != nil {
				logger.Printf("数据解析失败: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, "数据解析失败", "bestlap")
				return
			}
			allLaps = append(allLaps, lap)
		}

		if err = rows.Err(); err != nil {
			logger.Printf("行迭代错误: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "行迭代错误", "bestlap")
			return
		}

	default:
		logger.Printf("无效的查询类型")
		sendErrorResponse(w, http.StatusBadRequest, "无效的查询类型", "bestlap")
		return
	}

	// 按时间升序排序
	sort.Slice(allLaps, func(i, j int) bool {
		return allLaps[i].Time < allLaps[j].Time
	})

	// 计算差距和最佳圈速标记
	var classBestTime int64 = math.MaxInt64
	var sector1Best, sector2Best, sector3Best int = math.MaxInt32, math.MaxInt32, math.MaxInt32

	// 1. 先计算每个车的最佳圈速和全局最佳
	carBestTimes := make(map[int]int64)
	carBestSector1 := make(map[int]int)
	carBestSector2 := make(map[int]int)
	carBestSector3 := make(map[int]int)

	// 第一遍：找出全局最佳和每个车的最佳圈速和扇区
	for _, lap := range allLaps {
		carID := lap.Car_id
		// 全局最佳圈速
		if lap.Time < float64(classBestTime) {
			classBestTime = int64(lap.Time)
		}
		// 全局最佳扇区1
		if lap.Sector1 > 0 && lap.Sector1 < sector1Best {
			sector1Best = lap.Sector1
		}
		// 全局最佳扇区2
		if lap.Sector2 > 0 && lap.Sector2 < sector2Best {
			sector2Best = lap.Sector2
		}
		// 全局最佳扇区3
		if lap.Sector3 > 0 && lap.Sector3 < sector3Best {
			sector3Best = lap.Sector3
		}
		// 圈速最佳
		if currentBest, exists := carBestTimes[carID]; !exists || float64(lap.Time) < float64(currentBest) {
			carBestTimes[carID] = int64(lap.Time)
		}
		// 扇区1最佳
		if currentBest, exists := carBestSector1[carID]; !exists || lap.Sector1 < currentBest {
			carBestSector1[carID] = lap.Sector1
		}
		// 扇区2最佳
		if currentBest, exists := carBestSector2[carID]; !exists || lap.Sector2 < currentBest {
			carBestSector2[carID] = lap.Sector2
		}
		// 扇区3最佳
		if currentBest, exists := carBestSector3[carID]; !exists || lap.Sector3 < currentBest {
			carBestSector3[carID] = lap.Sector3
		}
	}

	// 第二遍：设置最佳标记
	for i := range allLaps {
		lap := &allLaps[i]
		carID := lap.Car_id

		// 车辆最佳标记 (每个车只有1个)
		lap.CarBest = 0
		lap.Sector1CarBest = 0
		lap.Sector2CarBest = 0
		lap.Sector3CarBest = 0

		if lap.Time == float64(carBestTimes[carID]) {
			lap.CarBest = 1
		}
		if lap.Sector1 > 0 && lap.Sector1 == carBestSector1[carID] {
			lap.Sector1CarBest = 1
		}
		if lap.Sector2 > 0 && lap.Sector2 == carBestSector2[carID] {
			lap.Sector2CarBest = 1
		}
		if lap.Sector3 > 0 && lap.Sector3 == carBestSector3[carID] {
			lap.Sector3CarBest = 1
		}

		// 计算与全局最佳的差距
		lap.Gap = int(lap.Time - float64(classBestTime))
		if classBestTime > 0 {
			lap.GapPer = float64(lap.Gap) / float64(classBestTime)
		}

		// 全局最佳标记
		lap.ClassBest = 0
		lap.Sector1ClassBest = 0
		lap.Sector2ClassBest = 0
		lap.Sector3ClassBest = 0

		if lap.Time == float64(classBestTime) {
			lap.ClassBest = 1
		}
		if lap.Sector1 == int(sector1Best) && lap.Sector1 > 0 {
			lap.Sector1ClassBest = 1
		}
		if lap.Sector2 == int(sector2Best) && lap.Sector2 > 0 {
			lap.Sector2ClassBest = 1
		}
		if lap.Sector3 == int(sector3Best) && lap.Sector3 > 0 {
			lap.Sector3ClassBest = 1
		}
	}

	// 应用层分页
	start := (page - 1) * entries
	end := start + entries
	if end > len(allLaps) {
		end = len(allLaps)
	}
	if start > len(allLaps) {
		start = len(allLaps)
	}

	// 处理查询结果
	var response struct {
		Status   string `json:"status"`
		Bestlaps struct {
			Laps    []ACBestLap `json:"laps"`
			First   int         `json:"first"`
			Prev    int         `json:"prev"`
			Next    int         `json:"next"`
			Last    int         `json:"last"`
			Page    int         `json:"page"`
			PerPage int         `json:"per_page"`
		} `json:"bestlaps"`
	}
	response.Bestlaps.Laps = allLaps[start:end]
	response.Bestlaps.Page = page
	response.Bestlaps.PerPage = entries
	response.Bestlaps.Last = (len(allLaps) + entries - 1) / entries
	response.Bestlaps.First = 1
	response.Bestlaps.Prev = 0
	if page > 1 {
		response.Bestlaps.Prev = page - 1
	}
	response.Bestlaps.Next = 0
	if page < response.Bestlaps.Last {
		response.Bestlaps.Next = page + 1
	}

	// 返回JSON响应
	sendSuccessResponse(w, "最佳圈速数据获取成功", "bestlaps", response.Bestlaps)
}

func handleACTracks(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT track_config_id, track_name, config_name, display_name, country, city, length FROM track_config")
	if err != nil {
		logger.Printf("数据库查询失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "数据库查询失败", "tracks")
		return
	}
	defer rows.Close()

	var tracks []ACTrackConfig

	for rows.Next() {
		t := ACTrackConfig{}
		if err := rows.Scan(&t.TrackConfigID, &t.TrackName, &t.ConfigName, &t.DisplayName, &t.Country, &t.City, &t.Length); err != nil {
			logger.Printf("数据解析失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "数据解析失败", "tracks")
			return
		}
		tracks = append(tracks, t)
	}

	if err := rows.Err(); err != nil {
		logger.Printf("行迭代错误: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "行迭代错误", "tracks")
		return
	}

	sendSuccessResponse(w, "赛道数据获取成功", "tracks", tracks)
}

// 处理AC会话详情请求
func handleACSession(w http.ResponseWriter, r *http.Request) {
	// 解析URL路径获取sessionId和资源类型
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(pathParts) < 4 || pathParts[2] != "session" {
		sendErrorResponse(w, http.StatusBadRequest, "无效的URL格式", "sessionDetails")
		return
	}

	sessionID := pathParts[3]
	var resourceType string
	if len(pathParts) >= 5 {
		resourceType = strings.Join(pathParts[4:], "/")
	}

	// 验证sessionID
	if sessionID == "" {
		sendErrorResponse(w, http.StatusBadRequest, "缺少sessionId参数", "sessionDetails")
		return
	}

	// 验证sessionID是否为有效整数
	sessionIDInt, err := strconv.Atoi(sessionID)
	if err != nil {
		sendErrorResponse(w, http.StatusBadRequest, "无效的sessionId格式", "sessionDetails")
		return
	}

	// 根据资源类型处理不同请求
	switch resourceType {
	case "":
		// 单个会话详情
		session := ACSession{}
		err := db.QueryRow("SELECT session_id, event_id, type, track_time, name, CAST(UNIX_TIMESTAMP(start_time) * 1000000 AS SIGNED) AS start_time, duration_min, elapsed_ms, laps, weather, air_temp, road_temp, start_grip, current_grip, is_finished, CAST(UNIX_TIMESTAMP(finish_time) * 1000000 AS SIGNED) AS finish_time, CAST(UNIX_TIMESTAMP(last_activity) * 1000000 AS SIGNED) AS last_activity FROM session WHERE session_id = ?", sessionIDInt).Scan(
			&session.SessionID, &session.EventID, &session.Type, &session.TrackTime, &session.Name, &session.StartTime, &session.DurationMin, &session.ElapsedMs, &session.Laps, &session.Weather, &session.AirTemp, &session.RoadTemp, &session.StartGrip, &session.CurrentGrip, &session.IsFinished, &session.FinishTime, &session.LastActivity)
		if err != nil {
			if err == sql.ErrNoRows {
				sendErrorResponse(w, http.StatusNotFound, "会话不存在", "sessionDetails")
				return
			}
			logger.Printf("查询会话失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("查询会话失败: %v", err), "sessionDetails")
			return
		}
		sendSuccessResponse(w, "会话详情获取成功", "session", session)

	case "live":
		// 单个会话实时数据
		liveData := ACLiveData{}
		err := db.QueryRow("SELECT session_id, timestamp, status FROM live_data WHERE session_id = ? ORDER BY timestamp DESC LIMIT 1", sessionID).Scan(
			&liveData.SessionID, &liveData.Timestamp, &liveData.Status)
		if err != nil {
			if err == sql.ErrNoRows {
				sendErrorResponse(w, http.StatusNotFound, "实时数据不存在", "sessionLive")
				return
			}
			logger.Printf("查询实时数据失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("查询实时数据失败: %v", err), "sessionLive")
			return
		}
		sendSuccessResponse(w, "实时数据获取成功", "liveData", liveData)

	case "live/positions":
		// 单个会话实时位置
		rows, err := db.Query("SELECT driver_id, driver_name, position, x, y, z FROM session_feed WHERE session_id = ? ORDER BY position", sessionID)
		if err != nil {
			logger.Printf("查询位置数据失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("查询位置数据失败: %v", err), "sessionPositions")
			return
		}
		defer rows.Close()

		var positions []ACLivePosition
		for rows.Next() {
			pos := ACLivePosition{}
			if err := rows.Scan(&pos.DriverID, &pos.DriverName, &pos.Position, &pos.X, &pos.Y, &pos.Z); err != nil {
				logger.Printf("解析位置数据失败: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, "解析位置数据失败", "sessionPositions")
				return
			}
			positions = append(positions, pos)
		}
		sendSuccessResponse(w, "位置数据获取成功", "positions", positions)

	case "result":
		// 单个会话结果
		result := ACResult{}
		err := db.QueryRow("SELECT session_id, winner_driver, winner_team, laps_completed, fastest_lap FROM results WHERE session_id = ?", sessionID).Scan(
			&result.SessionID, &result.WinnerDriver, &result.WinnerTeam, &result.LapsCompleted, &result.FastestLap)
		if err != nil {
			if err == sql.ErrNoRows {
				sendErrorResponse(w, http.StatusNotFound, "结果数据不存在", "sessionResult")
				return
			}
			logger.Printf("查询结果数据失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("查询结果数据失败: %v", err), "sessionResult")
			return
		}
		sendSuccessResponse(w, "结果数据获取成功", "result", result)

	case "result/sectors":
		// 定义分段成绩结构体

		// 查询每个分段的最佳成绩
		querySector := func(sector int) ([]SectorResult, error) {
			// 根据扇区选择不同的查询字段
			var sectorField string
			switch sector {
			case 1:
				sectorField = "sl.sector_1"
			case 2:
				sectorField = "sl.sector_2"
			case 3:
				sectorField = "sl.sector_3"
			default:
				return nil, fmt.Errorf("无效的扇区编号: %d", sector)
			}

			// 构建查询SQL
			query := fmt.Sprintf(`
				SELECT ss.user_id, ss.car_id, MIN(%s) AS best_sector_time
				FROM stint_lap sl
				JOIN session_stint ss ON sl.stint_id = ss.session_stint_id
				WHERE ss.session_id = ? AND ss.valid_laps > 0 AND %s > 0
				GROUP BY ss.user_id, ss.car_id
			`, sectorField, sectorField)

			rows, err := db.Query(query, sessionID)
			if err != nil {
				return nil, err
			}
			defer rows.Close()

			var results []SectorResult
			// 获取所有用户ID以确保即使没有成绩也显示
			type UserCarPair struct {
				UserID int
				CarID  int
			}
			var allUserIDs []UserCarPair
			rowsUserIDs, err := db.Query(`
				SELECT DISTINCT ss.user_id, ss.car_id FROM session_stint ss
				WHERE ss.session_id = ? AND ss.valid_laps > 0
			`, sessionID)
			if err != nil {
				return nil, err
			}
			defer rowsUserIDs.Close()

			// 扫描用户ID数据
			for rowsUserIDs.Next() {
				pair := UserCarPair{}
				if err := rowsUserIDs.Scan(&pair.UserID, &pair.CarID); err != nil {
					return nil, err
				}
				allUserIDs = append(allUserIDs, pair)
			}

			if err = rowsUserIDs.Err(); err != nil {
				return nil, err
			}

			// 构建用户成绩映射
			sectorMap := make(map[[2]int]int)
			for rows.Next() {
				var userID, carID, bestTime int
				if err := rows.Scan(&userID, &carID, &bestTime); err != nil {
					return nil, err
				}
				sectorMap[[2]int{userID, carID}] = bestTime
			}

			// 生成结果列表，包含所有用户
			for _, uc := range allUserIDs {
				bestTime, hasTime := sectorMap[[2]int{uc.UserID, uc.CarID}]
				if !hasTime {
					bestTime = 0
				}
				results = append(results, SectorResult{
					UserID:         uc.UserID,
					CarID:          uc.CarID,
					BestSectorTime: bestTime,
					Gap:            nil, // 初始化为nil，不包含在JSON输出中
					Interval:       nil, // 初始化为nil，不包含在JSON输出中
				})
			}
			return results, nil
		}

		// 获取三个分段的成绩
		s1, err := querySector(1)
		if err != nil {
			logger.Printf("查询sector1失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "分段成绩查询失败", "sessionSectors")
			return
		}

		s2, err := querySector(2)
		if err != nil {
			logger.Printf("查询sector2失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "分段成绩查询失败", "sessionSectors")
			return
		}

		s3, err := querySector(3)
		if err != nil {
			logger.Printf("查询sector3失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "分段成绩查询失败", "sessionSectors")
			return
		}

		// 计算Gap和Interval
		calculateGaps := func(results []SectorResult) []SectorResult {
			if len(results) == 0 {
				return results
			}
			// 按有效成绩优先并按时间排序
			sort.Slice(results, func(i, j int) bool {
				// 有效成绩排在前面
				iValid := results[i].BestSectorTime > 0
				jValid := results[j].BestSectorTime > 0
				if iValid && !jValid {
					return true
				}
				if !iValid && jValid {
					return false
				}
				// 同为有效或无效时按时间排序
				return results[i].BestSectorTime < results[j].BestSectorTime
			})

			// 计算差距（仅对有效成绩）
			bestTime := 0
			prevTime := 0
			validCount := 0
			for i, r := range results {
				if r.BestSectorTime > 0 {
					if validCount == 0 {
						bestTime = r.BestSectorTime
						// 最佳成绩不设置Gap和Interval
						r.Gap = nil
						r.Interval = nil
						prevTime = r.BestSectorTime
					} else {
						gap := r.BestSectorTime - bestTime
						interval := r.BestSectorTime - prevTime
						// 只在值大于0时设置字段
						if gap > 0 {
							r.Gap = &gap
						} else {
							r.Gap = nil
						}
						if interval > 0 {
							r.Interval = &interval
						} else {
							r.Interval = nil
						}
						prevTime = r.BestSectorTime
					}
					validCount++
				} else {
					// 无效成绩不设置Gap和Interval
					r.Gap = nil
					r.Interval = nil
				}
				results[i] = r
			}
			return results
		}

		// 处理所有分段
		// 按用户ID排序结果
		sort.Slice(s1, func(i, j int) bool { return s1[i].UserID < s1[j].UserID })
		sort.Slice(s2, func(i, j int) bool { return s2[i].UserID < s2[j].UserID })
		sort.Slice(s3, func(i, j int) bool { return s3[i].UserID < s3[j].UserID })

		response := SectorResponse{
			Sector1: calculateGaps(s1),
			Sector2: calculateGaps(s2),
			Sector3: calculateGaps(s3),
		}

		sendSuccessResponse(w, "分段成绩获取成功", "sectors", response)

	case "race/result/laps":
		// 获取事件ID
		var eventID int
		err = db.QueryRow("SELECT event_id FROM session WHERE session_id = ?", sessionID).Scan(&eventID)
		if err != nil {
			logger.Printf("查询事件ID失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "查询事件ID失败", "sessionLaps")
			return
		}

		// 检查是否为团队事件
		var isTeamEvent bool
		err = db.QueryRow("SELECT team_event FROM event WHERE event_id = ?", eventID).Scan(&isTeamEvent)
		if err != nil {
			logger.Printf("查询事件类型失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "查询事件类型失败", "sessionLaps")
			return
		}

		// 声明团队和用户圈速结构体

		var rows *sql.Rows
		var err error

		var teamLaps []TeamLapTimes
		var userLaps []UserLapTimes

		if isTeamEvent {
			// 团队事件圈速结果（按团队分组）
			rows, err = db.Query(`
				SELECT t.team_id, ss.car_id, lt.time 
				FROM stint_lap lt
				JOIN session_stint ss ON lt.stint_id = ss.session_stint_id
				JOIN team_member tm ON ss.user_id = tm.user_id
				JOIN team t ON tm.team_id = t.team_id
				WHERE ss.session_id = ? 
				ORDER BY t.team_id, ss.car_id, lt.stint_lap_id`, sessionID)
		} else {
			// 个人事件圈速结果（按用户分组）
			rows, err = db.Query(`
				SELECT ss.user_id, ss.car_id, lt.time 
				FROM stint_lap lt
				JOIN session_stint ss ON lt.stint_id = ss.session_stint_id
				WHERE ss.session_id = ? 
				ORDER BY ss.user_id, lt.stint_lap_id`, sessionID)
		}
		if err != nil {
			logger.Printf("查询圈速数据失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("查询圈速数据失败: %v", err), "sessionLaps")
			return
		}
		defer rows.Close()

		// 按用户/团队和车辆分组收集圈速（团队事件按team_id，个人事件按user_id）
		lapGroups := make(map[[2]int][]int)
		for rows.Next() {
			var id, carID, lapTime int
			scanErr := rows.Scan(&id, &carID, &lapTime)
			if scanErr != nil {
				logger.Printf("解析圈速数据失败: %v", scanErr)
				sendErrorResponse(w, http.StatusInternalServerError, "解析圈速数据失败", "sessionLaps")
				return
			}
			key := [2]int{id, carID}
			lapGroups[key] = append(lapGroups[key], lapTime)
		}

		// 转换为响应结构体
		if isTeamEvent {
			teamLaps = []TeamLapTimes{}
			for key, times := range lapGroups {
				teamLaps = append(teamLaps, TeamLapTimes{
					TeamID:   key[0],
					CarID:    key[1],
					LapTimes: times,
				})
			}
		} else {
			userLaps = []UserLapTimes{}
			for key, times := range lapGroups {
				userLaps = append(userLaps, UserLapTimes{
					UserID:   key[0],
					CarID:    key[1],
					LapTimes: times,
				})
			}
		}

		// 计算圈速稳定性并排序
		if isTeamEvent {

			var rankedLaps []rankedTeamLapData

			for _, lapData := range teamLaps {
				lapTimes := lapData.LapTimes
				if len(lapTimes) < 3 {
					rankedLaps = append(rankedLaps, rankedTeamLapData{TeamLapTimes: lapData, Stability: 0, bestLapTime: 0})
					continue
				}

				// 计算最佳圈速
				bestLapTime := lapTimes[0]
				for _, t := range lapTimes {
					if t < bestLapTime {
						bestLapTime = t
					}
				}

				// 排除最佳圈速和第一圈计算平均
				totalLapTime := 0
				count := 0
				for i, t := range lapTimes {
					if i == 0 || t == bestLapTime {
						continue
					}
					totalLapTime += t
					count++
				}

				if count == 0 {
					rankedLaps = append(rankedLaps, rankedTeamLapData{TeamLapTimes: lapData, Stability: 0, bestLapTime: 0})
					continue
				}

				avgLapTime := totalLapTime / count
				error := avgLapTime - bestLapTime
				stability := 1 - float64(error)/float64(bestLapTime)
				rankedLaps = append(rankedLaps, rankedTeamLapData{TeamLapTimes: lapData, Stability: stability, bestLapTime: bestLapTime})
			}

			// 按规则排序：圈数>=3的按稳定性降序，圈数<3的按最快圈速升序并排在最后
			sort.Slice(rankedLaps, func(i, j int) bool {
				// 分组判断：圈数>=3的排在前面
				lenI := len(rankedLaps[i].LapTimes)
				lenJ := len(rankedLaps[j].LapTimes)

				// 都满足圈数要求时按稳定性排序
				if lenI >= 3 && lenJ >= 3 {
					return rankedLaps[i].Stability > rankedLaps[j].Stability
				}

				// 只有一方满足圈数要求时，满足的排在前面
				if lenI >= 3 {
					return true
				}
				if lenJ >= 3 {
					return false
				}

				// 双方都不满足时按最快圈速升序
				return rankedLaps[i].bestLapTime < rankedLaps[j].bestLapTime
			})

			// 转换回原始结构体
			teamLaps = make([]TeamLapTimes, len(rankedLaps))
			for i, ranked := range rankedLaps {
				teamLaps[i] = ranked.TeamLapTimes
			}

			sendSuccessResponse(w, "圈速数据获取成功", "laps", teamLaps)
		} else {

			var rankedLaps []rankedLapData

			for _, lapData := range userLaps {
				lapTimes := lapData.LapTimes
				if len(lapTimes) < 3 {
					rankedLaps = append(rankedLaps, rankedLapData{UserLapTimes: lapData, Stability: 0, bestLapTime: 0})
					continue
				}

				// 计算最佳圈速
				bestLapTime := lapTimes[0]
				for _, t := range lapTimes {
					if t < bestLapTime {
						bestLapTime = t
					}
				}

				// 排除最佳圈速和第一圈计算平均
				totalLapTime := 0
				count := 0
				for i, t := range lapTimes {
					if i == 0 || t == bestLapTime {
						continue
					}
					totalLapTime += t
					count++
				}

				if count == 0 {
					rankedLaps = append(rankedLaps, rankedLapData{UserLapTimes: lapData, Stability: 0, bestLapTime: 0})
					continue
				}

				avgLapTime := totalLapTime / count
				error := avgLapTime - bestLapTime
				stability := 1 - float64(error)/float64(bestLapTime)
				rankedLaps = append(rankedLaps, rankedLapData{UserLapTimes: lapData, Stability: stability, bestLapTime: bestLapTime})
			}

			// 按规则排序：圈数>=3的按稳定性降序，圈数<3的按最快圈速升序并排在最后
			sort.Slice(rankedLaps, func(i, j int) bool {
				// 分组判断：圈数>=3的排在前面
				lenI := len(rankedLaps[i].LapTimes)
				lenJ := len(rankedLaps[j].LapTimes)

				// 都满足圈数要求时按稳定性排序
				if lenI >= 3 && lenJ >= 3 {
					return rankedLaps[i].Stability > rankedLaps[j].Stability
				}

				// 只有一方满足圈数要求时，满足的排在前面
				if lenI >= 3 {
					return true
				}
				if lenJ >= 3 {
					return false
				}

				// 双方都不满足时按最快圈速升序
				return rankedLaps[i].bestLapTime < rankedLaps[j].bestLapTime
			})

			// 转换回原始结构体
			userLaps = make([]UserLapTimes, len(rankedLaps))
			for i, ranked := range rankedLaps {
				userLaps[i] = ranked.UserLapTimes
			}
			sendSuccessResponse(w, "圈速数据获取成功", "laps", userLaps)
		}

	case "race/pitstops":
		// 单个竞速会话进站记录
		// 获取事件ID
		var eventID int
		err = db.QueryRow("SELECT event_id FROM session WHERE session_id = ?", sessionID).Scan(&eventID)
		if err != nil {
			logger.Printf("查询事件ID失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "查询事件ID失败", "sessionPitstops")
			return
		}

		// 检查是否为团队事件
		var isTeamEvent bool
		err = db.QueryRow("SELECT team_event FROM event WHERE event_id = ?", eventID).Scan(&isTeamEvent)
		if err != nil {
			logger.Printf("查询事件类型失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "查询事件类型失败", "sessionPitstops")
			return
		}

		// 根据事件类型执行不同的查询
		var rows *sql.Rows
		if isTeamEvent {
			// 团队事件：查询team_id, car_id以及stint信息
			rows, err = db.Query(`
				SELECT t.team_id, ss.car_id, ss.user_id, CAST(UNIX_TIMESTAMP(ss.started_at) * 1000000 AS SIGNED) AS started_at, CAST(UNIX_TIMESTAMP(ss.finished_at) * 1000000 AS SIGNED) AS finished_at 
				FROM session_stint ss
				JOIN team_member tm ON ss.user_id = tm.user_id
				JOIN team t ON tm.team_id = t.team_id
				WHERE ss.session_id = ? 
				ORDER BY t.team_id, ss.car_id, ss.started_at`, sessionID)
		} else {
			// 个人事件：查询user_id, car_id以及stint信息
			rows, err = db.Query(`
				SELECT user_id, car_id, NULL as team_id, CAST(UNIX_TIMESTAMP(started_at) * 1000000 AS SIGNED) AS started_at, CAST(UNIX_TIMESTAMP(finished_at) * 1000000 AS SIGNED) AS finished_at 
				FROM session_stint 
				WHERE session_id = ? 
				ORDER BY user_id, car_id, started_at`, sessionID)
		}

		if err != nil {
			logger.Printf("查询进站数据失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("查询进站数据失败: %v", err), "sessionPitstops")
			return
		}
		defer rows.Close()

		// 存储stint信息，包含可能的team_id

		var stints []StintInfo
		for rows.Next() {
			s := StintInfo{}
			var startedAt, finishedAt TimeWithTimezone
			if isTeamEvent {
				// 团队事件: 扫描team_id, car_id, user_id, started_at, finished_at
				if err := rows.Scan(&s.TeamID, &s.CarID, &s.UserID, &startedAt, &finishedAt); err != nil {
					logger.Printf("解析stint数据失败: %v", err)
					sendErrorResponse(w, http.StatusInternalServerError, "解析stint数据失败", "sessionPitstops")
					return
				}
			} else {
				// 个人事件: 扫描user_id, car_id, team_id(NULL), started_at, finished_at
				if err := rows.Scan(&s.UserID, &s.CarID, &s.TeamID, &startedAt, &finishedAt); err != nil {
					logger.Printf("解析stint数据失败: %v", err)
					sendErrorResponse(w, http.StatusInternalServerError, "解析stint数据失败", "sessionPitstops")
					return
				}
			}

			stints = append(stints, s)
		}

		// 计算pitstops次数

		pitstops := make([]Pitstop, 0)

		// 创建用于跟踪前一个stint的map，键为[team_id, car_id]或[user_id, car_id]
		type StintKey struct {
			ID    sql.NullInt64
			CarID int64
		}
		prevStints := make(map[StintKey]*StintInfo)

		for _, s := range stints {
			var key StintKey
			if isTeamEvent {
				// 团队事件使用team_id作为键
				key = StintKey{ID: s.TeamID, CarID: s.CarID}
			} else {
				// 个人事件使用user_id作为键
				key = StintKey{ID: sql.NullInt64{Int64: s.UserID, Valid: true}, CarID: s.CarID}
			}

			if prevStint, exists := prevStints[key]; exists {
				// 计算进站时间 (当前stint开始时间 - 上一个stint结束时间)
				pitTime := (int64(s.StartedAt) - int64(prevStint.FinishedAt)) / 1e6 // 微秒转毫秒
				// 忽略超过10分钟(600000毫秒)的进站
				if pitTime > 0 && pitTime <= 600000 {
					pitstop := Pitstop{
						CarID:   s.CarID,
						PitTime: pitTime,
					}
					if isTeamEvent && s.TeamID.Valid {
						teamID := s.TeamID.Int64
						pitstop.TeamID = &teamID
					} else if !isTeamEvent {
						userID := s.UserID
						pitstop.UserID = &userID
					}
					pitstops = append(pitstops, pitstop)
				}
			}
			// 更新前一个stint
			prevStints[key] = &s
		}
		sendSuccessResponse(w, "进站数据获取成功", "pitstops", pitstops)

	case "result/stints":
		// 单个会话连续驾驶时段结果
		rows, err := db.Query("SELECT id, driver_id, CAST(UNIX_TIMESTAMP(start_time) * 1000000 AS SIGNED) AS start_time, CAST(UNIX_TIMESTAMP(end_time) * 1000000 AS SIGNED) AS end_time, duration FROM stints WHERE session_id = ?", sessionID)
		if err != nil {
			logger.Printf("查询连续驾驶数据失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("查询连续驾驶数据失败: %v", err), "sessionStints")
			return
		}
		defer rows.Close()

		var stints []ACStint
		for rows.Next() {
			stint := ACStint{}
			if err := rows.Scan(&stint.ID, &stint.DriverID, &stint.StartTime, &stint.EndTime, &stint.Duration); err != nil {
				logger.Printf("解析连续驾驶数据失败: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, "解析连续驾驶数据失败", "sessionStints")
				return
			}
			stints = append(stints, stint)
		}
		sendSuccessResponse(w, "连续驾驶数据获取成功", "stints", stints)

	case "result/stints/driver":
		// 单个会话车手连续驾驶结果
		rows, err := db.Query("SELECT s.id, d.name, CAST(UNIX_TIMESTAMP(s.start_time) * 1000000 AS SIGNED) AS start_time, CAST(UNIX_TIMESTAMP(s.end_time) * 1000000 AS SIGNED) AS end_time, s.duration FROM session_stint s JOIN drivers d ON s.driver_id = d.id WHERE s.session_id = ?", sessionID)
		if err != nil {
			logger.Printf("查询车手连续驾驶数据失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("查询车手连续驾驶数据失败: %v", err), "sessionDriverStints")
			return
		}
		defer rows.Close()

		var driverStints []ACDriverStint
		for rows.Next() {
			stint := ACDriverStint{}
			if err := rows.Scan(&stint.ID, &stint.DriverName, &stint.StartTime, &stint.EndTime, &stint.Duration); err != nil {
				logger.Printf("解析车手连续驾驶数据失败: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, "解析车手连续驾驶数据失败", "sessionDriverStints")
				return
			}
			driverStints = append(driverStints, stint)
		}
		sendSuccessResponse(w, "车手连续驾驶数据获取成功", "driverStints", driverStints)

	default:
		// 处理带参数的资源类型
		switch {
		case strings.HasPrefix(resourceType, "result/stints/team/"):
			// 单个会话团队连续驾驶结果
			teamID := strings.TrimPrefix(resourceType, "result/stints/team/")
			// 先获取团队信息
			var tTeamID, tCarID int
			err = db.QueryRow("SELECT team_id, car_id FROM team WHERE team_id = ?", teamID).Scan(&tTeamID, &tCarID)
			if err != nil {
				logger.Printf("查询团队信息失败: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, "查询团队信息失败", "sessionTeamStints")
				return
			}

			// 获取用户列表
			userRows, err := db.Query("SELECT user_id FROM team_member WHERE team_id = ?", teamID)
			if err != nil {
				logger.Printf("查询团队成员失败: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, "查询团队成员失败", "sessionTeamStints")
				return
			}
			var userIDs []int
			for userRows.Next() {
				var userID int
				if err := userRows.Scan(&userID); err != nil {
					logger.Printf("解析团队成员失败: %v", err)
					sendErrorResponse(w, http.StatusInternalServerError, "解析团队成员失败", "sessionTeamStints")
					return
				}
				userIDs = append(userIDs, userID)
			}
			userRows.Close()

			// 为每个用户获取圈速数据
			var teamStints []ACTeamStint
			for _, userID := range userIDs {
				// 获取圈速列表
				lapRows, err := db.Query(`SELECT l.stint_lap_id, l.time, l.sector_1, l.sector_2, l.sector_3, l.grip, l.tyre, l.avg_speed, l.max_speed, l.cuts, l.crashes, l.car_crashes, CAST(UNIX_TIMESTAMP(l.finished_at) * 1000000 AS SIGNED) AS finished_at FROM stint_lap l JOIN session_stint s ON l.stint_id = s.session_stint_id JOIN team_member tm1 ON s.team_member_id = tm1.team_member_id JOIN team t ON tm1.team_id = t.team_id WHERE s.session_id = ? AND t.team_id = ? AND tm1.user_id = ? ORDER BY l.stint_lap_id`, sessionID, teamID, userID)
				if err != nil {
					logger.Printf("查询圈速数据失败: %v", err)
					sendErrorResponse(w, http.StatusInternalServerError, "查询圈速数据失败", "sessionTeamStints")
					return
				}

				var laps []Lap
				totalLaps := 0
				validLaps := 0
				var bestLapTime int64 = 9999999999
				var totalLapTime int64 = 0
				var totalGap int64 = 0

				for lapRows.Next() {
					lap := Lap{}
					if err := lapRows.Scan(&lap.LapID, &lap.LapTime, &lap.Sector1, &lap.Sector2, &lap.Sector3, &lap.Grip, &lap.Tyre, &lap.AvgSpeed, &lap.MaxSpeed, &lap.Cuts, &lap.Crashes, &lap.CarCrashes, &lap.FinishAt); err != nil {
						logger.Printf("解析圈速数据失败: %v", err)
						sendErrorResponse(w, http.StatusInternalServerError, "解析圈速数据失败", "sessionTeamStints")
						return
					}

					laps = append(laps, lap)
					totalLaps++

					// 判断有效圈：无碰撞且无切弯
					isValid := lap.Crashes == 0 && lap.CarCrashes == 0 && lap.Cuts == 0
					if isValid {
						validLaps++
						totalLapTime += lap.LapTime

						// 计算圈速差距（与最佳圈速的差值）
						if bestLapTime == 0 || lap.LapTime < bestLapTime {
							bestLapTime = lap.LapTime
						}
						lapGap := lap.LapTime - bestLapTime
						totalGap += lapGap
					}
				}
				lapRows.Close()

				// 计算平均圈速和平均差距
				var avgLapTime, avgLapGap int64
				if validLaps > 0 {
					avgLapTime = totalLapTime / int64(validLaps)
					avgLapGap = totalGap / int64(validLaps)
				} else {
					bestLapTime = 0 // 无有效圈时重置最佳圈速
				}

				// 标记最佳圈速
				for i := range laps {
					if laps[i].LapTime == bestLapTime && laps[i].Cuts == 0 && laps[i].Crashes == 0 && laps[i].CarCrashes == 0 {
						laps[i].BestLap = 1
					}
				}

				// 创建stint数据
				stint := ACTeamStint{
					UserID:      userID,
					TotalLaps:   totalLaps,
					ValidLaps:   validLaps,
					BestLapTime: bestLapTime,
					AvgLapTime:  avgLapTime,
					AvgLapGap:   avgLapGap,
					Laps:        laps,
				}
				// 只添加有圈速数据的stint
				if totalLaps > 0 {
					teamStints = append(teamStints, stint)
				}
			}

			// 构建响应结构
			response := struct {
				TeamID int           `json:"team_id"`
				CarID  int           `json:"car_id"`
				Stints []ACTeamStint `json:"stints"`
			}{TeamID: tTeamID, CarID: tCarID, Stints: teamStints}
			sendSuccessResponse(w, "success", "stints", response)

		case strings.HasPrefix(resourceType, "result/stints/user/"):
			// 单个会话用户+车辆连续驾驶结果
			userCarParts := strings.Split(strings.TrimPrefix(resourceType, "result/stints/user/"), "/car/")
			if len(userCarParts) != 2 {
				sendErrorResponse(w, http.StatusBadRequest, "无效的用户车辆参数格式", "sessionUserCarStints")
				return
			}
			userId := userCarParts[0]
			carId := userCarParts[1]

			// 转换为整数
			userIdInt, err := strconv.Atoi(userId)
			if err != nil {
				logger.Printf("无效的用户ID: %v", err)
				sendErrorResponse(w, http.StatusBadRequest, "无效的用户ID格式", "sessionUserCarStints")
				return
			}

			carIdInt, err := strconv.Atoi(carId)
			if err != nil {
				logger.Printf("无效的车辆ID: %v", err)
				sendErrorResponse(w, http.StatusBadRequest, "无效的车辆ID格式", "sessionUserCarStints")
				return
			}

			// 使用全局定义的ACTeamStint结构体
			var stints []ACTeamStint

			// 查询用户圈速数据
			lapRows, err := db.Query(`SELECT l.stint_lap_id, l.time, l.sector_1, l.sector_2, l.sector_3, l.grip, l.tyre, l.avg_speed, l.max_speed, l.cuts, l.crashes, l.car_crashes, CAST(UNIX_TIMESTAMP(l.finished_at) * 1000000 AS SIGNED) AS finished_at FROM stint_lap l JOIN session_stint s ON l.stint_id = s.session_stint_id WHERE s.session_id = ? AND s.user_id = ? AND s.car_id = ? ORDER BY l.stint_lap_id`, sessionID, userId, carId)
			if err != nil {
				logger.Printf("查询用户车辆圈速数据失败: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("查询用户车辆圈速数据失败: %v", err), "sessionUserCarStints")
				return
			}
			defer lapRows.Close()

			var laps []Lap
			var totalLaps, validLaps int
			var bestLapTime, totalLapTime, totalGap int64 = 0, 0, 0

			for lapRows.Next() {
				lap := Lap{}
				if err := lapRows.Scan(&lap.LapID, &lap.LapTime, &lap.Sector1, &lap.Sector2, &lap.Sector3, &lap.Grip, &lap.Tyre, &lap.AvgSpeed, &lap.MaxSpeed, &lap.Cuts, &lap.Crashes, &lap.CarCrashes, &lap.FinishAt); err != nil {
					logger.Printf("解析圈速数据失败: %v", err)
					sendErrorResponse(w, http.StatusInternalServerError, "解析圈速数据失败", "sessionUserCarStints")
					return
				}

				laps = append(laps, lap)
				totalLaps++

				// 判断有效圈：无碰撞且无切弯
				isValid := lap.Crashes == 0 && lap.CarCrashes == 0 && lap.Cuts == 0
				if isValid {
					validLaps++
					totalLapTime += lap.LapTime

					// 计算圈速差距（与最佳圈速的差值）
					if bestLapTime == 0 || lap.LapTime < bestLapTime {
						bestLapTime = lap.LapTime
					}
					lapGap := lap.LapTime - bestLapTime
					totalGap += lapGap
				}
			}

			// 计算平均圈速和差距
			var avgLapTime, avgLapGap int64
			if validLaps > 0 {
				avgLapTime = totalLapTime / int64(validLaps)
				avgLapGap = totalGap / int64(validLaps)
			} else {
				bestLapTime = 0 // 无有效圈时重置最佳圈速
			}

			// 标记最佳圈速
			for i := range laps {
				if laps[i].LapTime == bestLapTime && laps[i].Cuts == 0 && laps[i].Crashes == 0 && laps[i].CarCrashes == 0 {
					laps[i].BestLap = 1
				}
			}

			// 创建stint数据
			stint := ACTeamStint{
				UserID:      userIdInt,
				TotalLaps:   totalLaps,
				ValidLaps:   validLaps,
				BestLapTime: bestLapTime,
				AvgLapTime:  avgLapTime,
				AvgLapGap:   avgLapGap,
				Laps:        laps,
			}
			stints = append(stints, stint)

			// 构建响应结构
			response := struct {
				UserID int           `json:"user_id"`
				CarID  int           `json:"car_id"`
				Stints []ACTeamStint `json:"stints"`
			}{UserID: userIdInt, CarID: carIdInt, Stints: stints}
			sendSuccessResponse(w, "success", "stints", response)

		case strings.Contains(resourceType, "/result/standings"):
			// 获取赛事团队属性
			var eventID int
			if err := db.QueryRow("SELECT event_id FROM session WHERE session_id = ?", sessionID).Scan(&eventID); err != nil {
				logger.Printf("获取赛事ID失败: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, "获取赛事信息失败", "event")
				return
			}
			var isTeamEvent bool
			if err := db.QueryRow("SELECT team_event FROM event WHERE event_id = ?", eventID).Scan(&isTeamEvent); err != nil {
				logger.Printf("获取团队赛事属性失败: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, "获取赛事类型失败", "event")
				return
			}
			// 单个会话指定类型的排名
			sessionTypeStr := strings.Split(resourceType, "/")[0]
			var sessionType int
			switch sessionTypeStr {
			case "race":
				sessionType = 1
			case "qualifying":
				sessionType = 2
			default:
				sessionType = 0
			}
			var query string
			if sessionType == 1 {
				if isTeamEvent {
					query = "SELECT ss.session_stint_id, tm.team_id, tm.user_id, t.car_id, ss.laps, ss.valid_laps, COALESCE(SUM(sl.time), 0) as total_time FROM session_stint ss JOIN session s ON ss.session_id = s.session_id JOIN team_member tm ON ss.team_member_id = tm.team_member_id JOIN team t ON tm.team_id = t.team_id LEFT JOIN stint_lap sl ON ss.session_stint_id = sl.stint_id WHERE ss.session_id = ? AND ss.laps > 0 AND ss.valid_laps > 0 GROUP BY ss.session_stint_id, tm.team_id, tm.user_id, t.car_id, ss.laps, ss.valid_laps ORDER BY ss.laps DESC, total_time ASC"
				} else {
					query = "SELECT ss.session_stint_id, ss.user_id, ss.car_id, ss.laps, ss.valid_laps, COALESCE(SUM(sl.time), 0) as total_time FROM session_stint ss JOIN session s ON ss.session_id = s.session_id LEFT JOIN stint_lap sl ON ss.session_stint_id = sl.stint_id WHERE ss.session_id = ? AND ss.laps > 0 AND ss.valid_laps > 0 GROUP BY ss.session_stint_id, ss.user_id, ss.car_id, ss.laps, ss.valid_laps ORDER BY ss.laps DESC, total_time ASC"
				}
			} else {
				if isTeamEvent {
					query = "SELECT tm.team_id, tm.user_id, t.car_id, COALESCE(sl.time, 0) AS best_lap_time, COALESCE(sl.sector_1, 0) AS sector_1, COALESCE(sl.sector_2, 0) AS sector_2, COALESCE(sl.sector_3, 0) AS sector_3, ss.valid_laps, ss.best_lap_id FROM session_stint ss JOIN stint_lap sl ON ss.best_lap_id = sl.stint_lap_id JOIN team_member tm ON ss.team_member_id = tm.team_member_id JOIN team t ON tm.team_id = t.team_id WHERE ss.session_id = ? AND ss.valid_laps > 0 GROUP BY tm.team_id, tm.user_id, t.car_id, ss.valid_laps, ss.best_lap_id ORDER BY sl.time ASC"
				} else {
					query = "SELECT ss.user_id, ss.car_id, COALESCE(sl.time, 0) AS best_lap_time, COALESCE(sl.sector_1, 0) AS sector_1, COALESCE(sl.sector_2, 0) AS sector_2, COALESCE(sl.sector_3, 0) AS sector_3, ss.valid_laps, ss.best_lap_id FROM session_stint ss JOIN stint_lap sl ON ss.best_lap_id = sl.stint_lap_id WHERE ss.session_id = ? AND ss.valid_laps > 0 ORDER BY sl.time ASC"
				}
			}
			rows, err := db.Query(query, sessionID)
			if err != nil {
				logger.Printf("查询排名数据失败: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("查询排名数据失败: %v", err), "sessionStandings")
				return
			}
			defer rows.Close()

			var standings []ACStanding
			for rows.Next() {
				standing := ACStanding{}
				if sessionTypeStr == "race" {
					if isTeamEvent {
						if err := rows.Scan(&standing.SessionStintID, &standing.TeamID, &standing.UserID, &standing.CarID, &standing.Laps, &standing.ValidLaps, &standing.TotalTime); err != nil {
							logger.Printf("解析团队排名数据失败: %v", err)
							sendErrorResponse(w, http.StatusInternalServerError, "解析排名数据失败", "sessionStandings")
							return
						}
					} else {
						if err := rows.Scan(&standing.SessionStintID, &standing.UserID, &standing.CarID, &standing.Laps, &standing.ValidLaps, &standing.TotalTime); err != nil {
							logger.Printf("解析个人排名数据失败: %v", err)
							sendErrorResponse(w, http.StatusInternalServerError, "解析排名数据失败", "sessionStandings")
							return
						}
					}
				} else {
					if isTeamEvent {
						if err := rows.Scan(&standing.TeamID, &standing.UserID, &standing.CarID, &standing.BestLapTime, &standing.Sector1, &standing.Sector2, &standing.Sector3, &standing.ValidLaps, &standing.LapID); err != nil {
							logger.Printf("解析团队排位数据失败: %v", err)
							sendErrorResponse(w, http.StatusInternalServerError, "解析排名数据失败", "sessionStandings")
							return
						}
					} else {
						if err := rows.Scan(&standing.UserID, &standing.CarID, &standing.BestLapTime, &standing.Sector1, &standing.Sector2, &standing.Sector3, &standing.ValidLaps, &standing.LapID); err != nil {
							logger.Printf("解析个人排位数据失败: %v", err)
							sendErrorResponse(w, http.StatusInternalServerError, "解析排名数据失败", "sessionStandings")
							return
						}
					}
				}
				standings = append(standings, standing)
			}

			if len(standings) > 0 {
				if sessionTypeStr == "race" {
					firstTime := standings[0].TotalTime
					firstLaps := standings[0].Laps
					prevTime := firstTime
					prevLaps := firstLaps
					standings[0].Gap = 0
					standings[0].Interval = 0
					for i := 1; i < len(standings); i++ {
						currentLaps := standings[i].Laps
						currentTime := standings[i].TotalTime

						// 计算Gap: 圈数相同用时间差，不同用圈数差
						if currentLaps == firstLaps {
							standings[i].Gap = (currentTime - firstTime) * 2
						} else {
							standings[i].Gap = (firstLaps-currentLaps)*2 + 1
						}

						// 计算Interval: 圈数相同用时间差，不同用圈数差
						if i == 1 {
							// 第二名特殊处理，与第一名比较
							if currentLaps == firstLaps {
								standings[i].Interval = (currentTime - firstTime) * 2
							} else {
								standings[i].Interval = (firstLaps-currentLaps)*2 + 1
							}
						} else {
							// 与前一名比较
							if currentLaps == prevLaps {
								standings[i].Interval = (currentTime - prevTime) * 2
							} else {
								standings[i].Interval = (prevLaps-currentLaps)*2 + 1
							}
						}

						prevTime = currentTime
						prevLaps = currentLaps
					}
				} else {
					firstBestTime := standings[0].BestLapTime
					prevBestTime := firstBestTime
					standings[0].Gap = 0
					standings[0].Interval = 0
					for i := 1; i < len(standings); i++ {
						currentBestTime := standings[i].BestLapTime
						// 直接使用最佳圈速时间差计算
						standings[i].Gap = currentBestTime - firstBestTime
						standings[i].Interval = currentBestTime - prevBestTime
						prevBestTime = currentBestTime
					}
				}
			}
			sendSuccessResponse(w, "排名数据获取成功", "standings", standings)

		default:
			sendErrorResponse(w, http.StatusNotFound, "未知的会话资源类型", "sessionDetails")
			return
		}
	}
}

// 处理AC事件详情请求
func handleACEventDetails(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(pathParts) < 4 || pathParts[2] != "event" {
		logger.Printf("无效的URL格式: %s", r.URL.Path)
		sendErrorResponse(w, http.StatusBadRequest, "无效的URL格式", "events")
		return
	}
	eventID := pathParts[3]
	resourceType := ""
	// 正确提取资源类型，例如"session/latest"或"cars"
	if len(pathParts) >= 5 {
		// 如果路径包含更多部分（如"session/latest"），将它们连接起来
		if len(pathParts) >= 6 {
			resourceType = pathParts[4] + "/" + pathParts[5]
		} else {
			// 单一资源类型（如"cars"）
			resourceType = pathParts[4]
		}
	}
	if eventID == "" {
		logger.Printf("缺少event_id参数")
		sendErrorResponse(w, http.StatusBadRequest, "缺少event_id参数", "events")
		return
	}

	// 根据资源类型返回对应数据
	switch resourceType {

	case "session/latest":
		// 将eventID转换为整数
		id, err := strconv.Atoi(eventID)
		if err != nil {
			logger.Printf("无效的事件ID: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "无效的事件ID", "session")
			return
		}
		// 直接查询最新会话
		Session := ACSession{}

		// 直接查询并扫描最新会话
		err = db.QueryRow(`SELECT session_id, event_id, name, type, CAST(UNIX_TIMESTAMP(start_time) * 1000000 AS SIGNED) AS start_time, CAST(UNIX_TIMESTAMP(finish_time) * 1000000 AS SIGNED) AS finish_time, is_finished, duration_min, weather, air_temp, road_temp, track_time, elapsed_ms, laps, start_grip, current_grip, CAST(UNIX_TIMESTAMP(last_activity) * 1000000 AS SIGNED) AS last_activity, http_port 
		FROM session WHERE event_id = ? ORDER BY start_time DESC LIMIT 1`, id).Scan(
			&Session.SessionID, &Session.EventID, &Session.Name, &Session.Type,
			&Session.StartTime, &Session.FinishTime, &Session.IsFinished, &Session.DurationMin,
			&Session.Weather, &Session.AirTemp, &Session.RoadTemp, &Session.TrackTime,
			&Session.ElapsedMs, &Session.Laps, &Session.StartGrip, &Session.CurrentGrip,
			&Session.LastActivity, &Session.HttpPort)

		// Convert session type from integer to string
		if err != nil {
			if err == sql.ErrNoRows {
				sendErrorResponse(w, http.StatusNotFound, "未找到会话数据", "session")
			} else {
				logger.Printf("会话查询失败: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, "会话查询失败", "session")
			}
			return
		}

		sendSuccessResponse(w, "事件会话数据获取成功", "session", Session)
		return
	case "sessions":
		// 查询事件会话数据
		rows, err := db.Query("SELECT session_id, event_id, type, track_time, name, CAST(UNIX_TIMESTAMP(start_time) * 1000000 AS SIGNED) AS start_time, duration_min, elapsed_ms, laps, weather, air_temp, road_temp, start_grip, current_grip, is_finished, CAST(UNIX_TIMESTAMP(finish_time) * 1000000 AS SIGNED) AS finish_time, CAST(UNIX_TIMESTAMP(last_activity) * 1000000 AS SIGNED) AS last_activity, http_port FROM session WHERE event_id = ? AND NOT (current_grip = -1 AND is_finished = 1) ORDER BY session_id DESC", eventID)
		if err != nil {
			logger.Printf("会话查询失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "会话数据查询失败", "sessions")
			return
		}
		defer rows.Close()

		sessions := []ACSession{}
		for rows.Next() {
			s := ACSession{}
			if err := rows.Scan(&s.SessionID, &s.EventID, &s.Type, &s.TrackTime, &s.Name, &s.StartTime, &s.DurationMin, &s.ElapsedMs, &s.Laps, &s.Weather, &s.AirTemp, &s.RoadTemp, &s.StartGrip, &s.CurrentGrip, &s.IsFinished, &s.FinishTime, &s.LastActivity, &s.HttpPort); err != nil {
				logger.Printf("会话行扫描失败: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, "会话数据解析失败", "sessions")
				return
			}
			sessions = append(sessions, s)
		}

		if err = rows.Err(); err != nil {
			logger.Printf("会话行迭代错误: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "会话数据处理失败", "sessions")
			return
		}

		sendSuccessResponse(w, "事件会话数据获取成功", "sessions", sessions)
		return
	case "cars":
		// 查询事件车辆数据
		carRows, err := db.Query("SELECT DISTINCT c.car_id, c.name, c.display_name, c.manufacturer, c.car_class FROM session_stint ss JOIN session s ON ss.session_id = s.session_id JOIN car c ON ss.car_id = c.car_id WHERE s.event_id = ?", eventID)
		if err != nil {
			logger.Printf("车辆查询失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "车辆数据查询失败", "vehicles")
			return
		}
		defer carRows.Close()

		var cars []ACCar
		for carRows.Next() {
			c := ACCar{}
			if err := carRows.Scan(&c.CarID, &c.Name, &c.DisplayName, &c.Manufacturer, &c.CarClass); err != nil {
				logger.Printf("车辆行扫描失败: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, "车辆数据解析失败", "vehicles")
				return
			}
			cars = append(cars, c)
		}

		if err = carRows.Err(); err != nil {
			logger.Printf("车辆行迭代错误: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "车辆数据处理失败", "vehicles")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"status": "success", "cars": cars})
		return
	}

	// 查询事件基本信息（包含完整元数据）
	event := ACEventResponse{}
	row := db.QueryRow(`SELECT event_id, name, server_name, track_config_id, team_event, active, livery_preview, use_number, practice_duration, quali_duration, race_duration, race_duration_type, race_wait_time, race_extra_laps, reverse_grid_positions FROM event WHERE event_id = ?`, eventID)
	err := row.Scan(&event.EventID, &event.Name, &event.ServerName, &event.TrackConfigID, &event.TeamEvent, &event.Active, &event.LiveryPreview, &event.UseNumber, &event.PracticeDuration, &event.QualiDuration, &event.RaceDuration, &event.RaceDurationType, &event.RaceWaitTime, &event.RaceExtraLaps, &event.ReverseGridPositions)
	if err != nil {
		if err == sql.ErrNoRows {
			logger.Printf("事件不存在: %v", err)
			sendErrorResponse(w, http.StatusNotFound, "事件不存在", "events")
			return
		} else {
			logger.Printf("扫描事件详情失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "扫描事件详情失败", "events")
			return
		}
	}

	sendSuccessResponse(w, "事件详情获取成功", "event", event)
}

// 处理AC事件列表请求
func handleACEvents(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) >= 7 && pathParts[5] == "session" && pathParts[6] == "latest" {
		eventIDsStr := pathParts[4]
		eventIDs := strings.Split(eventIDsStr, ",")

		if len(eventIDs) == 0 || eventIDs[0] == "" {
			sendErrorResponse(w, http.StatusBadRequest, "事件ID不能为空", "events")
			return
		}

		for _, id := range eventIDs {
			if _, err := strconv.Atoi(id); err != nil {
				sendErrorResponse(w, http.StatusBadRequest, "无效的事件ID格式", "events")
				return
			}
		}
		// 从WebSocket获取实时会话数据
		liveEventsMutex.Lock()
		defer liveEventsMutex.Unlock()

		var sessions []ACSession
		// 过滤匹配当前eventIDs的会话
		if liveEventsData.Session.EventID > 0 {
			for _, idStr := range eventIDs {
				id, _ := strconv.Atoi(idStr)
				if liveEventsData.Session.EventID == id {
					sessions = append(sessions, liveEventsData.Session)
					break
				}
			}
		}
		sendSuccessResponse(w, "最新事件会话获取成功", "sessions", sessions)
		return
	}

	// 原handleACEvents函数逻辑

	rows, err := db.Query(`SELECT event_id, name, server_name, track_config_id, practice_duration, quali_duration, race_duration, race_duration_type, race_extra_laps, reverse_grid_positions FROM event ORDER BY event_id DESC`)
	if err != nil {
		logger.Printf("AC事件查询SQL错误: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "数据库查询失败", "events")
		return
	}
	defer rows.Close()

	events := []ACEvent{}
	for rows.Next() {
		e := ACEvent{}
		if err := rows.Scan(&e.EventID, &e.Name, &e.ServerName, &e.TrackConfigID, &e.PracticeDuration, &e.QualiDuration, &e.RaceDuration, &e.RaceDurationType, &e.RaceExtraLaps, &e.ReverseGridPositions); err != nil {
			logger.Printf("数据库行扫描失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "数据解析失败", "events")
			return
		}
		events = append(events, e)
	}

	if err = rows.Err(); err != nil {
		logger.Printf("行迭代错误: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "行迭代错误", "events")
		return
	}

	sendSuccessResponse(w, "AC事件数据获取成功", "events", events)
}

// 定义WebSocket消息结构体
type WebSocketMessage struct {
	Version           uint8     `json:"version"`
	BroadcastInterval int32     `json:"broadcastInterval"`
	ViewerCount       int32     `json:"viewerCount"`
	EventID           int       `json:"event_id"`
	Session           ACSession `json:"session"`
	Entries           []Entry   `json:"entries"`
}

// 存储从WebSocket接收的事件数据
var liveEventsData WebSocketMessage
var liveEventsMutex sync.Mutex

// 从WebSocket接收的事件数据中获取活跃事件
func getLiveEventsFromWebSocket() (WebSocketMessage, error) {
	liveEventsMutex.Lock()
	defer liveEventsMutex.Unlock()
	return liveEventsData, nil
}

// 处理AC活跃事件请求
func handleACEventsLive(w http.ResponseWriter, r *http.Request) {
	// 从WebSocket获取活跃事件数据
	events, err := getLiveEventsFromWebSocket()
	if err != nil {
		logger.Printf("获取活跃事件失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "获取事件数据失败", "events")
		return
	}

	// 直接返回WebSocket接收的事件数据
	sendSuccessResponse(w, "AC活跃事件数据获取成功", "events", events)
}
