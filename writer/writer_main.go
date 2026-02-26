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
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"gosimview/config"
	"gosimview/kn5conv"
	"gosimview/udp"

	"github.com/go-ini/ini"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"
)

// 计算单圈最大速度
func calculateMaxSpeed(laps []udp.CarUpdate) float64 {
	var maxSpeed float64 = 0
	for _, lap := range laps {
		// 将Velocity从m/s转换为km/h
		// 计算速度向量模长(m/s)并转换为km/h
		velocityMagnitude := math.Sqrt(float64(lap.Velocity.X*lap.Velocity.X + lap.Velocity.Y*lap.Velocity.Y + lap.Velocity.Z*lap.Velocity.Z))
		currentSpeed := velocityMagnitude * 3.6
		if currentSpeed > maxSpeed {
			maxSpeed = currentSpeed
		}
	}
	return maxSpeed
}

// boolToInt 将布尔值转换为整数，true 转换为 1，false 转换为 0
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// generateLapTelemetryFile 创建符合LapTelemetry格式的二进制文件
func generateLapTelemetry(carID int, trackLength int32, telemetryData []udp.CarUpdate) ([]byte, error) {
	// 使用内存缓冲区替代文件存储
	var buffer bytes.Buffer

	// 写入版本号 (1)，小端序
	version := int32(1)
	if err := binary.Write(&buffer, binary.LittleEndian, version); err != nil {
		return nil, fmt.Errorf("写入版本号失败: %v", err)
	}

	// 写入赛道长度，使用小端序
	length := trackLength
	if err := binary.Write(&buffer, binary.LittleEndian, length); err != nil {
		return nil, fmt.Errorf("写入赛道长度失败: %v", err)
	}

	// 写入每条遥测记录
	for _, data := range telemetryData {
		// 写入NSP (NormalisedSplinePos) - float32
		if err := binary.Write(&buffer, binary.LittleEndian, data.NormalisedSplinePos); err != nil {
			return nil, fmt.Errorf("写入NSP失败: %v", err)
		}

		// 写入档位 (加1后存储，与前端解析对应)
		gear := uint8(data.Gear + 1)
		if err := binary.Write(&buffer, binary.LittleEndian, gear); err != nil {
			return nil, fmt.Errorf("写入档位失败: %v", err)
		}

		// 写入速度 - float32
		if err := binary.Write(&buffer, binary.LittleEndian, data.Velocity); err != nil {
			return nil, fmt.Errorf("写入速度失败: %v", err)
		}

		// 写入X坐标 - float32
		if err := binary.Write(&buffer, binary.LittleEndian, data.Pos.X); err != nil {
			return nil, fmt.Errorf("写入X坐标失败: %v", err)
		}

		// 写入Z坐标 - float32
		if err := binary.Write(&buffer, binary.LittleEndian, data.Pos.Z); err != nil {
			return nil, fmt.Errorf("写入Z坐标失败: %v", err)
		}
	}

	logger.Printf("成功生成遥测数据: 车辆 %d，共包含 %d 条记录", carID, len(telemetryData))
	return buffer.Bytes(), nil
}

// ACSession 会话信息结构体
type ACSession struct {
	SessionID   int64   `json:"session_id"`
	ElapsedMs   int64   `json:"elapsed_ms"`
	DurationMin int     `json:"duration_min"`
	Type        int     `json:"type"`
	StartGrip   float64 `json:"start_grip"`
	CurrentGrip float64 `json:"current_grip"`
}

// Entry 排行榜条目结构体
type Entry struct {
	TeamID        uint32  `json:"team_id"`
	UserID        uint32  `json:"user_id"`
	CarID         uint16  `json:"car_id"`
	StatusMask    uint8   `json:"status_mask"`
	Laps          uint16  `json:"laps"`
	ValidLaps     uint16  `json:"valid_laps"`
	NSP           float32 `json:"nsp"`
	PosX          float32 `json:"pos_x"`
	PosZ          float32 `json:"pos_z"`
	TelemetryMask uint16  `json:"telemetry_mask"`
	RPM           uint16  `json:"rpm"`
	TyreLength    uint8   `json:"tyre_length"`
	Tyre          string  `json:"tyre"`
	CurrentLapS1  uint32  `json:"current_lap_s1"`
	CurrentLapS2  uint32  `json:"current_lap_s2"`
	CurrentLapS3  uint32  `json:"current_lap_s3"`
	BestLapS1     uint32  `json:"best_lap_s1"`
	BestLapS2     uint32  `json:"best_lap_s2"`
	BestLapS3     uint32  `json:"best_lap_s3"`
	SectorMask    uint8   `json:"sector_mask"`
	LastLapTime   int32   `json:"last_lap_time"`
	Gap           int32   `json:"gap"`
	Interval      int32   `json:"interval"`
	PosChange     int8    `json:"pos_change"`
}

// GetStintLapsBySessionID 根据SessionID查询所有圈速
func (db *DBWriter) GetStintLapsBySessionID(sessionID int) ([]*StintLap, error) {
	query := `
		SELECT sl.id, sl.stint_id, sl.sector_1, sl.sector_2, sl.sector_3, sl.time, sl.tyre, sl.max_speed 
		FROM stint_lap sl
		JOIN session_stint ss ON sl.stint_id = ss.id
		WHERE ss.session_id = ?
	`

	rows, err := db.DB.Query(query, sessionID)
	if err != nil {
		return nil, fmt.Errorf("查询圈速失败: %v", err)
	}
	defer rows.Close()

	var laps []*StintLap
	for rows.Next() {
		lap := &StintLap{}
		err := rows.Scan(&lap.ID, &lap.StintID, &lap.Sector1, &lap.Sector2, &lap.Sector3, &lap.Time, &lap.Tyre, &lap.MaxSpeed)
		if err != nil {
			return nil, fmt.Errorf("解析圈速数据失败: %v", err)
		}
		laps = append(laps, lap)
	}

	return laps, nil
}

// FindSessionStintBySessionID 根据SessionID查询session_stint
func (db *DBWriter) FindSessionStintBySessionID(sessionID int64) (*SessionStint, error) {
	stint := &SessionStint{}
	query := `SELECT id, user_id, session_id, event_id, car_id, laps, valid_laps, is_finished, started_at, finished_at, created_at, updated_at FROM session_stint WHERE session_id = ? LIMIT 1`
	err := db.DB.QueryRow(query, sessionID).Scan(
		&stint.ID, &stint.UserID, &stint.TeamMemberID, &stint.SessionID, &stint.CarID, &stint.GameCarID,
		&stint.Laps, &stint.ValidLaps, &stint.BestLapID, &stint.IsFinished, &stint.StartedAt, &stint.FinishedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("查询session_stint失败: %v", err)
	}
	return stint, nil
}

func (db *DBWriter) FindSessionStintsBySessionID(sessionID int64) ([]*SessionStint, error) {
	query := `SELECT id, user_id, session_id, event_id, car_id, laps, valid_laps, is_finished, started_at, finished_at, created_at, updated_at FROM session_stint WHERE session_id = ?`
	rows, err := db.DB.Query(query, sessionID)
	if err != nil {
		return nil, fmt.Errorf("查询session_stints失败: %v", err)
	}
	defer rows.Close()

	var stints []*SessionStint
	for rows.Next() {
		stint := &SessionStint{}
		err := rows.Scan(
			&stint.ID, &stint.UserID, &stint.TeamMemberID, &stint.SessionID, &stint.CarID, &stint.GameCarID,
			&stint.Laps, &stint.ValidLaps, &stint.BestLapID, &stint.IsFinished, &stint.StartedAt, &stint.FinishedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("扫描session_stint记录失败: %v", err)
		}
		stints = append(stints, stint)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("行迭代错误: %v", err)
	}

	return stints, nil
}

type DBWriter struct {
	DB          *sql.DB
	simulateSQL bool
}

// NewDBWriter 创建新的数据库写入器
func NewDBWriter(dsn string) (*DBWriter, error) {
	// 实际数据库连接
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("数据库连接失败: %v", err)
	}
	// 验证连接
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("数据库连接验证失败: %v", err)
	}
	logger.Println("数据库连接成功")
	// 从配置读取SQL模拟开关状态
	simulateSQL := true
	return &DBWriter{DB: db, simulateSQL: simulateSQL},
		nil
}

// Close 关闭数据库连接
func (w *DBWriter) Close() error {
	// 实际关闭数据库连接
	logger.Println("正在关闭数据库连接")
	return w.DB.Close()
}

// mockResult 模拟sql.Result接口实现
type mockResult struct{}

func (m *mockResult) LastInsertId() (int64, error) { return 0, nil }
func (m *mockResult) RowsAffected() (int64, error) { return 0, nil }

// executeSQL 根据simulateSQL标志执行SQL或模拟日志输出
func (w *DBWriter) executeSQL(query string, args ...interface{}) (sql.Result, error) {
	if w.simulateSQL {
		return &mockResult{}, nil
	}
	// 实际执行
	return w.DB.Exec(query, args...)
}

// TrackConfig 赛道配置表数据结构
type TrackConfig struct {
	ID          int    `db:"track_config_id"`
	TrackName   string `db:"track_name"`
	ConfigName  string `db:"config_name"`
	DisplayName string `db:"display_name"`
	Country     string `db:"country"`
	City        string `db:"city"`
	Length      int    `db:"length"`
}

// 定义JSON结构体以映射ui_track.json格式
type UITrackData struct {
	Name    string `json:"name"`
	Country string `json:"country"`
	City    string `json:"city"`
	Length  string `json:"length"`
}

// FindOrCreateTrackConfigID 查找或创建赛道配置ID
func (w *DBWriter) FindOrCreateTrackConfigID(tc *TrackConfig) (int, error) {
	// 查询现有赛道配置
	var trackConfigID int
	query := "SELECT track_config_id FROM track_config WHERE track_name = ? AND config_name = ?"
	err := w.DB.QueryRow(query, tc.TrackName, tc.ConfigName).Scan(&trackConfigID)
	if err == nil {
		tc.ID = trackConfigID
		globalTrackConfigs[trackConfigID] = tc
		return trackConfigID, nil
	}
	if err != sql.ErrNoRows {
		return 0, fmt.Errorf("查询赛道配置失败: %v", err)
	}
	// 创建新赛道配置
	id, err := w.InsertTrackConfig(tc)
	if err != nil {
		return 0, fmt.Errorf("创建赛道配置失败: %v", err)
	}
	tc.ID = id
	globalTrackConfigs[id] = tc
	return id, nil
}

// InsertTrackConfig 仅负责插入新赛道配置
func (w *DBWriter) InsertTrackConfig(tc *TrackConfig) (int, error) {
	gameConfigParentDir := filepath.Dir(cfg.Game.Path)
	var UiPath string
	var file *os.File
	var err error

	// 尝试路径组合: trackName/configName/ui
	UiPath = filepath.Join(gameConfigParentDir, "content", "tracks", tc.TrackName, tc.ConfigName, "ui", "ui_track.json")
	file, err = os.Open(UiPath)
	if err != nil {
		// 尝试新的路径组合: trackName/ui/configName
		UiPath = filepath.Join(gameConfigParentDir, "content", "tracks", tc.TrackName, "ui", tc.ConfigName, "ui_track.json")
		file, err = os.Open(UiPath)
		if err != nil {
			// 尝试新的路径组合: trackName/ui
			UiPath = filepath.Join(gameConfigParentDir, "content", "tracks", tc.TrackName, "ui", "ui_track.json")
			file, err = os.Open(UiPath)
			if err != nil {
				return -1, fmt.Errorf("赛道JSON文件路径加载失败: %v", err)
			}
		}
	}
	defer file.Close()

	// 解析JSON文件
	var trackData UITrackData
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&trackData)
	if err != nil {
		return -1, fmt.Errorf("解析赛道JSON文件失败: %v", err)
	}

	// 更新TrackConfig结构体字段
	if trackData.Name != "" {
		tc.DisplayName = trackData.Name
	}
	if trackData.Country != "" {
		tc.Country = trackData.Country
	}
	if trackData.City != "" {
		tc.City = trackData.City
	}
	// 尝试将长度转换为整数
	if trackData.Length != "" {
		// 移除非数字字符，只保留数字和小数点
		lengthStr := strings.Map(func(r rune) rune {
			if unicode.IsDigit(r) || r == '.' {
				return r
			}
			return -1
		}, trackData.Length)
		length, err := strconv.ParseFloat(lengthStr, 64)
		if err == nil {
			tc.Length = int(length)
		} else {
			return -1, fmt.Errorf("解析赛道长度失败: %v", err)
		}
	}

	query := `
	INSERT INTO track_config (
		track_name, config_name, display_name, country, city, length
	) VALUES (?, ?, ?, ?, ?, ?)
	`

	result, err := w.executeSQL(query,
		tc.TrackName, tc.ConfigName, tc.DisplayName, tc.Country, tc.City, tc.Length,
	)
	if err != nil {
		return -1, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return -1, err
	}
	return int(id), nil
}

// Event 赛事表数据结构
type Event struct {
	ID                        int     `db:"event_id"`
	ServerName                string  `db:"server_name"`
	TrackConfigID             int     `db:"track_config_id"`
	Name                      string  `db:"name"`
	TeamEvent                 int     `db:"team_event"`
	Active                    int     `db:"active"`
	LiveryPreview             int     `db:"livery_preview"`
	UseNumber                 int     `db:"use_number"`
	PracticeDuration          int     `db:"practice_duration"`
	QualiDuration             int     `db:"quali_duration"`
	RaceDuration              int     `db:"race_duration"`
	RaceDurationType          int     `db:"race_duration_type"`
	RaceWaitTime              int     `db:"race_wait_time"`
	RaceExtraLaps             int     `db:"race_extra_laps"`
	ReverseGridPos            int     `db:"reverse_grid_positions"`
	PickupModeEnabled         int     `db:"pickup_mode_enabled"`
	LoopMode                  int     `db:"loop_mode"`
	RaceExtraLap              int     `db:"race_extra_lap"`
	ReversedGridRacePositions int     `db:"reversed_grid_race_positions"`
	ResultScreenTime          int     `db:"result_screen_time"`
	LockedEntryList           int     `db:"locked_entry_list"`
	StartGrip                 float64 `db:"start_grip"`
}

// InsertEvent 插入赛事数据
func (w *DBWriter) FindEventByName(name string) (*Event, error) {
	query := `SELECT event_id, server_name, track_config_id, name, team_event, active, livery_preview, use_number, practice_duration, quali_duration, race_duration, race_duration_type, race_wait_time, race_extra_laps, reverse_grid_positions FROM event WHERE name = ?`

	row := w.DB.QueryRow(query, name)
	e := &Event{}
	err := row.Scan(
		&e.ID, &e.ServerName, &e.TrackConfigID, &e.Name, &e.TeamEvent, &e.Active, &e.LiveryPreview,
		&e.UseNumber, &e.PracticeDuration, &e.QualiDuration, &e.RaceDuration, &e.RaceDurationType,
		&e.RaceWaitTime, &e.RaceExtraLaps, &e.ReverseGridPos,
	)

	if err != nil {
		return nil, err
	}
	return e, nil
}

// InsertEvent 插入新赛事数据
func (w *DBWriter) InsertEvent(e *Event) error {
	query := `
	INSERT INTO event (
		server_name, track_config_id, name, team_event, active, livery_preview,
		use_number, practice_duration, quali_duration, race_duration, race_duration_type,
		race_wait_time, race_extra_laps, reverse_grid_positions
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err := w.executeSQL(query,
		e.ServerName, e.TrackConfigID, e.Name, e.TeamEvent, e.Active, e.LiveryPreview,
		e.UseNumber, e.PracticeDuration, e.QualiDuration, e.RaceDuration, e.RaceDurationType,
		e.RaceWaitTime, e.RaceExtraLaps, e.ReverseGridPos,
	)
	return err
}

// UpdateEvent 更新现有赛事数据
func (w *DBWriter) UpdateEvent(e *Event) error {
	query := `
	UPDATE event SET
		server_name = ?, track_config_id = ?, name = ?, team_event = ?, active = ?,
		livery_preview = ?, use_number = ?, practice_duration = ?, quali_duration = ?,
		race_duration = ?, race_duration_type = ?, race_wait_time = ?, race_extra_laps = ?,
		reverse_grid_positions = ?
	WHERE event_id = ?
	`

	_, err := w.executeSQL(query,
		e.ServerName, e.TrackConfigID, e.Name, e.TeamEvent, e.Active, e.LiveryPreview,
		e.UseNumber, e.PracticeDuration, e.QualiDuration, e.RaceDuration, e.RaceDurationType,
		e.RaceWaitTime, e.RaceExtraLaps, e.ReverseGridPos, e.ID,
	)
	return err
}

// Session 会话表数据结构
type Session struct {
	ID           int       `db:"session_id"`
	EventID      int       `db:"event_id"`
	Type         int       `db:"type"`
	TrackTime    string    `db:"track_time"`
	Name         string    `db:"name"`
	StartTime    time.Time `db:"start_time"`
	DurationMin  int       `db:"duration_min"`
	ElapsedMs    int       `db:"elapsed_ms"`
	Laps         int       `db:"laps"`
	Weather      string    `db:"weather"`
	AirTemp      float64   `db:"air_temp"`
	RoadTemp     float64   `db:"road_temp"`
	StartGrip    float64   `db:"start_grip"`
	CurrentGrip  float64   `db:"current_grip"`
	IsFinished   int       `db:"is_finished"`
	FinishTime   time.Time `db:"finish_time"`
	LastActivity time.Time `db:"last_activity"`
	HttpPort     int       `db:"http_port"`
}

// InsertSession 插入会话数据
func (w *DBWriter) InsertSession(s *Session) (int64, error) {
	query := `
	INSERT INTO session (
		event_id, type, track_time, name, start_time, duration_min, elapsed_ms,
		laps, weather, air_temp, road_temp, start_grip, current_grip, is_finished,
		finish_time, last_activity, http_port
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	result, err := w.executeSQL(query,
		s.EventID, s.Type, s.TrackTime, s.Name, s.StartTime.UTC(), s.DurationMin, s.ElapsedMs,
		s.Laps, s.Weather, s.AirTemp, s.RoadTemp, s.StartGrip, s.CurrentGrip, s.IsFinished,
		s.FinishTime.UTC(), s.LastActivity.UTC(), s.HttpPort,
	)
	id, err := result.LastInsertId()
	return id, err
}

// UpdateSession 更新会话数据
func (w *DBWriter) UpdateSession(s *Session) error {
	query := `
	UPDATE session SET
		event_id = ?, type = ?, track_time = ?, name = ?, start_time = ?, duration_min = ?, elapsed_ms = ?,
		laps = ?, weather = ?, air_temp = ?, road_temp = ?, start_grip = ?, current_grip = ?, is_finished = ?,
		finish_time = ?, last_activity = ?, http_port = ?
	WHERE session_id = ?
	`

	_, err := w.executeSQL(query,
		s.EventID, s.Type, s.TrackTime, s.Name, s.StartTime.UTC(), s.DurationMin, s.ElapsedMs,
		s.Laps, s.Weather, s.AirTemp, s.RoadTemp, s.StartGrip, s.CurrentGrip, s.IsFinished,
		s.FinishTime.UTC(), s.LastActivity.UTC(), s.HttpPort, s.ID,
	)
	return err
}

// SchemaInfo 架构信息表数据结构
type SchemaInfo struct {
	Version string `db:"version"`
	Game    string `db:"game"`
}

// InsertSchemaInfo 插入架构信息数据
func (w *DBWriter) InsertSchemaInfo(si *SchemaInfo) error {
	query := `
	INSERT INTO schema_info (
		version, game
	) VALUES (?, ?)
	`

	_, err := w.executeSQL(query,
		si.Version, si.Game,
	)
	return err
}

// UpdateSchemaInfo 更新架构信息数据
func (w *DBWriter) UpdateSchemaInfo(si *SchemaInfo) error {
	query := `
	UPDATE schema_info SET
		game = ?
	WHERE version = ?
	`

	_, err := w.executeSQL(query,
		si.Game, si.Version,
	)
	return err
}

// Car 车辆表数据结构
type Car struct {
	ID           int    `db:"car_id"`
	DisplayName  string `db:"display_name"`
	Name         string `db:"name"`
	Manufacturer string `db:"manufacturer"`
	CarClass     string `db:"car_class"`
}

var carData struct {
	Name  string `json:"name"`
	Brand string `json:"brand"`
	Class string `json:"class"`
}

func (w *DBWriter) FindOrCreateCarID(c *Car) (int, error) {
	// 查询现有车辆配置
	var carID int
	query := "SELECT car_id FROM car WHERE name = ?"
	err := w.DB.QueryRow(query, c.Name).Scan(&carID)
	if err == nil {
		return carID, nil
	}
	if err != sql.ErrNoRows {
		return -1, fmt.Errorf("查询车辆配置失败: %v", err)
	}

	// 创建新车辆配置
	id, err := w.InsertCar(c)
	if err != nil {
		return -1, fmt.Errorf("创建车辆配置失败: %v", err)
	}
	return id, nil
}

// InsertCar 插入车辆数据
func (w *DBWriter) InsertCar(c *Car) (int, error) {

	gameConfigParentDir := filepath.Dir(cfg.Game.Path)
	UiPath := filepath.Join(gameConfigParentDir, "content", "cars", c.Name, "ui", "ui_car.json")
	file, err := os.Open(UiPath)
	if err != nil {
		return -1, fmt.Errorf("车辆JSON文件路径加载失败: %v", err)
	}
	defer file.Close()

	if err := json.NewDecoder(file).Decode(&carData); err != nil {
		return -1, fmt.Errorf("解析车辆JSON失败: %v", err)
	}

	c.DisplayName = carData.Name
	c.Manufacturer = carData.Brand
	c.CarClass = carData.Class

	query := `
	INSERT INTO car (
		display_name, name, manufacturer, car_class
	) VALUES (?, ?, ?, ?)
	`

	_, err = w.executeSQL(query,
		c.DisplayName, c.Name, c.Manufacturer, c.CarClass,
	)
	if err != nil {
		return -1, err
	}
	if w.simulateSQL {
		return 0, nil
	}
	// 获取插入后的记录ID
	var carID int
	row := w.DB.QueryRow("SELECT car_id FROM car WHERE name = ?", c.Name)
	if err := row.Scan(&carID); err != nil {
		return -1, fmt.Errorf("获取车辆ID失败: %v", err)
	}

	return carID, nil
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

// FindOrCreateTeam 查询或创建团队
func (w *DBWriter) FindOrCreateTeam(t *Team) (int, error) {
	if w.simulateSQL {
		log.Printf("模拟查询/创建团队: team_no=%d, event_id=%d", t.TeamNo, t.EventID)
		return int(t.TeamNo%1000 + t.EventID), nil
	}

	// 查询数据库
	teamID, err := w.FindTeamByTeamNoAndEventID(t.TeamNo, t.EventID)
	if err != nil {
		return 0, fmt.Errorf("查询团队失败: %v", err)
	}
	if teamID > 0 {
		return teamID, nil
	}

	// 团队不存在，创建新团队
	return w.InsertTeam(t)
}

// FindTeamByTeamNoAndEventID 根据TeamNo和EventID查询团队ID
func (w *DBWriter) FindTeamByTeamNoAndEventID(teamNo int, eventID int) (int, error) {
	var teamID int
	err := w.DB.QueryRow("SELECT team_id FROM team WHERE team_no = ? AND event_id = ?", teamNo, eventID).Scan(&teamID)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil // 没有找到团队
		}
		return 0, err
	}
	return teamID, nil
}

// InsertTeam 插入团队数据
func (w *DBWriter) InsertTeam(t *Team) (int, error) {
	query := `
	INSERT INTO team (event_id, name, team_no, car_id, livery_name, active, created_at)
	VALUES (?, ?, ?, ?, ?, ?, ?)
	`
	result, err := w.executeSQL(query,
		t.EventID, t.Name, t.TeamNo, t.CarID, t.LiveryName, t.Active, t.CreatedAt.UTC(),
	)
	if err != nil {
		return 0, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	return int(id), nil
}

// User 用户表数据结构
type User struct {
	UserID    int    `db:"user_id"`
	Name      string `db:"name"`
	PrevName  string `db:"prev_name"`
	Steam64ID int64  `db:"steam64_id"`
	Country   string `db:"country"`
}

// FindOrCreateUser 查询或创建用户
func (w *DBWriter) FindOrCreateUser(u *User) (int, bool, error) {
	// 先检查内存缓存
	for _, user := range globalUsers {
		if user.Steam64ID == u.Steam64ID {
			// 检查用户名是否变更
			if user.Name != u.Name {
				// 更新数据库用户名
				if err := w.updateUserName(user.UserID, user.Name, u.Name); err != nil {
					return 0, false, fmt.Errorf("更新用户名失败: %v", err)
				}
				// 更新缓存用户名
				user.Name = u.Name
				globalUsers[user.UserID] = user
			}
			return user.UserID, false, nil
		}
	}

	query := "SELECT user_id, name, prev_name, steam64_id, country FROM user WHERE steam64_id = ? LIMIT 1"
	var existingUser User
	err := w.DB.QueryRow(query, u.Steam64ID).Scan(&existingUser.UserID, &existingUser.Name, &existingUser.PrevName, &existingUser.Steam64ID, &existingUser.Country)
	if err == nil {
		// 检查用户名是否变更
		if existingUser.Name != u.Name {
			// 更新数据库用户名
			if err := w.updateUserName(existingUser.UserID, existingUser.Name, u.Name); err != nil {
				return 0, false, fmt.Errorf("更新用户名失败: %v", err)
			}
			// 更新用户名称
			existingUser.Name = u.Name
		}
		globalUsers[existingUser.UserID] = &existingUser
		return existingUser.UserID, false, nil
	}
	if err != sql.ErrNoRows {
		return 0, false, fmt.Errorf("查询用户失败: %v", err)
	}

	// 用户不存在，创建新用户
	user := &User{
		Name:      u.Name,
		PrevName:  u.PrevName,
		Steam64ID: u.Steam64ID,
		Country:   u.Country,
	}
	userID, err := w.InsertUser(user)
	if err == nil {
		globalUsers[userID] = user
	}
	if err != nil {
		return 0, false, fmt.Errorf("创建用户失败: %v", err)
	}
	return userID, true, nil
}

func (w *DBWriter) updateUserName(userID int, oldName string, newName string) error {
	if w.simulateSQL {
		logger.Printf("模拟更新用户 %d 名称为 %s (旧名称: %s)", userID, newName, oldName)
		return nil
	}
	_, err := w.executeSQL("UPDATE user SET name = ?, prev_name = ? WHERE user_id = ?", newName, oldName, userID)
	logger.Printf("更新用户 %d 名称为 %s (旧名称: %s)", userID, newName, oldName)
	return err
}

// InsertUser 插入用户数据
func (w *DBWriter) InsertUser(u *User) (int, error) {
	query := `
	INSERT INTO user (name, prev_name, steam64_id, country) VALUES (?, ?, ?, ?)
	`

	result, err := w.executeSQL(query,
		u.Name, u.PrevName, u.Steam64ID, u.Country,
	)
	if err != nil {
		return 0, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	return int(id), nil
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

// FindOrCreateTeamMember 查询或创建团队成员
func (w *DBWriter) FindOrCreateTeamMember(tm *TeamMember) error {
	if w.simulateSQL {
		log.Printf("模拟查询/创建团队成员: user_id=%d, team_id=%d", tm.UserID, tm.TeamID)
		return nil
	}

	// 查询成员是否存在
	exists, err := w.FindTeamMemberByUserIDAndTeamID(tm.UserID, tm.TeamID)
	if err != nil {
		return fmt.Errorf("查询团队成员失败: %v", err)
	}
	if exists {
		return nil
	}
	// 成员不存在，执行插入
	return w.InsertTeamMember(tm)
}

// FindTeamMemberByUserIDAndTeamID 检查团队成员是否存在
func (w *DBWriter) FindTeamMemberByUserIDAndTeamID(userID int, teamID int) (bool, error) {
	var exists bool
	err := w.DB.QueryRow("SELECT EXISTS(SELECT 1 FROM team_member WHERE user_id = ? AND team_id = ?)", userID, teamID).Scan(&exists)
	return exists, err
}

// FindLatestSessionByEventID 查询指定event_id且未完成的最新会话
func (w *DBWriter) FindLatestSessionByEventID(eventID int) (*Session, error) {
	session := &Session{}
	query := `
		SELECT session_id, event_id, start_time, is_finished
		FROM session
		WHERE event_id = ? AND is_finished = 0
		ORDER BY session_id DESC
		LIMIT 1
	`

	err := w.DB.QueryRow(query, eventID).Scan(&session.ID, &session.EventID, &session.StartTime, &session.IsFinished)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("查询会话失败: %v", err)
	}
	return session, nil
}

// InsertTeamMember 插入或更新团队成员数据
func (w *DBWriter) InsertTeamMember(tm *TeamMember) error {
	query := `
	INSERT INTO team_member (team_id, user_id, role, active, created_at) VALUES (?, ?, ?, ?, ?)
	`
	_, err := w.executeSQL(query,
		tm.TeamID, tm.UserID, tm.Role, tm.Active, tm.CreatedAt.UTC(),
	)
	return err
}

// StintLap 赛段圈速表数据结构
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

// InsertStintLap 插入赛段圈速数据
// 计算会话已用时间
func calculateElapsedMs(sessionID int64) (int64, error) {
	var startTime time.Time
	if sessionID != int64(globalSession.ID) {
		query := "SELECT start_time FROM session WHERE session_id = ?"
		err := globalDBWriter.DB.QueryRow(query, sessionID).Scan(&startTime)
		if err != nil {
			return 0, fmt.Errorf("查询会话开始时间失败: %v", err)
		}
	} else {
		startTime = globalSession.StartTime
	}
	elapsed := time.Since(startTime).Milliseconds()
	return elapsed, nil
}

// 根据车辆ID获取团队ID
func getTeamIDByCarID(carID int) uint32 {
	globalMutex.Lock()
	defer globalMutex.Unlock()
	for _, team := range globalTeams {
		if team.CarID == carID && team.Active == 1 {
			return uint32(team.ID)
		}
	}
	return 0 // 未找到时返回0
}

// 根据团队ID获取团队名称
func getTeamNameByID(teamID int) string {
	globalMutex.Lock()
	defer globalMutex.Unlock()
	for _, team := range globalTeams {
		if team.ID == teamID && team.Active == 1 {
			return team.Name
		}
	}
	return "" // 未找到时返回空字符串
}

// 根据用户ID获取用户名
func getUserNameByID(userID int) string {
	globalMutex.Lock()
	defer globalMutex.Unlock()
	for _, user := range globalUsers {
		if user.UserID == userID {
			return user.Name
		}
	}
	return "Unknown"
}

// 根据车辆ID获取用户ID (非团队赛事)
func getUserIDByCarID(carID int) uint32 {
	entryListMutex.Lock()
	defer entryListMutex.Unlock()
	if driver, exists := globalEntryList[carID]; exists {
		// 通过驾驶员名称查找或创建用户
		userID, _, _ := globalDBWriter.FindOrCreateUser(&User{Name: driver.Name})
		return uint32(userID)
	}
	return 0
}

// 根据团队ID获取用户ID
func getUserIDByTeamID(teamID uint32) uint32 {
	globalMutex.Lock()
	defer globalMutex.Unlock()
	for _, member := range globalTeamMembers {
		if uint32(member.TeamID) == teamID && member.Active == 1 {
			return uint32(member.UserID)
		}
	}
	return 0 // 未找到时返回0
}

// 解析排行榜条目
func parseLeaderboardEntries(sessionID int, carUpdates map[udp.CarID]udp.CarUpdate) ([]Entry, error) {
	var globalBestS1, globalBestS2, globalBestS3 int = math.MaxInt32, math.MaxInt32, math.MaxInt32
	var entries []Entry
	lapDataMutex.Lock()
	defer lapDataMutex.Unlock()

	// 遍历最新车辆更新数据
	for carID := range carUpdates {
		ServerCarID := int(carID)
		var laps, validLaps int = 0, 0
		var currentS1, currentS2, currentS3, currentTime int = 0, 0, 0, 0
		var tyre string = ""
		var bestS1, bestS2, bestS3 int = 0, 0, 0

		// 1. 获取圈速数据
		if lapData, ok := carLapDataMap[ServerCarID]; ok && len(lapData) > 0 {
			latestLap := lapData[len(lapData)-1]
			laps = int(latestLap.Laps)
			validLaps = int(latestLap.Laps) // 简化处理，实际应区分有效圈
			currentTime = latestLap.LapTime
			if dataCollectionEnabled {
				currentS1 = latestLap.Sector1
				currentS2 = latestLap.Sector2
				currentS3 = latestLap.Sector3
				tyre = latestLap.Tyre
			}
		}

		// 2. 初始化Entry结构体
		// 获取连接状态（0=已连接, 1=加载中, 2=已断开连接）
		var status uint8
		connectionsMutex.Lock()
		if conn, ok := globalConnections[ServerCarID]; ok {
			status = conn.ConnectionStatus // 连接状态掩码（0=已连接, 1=加载中, 2=已断开连接）
		} else {
			status = 0 // 默认离线状态
		}
		connectionsMutex.Unlock()

		// 获取赛道状态（0=正常赛道, 1=偏离赛道, 2=维修区入口, 3=维修区, 4=维修区出口）
		var trackStatus uint8
		if conn, ok := globalConnections[ServerCarID]; ok {
			trackStatus = conn.TrackStatus // 赛道状态掩码（0=正常赛道, 1=偏离赛道, 2=维修区入口, 3=维修区, 4=维修区出口）
		} else {
			trackStatus = 0 // 默认正常赛道状态
		}
		// 组合状态：高4位存储连接状态，低4位存储赛道状态
		combinedStatus := (status << 4) | trackStatus

		e := Entry{
			TeamID:        0, // 团队赛事中会覆盖此值
			UserID:        0, // 根据赛事类型在后续逻辑中设置
			CarID:         uint16(ServerCarID),
			Laps:          uint16(laps),
			ValidLaps:     uint16(validLaps),
			CurrentLapS1:  uint32(currentS1),
			CurrentLapS2:  uint32(currentS2),
			CurrentLapS3:  uint32(currentS3),
			LastLapTime:   int32(currentTime),
			Tyre:          tyre,
			StatusMask:    combinedStatus,   // 组合状态掩码（高4位：连接状态，低4位：赛道状态）
			TyreLength:    uint8(len(tyre)), // 轮胎信息字符串长度
			SectorMask:    0,
			Gap:           0,
			Interval:      0,
			PosChange:     0,
			NSP:           0,
			PosX:          0,
			PosZ:          0,
			TelemetryMask: 0,
			RPM:           0,
			BestLapS1:     0,
			BestLapS2:     0,
			BestLapS3:     0,
		}

		// 根据赛事类型设置TeamID和UserID
		if globalEvent.TeamEvent == 1 {
			// 团队赛事: 通过团队获取用户ID
			e.TeamID = getTeamIDByCarID(ServerCarID)
			e.UserID = getUserIDByTeamID(e.TeamID)
		} else {
			// 非团队赛事: 直接通过车辆ID获取用户ID
			e.UserID = getUserIDByCarID(ServerCarID)
		}

		// 3. 计算赛段掩码 (数据收集开启时)
		if dataCollectionEnabled {
			if stintData, ok := stintLapData[sessionID]; ok && len(stintData[ServerCarID]) > 0 {
				// 3.1 计算个人最佳赛段
				for _, lap := range stintData[ServerCarID] {
					if lap.Sector1 > 0 && (bestS1 == 0 || lap.Sector1 < bestS1) {
						bestS1 = lap.Sector1
					}
					if lap.Sector2 > 0 && (bestS2 == 0 || lap.Sector2 < bestS2) {
						bestS2 = lap.Sector2
					}
					if lap.Sector3 > 0 && (bestS3 == 0 || lap.Sector3 < bestS3) {
						bestS3 = lap.Sector3
					}
				}

				// 更新全局最佳赛段
				if bestS1 > 0 && (globalBestS1 == 0 || bestS1 < globalBestS1) {
					globalBestS1 = bestS1
				}
				if bestS2 > 0 && (globalBestS2 == 0 || bestS2 < globalBestS2) {
					globalBestS2 = bestS2
				}
				if bestS3 > 0 && (globalBestS3 == 0 || bestS3 < globalBestS3) {
					globalBestS3 = bestS3
				}

				// 3.2 设置赛段掩码 (紫色:全局最佳, 绿色:个人最佳)
				const (
					SECTOR1_PERSONAL_BEST = 1 << 0
					SECTOR2_PERSONAL_BEST = 1 << 2
					SECTOR3_PERSONAL_BEST = 1 << 4
					SECTOR1_GLOBAL_BEST   = 1 << 3
					SECTOR2_GLOBAL_BEST   = 1 << 5
					SECTOR3_GLOBAL_BEST   = 1 << 6
				)

				if currentS1 > 0 {
					if globalBestS1 > 0 && currentS1 == globalBestS1 {
						e.SectorMask |= SECTOR1_GLOBAL_BEST
					} else if bestS1 > 0 && currentS1 == bestS1 {
						e.SectorMask |= SECTOR1_PERSONAL_BEST
					}
				}
				if currentS2 > 0 {
					if globalBestS2 > 0 && currentS2 == globalBestS2 {
						e.SectorMask |= SECTOR2_GLOBAL_BEST
					} else if bestS2 > 0 && currentS2 == bestS2 {
						e.SectorMask |= SECTOR2_PERSONAL_BEST
					}
				}
				if currentS3 > 0 {
					if globalBestS3 > 0 && currentS3 == globalBestS3 {
						e.SectorMask |= SECTOR3_GLOBAL_BEST
					} else if bestS3 > 0 && currentS3 == bestS3 {
						e.SectorMask |= SECTOR3_PERSONAL_BEST
					}

				}
				// 设置最佳赛段时间
				e.BestLapS1 = uint32(bestS1)
				e.BestLapS2 = uint32(bestS2)
				e.BestLapS3 = uint32(bestS3)
			}
		}

		// 4. 获取实时车辆数据
		if update, ok := carUpdates[udp.CarID(ServerCarID)]; ok {
			e.NSP = update.NormalisedSplinePos
			e.PosX = float32(update.Pos.X)
			e.PosZ = float32(update.Pos.Z)

			// 计算TelemetryMask (挡位和速度)
			gear := update.Gear
			speedKMH := uint16(update.Velocity.X * 3.6) // 转换m/s到km/h
			if speedKMH > 1023 {                        // 限制在10位范围内
				speedKMH = 1023
			}
			e.TelemetryMask = uint16(gear)<<10 | speedKMH
			e.RPM = uint16(update.EngineRPM)
		} else {
			log.Printf("警告: 未找到车辆 %d 的实时更新数据", ServerCarID)
		}

		entries = append(entries, e)
	}

	// 3. 计算Gap和Interval
	if len(entries) > 0 {
		leaderTime := entries[0].LastLapTime
		entries[0].Gap = 0
		entries[0].Interval = 0
		for i := 1; i < len(entries); i++ {
			entries[i].Gap = entries[i].LastLapTime - leaderTime
			entries[i].Interval = entries[i].LastLapTime - entries[i-1].LastLapTime
		}
	}

	// 按圈数和最后圈速排序
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Laps != entries[j].Laps {
			return entries[i].Laps > entries[j].Laps
		}
		return entries[i].LastLapTime < entries[j].LastLapTime
	})
	return entries, nil
}

func (w *DBWriter) InsertStintLap(sl *StintLap) (int64, error) {
	query := `
	INSERT INTO stint_lap (stint_id, sector_1, sector_2, sector_3, grip, tyre, time, cuts, crashes, car_crashes, max_speed, avg_speed, finished_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	result, err := w.executeSQL(query,
		sl.StintID, sl.Sector1, sl.Sector2, sl.Sector3, sl.Grip, sl.Tyre, sl.Time,
		sl.Cuts, sl.Crashes, sl.CarCrashes, sl.MaxSpeed, sl.AvgSpeed, sl.FinishedAt.UTC(),
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// FindStintLapsByStintID 根据StintID查找所有圈速
func (w *DBWriter) FindStintLapsByStintID(stintID int) ([]StintLap, error) {
	query := "SELECT * FROM stint_lap WHERE stint_id = ?"
	rows, err := w.DB.Query(query, stintID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var laps []StintLap
	for rows.Next() {
		var lap StintLap
		if err := rows.Scan(&lap.ID, &lap.StintID, &lap.Sector1, &lap.Sector2, &lap.Sector3, &lap.Grip, &lap.Tyre, &lap.Time, &lap.Cuts, &lap.Crashes, &lap.CarCrashes, &lap.MaxSpeed, &lap.AvgSpeed, &lap.FinishedAt); err != nil {
			return nil, err
		}
		laps = append(laps, lap)
	}
	return laps, nil
}

// FindStintLapByStintIDAndTime 检查指定StintID和时间的圈速是否存在
func (w *DBWriter) FindStintLapByStintIDAndTime(stintID, time int) (bool, error) {
	var count int
	query := "SELECT COUNT(*) FROM stint_lap WHERE stint_id = ? AND time = ?"
	if err := w.DB.QueryRow(query, stintID, time).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

// UpdateStintLap 更新赛段圈速数据
func (w *DBWriter) UpdateStintLap(sl *StintLap) error {
	query := `
	UPDATE stint_lap SET
		sector_1 = ?, sector_2 = ?, sector_3 = ?, grip = ?, tyre = ?, time = ?,
		cuts = ?, crashes = ?, car_crashes = ?, max_speed = ?, avg_speed = ?, finished_at = ?
	WHERE stint_lap_id = ?
	`

	_, err := w.executeSQL(query,
		sl.Sector1, sl.Sector2, sl.Sector3, sl.Grip, sl.Tyre, sl.Time,
		sl.Cuts, sl.Crashes, sl.CarCrashes, sl.MaxSpeed, sl.AvgSpeed, sl.FinishedAt.UTC(), sl.ID,
	)
	return err
}

// 检查SessionStint是否存在
func (w *DBWriter) checkSessionStintExists(sessionID, stintID int) (bool, error) {
	var count int
	query := "SELECT COUNT(*) FROM session_stint WHERE session_id = ? AND session_stint_id = ?"
	if err := w.DB.QueryRow(query, sessionID, stintID).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

// 补充SessionStint数据
func (w *DBWriter) SupplementSessionStintData(sessionID int, stints []SessionStint) error {
	// 处理单个stint
	for _, stint := range stints {
		exists, err := w.checkSessionStintExists(sessionID, stint.ID)
		if err != nil {
			return err
		}
		stint.SessionID = sessionID
		if exists {
			if _, err := w.UpdateSessionStint(&stint); err != nil {
				return err
			}
		} else {
			if _, err := w.InsertSessionStint(&stint); err != nil {
				return err
			}
		}
	}
	return nil
}

// 补充StintLap数据
func (w *DBWriter) SupplementStintLapData(sessionID int, laps []StintLap) error {
	for _, lap := range laps {
		exists, err := w.FindStintLapByStintIDAndTime(lap.StintID, lap.Time)
		if err != nil {
			return err
		}
		if exists {
			if err := w.UpdateStintLap(&lap); err != nil {
				return err
			}
		} else {
			if _, err := w.InsertStintLap(&lap); err != nil {
				return err
			}
		}
	}
	return nil
}

// Driver 定义JSON中的驾驶员信息
// GuidsList 仅用于团队赛事(team event)场景
type Driver struct {
	Name      string   `json:"Name"`
	Team      string   `json:"Team"`
	Nation    string   `json:"Nation"`
	Guid      string   `json:"Guid"`
	GuidsList []string `json:"GuidsList"` // 团队赛事中的多个GUID列表
}

// Car 定义JSON中的车辆信息
type ResultCar struct {
	CarId      int    `json:"CarId"`
	Driver     Driver `json:"Driver"`
	Model      string `json:"Model"`
	Skin       string `json:"Skin"`
	BallastKG  int    `json:"BallastKG"`
	Restrictor int    `json:"Restrictor"`
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

// SessionStint 会话赛段表数据结构
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

// InsertSessionStint 插入会话赛段数据
func (w *DBWriter) InsertSessionStint(ss *SessionStint) (int64, error) {
	query := `
	INSERT INTO session_stint (user_id, team_member_id, session_id, car_id, game_car_id, laps, valid_laps, best_lap_id, is_finished, started_at, finished_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	result, err := w.executeSQL(query,
		ss.UserID, ss.TeamMemberID, ss.SessionID, ss.CarID, ss.GameCarID, ss.Laps,
		ss.ValidLaps, ss.BestLapID, ss.IsFinished, ss.StartedAt.UTC(), ss.FinishedAt.UTC(),
	)
	if err != nil {
		return 0, err
	}
	id, err := result.LastInsertId()
	return id, err
}

// UpdateSessionStint 更新会话赛段数据
func (w *DBWriter) UpdateSessionStint(ss *SessionStint) (int64, error) {
	query := `
	UPDATE session_stint SET
		user_id = ?, team_member_id = ?, session_id = ?, car_id = ?, game_car_id = ?, laps = ?,
		valid_laps = ?, best_lap_id = ?, is_finished = ?, started_at = ?, finished_at = ?
	WHERE session_stint_id = ?
	`

	result, err := w.executeSQL(query,
		ss.UserID, ss.TeamMemberID, ss.SessionID, ss.CarID, ss.GameCarID, ss.Laps,
		ss.ValidLaps, ss.BestLapID, ss.IsFinished, ss.StartedAt.UTC(), ss.FinishedAt.UTC(), ss.ID,
	)
	if err != nil {
		return 0, err
	}
	id, err := result.LastInsertId()
	return id, err
}

// SessionFeed 会话消息表数据结构
type SessionFeed struct {
	ID        int       `db:"session_feed_id"`
	SessionID int       `db:"session_id"`
	Type      int       `db:"type"`
	Detail    string    `db:"detail"`
	Time      time.Time `db:"time"`
}

// InsertSessionFeed 插入会话消息数据
func (w *DBWriter) InsertSessionFeed(sf *SessionFeed) error {
	query := `
	INSERT INTO session_feed (session_id, type, detail, time) VALUES (?, ?, ?, ?)
	`

	_, err := w.executeSQL(query,
		sf.SessionID, sf.Type, sf.Detail, sf.Time.UTC(),
	)
	return err
}

// LapTelemetry 圈速遥测数据表结构
type LapTelemetry struct {
	LapID     int    `db:"lap_id"`
	Telemetry []byte `db:"telemetry"`
}

// InsertLapTelemetry 插入圈速遥测数据
func (w *DBWriter) InsertLapTelemetry(lapID int, telemetry []byte) error {
	query := `
	INSERT INTO lap_telemetry (lap_id, telemetry)
	VALUES (?, ?)
	`

	_, err := w.executeSQL(query,
		lapID, telemetry,
	)
	return err
}

// UpdateLapTelemetry 更新圈速遥测数据
func (w *DBWriter) UpdateLapTelemetry(lapID int, telemetry []byte) error {
	query := `
	UPDATE lap_telemetry SET
		telemetry = ?
	WHERE lap_id = ?
	`

	_, err := w.executeSQL(query,
		telemetry, lapID,
	)
	return err
}

// 全局配置变量
var cfg *config.Config       // 全局配置实例
var globalDBWriter *DBWriter // 全局数据库写入器实例
var globalEvent *Event
var globalUsers map[int]*User
var globalTeams map[int]*Team
var globalTeamMembers map[int]*TeamMember
var globalCars map[int]*Car
var globalTrackConfigs map[int]*TrackConfig
var globalMutex sync.Mutex
var latestFile os.DirEntry = nil

var (
	globalEntryList map[int]EntryListDriver
	entryListMutex  sync.Mutex
)
var (
	receivedSessionInfo bool
	stintLapData        = make(map[int]map[int][]StintLap)
)
var (
	receivedVersionInfo bool
)

var (
	globalConnections = make(map[int]*ConnectionInfo)
	connectionsMutex  sync.Mutex
)
var (
	sessionMutex           sync.Mutex
	globalSession          Session
	staticSessionIDCounter int64 = 0
)

func init() {
	// 初始化全局映射
	globalUsers = make(map[int]*User)
	globalTeams = make(map[int]*Team)
	globalTeamMembers = make(map[int]*TeamMember)
	globalCars = make(map[int]*Car)
	globalTrackConfigs = make(map[int]*TrackConfig)
	globalEntryList = make(map[int]EntryListDriver)
	globalEvent = &Event{}
	globalSession = Session{}
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

var pitBoundary [][3]float32

// 加载并验证server_cfg.ini文件并提取event信息到现有Event结构体
func (w *DBWriter) LoadServerConfig(cfgPath string) error {
	// 读取INI文件
	serverCfgPath := filepath.Join(cfgPath, "cfg", "server_cfg.ini")
	serverCfg, err := ini.Load(serverCfgPath)
	if err != nil {
		fatalLogger.Fatalf("无法加载配置文件: %v", err)
	}

	// 验证必填字段
	serverSection := serverCfg.Section("SERVER")
	if serverSection == nil {
		fatalLogger.Fatalf("配置文件缺少[SERVER]部分")
	}

	// 读取UDP配置
	udpPluginLocalPort := serverSection.Key("UDP_PLUGIN_LOCAL_PORT").MustInt(0)
	udpPluginAddress := serverSection.Key("UDP_PLUGIN_ADDRESS").String()

	// 解析UDP_PLUGIN_ADDRESS获取端口
	if udpPluginAddress != "" {
		parts := strings.Split(udpPluginAddress, ":")
		if len(parts) == 2 {
			remotePort, err := strconv.Atoi(parts[1])
			if err == nil && cfg.ACServer.UDP.Port == 0 {
				cfg.ACServer.UDP.Port = remotePort
			}
		}
	}

	// 设置本地端口
	if cfg.ACServer.UDP.LocalPort == 0 && udpPluginLocalPort > 0 {
		cfg.ACServer.UDP.LocalPort = udpPluginLocalPort
	}

	requiredFields := []string{"NAME", "TRACK"}
	for _, field := range requiredFields {
		if serverSection.Key(field).String() == "" {
			fatalLogger.Fatalf("缺少必填字段: %s", field)
		}
	}

	// 解析赛事时长配置
	practiceDuration := -1
	qualiDuration := -1
	raceDuration := -1
	raceDurationType := 0 // 0 = minutes, 1 = laps
	waitTime := 0
	StartGrip := 0.0

	// 遍历所有配置节以查找会话配置
	for _, section := range serverCfg.Sections() {
		name := section.Name()
		// 检查是否为禁用节
		disabled := strings.HasSuffix(name, "_OFF")
		sessionType := strings.TrimPrefix(strings.TrimSuffix(name, "_OFF"), "__CM_")
		disabledynamicTrack := strings.HasPrefix(name, "DYNAMIC_TRACK_OFF")

		switch sessionType {
		case "PRACTICE":
			if !disabled {
				practiceDuration = section.Key("TIME").MustInt(-1)
			}
		case "QUALIFY":
			if !disabled {
				qualiDuration = section.Key("TIME").MustInt(-1)
			}
		case "RACE":
			if !disabled {
				raceTime := section.Key("TIME").MustInt(0)
				raceLaps := section.Key("LAPS").MustInt(0)

				// 根据非零值判断时长类型
				if raceLaps > 0 {
					raceDuration = raceLaps
					raceDurationType = 1 // 圈数模式
				} else if raceTime > 0 {
					raceDuration = raceTime
					raceDurationType = 0 // 时间模式
				} else {
					fatalLogger.Fatalf("RACE会话配置无法确定RaceDuration")
				}
			}
		case "DYNAMIC_TRACK":
			if disabledynamicTrack {
				StartGrip = 1
			} else {
				StartGrip = float64(section.Key("SESSION_START").MustInt(0) / 100)
			}
		}
	}

	waitTime = serverSection.Key("WAIT_TIME").MustInt(60)

	config := &TrackConfig{
		TrackName:  serverSection.Key("TRACK").String(),
		ConfigName: serverSection.Key("CONFIG_TRACK").String(), // 可从配置文件获取实际值
	}
	var trackConfigID int
	if w.simulateSQL {
		trackConfigID, err = w.FindOrCreateTrackConfigID(&TrackConfig{
			TrackName:  "singapore_2020",
			ConfigName: "layout_f1_2024",
		})
		if err != nil {
			fatalLogger.Fatalf("获取TrackConfigID失败: %v", err)
		}
		log.Printf("模拟模式下使用TrackConfigID: %d", trackConfigID)
	} else {
		trackConfigID, err = w.FindOrCreateTrackConfigID(config)
		if err != nil {
			fatalLogger.Fatalf("获取TrackConfigID失败: %v", err)
		}
	}
	// 调用kn5conv处理
	pitBoundary, err = kn5conv.BatchProcess(filepath.Dir(cfg.Game.Path), cfg.App.Server.CachePath, config.TrackName, config.ConfigName)
	if err != nil {
		fatalLogger.Fatalf("kn5conv处理失败: %v", err)
	}
	// 确定server name，优先使用配置中的event name
	Name := serverSection.Key("NAME").String()
	if cfg.ACServer.Event.Name != "" {
		Name = cfg.ACServer.Event.Name
		logger.Printf("使用配置的event name: %s", Name)
	}
	globalEvent = &Event{
		ServerName:                serverSection.Key("NAME").String(),
		TrackConfigID:             trackConfigID,
		Name:                      Name,
		TeamEvent:                 boolToInt(cfg.ACServer.Event.Team.Enable),
		Active:                    1,
		LiveryPreview:             boolToInt(cfg.ACServer.Event.Team.LiveryPreviewEnable),
		UseNumber:                 boolToInt(cfg.ACServer.Event.Team.UseNumber),
		PracticeDuration:          practiceDuration,
		QualiDuration:             qualiDuration,
		RaceDuration:              raceDuration,
		RaceDurationType:          raceDurationType,
		RaceWaitTime:              waitTime,
		RaceExtraLaps:             serverSection.Key("RACE_EXTRA_LAPS").MustInt(0),
		ReverseGridPos:            serverSection.Key("REVERSE_GRID_POSITIONS").MustInt(0),
		PickupModeEnabled:         serverSection.Key("PICKUP_MODE_ENABLED").MustInt(0),
		LoopMode:                  serverSection.Key("LOOP_MODE").MustInt(0),
		RaceExtraLap:              serverSection.Key("RACE_EXTRA_LAP").MustInt(0),
		ReversedGridRacePositions: serverSection.Key("REVERSED_GRID_RACE_POSITIONS").MustInt(0),
		ResultScreenTime:          serverSection.Key("RESULT_SCREEN_TIME").MustInt(60),
		LockedEntryList:           serverSection.Key("LOCKED_ENTRY_LIST").MustInt(0),
		StartGrip:                 StartGrip,
	}
	logger.Println("server_cfg.ini文件验证成功")
	return nil
}

// LoadEntryList 加载并验证entry_list.ini文件
func (w *DBWriter) LoadEntryList(gamePath string) error {
	entryListPath := filepath.Join(gamePath, "cfg", "entry_list.ini")
	// 加载INI文件
	entrylist, err := ini.Load(entryListPath)
	if err != nil {
		fatalLogger.Fatalf("无法加载entry_list.ini: %v", err)
	}

	carSections := make([]*ini.Section, 0)
	for _, section := range entrylist.Sections() {
		if strings.HasPrefix(section.Name(), "CAR_") {
			carSections = append(carSections, section)
		}
	}

	if len(carSections) == 0 {
		fatalLogger.Fatalf("entry_list.ini缺少[CAR_*]部分")
	}

	// 检查CAR_节索引连续性
	indices := make([]int, len(carSections))
	for i, section := range carSections {
		name := section.Name()
		indexStr := strings.TrimPrefix(name, "CAR_")
		index, err := strconv.Atoi(indexStr)
		if err != nil {
			fatalLogger.Fatalf("无效的CAR_节名称: %s", name)
		}
		indices[i] = index
	}
	sort.Ints(indices)
	for i := 1; i < len(indices); i++ {
		if indices[i] != indices[i-1]+1 {
			fatalLogger.Fatalf("CAR_节索引不连续: 找到 %d 和 %d", indices[i-1], indices[i])
		}
	}

	// 验证每个CAR节
	for i := 0; i < len(carSections); i++ {
		section := carSections[i]
		driver := EntryListDriver{
			Name:       section.Key("DRIVERNAME").MustString("test"),
			Team:       section.Key("TEAM").MustString(""),
			Car:        section.Key("MODEL").MustString(""),
			Number:     "",
			Skin:       section.Key("SKIN").MustString(""),
			Ballast:    section.Key("BALLAST").MustInt(0),
			Restrictor: section.Key("RESTRICTOR").MustInt(0),
			GUID:       section.Key("GUID").MustString(""),
		}

		// 验证必填字段
		if !cfg.ACServer.Event.Team.Enable && driver.Name == "" {
			fatalLogger.Fatalf("%s缺少DRIVERNAME字段", section.Name())
		}
		if driver.Car == "" {
			fatalLogger.Fatalf("%s缺少MODEL字段", section.Name())
		}
		if driver.Ballast < 0 {
			fatalLogger.Fatalf("%s BALLAST不能为负数", section.Name())
		}
		if driver.Restrictor < 0 {
			fatalLogger.Fatalf("%s RESTRICTOR不能为负数", section.Name())
		}

		config := &Car{
			Name: driver.Car,
		}
		if globalCars == nil {
			globalCars = make(map[int]*Car)
		}
		carID, err := w.FindOrCreateCarID(config)
		if err == nil {
			globalCars[carID] = config
		}
		if err != nil {
			fatalLogger.Fatalf("获取CarID失败: %v", err)
		}
		config.ID = carID
		// 获取完整的车辆信息
		if w.simulateSQL {
			row := w.DB.QueryRow("SELECT display_name, manufacturer, car_class FROM car WHERE car_id = ?", 1)
			if err := row.Scan(&config.DisplayName, &config.Manufacturer, &config.CarClass); err != nil {
				fatalLogger.Fatalf("模拟SQL模式下获取车辆详细信息失败: %v", err)
			}
		} else {
			row := w.DB.QueryRow("SELECT display_name, manufacturer, car_class FROM car WHERE car_id = ?", carID)
			if err := row.Scan(&config.DisplayName, &config.Manufacturer, &config.CarClass); err != nil {
				fatalLogger.Fatalf("警告: 获取车辆 %s 详细信息失败: %v", driver.Car, err)
			}
		}
		globalCars[carID] = config
		if err != nil {
			fatalLogger.Fatalf("获取CarID失败: %v", err)
		}
		// Process user data if GUID is present
		var userIDs []int
		var userInserted = false

		if driver.GUID != "" {
			// 分割GUID列表
			guids := strings.Split(driver.GUID, ";")
			if len(guids) < 2 && cfg.ACServer.Event.Team.Enable {
				fatalLogger.Fatalf("车队成员需要至少2个有效的GUID，当前只找到%d个", len(guids))
			}
			for _, guid := range guids {
				guid = strings.TrimSpace(guid)
				if guid != "" && driver.Name != "" && len(guid) == 17 && strings.HasPrefix(guid, "7656") {
					steam64ID, err := strconv.ParseInt(guid, 10, 64)
					if err != nil {
						fatalLogger.Fatalf("无效的GUID %s: %v", guid, err)
					}
					user := &User{
						Steam64ID: steam64ID,
						Name:      driver.Name,
					}
					userID, newUser, err := w.FindOrCreateUser(user)
					if err != nil {
						fatalLogger.Fatalf("创建/查询用户 %s 失败: %v", driver.Name, err)
					}
					if newUser {
						logger.Printf("新用户 %s (ID: %d)", driver.Name, userID)
					}
					userIDs = append(userIDs, userID)
					userInserted = true
				}
			}
		}

		if cfg.ACServer.Event.Team.Enable && driver.Team == "" {
			fatalLogger.Fatalf("%s缺少TEAM字段", section.Name())
		}

		if driver.Skin == "" {
			fatalLogger.Fatalf("%s缺少SKIN字段", section.Name())
		}
		// 团队事件处理
		if cfg.ACServer.Event.Team.Enable && driver.GUID != "" {
			// 先定义并赋值teamName和teamNo变量
			var teamName string
			if cfg.ACServer.Event.Team.UseNumber {
				parts := strings.Split(driver.Team, "|")
				if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
					teamName = parts[1]
				} else {
					fatalLogger.Fatalf("无效的车队格式 %s: %s. 预期 'no.|Team Name'", driver.Name, driver.Team)
				}
			} else {
				teamName = driver.Team
			}

			teamNo, err := strconv.Atoi(driver.Number)
			if err != nil {
				fatalLogger.Fatalf("车队车号转换失败: %v", err)
			}
			if teamNo <= 0 {
				fatalLogger.Fatalf("%s NUMBER字段必须是正整数: %s", section.Name(), driver.Number)
			}

			// 创建团队
			team := &Team{
				Name:       teamName,
				TeamNo:     teamNo,
				EventID:    globalEvent.ID,
				CarID:      carID,
				LiveryName: driver.Skin,
				Active:     1,
				CreatedAt:  time.Now().UTC(),
			}
			teamID, err := w.FindOrCreateTeam(team)
			if err == nil {
				globalTeams[teamID] = team
			}
			if err != nil {
				fatalLogger.Fatalf("创建车队 '%s' 失败: %v", teamName, err)
			}

			// 获取现有团队成员
			existingUserIDs := make(map[int]bool)
			rows, err := w.DB.Query("SELECT user_id FROM team_member WHERE team_id = ?", teamID)
			if err != nil {
				fatalLogger.Fatalf("查询团队成员失败: %v", err)
			}
			defer rows.Close()
			for rows.Next() {
				var userID int
				if err := rows.Scan(&userID); err != nil {
					fatalLogger.Fatalf("扫描团队成员失败: %v", err)
				}
				existingUserIDs[userID] = true
			}

			// 将不在新列表中的成员标记为非活动
			if len(existingUserIDs) > 0 {
				for uid := range existingUserIDs {
					found := false
					for _, newUID := range userIDs {
						if uid == newUID {
							found = true
							break
						}
					}
					if !found {
						_, err := w.DB.Exec("UPDATE team_member SET active = 0 WHERE team_id = ? AND user_id = ?", teamID, uid)
						if err != nil {
							fatalLogger.Fatalf("更新成员状态失败: %v", err)
						}
					}
				}
			}

			// 创建或更新团队成员
			if userInserted && len(userIDs) > 0 {
				for _, userID := range userIDs {
					teamMember := &TeamMember{
						TeamID:    teamID,
						UserID:    userID,
						Role:      "",
						Active:    1,
						CreatedAt: time.Now().UTC(),
					}
					if err := w.FindOrCreateTeamMember(teamMember); err != nil {
						fatalLogger.Fatalf("创建团队成员失败: %v", err)
					}
					globalTeamMembers[teamMember.ID] = teamMember
				}
			}
		}
		entryListMutex.Lock()
		if len(globalEntryList) <= i {
			globalEntryList = make(map[int]EntryListDriver)
			for j := 0; j <= i; j++ {
				globalEntryList[j] = EntryListDriver{}
			}
		}
		globalEntryList[i] = driver
		entryListMutex.Unlock()
	}
	logger.Println("entry_list.ini文件验证成功")
	return nil
}

var (
	logger      *log.Logger
	fatalLogger *log.Logger
)

// 初始化日志系统
func initLogger() {
	loggerDir := "logs"
	if err := os.MkdirAll(loggerDir, 0755); err != nil {
		fatalLogger.Fatalf("创建日志目录失败: %v", err)
	}

	logFile, err := os.OpenFile(
		filepath.Join(loggerDir, fmt.Sprintf("simview-writer-%s.log", time.Now().Format("2006-01-02-15-04-05"))),
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

// 事件处理函数 - 重构为外部函数
func handleVersionMessage(versionMsg udp.Version, lastDataTime *time.Time) {
	*lastDataTime = time.Now()
	if versionMsg != 4 {
		fatalLogger.Fatalf("AC服务器版本不匹配")
	}
	dataCollectionEnabled = true
}

// 暂存车辆圈速信息的数据结构
type CarLapData struct {
	SessionID  int
	CarID      int
	GameCarID  int
	LapTime    int
	Sector1    int
	Sector2    int
	Sector3    int
	Tyre       string
	Cuts       uint8
	Crashes    uint8
	CarCrashes uint8
	Laps       uint16
	Completed  uint8
	Timestamp  time.Time
}

// 全局变量用于暂存车辆信息，带互斥锁保证并发安全
var carLapDataMap = make(map[int][]CarLapData)
var lapDataMutex sync.Mutex

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
		Completed:  car.Completed,
		Timestamp:  time.Now(),
	}

	// 从stintLapData获取最新圈数据
	if dataCollectionEnabled {
		sessionMap, sessionOk := stintLapData[lapData.SessionID]
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
					SessionID:    globalSession.ID,
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
		HttpPort:     cfg.ACServer.UDP.LocalPort,
	}
	sessionID, err := globalDBWriter.InsertSession(session)
	if err != nil {
		logger.Printf("插入session记录失败: %v", err)
		return
	}
	// 处理模拟环境下返回ID为0的情况
	if sessionID == 0 && globalDBWriter.simulateSQL {
		staticSessionIDCounter++
		sessionID = staticSessionIDCounter
	}
	session.ID = int(sessionID)
	logger.Printf("创建session %d, 名称:%s", session.ID, session.Name)
	sessionMutex.Lock()
	globalSession = *session
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

var carLapData = make(map[int][]udp.CarUpdate)
var telemetryData = make(map[int][]byte)
var latestCarUpdates = make(map[udp.CarID]udp.CarUpdate)

// pointInPolygon 判断点是否在多边形内（射线法）
func pointInPolygon(point [2]float32, polygon [][3]float32) bool {
	if len(polygon) < 3 {
		return false
	}
	inside := false
	n := len(polygon)

	for i, j := 0, n-1; i < n; j, i = i, i+1 {
		iPoint := polygon[i]
		jPoint := polygon[j]

		// 检查点是否在多边形顶点上
		if (iPoint[0] == point[0] && iPoint[2] == point[1]) ||
			(jPoint[0] == point[0] && jPoint[2] == point[1]) {
			return true
		}

		// 检查边是否与射线相交
		if (iPoint[2] > point[1]) != (jPoint[2] > point[1]) {
			xIntersect := (point[1]-iPoint[2])*(jPoint[0]-iPoint[0])/(jPoint[2]-iPoint[2]) + iPoint[0]
			if point[0] < xIntersect {
				inside = !inside
			}
		}
	}
	return inside
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

var logDir string = ""
var currentLogFile string = ""
var globalNextSessionLineIdx int = -1
var userTyres = make(map[string]string)
var userStintIDs = make(map[string]int)
var tyreRegex = regexp.MustCompile(`(\w+) \[[^\]]*\] changed tyres to (\w+)`)
var connectionRegex = regexp.MustCompile(`(?s)NEW PICKUP CONNECTION from\s+(.*?)\s*\nVERSION .*?\n(.*?)\nREQUESTED CAR: (.*?)\n.*?GUID (\d+) .*?DRIVER ACCEPTED FOR CAR (\d+)`)
var disconnectRegex = regexp.MustCompile(`.*driver disconnected: (\w+) \[[^\]]*\]`)

// 查找最新会话日志文件
func findLatestSessionLogFile(Path string) (os.DirEntry, error) {

	files, err := os.ReadDir(Path)
	if err != nil {
		return nil, fmt.Errorf("无法读取日志目录: %w", err)
	}

	var latestTime time.Time

	timestampRegex := regexp.MustCompile(`output(\d{8})_(\d{5,6})\.log`)

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		match := timestampRegex.FindStringSubmatch(file.Name())
		if len(match) < 3 {
			continue
		}
		// 获取文件信息以检查修改时间
		fileInfo, err := file.Info()
		if err != nil {
			logger.Printf("获取文件信息失败: %v", err)
			continue
		}
		var duration = math.Max(math.Max(float64(globalEvent.PracticeDuration), float64(globalEvent.QualiDuration)), float64(globalEvent.RaceDuration))
		if fileInfo.ModTime().Before(time.Now().Add(-time.Duration(duration+1) * time.Minute)) {
			continue
		}
		timeStr := match[2]
		if len(timeStr) == 5 {
			timeStr = "0" + timeStr
		}
		timestampStr := match[1] + timeStr
		fileTime, err := time.Parse("20060102150405", timestampStr)
		if err != nil {
			logger.Printf("解析文件时间失败: %v", err)
			continue
		}
		if latestFile == nil || fileTime.After(latestTime) {
			latestTime = fileTime
			latestFile = file
		}
	}

	if latestFile == nil {
		return nil, fmt.Errorf("未找到日志文件")
	}

	// 文件切换检测与索引重置
	if latestFile.Name() != currentLogFile {
		globalNextSessionLineIdx = -1
		currentLogFile = latestFile.Name()
		logger.Printf("新日志文件: %s", latestFile.Name())
	}

	return latestFile, nil
}

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

// 创建UDP客户端连接
func createUDPClient(host string, localPort, serverPort int, lastDataTime *time.Time, sessionInfoChan chan<- struct{}, versionInfoChan chan<- struct{}) (*udp.AssettoServerUDP, error) {
	return udp.NewServerClient(host, localPort, serverPort, false, "", 0, func(msg udp.Message) {
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
}

var (
	lastFileSize          int64
	lastFileModTime       time.Time
	lastLineIdx           int
	partialLine           string
	dataCollectionEnabled bool = false
)

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
			if stintLapData[currentSessionID] == nil {
				stintLapData[currentSessionID] = make(map[int][]StintLap)
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
						stintLapData[currentSessionID][carID] = append(stintLapData[currentSessionID][carID], newLap)
						// 仅初始化Sector1，Sector3和Time将在Lap完成时计算
					case 1:
						// split为1时更新最新StintLap的Sector2
						if laps, ok := stintLapData[currentSessionID][carID]; ok && len(laps) > 0 {
							lastIdx := len(laps) - 1
							stintLapData[currentSessionID][carID][lastIdx].Sector2 = int(time)
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
						if laps, ok := stintLapData[currentSessionID][carID]; ok && len(laps) > 0 {
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
							stintLapData[currentSessionID][carID] = append(stintLapData[currentSessionID][carID], newLap)
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
						if _, ok := stintLapData[currentSessionID]; !ok {
							stintLapData[currentSessionID] = make(map[int][]StintLap)
						}
						if laps, ok := stintLapData[currentSessionID][carID]; !ok || len(laps) == 0 {
							stintLapData[currentSessionID][carID] = []StintLap{{
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
							stintLapData[currentSessionID][carID] = laps
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
						if _, ok := stintLapData[currentSessionID]; !ok {
							stintLapData[currentSessionID] = make(map[int][]StintLap)
						}
						if laps, ok := stintLapData[currentSessionID][carID]; !ok || len(laps) == 0 {
							stintLapData[currentSessionID][carID] = []StintLap{{
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
							stintLapData[currentSessionID][carID] = laps
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
						if _, ok := stintLapData[currentSessionID]; !ok {
							stintLapData[currentSessionID] = make(map[int][]StintLap)
						}
						if laps, ok := stintLapData[currentSessionID][carID]; !ok || len(laps) == 0 {
							stintLapData[currentSessionID][carID] = []StintLap{{
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
							stintLapData[currentSessionID][carID] = laps
						}
					}
				}
			}
		}
	}
}

var (
	wsConn          *websocket.Conn
	wsMutex         sync.Mutex
	wsReconnectChan chan struct{}
)

func initWebSocket() error {
	logger.Println("尝试连接WebSocket服务器...")
	url := fmt.Sprintf("ws://127.0.0.1:%d/live", cfg.ACServer.UDP.LocalPort)
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return fmt.Errorf("WebSocket连接失败: %v", err)
	}
	logger.Println("WebSocket连接成功")
	wsConn = conn
	return nil
}

func startWebSocketReconnect() {
	wsReconnectChan = make(chan struct{})
	go func() {
		defer close(wsReconnectChan)
		for {
			if wsConn != nil {
				_, _, err := wsConn.ReadMessage()
				if err != nil {
					logger.Printf("WebSocket连接断开: %v, 尝试重连...", err)
					wsConn.Close()
				}
			}

			// 重连逻辑
			for {
				err := initWebSocket()
				if err != nil {
					logger.Printf("重连失败: %v, 5秒后重试...", err)
					time.Sleep(5 * time.Second)
					continue
				}
				break
			}
		}
	}()
}

// 构建并发送WebSocket消息
func buildAndSendWebSocketMessage() {
	wsMutex.Lock()
	defer wsMutex.Unlock()

	if wsConn == nil {
		return
	}

	// 获取会话信息
	var session ACSession
	// 优先使用全局缓存的会话信息，避免重复查询
	if globalSession.ID > 0 {
		session.SessionID = int64(globalSession.ID)
		session.Type = globalSession.Type
		session.DurationMin = globalSession.DurationMin
		session.StartGrip = globalSession.StartGrip
		session.CurrentGrip = globalSession.CurrentGrip
	} else {
		// 获取最新会话ID
		var latestSessionID int64
		query := "SELECT MAX(session_id) FROM session WHERE event_id = ?"
		err := globalDBWriter.DB.QueryRow(query, globalEvent.ID).Scan(&latestSessionID)
		if err != nil {
			logger.Printf("获取最新会话ID失败: %v", err)
			return
		}

		// 查询会话详情
		query = "SELECT session_id, type, duration_min, start_grip, current_grip FROM session WHERE session_id = ?"
		err = globalDBWriter.DB.QueryRow(query, latestSessionID).Scan(&session.SessionID, &session.Type, &session.DurationMin, &session.StartGrip, &session.CurrentGrip)
		if err != nil {
			logger.Printf("获取会话信息失败: %v", err)
			return
		}
	}

	// 计算已用时间
	elapsedMs, err := calculateElapsedMs(session.SessionID)
	if err == nil {
		session.ElapsedMs = elapsedMs
	}

	// 查询并解析排行榜数据
	entries, err := parseLeaderboardEntries(int(session.SessionID), latestCarUpdates)
	if err != nil {
		logger.Printf("解析排行榜失败: %v", err)
		return
	}

	// 构建JSON消息
	message := struct {
		Version           uint8     `json:"version"`
		BroadcastInterval int32     `json:"broadcastInterval"`
		ViewerCount       int32     `json:"viewerCount"`
		Session           ACSession `json:"session"`
		Entries           []Entry   `json:"entries"`
	}{Version: 10, BroadcastInterval: int32(cfg.Writer.HTTP.Leaderboard.Broadcast.Interval.MS), ViewerCount: 0, Session: session, Entries: entries}

	// 序列化为JSON
	jsonData, err := json.Marshal(message)
	if err != nil {
		logger.Printf("JSON序列化失败: %v", err)
		return
	}

	// 发送消息
	err = wsConn.WriteMessage(websocket.TextMessage, jsonData)
	if err != nil {
		logger.Printf("发送WebSocket消息失败: %v", err)
		wsConn.Close()
		wsConn = nil
	}
}

func main() {
	initLogger()
	logger.Println("Writer应用程序启动")

	// 加载配置
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
			retryCount++
			err = acClient.SendMessage(udp.GetSessionInfo{})
			if err != nil {
				logger.Printf("连接AC服务器失败: %v", err)
			} else {
				if !receivedVersionInfo {
					if retryCount == 1 {
						logger.Printf("连接AC服务器")
					} else {
						logger.Printf("连接AC服务器-第%d次尝试", retryCount)
					}
				}
				// 等待SessionInfo响应
				select {
				case <-sessionInfoChan:
					if receivedVersionInfo {
					} else {
						logger.Println("AC服务器已连接")
					}
					receivedSessionInfo = true

					break
				case <-time.After(retryInterval):
					logger.Printf("AC服务器超时")
					if latestFile != nil && retryCount >= 3 {
						logger.Println("服务器已关闭")
						latestFile = nil
						acClient.Close()
						continue outerLoop
					}
				}

				if receivedSessionInfo {
					break
				}
			}
		}
		break
	}

	// WebSocket连接

	go func() {
		wsPort := cfg.ACServer.UDP.LocalPort
		if err := waitForWebSocketServer(wsPort); err != nil {
			logger.Printf("等待WebSocket服务器失败: %v", err)
			return
		}

		if err := initWebSocket(); err != nil {
			logger.Printf("WebSocket初始连接失败: %v", err)
		}
		startWebSocketReconnect()
	}()

	// 启动循环读取latestfile的goroutine
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			collectCarUpdates()
		}
	}()

	defer func() {
		if acClient != nil {
			acClient.Close()
			logger.Println("UDP客户端已关闭")
		}
	}()

	broadcastInterval := time.Duration(cfg.Writer.HTTP.Leaderboard.Broadcast.Interval.MS) * time.Millisecond
	for range time.Tick(broadcastInterval) {
		// 构建并发送WebSocket消息
		buildAndSendWebSocketMessage()
	}

	// 阻塞主goroutine，防止程序退出
	select {}
}

// 等待WebSocket服务器启动
func waitForWebSocketServer(port int) error {
	retryInterval := 2 * time.Second
	address := fmt.Sprintf("127.0.0.1:%d", port)

	for {
		conn, err := net.Dial("tcp", address)
		if err == nil {
			conn.Close()
			logger.Println("WebSocket服务器已启动")
			return nil
		}
		logger.Printf("等待WebSocket服务器启动...")
		time.Sleep(retryInterval)
	}
}
