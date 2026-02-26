// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-ini/ini"
	_ "github.com/go-sql-driver/mysql"
)

// DBWriter 数据库操作结构体
type DBWriter struct {
	DB             *sql.DB
	Logger         *log.Logger
	SimulateSQL    bool
	EventID        int
	SessionID      int64
	SessionStintID int64
	ServerCfg      *ServerConfig
	Event          *Event
	Session        *Session
}

// NewDBWriter 创建新的DBWriter实例
func NewDBWriter(dsn string) (*DBWriter, error) {
	// 打开数据库连接
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("无法打开数据库连接: %v", err)
	}

	// 验证连接
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("无法 ping 数据库: %v", err)
	}

	// 创建日志目录
	logDir := "logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("创建日志目录失败: %v", err)
	}

	// 打开日志文件
	logFile, err := os.OpenFile(
		filepath.Join(logDir, fmt.Sprintf("simview-db-%s.log", time.Now().Format("2006-01-02-15-04-05"))),
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		0644,
	)
	if err != nil {
		return nil, fmt.Errorf("打开日志文件失败: %v", err)
	}

	// 创建DBWriter实例
	dbWriter := &DBWriter{
		DB:          db,
		Logger:      log.New(logFile, "[DBWriter] ", log.Ldate|log.Ltime|log.Lshortfile),
		SimulateSQL: true, // 默认开启SQL模拟模式
	}

	// 执行SQL schema文件（如果存在）
	dbWriter.executeSQLSchema()

	return dbWriter, nil
}

// executeSQLSchema 执行SQL schema文件
func (w *DBWriter) executeSQLSchema() {
	// 只在data目录下查找SQL文件
	dataSQLFiles, err := filepath.Glob("data/*.sql")
	if err != nil {
		w.Logger.Printf("查找data目录下的SQL文件失败: %v", err)
		return
	}

	var sqlFile string
	// 优先使用simview-1.2.sql
	for _, file := range dataSQLFiles {
		if strings.Contains(file, "simview-1.2.sql") {
			sqlFile = file
			break
		}
	}

	// 如果没有找到simview-1.2.sql，使用第一个找到的SQL文件
	if sqlFile == "" && len(dataSQLFiles) > 0 {
		sqlFile = dataSQLFiles[0]
	}

	if sqlFile == "" {
		w.Logger.Println("未找到data目录下的SQL schema文件")
		w.Logger.Println("请将原版SimView的SQL文件放置在data目录下")
		return
	}

	// 读取SQL文件内容
	sqlContent, err := os.ReadFile(sqlFile)
	if err != nil {
		w.Logger.Printf("读取SQL文件失败: %v", err)
		return
	}

	// 从SQL文件中提取版本号
	fileVersion := w.extractSQLVersion(sqlContent)

	// 从schema_info表获取数据库版本
	dbVersion := w.getDatabaseVersion()

	// 比较版本并决定是否执行
	if dbVersion == "" || w.isNewerVersion(fileVersion, dbVersion) {
		// 禁用模拟SQL模式，确保schema语句被执行
		originalSimulateSQL := w.SimulateSQL
		w.SimulateSQL = false
		defer func() { w.SimulateSQL = originalSimulateSQL }()

		// 执行SQL语句
		w.Logger.Printf("执行SQL schema: %s", sqlFile)
		sqlStatements := strings.Split(string(sqlContent), ";")
		for i, stmt := range sqlStatements {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" || strings.HasPrefix(stmt, "--") {
				continue
			}

			stmt += ";"
			if _, err := w.executeSQL(stmt); err != nil {
				w.Logger.Printf("SQL执行失败 (行 %d): %v", i+1, err)
				continue
			}
		}
		w.Logger.Println("SQL schema执行完成")
	} else {
		w.Logger.Printf("数据库版本已更新，跳过SQL schema执行")
	}
}

// extractSQLVersion 从SQL文件中提取版本号
func (w *DBWriter) extractSQLVersion(sqlContent []byte) string {
	content := string(sqlContent)
	// 尝试从SQL文件中提取版本号
	// 支持多种版本格式，包括 INSERT INTO `schema_info` VALUES ("1.2", "ac")
	versionPatterns := []string{
		`schema_info.*VALUES.*["']([0-9.]+)["']`,
		`--\s*Version:\s*([0-9.]+)`,
		`/\*\s*Version:\s*([0-9.]+)\s*\*/`,
		`simview-([0-9.]+)\.sql`,
	}

	for _, pattern := range versionPatterns {
		match := regexp.MustCompile(pattern).FindStringSubmatch(content)
		if len(match) > 1 {
			return match[1]
		}
	}
	return ""
}

// getDatabaseVersion 获取数据库版本
func (w *DBWriter) getDatabaseVersion() string {
	// 检查是否存在schema_info表
	if !w.tableExists("schema_info") {
		return ""
	}

	// 查询版本号
	query := "SELECT version FROM schema_info LIMIT 1"
	row := w.DB.QueryRow(query)
	var version string
	err := row.Scan(&version)
	if err != nil {
		if err != sql.ErrNoRows {
			w.Logger.Printf("查询数据库版本失败: %v", err)
		}
		return ""
	}
	return version
}

// isNewerVersion 比较两个版本号，判断fileVersion是否比dbVersion新
func (w *DBWriter) isNewerVersion(fileVersion, dbVersion string) bool {
	// 如果数据库版本为空，认为文件版本更新
	if dbVersion == "" {
		return true
	}
	// 如果文件版本为空，无法比较，默认不更新
	if fileVersion == "" {
		return false
	}

	// 简单的版本比较（仅支持数字和点）
	fileParts := strings.Split(fileVersion, ".")
	dbParts := strings.Split(dbVersion, ".")

	// 取较长的长度
	maxLength := len(fileParts)
	if len(dbParts) > maxLength {
		maxLength = len(dbParts)
	}

	// 比较每一部分
	for i := 0; i < maxLength; i++ {
		filePart := 0
		dbPart := 0

		if i < len(fileParts) {
			filePart, _ = strconv.Atoi(fileParts[i])
		}

		if i < len(dbParts) {
			dbPart, _ = strconv.Atoi(dbParts[i])
		}

		if filePart > dbPart {
			return true
		} else if filePart < dbPart {
			return false
		}
	}

	// 版本相同
	return false
}

// Close 关闭数据库连接
func (w *DBWriter) Close() error {
	return w.DB.Close()
}

// tableExists 检查表是否存在
func (w *DBWriter) tableExists(tableName string) bool {
	query := "SHOW TABLES LIKE ?"
	row := w.DB.QueryRow(query, tableName)
	var name string
	err := row.Scan(&name)
	return err == nil
}

// UpdateSessionStint 更新SessionStint记录
func (w *DBWriter) UpdateSessionStint(ss *SessionStint) (int64, error) {
	query := "UPDATE session_stint SET end_time = ? WHERE session_stint_id = ?"
	result, err := w.DB.Exec(query, ss.EndTime, ss.SessionStintID)
	if err != nil {
		return 0, fmt.Errorf("更新SessionStint失败: %v", err)
	}

	// 使用RowsAffected()而非LastInsertId()，因为这是更新操作
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("获取受影响行数失败: %v", err)
	}

	return affected, nil
}

// executeSQL 执行SQL语句并记录日志
func (w *DBWriter) executeSQL(query string, args ...interface{}) (sql.Result, error) {
	// 记录SQL语句
	w.Logger.Printf("执行SQL: %s, 参数: %v", query, args)

	if w.SimulateSQL {
		// 模拟SQL执行
		w.Logger.Println("SQL模拟模式已开启，跳过实际执行")
		// 返回模拟结果
		return nil, nil
	}

	// 执行SQL语句
	result, err := w.DB.Exec(query, args...)
	if err != nil {
		w.Logger.Printf("SQL执行失败: %v", err)
		return nil, err
	}

	// 记录执行结果
	affected, _ := result.RowsAffected()
	lastID, _ := result.LastInsertId()
	w.Logger.Printf("SQL执行成功，受影响行数: %d, 最后插入ID: %d", affected, lastID)

	return result, nil
}

// LoadServerConfig 加载服务器配置
func (w *DBWriter) LoadServerConfig(gamePath string) error {
	serverCfgPath := filepath.Join(gamePath, "server_cfg.ini")
	if _, err := os.Stat(serverCfgPath); os.IsNotExist(err) {
		return fmt.Errorf("server_cfg.ini文件不存在: %s", serverCfgPath)
	}

	cfg, err := ini.Load(serverCfgPath)
	if err != nil {
		return fmt.Errorf("加载server_cfg.ini失败: %v", err)
	}

	w.ServerCfg = &ServerConfig{
		Name:                cfg.Section("SERVER").Key("NAME").String(),
		Slots:               cfg.Section("SERVER").Key("SLOTS").MustInt(16),
		Port:                cfg.Section("SERVER").Key("UDP_PORT").MustInt(9600),
		HTTP:                cfg.Section("HTTP").Key("HOST").String(),
		HTTPPort:            cfg.Section("HTTP").Key("PORT").MustInt(8081),
		AdminPassword:       cfg.Section("SERVER").Key("ADMIN_PASSWORD").String(),
		Password:            cfg.Section("SERVER").Key("PASSWORD").String(),
		MaxCarSlots:         cfg.Section("SERVER").Key("MAX_CAR_SLOTS").MustInt(50),
		IdleTimeout:         cfg.Section("SERVER").Key("IDLE_TIMEOUT").MustInt(120),
		VoteTimeout:         cfg.Section("SERVER").Key("VOTE_TIMEOUT").MustInt(60),
		QualifyStandingType: cfg.Section("SERVER").Key("QUALIFY_STANDING_TYPE").MustInt(0),
		PitWindowStart:      cfg.Section("PIT").Key("PIT_WINDOW_START").MustInt(-1),
		PitWindowEnd:        cfg.Section("PIT").Key("PIT_WINDOW_END").MustInt(-1),
		ShortFormationLap:   cfg.Section("SERVER").Key("SHORT_FORMATION_LAP").MustInt(0),
	}

	return nil
}

// LoadEntryList 加载参赛列表
func (w *DBWriter) LoadEntryList(gamePath string) error {
	entryListPath := filepath.Join(gamePath, "entry_list.ini")
	if _, err := os.Stat(entryListPath); os.IsNotExist(err) {
		return fmt.Errorf("entry_list.ini文件不存在: %s", entryListPath)
	}

	cfg, err := ini.Load(entryListPath)
	if err != nil {
		return fmt.Errorf("加载entry_list.ini失败: %v", err)
	}

	// 处理条目列表
	for _, section := range cfg.Sections() {
		if strings.HasPrefix(section.Name(), "CAR_") {
			// 处理车辆配置
			// 可以在这里添加车辆配置到全局列表或进行其他处理
		}
	}

	return nil
}

// CreateEvent 创建事件
func (w *DBWriter) CreateEvent(event *Event) (int, error) {
	query := `INSERT INTO event (name, server_name, track_config_id, practice_duration, quali_duration, race_duration, race_duration_type, race_extra_laps, reverse_grid_positions, team_event, active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	result, err := w.executeSQL(query, event.Name, event.ServerName, event.TrackConfigID, event.PracticeDuration, event.QualiDuration, event.RaceDuration, event.RaceDurationType, event.RaceExtraLaps, event.ReverseGridPositions, event.TeamEvent, event.Active)
	if err != nil {
		return 0, err
	}

	if result != nil {
		eventID, err := result.LastInsertId()
		if err != nil {
			return 0, err
		}
		event.ID = int(eventID)
	}

	return event.ID, nil
}

// CreateSession 创建会话
func (w *DBWriter) CreateSession(session *Session) (int64, error) {
	query := `INSERT INTO session (event_id, type, name, track_time, start_time, duration_min, weather, air_temp, road_temp, start_grip, current_grip, http_port) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	result, err := w.executeSQL(query, session.EventID, session.Type, session.Name, session.TrackTime, session.StartTime, session.DurationMin, session.Weather, session.AirTemp, session.RoadTemp, session.StartGrip, session.CurrentGrip, session.HTTPPort)
	if err != nil {
		return 0, err
	}

	if result != nil {
		sessionID, err := result.LastInsertId()
		if err != nil {
			return 0, err
		}
		session.ID = sessionID
	}

	return session.ID, nil
}

// CreateSessionStint 创建SessionStint
func (w *DBWriter) CreateSessionStint(stint *SessionStint) (int64, error) {
	query := `INSERT INTO session_stint (session_id, team_member_id, user_id, car_id, start_time, end_time) VALUES (?, ?, ?, ?, ?, ?)`

	result, err := w.executeSQL(query, stint.SessionID, stint.TeamMemberID, stint.UserID, stint.CarID, stint.StartTime, stint.EndTime)
	if err != nil {
		return 0, err
	}

	if result != nil {
		stintID, err := result.LastInsertId()
		if err != nil {
			return 0, err
		}
		stint.SessionStintID = stintID
	}

	return stint.SessionStintID, nil
}

// CreateStintLap 创建StintLap
func (w *DBWriter) CreateStintLap(lap *StintLap) (int64, error) {
	query := `INSERT INTO stint_lap (stint_id, time, sector_1, sector_2, sector_3, grip, tyre, avg_speed, max_speed, cuts, crashes, car_crashes, finished_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	result, err := w.executeSQL(query, lap.StintID, lap.Time, lap.Sector1, lap.Sector2, lap.Sector3, lap.Grip, lap.Tyre, lap.AvgSpeed, lap.MaxSpeed, lap.Cuts, lap.Crashes, lap.CarCrashes, lap.FinishedAt)
	if err != nil {
		return 0, err
	}

	if result != nil {
		lapID, err := result.LastInsertId()
		if err != nil {
			return 0, err
		}
		lap.StintLapID = lapID
	}

	return lap.StintLapID, nil
}

// CreateLapTelemetry 创建LapTelemetry
func (w *DBWriter) CreateLapTelemetry(telemetry *LapTelemetry) (int64, error) {
	query := `INSERT INTO lap_telemetry (lap_id, telemetry) VALUES (?, ?)`

	result, err := w.executeSQL(query, telemetry.LapID, telemetry.Telemetry)
	if err != nil {
		return 0, err
	}

	if result != nil {
		telemetryID, err := result.LastInsertId()
		if err != nil {
			return 0, err
		}
		telemetry.ID = telemetryID
	}

	return telemetry.ID, nil
}

// CreateSessionFeed 创建SessionFeed
func (w *DBWriter) CreateSessionFeed(feed *SessionFeed) (int64, error) {
	query := `INSERT INTO session_feed (session_id, type, detail, time) VALUES (?, ?, ?, ?)`

	result, err := w.executeSQL(query, feed.SessionID, feed.Type, feed.Detail, feed.Time)
	if err != nil {
		return 0, err
	}

	if result != nil {
		feedID, err := result.LastInsertId()
		if err != nil {
			return 0, err
		}
		feed.ID = feedID
	}

	return feed.ID, nil
}

// UpdateSessionGrip 更新会话抓地力
func (w *DBWriter) UpdateSessionGrip(sessionID int64, currentGrip float64) error {
	query := "UPDATE session SET current_grip = ?, last_activity = ? WHERE session_id = ?"
	_, err := w.executeSQL(query, currentGrip, time.Now(), sessionID)
	return err
}

// GetLatestSessionID 获取最新会话ID
func (w *DBWriter) GetLatestSessionID(eventID int) (int64, error) {
	var latestSessionID int64
	query := "SELECT MAX(session_id) FROM session WHERE event_id = ?"
	err := w.DB.QueryRow(query, eventID).Scan(&latestSessionID)
	if err != nil {
		return 0, fmt.Errorf("获取最新会话ID失败: %v", err)
	}

	return latestSessionID, nil
}

// GetSessionDetails 获取会话详情
func (w *DBWriter) GetSessionDetails(sessionID int64) (*Session, error) {
	session := &Session{}
	query := "SELECT session_id, type, name, track_time, duration_min, weather, air_temp, road_temp, start_grip, current_grip, http_port FROM session WHERE session_id = ?"
	err := w.DB.QueryRow(query, sessionID).Scan(&session.ID, &session.Type, &session.Name, &session.TrackTime, &session.DurationMin, &session.Weather, &session.AirTemp, &session.RoadTemp, &session.StartGrip, &session.CurrentGrip, &session.HTTPPort)
	if err != nil {
		return nil, fmt.Errorf("获取会话详情失败: %v", err)
	}

	return session, nil
}

// GetEventByID 根据ID获取事件
func (w *DBWriter) GetEventByID(eventID int) (*Event, error) {
	event := &Event{}
	query := "SELECT event_id, name, server_name, track_config_id, practice_duration, quali_duration, race_duration, race_duration_type, race_extra_laps, reverse_grid_positions, team_event, active FROM event WHERE event_id = ?"
	err := w.DB.QueryRow(query, eventID).Scan(&event.ID, &event.Name, &event.ServerName, &event.TrackConfigID, &event.PracticeDuration, &event.QualiDuration, &event.RaceDuration, &event.RaceDurationType, &event.RaceExtraLaps, &event.ReverseGridPositions, &event.TeamEvent, &event.Active)
	if err != nil {
		return nil, fmt.Errorf("获取事件失败: %v", err)
	}

	return event, nil
}

// GetTrackConfigByID 根据ID获取赛道配置
func (w *DBWriter) GetTrackConfigByID(trackConfigID int) (*TrackConfig, error) {
	trackConfig := &TrackConfig{}
	query := "SELECT track_config_id, track_name, config_name, display_name, country, city, length FROM track_config WHERE track_config_id = ?"
	err := w.DB.QueryRow(query, trackConfigID).Scan(&trackConfig.ID, &trackConfig.TrackName, &trackConfig.ConfigName, &trackConfig.DisplayName, &trackConfig.Country, &trackConfig.City, &trackConfig.Length)
	if err != nil {
		return nil, fmt.Errorf("获取赛道配置失败: %v", err)
	}

	return trackConfig, nil
}

// UpdateEventActiveStatus 更新事件活跃状态
func (w *DBWriter) UpdateEventActiveStatus(eventID int, active int) error {
	query := "UPDATE event SET active = ? WHERE event_id = ?"
	_, err := w.executeSQL(query, active, eventID)
	return err
}

// UpdateSessionFinishTime 更新会话结束时间
func (w *DBWriter) UpdateSessionFinishTime(sessionID int64, finishTime time.Time) error {
	query := "UPDATE session SET finish_time = ?, is_finished = 1 WHERE session_id = ?"
	_, err := w.executeSQL(query, finishTime, sessionID)
	return err
}
