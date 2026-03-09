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
	"sort"
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
	result, err := w.DB.Exec(query, ss.FinishedAt, ss.ID)
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
		if w.SimulateSQL {
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
					memberID, err := w.FindOrCreateTeamMember(teamMember)
					if err != nil {
						fatalLogger.Fatalf("创建团队成员失败: %v", err)
					}
					teamMember.ID = memberID
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

	result, err := w.executeSQL(query, stint.SessionID, stint.TeamMemberID, stint.UserID, stint.CarID, stint.StartedAt, stint.FinishedAt)
	if err != nil {
		return 0, err
	}

	if result != nil {
		stintID, err := result.LastInsertId()
		if err != nil {
			return 0, err
		}
		stint.ID = int(stintID)
	}

	return int64(stint.ID), nil
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
		lap.ID = int(lapID)
	}

	return int64(lap.ID), nil
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

// FindOrCreateUser 根据Steam64ID查找或创建用户
func (w *DBWriter) FindOrCreateUser(user *User) (int, bool, error) {
	// 先查找用户
	var foundUser User
	query := "SELECT user_id, name, prev_name, steam64_id, country FROM user WHERE steam64_id = ?"
	err := w.DB.QueryRow(query, user.Steam64ID).Scan(&foundUser.UserID, &foundUser.Name, &foundUser.PrevName, &foundUser.Steam64ID, &foundUser.Country)
	if err == nil {
		// 用户已存在，返回用户ID和false
		return foundUser.UserID, false, nil
	}

	if err != sql.ErrNoRows {
		// 非"未找到"错误，返回错误
		return 0, false, fmt.Errorf("查询用户失败: %v", err)
	}

	// 用户不存在，创建新用户
	insertQuery := "INSERT INTO user (name, steam64_id) VALUES (?, ?)"
	result, err := w.executeSQL(insertQuery, user.Name, user.Steam64ID)
	if err != nil {
		return 0, false, fmt.Errorf("创建用户失败: %v", err)
	}

	userID, err := result.LastInsertId()
	if err != nil {
		return 0, false, fmt.Errorf("获取用户ID失败: %v", err)
	}

	return int(userID), true, nil
}

// InsertStintLap 插入StintLap记录（包装CreateStintLap）
func (w *DBWriter) InsertStintLap(lap *StintLap) (int, error) {
	lapID, err := w.CreateStintLap(lap)
	if err != nil {
		return 0, err
	}
	return int(lapID), nil
}

// InsertSessionFeed 插入SessionFeed记录（包装CreateSessionFeed）
func (w *DBWriter) InsertSessionFeed(feed *SessionFeed) error {
	_, err := w.CreateSessionFeed(feed)
	return err
}

// FindLatestSessionByEventID 查找指定事件的最新会话
func (w *DBWriter) FindLatestSessionByEventID(eventID int) (*Session, error) {
	session := &Session{}
	query := `SELECT session_id, event_id, type, name, track_time, start_time, duration_min, elapsed_ms, laps, weather, air_temp, road_temp, start_grip, current_grip, is_finished, finish_time, last_activity, http_port 
			 FROM session 
			 WHERE event_id = ? 
			 ORDER BY start_time DESC 
			 LIMIT 1`
	err := w.DB.QueryRow(query, eventID).Scan(
		&session.ID, &session.EventID, &session.Type, &session.Name, &session.TrackTime,
		&session.StartTime, &session.DurationMin, &session.ElapsedMs, &session.Laps,
		&session.Weather, &session.AirTemp, &session.RoadTemp, &session.StartGrip,
		&session.CurrentGrip, &session.IsFinished, &session.FinishTime, &session.LastActivity, &session.HTTPPort,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("查找最新会话失败: %v", err)
	}
	return session, nil
}

// FindSessionStintsBySessionID 查找指定会话的所有Stint
func (w *DBWriter) FindSessionStintsBySessionID(sessionID int64) ([]*SessionStint, error) {
	query := `SELECT id, user_id, team_member_id, session_id, car_id, game_car_id, laps, valid_laps, best_lap_id, is_finished, started_at, finished_at 
			 FROM session_stint 
			 WHERE session_id = ?`
	rows, err := w.DB.Query(query, sessionID)
	if err != nil {
		return nil, fmt.Errorf("查询Stint失败: %v", err)
	}
	defer rows.Close()

	var stints []*SessionStint
	for rows.Next() {
		stint := &SessionStint{}
		err := rows.Scan(
			&stint.ID, &stint.UserID, &stint.TeamMemberID, &stint.SessionID, &stint.CarID,
			&stint.GameCarID, &stint.Laps, &stint.ValidLaps, &stint.BestLapID, &stint.IsFinished,
			&stint.StartedAt, &stint.FinishedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("扫描Stint失败: %v", err)
		}
		stints = append(stints, stint)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历Stint失败: %v", err)
	}

	return stints, nil
}

// UpdateSession 更新会话信息
func (w *DBWriter) UpdateSession(session *Session) error {
	query := `UPDATE session SET is_finished = ?, finish_time = ?, last_activity = ? 
			 WHERE session_id = ?`
	_, err := w.executeSQL(query, session.IsFinished, session.FinishTime, session.LastActivity, session.ID)
	if err != nil {
		return fmt.Errorf("更新会话失败: %v", err)
	}
	return nil
}

// FindStintLapsByStintID 查找指定Stint的所有圈速
func (w *DBWriter) FindStintLapsByStintID(stintID int) ([]*StintLap, error) {
	query := `SELECT id, stint_id, sector1, sector2, sector3, grip, tyre, time, cuts, crashes, car_crashes, max_speed, avg_speed, finished_at 
			 FROM stint_lap 
			 WHERE stint_id = ?`
	rows, err := w.DB.Query(query, stintID)
	if err != nil {
		return nil, fmt.Errorf("查询圈速失败: %v", err)
	}
	defer rows.Close()

	var laps []*StintLap
	for rows.Next() {
		lap := &StintLap{}
		err := rows.Scan(
			&lap.ID, &lap.StintID, &lap.Sector1, &lap.Sector2, &lap.Sector3,
			&lap.Grip, &lap.Tyre, &lap.Time, &lap.Cuts, &lap.Crashes,
			&lap.CarCrashes, &lap.MaxSpeed, &lap.AvgSpeed, &lap.FinishedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("扫描圈速失败: %v", err)
		}
		laps = append(laps, lap)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历圈速失败: %v", err)
	}

	return laps, nil
}

// FindStintLapByStintIDAndTime 根据StintID和时间查找圈速
func (w *DBWriter) FindStintLapByStintIDAndTime(stintID int, time int) (bool, error) {
	query := "SELECT COUNT(*) FROM stint_lap WHERE stint_id = ? AND time = ?"
	var count int
	err := w.DB.QueryRow(query, stintID, time).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("查询圈速存在性失败: %v", err)
	}
	return count > 0, nil
}

// FindOrCreateTrackConfigID 查找或创建赛道配置ID
func (w *DBWriter) FindOrCreateTrackConfigID(trackConfig *TrackConfig) (int, error) {
	// 先查找赛道配置
	var configID int
	query := "SELECT track_config_id FROM track_config WHERE track_name = ? AND config_name = ?"
	err := w.DB.QueryRow(query, trackConfig.TrackName, trackConfig.ConfigName).Scan(&configID)
	if err == nil {
		// 配置已存在，返回ID
		return configID, nil
	}

	if err != sql.ErrNoRows {
		// 非"未找到"错误，返回错误
		return 0, fmt.Errorf("查询赛道配置失败: %v", err)
	}

	// 配置不存在，创建新配置
	insertQuery := "INSERT INTO track_config (track_name, config_name, display_name) VALUES (?, ?, ?)"
	result, err := w.executeSQL(insertQuery, trackConfig.TrackName, trackConfig.ConfigName, fmt.Sprintf("%s %s", trackConfig.TrackName, trackConfig.ConfigName))
	if err != nil {
		return 0, fmt.Errorf("创建赛道配置失败: %v", err)
	}

	newConfigID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("获取赛道配置ID失败: %v", err)
	}

	return int(newConfigID), nil
}

// FindOrCreateCarID 查找或创建车辆ID
func (w *DBWriter) FindOrCreateCarID(config *Car) (int, error) {
	// 先查找车辆
	var carID int
	query := "SELECT car_id FROM car WHERE name = ?"
	err := w.DB.QueryRow(query, config.Name).Scan(&carID)
	if err == nil {
		// 车辆已存在，返回ID
		return carID, nil
	}

	if err != sql.ErrNoRows {
		// 非"未找到"错误，返回错误
		return 0, fmt.Errorf("查询车辆失败: %v", err)
	}

	// 车辆不存在，创建新车辆
	insertQuery := "INSERT INTO car (name) VALUES (?)"
	result, err := w.executeSQL(insertQuery, config.Name)
	if err != nil {
		return 0, fmt.Errorf("创建车辆失败: %v", err)
	}

	newCarID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("获取车辆ID失败: %v", err)
	}

	return int(newCarID), nil
}

// FindOrCreateTeam 查找或创建团队
func (w *DBWriter) FindOrCreateTeam(team *Team) (int, error) {
	// 先查找团队
	var teamID int
	query := "SELECT team_id FROM team WHERE event_id = ? AND name = ?"
	err := w.DB.QueryRow(query, team.EventID, team.Name).Scan(&teamID)
	if err == nil {
		// 团队已存在，返回ID
		return teamID, nil
	}

	if err != sql.ErrNoRows {
		// 非"未找到"错误，返回错误
		return 0, fmt.Errorf("查询团队失败: %v", err)
	}

	// 团队不存在，创建新团队
	insertQuery := "INSERT INTO team (event_id, name) VALUES (?, ?)"
	result, err := w.executeSQL(insertQuery, team.EventID, team.Name)
	if err != nil {
		return 0, fmt.Errorf("创建团队失败: %v", err)
	}

	newTeamID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("获取团队ID失败: %v", err)
	}

	return int(newTeamID), nil
}

// FindOrCreateTeamMember 查找或创建团队成员
func (w *DBWriter) FindOrCreateTeamMember(teamMember *TeamMember) (int, error) {
	// 先查找团队成员
	var teamMemberID int
	query := "SELECT team_member_id FROM team_member WHERE team_id = ? AND user_id = ?"
	err := w.DB.QueryRow(query, teamMember.TeamID, teamMember.UserID).Scan(&teamMemberID)
	if err == nil {
		// 团队成员已存在，返回ID
		return teamMemberID, nil
	}

	if err != sql.ErrNoRows {
		// 非"未找到"错误，返回错误
		return 0, fmt.Errorf("查询团队成员失败: %v", err)
	}

	// 团队成员不存在，创建新团队成员
	insertQuery := "INSERT INTO team_member (team_id, user_id) VALUES (?, ?)"
	result, err := w.executeSQL(insertQuery, teamMember.TeamID, teamMember.UserID)
	if err != nil {
		return 0, fmt.Errorf("创建团队成员失败: %v", err)
	}

	newTeamMemberID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("获取团队成员ID失败: %v", err)
	}

	return int(newTeamMemberID), nil
}

// FindEventByName 根据名称查找事件
func (w *DBWriter) FindEventByName(name string) (*Event, error) {
	event := &Event{}
	query := `SELECT event_id, name, server_name, track_config_id, practice_duration, quali_duration, race_duration, race_duration_type, race_extra_laps, reverse_grid_positions, team_event, active 
			 FROM event 
			 WHERE name = ?`
	err := w.DB.QueryRow(query, name).Scan(
		&event.ID, &event.Name, &event.ServerName, &event.TrackConfigID,
		&event.PracticeDuration, &event.QualiDuration, &event.RaceDuration,
		&event.RaceDurationType, &event.RaceExtraLaps, &event.ReverseGridPositions,
		&event.TeamEvent, &event.Active,
	)
	if err != nil {
		return nil, err
	}
	return event, nil
}

// InsertEvent 插入事件记录（包装CreateEvent）
func (w *DBWriter) InsertEvent(event *Event) error {
	_, err := w.CreateEvent(event)
	return err
}

// UpdateEvent 更新事件信息
func (w *DBWriter) UpdateEvent(event *Event) error {
	query := `UPDATE event SET name = ?, server_name = ?, track_config_id = ?, team_event = ?, active = ?, livery_preview = ?, use_number = ? 
			 WHERE event_id = ?`
	_, err := w.executeSQL(query, event.Name, event.ServerName, event.TrackConfigID, event.TeamEvent, event.Active, event.LiveryPreview, event.UseNumber, event.ID)
	if err != nil {
		return fmt.Errorf("更新事件失败: %v", err)
	}
	return nil
}

// InsertSession 插入会话记录（包装CreateSession）
func (w *DBWriter) InsertSession(session *Session) (int64, error) {
	return w.CreateSession(session)
}

// InsertLapTelemetry 插入圈速遥测数据
func (w *DBWriter) InsertLapTelemetry(lapID int, telemetry []byte) error {
	query := "INSERT INTO lap_telemetry (lap_id, telemetry) VALUES (?, ?)"
	_, err := w.executeSQL(query, lapID, telemetry)
	if err != nil {
		return fmt.Errorf("插入遥测数据失败: %v", err)
	}
	return nil
}
