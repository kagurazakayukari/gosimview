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

// 模拟执行结果(simResult)、可空列扫描工具(nstr/nint/nf64/ntime)、
// SQL 脚本安全切分见 db_support.go。

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

// NewDBWriter 创建新的DBWriter实例。simulate 为 true 时不向数据库写入（仅记录 SQL）。
func NewDBWriter(dsn string, simulate bool) (*DBWriter, error) {
	// 打开数据库连接（sql.Open 只校验 DSN，不建连）
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("无法打开数据库连接: %v", err)
	}

	// 只有真实写入模式才验证连接：模拟模式不 Ping、不读库，本地无 MySQL 也能运行
	if !simulate {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			db.Close() // 连接验证失败，清理句柄
			return nil, fmt.Errorf("无法 ping 数据库: %v", err)
		}
	}

	// 创建日志目录
	logDir := "logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		db.Close()
		return nil, fmt.Errorf("创建日志目录失败: %v", err)
	}

	// 打开日志文件
	logFile, err := os.OpenFile(
		filepath.Join(logDir, fmt.Sprintf("simview-db-%s.log", time.Now().Format("2006-01-02-15-04-05"))),
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		0644,
	)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("打开日志文件失败: %v", err)
	}

	// 创建DBWriter实例
	dbWriter := &DBWriter{
		DB:          db,
		Logger:      log.New(logFile, "[DBWriter] ", log.Ldate|log.Ltime|log.Lshortfile),
		SimulateSQL: simulate,
	}

	// 不自动执行 SQL schema：原版 dump 含 DROP TABLE，误执行会清空真实数据。
	// 需要初始化时由调用方显式调用 ApplySchema()（只会执行非破坏性语句）。

	return dbWriter, nil
}

// ApplySchema 显式应用 SQL schema 文件（不会自动调用）。
// 只执行非破坏性语句：DROP/TRUNCATE/DELETE/ALTER/UPDATE 等一律跳过。
func (w *DBWriter) ApplySchema() {
	// 只在data目录下查找SQL文件（支持从仓库根或 simview/ 下启动）
	dataSQLFiles, _ := filepath.Glob("data/*.sql")
	if len(dataSQLFiles) == 0 {
		dataSQLFiles, _ = filepath.Glob("simview/data/*.sql")
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

	// 版本未变化则不处理
	if dbVersion != "" && !w.isNewerVersion(fileVersion, dbVersion) {
		w.Logger.Printf("数据库版本已更新，跳过SQL schema执行")
		return
	}

	// schema 语句必须真实执行（与 SimulateSQL 无关），但只执行非破坏性语句
	originalSimulateSQL := w.SimulateSQL
	w.SimulateSQL = false
	defer func() { w.SimulateSQL = originalSimulateSQL }()

	w.Logger.Printf("应用SQL schema: %s", sqlFile)
	applied, skipped := 0, 0
	for _, raw := range splitSQLStatements(string(sqlContent)) {
		stmt := cleanSQLStatement(raw)
		if stmt == "" {
			continue
		}
		if isDestructiveSQL(stmt) {
			skipped++
			w.Logger.Printf("跳过破坏性语句: %s", firstLine(stmt))
			continue
		}
		if _, err := w.executeSQL(stmt); err != nil {
			w.Logger.Printf("SQL执行失败(%s): %v", firstLine(stmt), err)
			continue
		}
		applied++
	}
	w.Logger.Printf("SQL schema应用完成：执行 %d 条，跳过 %d 条", applied, skipped)
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
	query := "UPDATE session_stint SET finished_at = ? WHERE session_stint_id = ?"
	// 零值时间写 NULL，避免 TIMESTAMP 越界
	var finishedAt interface{}
	if !ss.FinishedAt.IsZero() {
		finishedAt = ss.FinishedAt
	}
	result, err := w.executeSQL(query, finishedAt, ss.ID)
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
		// 模拟SQL执行：不写库，返回稳定且非零的模拟ID（下游会直接取 LastInsertId）
		id := nextSimulatedID()
		w.Logger.Printf("SQL模拟模式：跳过实际执行，返回模拟ID %d", id)
		return simResult{id: id}, nil
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
	// 标准布局：<server>/cfg/server_cfg.ini；兼容旧布局 <server>/server_cfg.ini
	serverCfgPath := ""
	for _, candidate := range []string{
		filepath.Join(gamePath, "cfg", "server_cfg.ini"),
		filepath.Join(gamePath, "server_cfg.ini"),
	} {
		if _, err := os.Stat(candidate); err == nil {
			serverCfgPath = candidate
			break
		}
	}
	if serverCfgPath == "" {
		return fmt.Errorf("server_cfg.ini文件不存在（已尝试 cfg/server_cfg.ini、server_cfg.ini）: %s", gamePath)
	}

	// 先捕获包级配置：下面 cfg 会被 ini 文件遮蔽
	appCfg := cfg

	cfg, err := ini.Load(serverCfgPath)
	if err != nil {
		return fmt.Errorf("加载server_cfg.ini失败: %v", err)
	}

	// UDP 端口映射（对齐 config.toml 注释，方向容易搞反）：
	//   ac.server.udp.port       = AC 服务器开给插件的端口 = UDP_PLUGIN_LOCAL_PORT（SimView 发送目标）
	//   ac.server.udp.local.port = SimView 的接收端口     = UDP_PLUGIN_ADDRESS 里的端口
	if appCfg != nil {
		serverSection := cfg.Section("SERVER")
		if pluginPort := serverSection.Key("UDP_PLUGIN_LOCAL_PORT").MustInt(0); pluginPort > 0 && appCfg.ACServer.UDP.Port == 0 {
			appCfg.ACServer.UDP.Port = pluginPort
		}
		if addr := serverSection.Key("UDP_PLUGIN_ADDRESS").String(); addr != "" {
			if parts := strings.Split(addr, ":"); len(parts) == 2 {
				if localPort, convErr := strconv.Atoi(strings.TrimSpace(parts[1])); convErr == nil && appCfg.ACServer.UDP.LocalPort == 0 {
					appCfg.ACServer.UDP.LocalPort = localPort
				}
			}
		}
	}

	// 从 server_cfg.ini 解析各 session 时长（取不到则为默认值）
	practiceDuration := cfg.Section("PRACTICE").Key("TIME").MustInt(-1)
	qualiDuration := cfg.Section("QUALIFY").Key("TIME").MustInt(-1)
	raceLaps := cfg.Section("RACE").Key("LAPS").MustInt(0)
	raceTime := cfg.Section("RACE").Key("TIME").MustInt(-1)
	raceDuration := raceTime
	raceDurationType := 0
	if raceLaps > 0 {
		raceDuration = raceLaps
		raceDurationType = 1 // 1 = 按圈数
	}

	w.ServerCfg = &ServerConfig{
		Name:                 cfg.Section("SERVER").Key("NAME").String(),
		Slots:                cfg.Section("SERVER").Key("SLOTS").MustInt(16),
		Port:                 cfg.Section("SERVER").Key("UDP_PORT").MustInt(9600),
		HTTP:                 cfg.Section("HTTP").Key("HOST").String(),
		HTTPPort:             cfg.Section("HTTP").Key("PORT").MustInt(8081),
		AdminPassword:        cfg.Section("SERVER").Key("ADMIN_PASSWORD").String(),
		Password:             cfg.Section("SERVER").Key("PASSWORD").String(),
		MaxCarSlots:          cfg.Section("SERVER").Key("MAX_CAR_SLOTS").MustInt(50),
		IdleTimeout:          cfg.Section("SERVER").Key("IDLE_TIMEOUT").MustInt(120),
		VoteTimeout:          cfg.Section("SERVER").Key("VOTE_TIMEOUT").MustInt(60),
		QualifyStandingType:  cfg.Section("SERVER").Key("QUALIFY_STANDING_TYPE").MustInt(0),
		PitWindowStart:       cfg.Section("PIT").Key("PIT_WINDOW_START").MustInt(-1),
		PitWindowEnd:         cfg.Section("PIT").Key("PIT_WINDOW_END").MustInt(-1),
		ShortFormationLap:    cfg.Section("SERVER").Key("SHORT_FORMATION_LAP").MustInt(0),
		PracticeDuration:     practiceDuration,
		QualiDuration:        qualiDuration,
		RaceDuration:         raceDuration,
		RaceDurationType:     raceDurationType,
		RaceWaitTime:         cfg.Section("RACE").Key("WAIT_TIME").MustInt(0),
		RaceExtraLaps:        cfg.Section("SERVER").Key("RACE_EXTRA_LAPS").MustInt(0),
		ReverseGridPositions: cfg.Section("SERVER").Key("REVERSED_GRID_RACE_POSITIONS").MustInt(0),
	}

	return nil
}

// LoadEntryList 加载参赛列表
func (w *DBWriter) LoadEntryList(gamePath string) error {
	entryListPath := filepath.Join(gamePath, "cfg", "entry_list.ini")
	// 加载INI文件
	entrylist, err := ini.LoadSources(ini.LoadOptions{IgnoreInlineComment: true}, entryListPath)
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

	// 以 [CAR_n] 的 n 作为槽位（UDP 的 CarID 即该槽位），按槽位升序处理
	sectionBySlot := make(map[int]*ini.Section, len(carSections))
	slots := make([]int, 0, len(carSections))
	for _, section := range carSections {
		name := section.Name()
		index, err := strconv.Atoi(strings.TrimPrefix(name, "CAR_"))
		if err != nil {
			fatalLogger.Fatalf("无效的CAR_节名称: %s", name)
		}
		if _, dup := sectionBySlot[index]; dup {
			fatalLogger.Fatalf("重复的CAR_节: %s", name)
		}
		sectionBySlot[index] = section
		slots = append(slots, index)
	}
	sort.Ints(slots)
	for i := 1; i < len(slots); i++ {
		if slots[i] != slots[i-1]+1 {
			fatalLogger.Fatalf("CAR_节索引不连续: 找到 %d 和 %d", slots[i-1], slots[i])
		}
	}

	// 验证每个CAR节
	for _, slot := range slots {
		section := sectionBySlot[slot]
		driver := EntryListDriver{
			Index:      slot,
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
			Name:         driver.Car,
			DisplayName:  driver.Car,
			Manufacturer: "N/A",
		}
		if cls, ok := cfg.Overrides.CarClass[driver.Car]; ok {
			config.CarClass = cls
		}
		// FindOrCreateCarID 已把车辆写进缓存（同一指针，后续字段修改自动生效）
		carID, err := w.FindOrCreateCarID(config)
		if err != nil {
			fatalLogger.Fatalf("获取CarID失败: %v", err)
		}
		config.ID = carID
		// 获取完整的车辆信息（查不到只告警，不终止启动；模拟模式不读库）
		if !w.SimulateSQL {
			var (
				displayName  sql.NullString
				manufacturer sql.NullString
				carClass     sql.NullString
			)
			row := w.DB.QueryRow("SELECT display_name, manufacturer, car_class FROM car WHERE car_id = ?", carID)
			if err := row.Scan(&displayName, &manufacturer, &carClass); err != nil {
				logger.Printf("警告: 获取车辆 %s 详细信息失败: %v", driver.Car, err)
			}
			if v := nstr(displayName); v != "" {
				config.DisplayName = v
			}
			if v := nstr(manufacturer); v != "" {
				config.Manufacturer = v
			}
			if v := nstr(carClass); v != "" {
				config.CarClass = v
			}
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
			// TEAM 字段：UseNumber 时为 "编号|车队名"，否则整串即车队名
			teamName, teamNo := parseEntryTeam(driver.Team, cfg.ACServer.Event.Team.UseNumber)
			if teamName == "" {
				fatalLogger.Fatalf("无效的车队格式 %s: %s. 预期 'no.|Team Name'", driver.Name, driver.Team)
			}
			if cfg.ACServer.Event.Team.UseNumber && teamNo <= 0 {
				fatalLogger.Fatalf("%s TEAM字段缺少合法车号: %s", section.Name(), driver.Team)
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
			if err != nil {
				fatalLogger.Fatalf("创建车队 '%s' 失败: %v", teamName, err)
			}
			cacheTeam(team)

			// 获取现有团队成员（模拟模式不读库）
			existingUserIDs := make(map[int]bool)
			if !w.SimulateSQL {
				rows, err := w.DB.Query("SELECT user_id FROM team_member WHERE team_id = ?", teamID)
				if err != nil {
					fatalLogger.Fatalf("查询团队成员失败: %v", err)
				}
				for rows.Next() {
					var userID int
					if err := rows.Scan(&userID); err != nil {
						fatalLogger.Fatalf("扫描团队成员失败: %v", err)
					}
					existingUserIDs[userID] = true
				}
				rows.Close()
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
						_, err := w.executeSQL("UPDATE team_member SET active = 0 WHERE team_id = ? AND user_id = ?", teamID, uid)
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
					cacheTeamMember(teamMember)
				}
			}
		}
		entryListMutex.Lock()
		if globalEntryList == nil {
			globalEntryList = make(map[int]EntryListDriver)
		}
		globalEntryList[slot] = driver
		entryListMutex.Unlock()
	}
	logger.Println("entry_list.ini文件验证成功")
	return nil
}

// CreateEvent 创建事件
func (w *DBWriter) CreateEvent(event *Event) (int, error) {
	query := `INSERT INTO event (name, server_name, track_config_id, practice_duration, quali_duration, race_duration, race_duration_type, race_wait_time, race_extra_laps, reverse_grid_positions, team_event, active, livery_preview, use_number) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	result, err := w.executeSQL(query, event.Name, event.ServerName, event.TrackConfigID, event.PracticeDuration, event.QualiDuration, event.RaceDuration, event.RaceDurationType, event.RaceWaitTime, event.RaceExtraLaps, event.ReverseGridPositions, event.TeamEvent, event.Active, event.LiveryPreview, event.UseNumber)
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

	if w.SimulateSQL {
		// 记住模拟 event，使 FindEventByName 返回稳定的 event_id（前端 cookie 依赖它）
		rememberSimEvent(event)
	}

	return event.ID, nil
}

// CreateSession 创建会话
func (w *DBWriter) CreateSession(session *Session) (int64, error) {
	query := `INSERT INTO session (event_id, type, name, track_time, start_time, duration_min, elapsed_ms, laps, weather, air_temp, road_temp, start_grip, current_grip, is_finished, last_activity, http_port) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	result, err := w.executeSQL(query, session.EventID, session.Type, session.Name, session.TrackTime, session.StartTime, session.DurationMin, session.ElapsedMs, session.Laps, session.Weather, session.AirTemp, session.RoadTemp, session.StartGrip, session.CurrentGrip, session.IsFinished, session.LastActivity, session.HTTPPort)
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

// CreateSessionStint 创建SessionStint。
// 字段语义：stint.CarID 为数据库 car_id，stint.GameCarID 为 UDP 槽位号。
// 可选外键（user_id / team_member_id / car_id / best_lap_id）缺省时写 NULL 而不是 0。
func (w *DBWriter) CreateSessionStint(stint *SessionStint) (int64, error) {
	if stint == nil {
		return 0, fmt.Errorf("session_stint为空")
	}

	query := `INSERT INTO session_stint (session_id, user_id, team_member_id, car_id, game_car_id, laps, valid_laps, best_lap_id, is_finished, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	// started_at 为 NOT NULL，零值时用当前时间
	startedAt := stint.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now()
	}
	// finished_at 可空，零值写 NULL
	var finishedAt interface{}
	if !stint.FinishedAt.IsZero() {
		finishedAt = stint.FinishedAt
	}

	result, err := w.executeSQL(query,
		optionalInt(stint.SessionID),
		optionalInt(stint.UserID),
		optionalInt(stint.TeamMemberID),
		optionalInt(stint.CarID),
		stint.GameCarID,
		stint.Laps,
		stint.ValidLaps,
		optionalInt(stint.BestLapID),
		stint.IsFinished,
		startedAt,
		finishedAt,
	)
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

// GetLatestSessionID 获取最新会话ID（无记录时返回 0）
func (w *DBWriter) GetLatestSessionID(eventID int) (int64, error) {
	if w.SimulateSQL {
		return 0, nil
	}
	var latestSessionID sql.NullInt64
	query := "SELECT MAX(session_id) FROM session WHERE event_id = ?"
	err := w.DB.QueryRow(query, eventID).Scan(&latestSessionID)
	if err != nil {
		return 0, fmt.Errorf("获取最新会话ID失败: %v", err)
	}

	return latestSessionID.Int64, nil
}

// GetSessionDetails 获取会话详情（track_time / 温度 / 抓地力允许为 NULL）
func (w *DBWriter) GetSessionDetails(sessionID int64) (*Session, error) {
	session := &Session{}
	query := "SELECT session_id, type, name, track_time, duration_min, weather, air_temp, road_temp, start_grip, current_grip, http_port FROM session WHERE session_id = ?"
	var (
		sid         sql.NullInt64
		sessionType sql.NullInt64
		name        sql.NullString
		trackTime   sql.NullString
		durationMin sql.NullInt64
		weather     sql.NullString
		airTemp     sql.NullFloat64
		roadTemp    sql.NullFloat64
		startGrip   sql.NullFloat64
		currentGrip sql.NullFloat64
		httpPort    sql.NullInt64
	)
	err := w.DB.QueryRow(query, sessionID).Scan(&sid, &sessionType, &name, &trackTime, &durationMin, &weather, &airTemp, &roadTemp, &startGrip, &currentGrip, &httpPort)
	if err != nil {
		return nil, fmt.Errorf("获取会话详情失败: %v", err)
	}
	session.ID = sid.Int64
	session.Type = nint(sessionType)
	session.Name = nstr(name)
	session.TrackTime = nstr(trackTime)
	session.DurationMin = nint(durationMin)
	session.Weather = nstr(weather)
	session.AirTemp = nf64(airTemp)
	session.RoadTemp = nf64(roadTemp)
	session.StartGrip = nf64(startGrip)
	session.CurrentGrip = nf64(currentGrip)
	session.HTTPPort = nint(httpPort)

	return session, nil
}

// GetEventByID 根据ID获取事件
func (w *DBWriter) GetEventByID(eventID int) (*Event, error) {
	event := &Event{}
	query := "SELECT event_id, name, server_name, track_config_id, practice_duration, quali_duration, race_duration, race_duration_type, race_extra_laps, reverse_grid_positions, team_event, active FROM event WHERE event_id = ?"
	var (
		id          sql.NullInt64
		name        sql.NullString
		serverName  sql.NullString
		trackConfID sql.NullInt64
		practiceDur sql.NullInt64
		qualiDur    sql.NullInt64
		raceDur     sql.NullInt64
		raceDurType sql.NullInt64
		raceExtra   sql.NullInt64
		reverseGrid sql.NullInt64
		teamEvent   sql.NullInt64
		active      sql.NullInt64
	)
	err := w.DB.QueryRow(query, eventID).Scan(&id, &name, &serverName, &trackConfID, &practiceDur, &qualiDur, &raceDur, &raceDurType, &raceExtra, &reverseGrid, &teamEvent, &active)
	if err != nil {
		return nil, fmt.Errorf("获取事件失败: %v", err)
	}
	event.ID = nint(id)
	event.Name = nstr(name)
	event.ServerName = nstr(serverName)
	event.TrackConfigID = nint(trackConfID)
	event.PracticeDuration = nint(practiceDur)
	event.QualiDuration = nint(qualiDur)
	event.RaceDuration = nint(raceDur)
	event.RaceDurationType = nint(raceDurType)
	event.RaceExtraLaps = nint(raceExtra)
	event.ReverseGridPositions = nint(reverseGrid)
	event.TeamEvent = nint(teamEvent)
	event.Active = nint(active)

	return event, nil
}

// readTrackMetadata 从游戏目录的 ui_track.json 读取赛道显示名/国家/城市/长度。
// 依次尝试 layout 专属与基础目录，读不到就返回空值。
func readTrackMetadata(trackName, configName string) (display, country, city string, length int) {
	if cfg == nil || cfg.Game.Path == "" {
		return "", "", "", 0
	}
	// 原配置指定 AC server 根目录，赛道资料从该目录的 content 读取。
	base := filepath.Join(cfg.Game.Path, "content", "tracks", trackName)
	candidates := []string{
		filepath.Join(base, configName, "ui", "ui_track.json"),
		filepath.Join(base, "ui", configName, "ui_track.json"),
		filepath.Join(base, "ui", "ui_track.json"),
	}
	for _, p := range candidates {
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var raw struct {
			Name    string          `json:"name"`
			City    string          `json:"city"`
			Country string          `json:"country"`
			Length  json.RawMessage `json:"length"`
		}
		if err := json.Unmarshal(data, &raw); err != nil {
			continue
		}
		return raw.Name, raw.Country, raw.City, parseTrackLength(raw.Length)
	}
	return "", "", "", 0
}

// parseTrackLength 兼容 ui_track.json 中 length 为字符串或数字的情况，统一返回米
func parseTrackLength(raw json.RawMessage) int {
	if len(raw) == 0 {
		return 0
	}
	// 数字形式
	var n float64
	if err := json.Unmarshal(raw, &n); err == nil {
		return int(n)
	}
	// 字符串形式（AC 的 ui_track.json 里 length 常带单位）
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0
	}
	return parseLengthString(s)
}

// parseLengthString 解析带单位的长度字符串，统一换算为米。
// 例如 "4304"、"4304 m"、"4.304 km" 都返回 4304。
func parseLengthString(s string) int {
	lower := strings.ToLower(strings.TrimSpace(s))
	if lower == "" {
		return 0
	}
	isKM := strings.Contains(lower, "km")

	// 取第一个数字（含小数点）
	var sb strings.Builder
	seenDigit := false
	for _, r := range lower {
		if r >= '0' && r <= '9' {
			sb.WriteRune(r)
			seenDigit = true
			continue
		}
		if r == '.' && seenDigit {
			sb.WriteRune(r)
			continue
		}
		if seenDigit {
			break
		}
	}
	if !seenDigit {
		return 0
	}
	value, err := strconv.ParseFloat(sb.String(), 64)
	if err != nil {
		return 0
	}
	if isKM {
		value *= 1000
	}
	return int(value)
}

// GetTrackConfigByID 根据ID获取赛道配置（文本列允许为 NULL），并回写缓存供主循环读取
func (w *DBWriter) GetTrackConfigByID(trackConfigID int) (*TrackConfig, error) {
	// 模拟模式不读库：缓存命中由 FindOrCreateTrackConfigID 保证，未命中直接返回空
	if w.SimulateSQL {
		return nil, fmt.Errorf("模拟模式下不读取 track_config 表(track_config_id=%d)", trackConfigID)
	}

	trackConfig := &TrackConfig{}
	var (
		id          sql.NullInt64
		trackName   sql.NullString
		configName  sql.NullString
		displayName sql.NullString
		country     sql.NullString
		city        sql.NullString
		length      sql.NullInt64
	)
	query := "SELECT track_config_id, track_name, config_name, display_name, country, city, length FROM track_config WHERE track_config_id = ?"
	err := w.DB.QueryRow(query, trackConfigID).Scan(&id, &trackName, &configName, &displayName, &country, &city, &length)
	if err != nil {
		return nil, fmt.Errorf("获取赛道配置失败: %v", err)
	}
	trackConfig.ID = nint(id)
	trackConfig.TrackName = nstr(trackName)
	trackConfig.ConfigName = nstr(configName)
	trackConfig.DisplayName = nstr(displayName)
	trackConfig.Country = nstr(country)
	trackConfig.City = nstr(city)
	trackConfig.Length = nint(length)
	cacheTrackConfig(trackConfig)

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

// FindOrCreateUser 按 GUID(steam64_id) 查找或创建用户。
// 1) 命中内存缓存直接返回；2) 模拟模式不读库，直接分配稳定且非零的模拟ID；
// 3) 真实模式按 GUID 查询，命中时同步 UserID 与缓存（名称变化时更新 name/prev_name）；
// 4) 未命中时插入显式 prev_name/country，避免留下 NULL 导致后续扫描失败。
func (w *DBWriter) FindOrCreateUser(user *User) (int, bool, error) {
	if user == nil {
		return 0, false, fmt.Errorf("用户信息为空")
	}
	if user.Steam64ID <= 0 {
		// 不再用 Steam64ID=0 去查/造用户
		return 0, false, fmt.Errorf("无效的Steam64ID: %d", user.Steam64ID)
	}

	// 1) 内存缓存（按 GUID）
	cacheMutex.Lock()
	for _, cached := range globalUsers {
		if cached != nil && cached.Steam64ID == user.Steam64ID {
			userID := cached.UserID
			cacheMutex.Unlock()
			user.UserID = userID
			return userID, false, nil
		}
	}
	cacheMutex.Unlock()

	// 2) 模拟模式：不读真实库，分配稳定的非零模拟ID并缓存
	if w.SimulateSQL {
		cacheMutex.Lock()
		for _, cached := range globalUsers {
			if cached != nil && cached.Steam64ID == user.Steam64ID {
				userID := cached.UserID
				cacheMutex.Unlock()
				user.UserID = userID
				return userID, false, nil
			}
		}
		id := int(nextSimulatedID())
		user.UserID = id
		user.PrevName = user.Name
		globalUsers[id] = user
		cacheMutex.Unlock()
		return id, true, nil
	}

	// 3) 真实模式：按 GUID 查询（prev_name/country 可空，用 Null* 扫描）
	var (
		foundID   sql.NullInt64
		foundName sql.NullString
		prevName  sql.NullString
		foundGUID sql.NullInt64
		country   sql.NullString
	)
	query := "SELECT user_id, name, prev_name, steam64_id, country FROM user WHERE steam64_id = ? LIMIT 1"
	err := w.DB.QueryRow(query, user.Steam64ID).Scan(&foundID, &foundName, &prevName, &foundGUID, &country)
	if err == nil {
		userID := int(foundID.Int64)
		user.UserID = userID
		if cfg != nil && cfg.ACServer.UpdateDriverName && user.Name != "" && user.Name != nstr(foundName) {
			if updErr := w.updateUserName(userID, nstr(foundName), user.Name); updErr != nil {
				logger.Printf("更新用户名失败(user_id=%d): %v", userID, updErr)
			}
		}
		cacheMutex.Lock()
		globalUsers[userID] = user
		cacheMutex.Unlock()
		return userID, false, nil
	}
	if err != sql.ErrNoRows {
		return 0, false, fmt.Errorf("查询用户失败: %v", err)
	}

	// 4) 创建新用户
	name := user.Name
	if name == "" {
		name = fmt.Sprintf("Player %d", user.Steam64ID)
	}
	ctry := "N/A"
	if cfg != nil {
		if v, ok := cfg.Overrides.UserCountry[strconv.FormatInt(user.Steam64ID, 10)]; ok && v != "" {
			ctry = v
		}
	}
	insertQuery := "INSERT INTO user (name, prev_name, steam64_id, country) VALUES (?, ?, ?, ?)"
	result, err := w.executeSQL(insertQuery, name, user.PrevName, user.Steam64ID, ctry)
	if err != nil {
		return 0, false, fmt.Errorf("创建用户失败: %v", err)
	}

	userID64, err := result.LastInsertId()
	if err != nil {
		return 0, false, fmt.Errorf("获取用户ID失败: %v", err)
	}
	if userID64 <= 0 {
		return 0, false, fmt.Errorf("获取用户ID失败: 非法的ID %d", userID64)
	}

	userID := int(userID64)
	user.UserID = userID
	user.Name = name
	cacheMutex.Lock()
	globalUsers[userID] = user
	cacheMutex.Unlock()
	return userID, true, nil
}

// updateUserName 同步用户改名（原版 SimView 行为：name 更新为当前名，prev_name 记住旧名）
func (w *DBWriter) updateUserName(userID int, oldName, newName string) error {
	if oldName == newName {
		return nil
	}
	_, err := w.executeSQL("UPDATE user SET name = ?, prev_name = ? WHERE user_id = ?", newName, oldName, userID)
	return err
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

// FindLatestSessionByEventID 查找指定事件的最新会话（finish_time/start_time 等允许为 NULL）
func (w *DBWriter) FindLatestSessionByEventID(eventID int) (*Session, error) {
	// 模拟模式不读真实 session：假 session id 可能撞上真实行
	if w.SimulateSQL {
		return nil, nil
	}

	session := &Session{}
	query := `SELECT session_id, event_id, type, name, track_time, start_time, duration_min, elapsed_ms, laps, weather, air_temp, road_temp, start_grip, current_grip, is_finished, finish_time, last_activity, http_port 
			 FROM session 
			 WHERE event_id = ? 
			 ORDER BY start_time DESC 
			 LIMIT 1`
	var (
		sid          sql.NullInt64
		eventIDCol   sql.NullInt64
		sessionType  sql.NullInt64
		name         sql.NullString
		trackTime    sql.NullString
		startTime    sql.NullTime
		durationMin  sql.NullInt64
		elapsedMs    sql.NullInt64
		laps         sql.NullInt64
		weather      sql.NullString
		airTemp      sql.NullFloat64
		roadTemp     sql.NullFloat64
		startGrip    sql.NullFloat64
		currentGrip  sql.NullFloat64
		isFinished   sql.NullInt64
		finishTime   sql.NullTime
		lastActivity sql.NullTime
		httpPort     sql.NullInt64
	)
	err := w.DB.QueryRow(query, eventID).Scan(
		&sid, &eventIDCol, &sessionType, &name, &trackTime,
		&startTime, &durationMin, &elapsedMs, &laps,
		&weather, &airTemp, &roadTemp, &startGrip,
		&currentGrip, &isFinished, &finishTime, &lastActivity, &httpPort,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("查找最新会话失败: %v", err)
	}
	session.ID = sid.Int64
	session.EventID = nint(eventIDCol)
	session.Type = nint(sessionType)
	session.Name = nstr(name)
	session.TrackTime = nstr(trackTime)
	session.StartTime = ntime(startTime)
	session.DurationMin = nint(durationMin)
	session.ElapsedMs = nint(elapsedMs)
	session.Laps = nint(laps)
	session.Weather = nstr(weather)
	session.AirTemp = nf64(airTemp)
	session.RoadTemp = nf64(roadTemp)
	session.StartGrip = nf64(startGrip)
	session.CurrentGrip = nf64(currentGrip)
	session.IsFinished = nint(isFinished)
	session.FinishTime = ntime(finishTime)
	session.LastActivity = ntime(lastActivity)
	session.HTTPPort = nint(httpPort)
	return session, nil
}

// FindSessionStintsBySessionID 查找指定会话的所有Stint（多个外键/时间列允许为 NULL）
func (w *DBWriter) FindSessionStintsBySessionID(sessionID int64) ([]*SessionStint, error) {
	// 模拟模式不读真实 session_stint：假ID可能撞上真实行
	if w.SimulateSQL {
		return nil, nil
	}
	query := `SELECT session_stint_id, user_id, team_member_id, session_id, car_id, game_car_id, laps, valid_laps, best_lap_id, is_finished, started_at, finished_at 
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
		var (
			id           sql.NullInt64
			userID       sql.NullInt64
			teamMemberID sql.NullInt64
			sessionIDCol sql.NullInt64
			carID        sql.NullInt64
			gameCarID    sql.NullInt64
			laps         sql.NullInt64
			validLaps    sql.NullInt64
			bestLapID    sql.NullInt64
			isFinished   sql.NullInt64
			startedAt    sql.NullTime
			finishedAt   sql.NullTime
		)
		err := rows.Scan(
			&id, &userID, &teamMemberID, &sessionIDCol, &carID,
			&gameCarID, &laps, &validLaps, &bestLapID, &isFinished,
			&startedAt, &finishedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("扫描Stint失败: %v", err)
		}
		stint.ID = nint(id)
		stint.UserID = nint(userID)
		stint.TeamMemberID = nint(teamMemberID)
		stint.SessionID = nint(sessionIDCol)
		stint.CarID = nint(carID)
		stint.GameCarID = nint(gameCarID)
		stint.Laps = nint(laps)
		stint.ValidLaps = nint(validLaps)
		stint.BestLapID = nint(bestLapID)
		stint.IsFinished = nint(isFinished)
		stint.StartedAt = ntime(startedAt)
		stint.FinishedAt = ntime(finishedAt)
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

// FindStintLapsByStintID 查找指定Stint的所有圈速（大部分数值/时间列允许为 NULL）
func (w *DBWriter) FindStintLapsByStintID(stintID int) ([]*StintLap, error) {
	if w.SimulateSQL {
		return nil, nil
	}
	query := `SELECT stint_lap_id, stint_id, sector_1, sector_2, sector_3, grip, tyre, time, cuts, crashes, car_crashes, max_speed, avg_speed, finished_at 
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
		var (
			id         sql.NullInt64
			stintCol   sql.NullInt64
			sector1    sql.NullInt64
			sector2    sql.NullInt64
			sector3    sql.NullInt64
			grip       sql.NullFloat64
			tyre       sql.NullString
			lapTime    sql.NullInt64
			cuts       sql.NullInt64
			crashes    sql.NullInt64
			carCrashes sql.NullInt64
			maxSpeed   sql.NullInt64
			avgSpeed   sql.NullInt64
			finishedAt sql.NullTime
		)
		err := rows.Scan(
			&id, &stintCol, &sector1, &sector2, &sector3,
			&grip, &tyre, &lapTime, &cuts, &crashes,
			&carCrashes, &maxSpeed, &avgSpeed, &finishedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("扫描圈速失败: %v", err)
		}
		lap.ID = nint(id)
		lap.StintID = nint(stintCol)
		lap.Sector1 = nint(sector1)
		lap.Sector2 = nint(sector2)
		lap.Sector3 = nint(sector3)
		lap.Grip = nf64(grip)
		lap.Tyre = nstr(tyre)
		lap.Time = nint(lapTime)
		lap.Cuts = nint(cuts)
		lap.Crashes = nint(crashes)
		lap.CarCrashes = nint(carCrashes)
		lap.MaxSpeed = nint(maxSpeed)
		lap.AvgSpeed = nint(avgSpeed)
		lap.FinishedAt = ntime(finishedAt)
		laps = append(laps, lap)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历圈速失败: %v", err)
	}

	return laps, nil
}

// FindStintLapByStintIDAndTime 根据StintID和时间查找圈速
func (w *DBWriter) FindStintLapByStintIDAndTime(stintID int, time int) (bool, error) {
	if w.SimulateSQL {
		return false, nil
	}
	query := "SELECT COUNT(*) FROM stint_lap WHERE stint_id = ? AND time = ?"
	var count sql.NullInt64
	err := w.DB.QueryRow(query, stintID, time).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("查询圈速存在性失败: %v", err)
	}
	return count.Int64 > 0, nil
}

// FindOrCreateTrackConfigID 查找或创建赛道配置ID，并回写缓存
func (w *DBWriter) FindOrCreateTrackConfigID(trackConfig *TrackConfig) (int, error) {
	// 模拟模式：不读库，只从本地 ui_track.json 补元数据并分配稳定模拟ID
	if w.SimulateSQL {
		display, country, city, length := readTrackMetadata(trackConfig.TrackName, trackConfig.ConfigName)
		if display == "" {
			display = fmt.Sprintf("%s %s", trackConfig.TrackName, trackConfig.ConfigName)
		}
		trackConfig.ID = int(nextSimulatedID())
		trackConfig.DisplayName = display
		trackConfig.Country = country
		trackConfig.City = city
		trackConfig.Length = length
		cacheTrackConfig(trackConfig)
		return trackConfig.ID, nil
	}

	// 先查找赛道配置
	var configID int
	query := "SELECT track_config_id FROM track_config WHERE track_name = ? AND config_name = ?"
	err := w.DB.QueryRow(query, trackConfig.TrackName, trackConfig.ConfigName).Scan(&configID)
	if err == nil {
		// 配置已存在，返回ID并补缓存
		trackConfig.ID = configID
		cacheTrackConfig(trackConfig)
		return configID, nil
	}

	if err != sql.ErrNoRows {
		// 非"未找到"错误，返回错误
		return 0, fmt.Errorf("查询赛道配置失败: %v", err)
	}

	// 配置不存在，创建新配置（尽量从游戏目录的 ui_track.json 补齐显示名/国家/城市/长度）
	display, country, city, length := readTrackMetadata(trackConfig.TrackName, trackConfig.ConfigName)
	if display == "" {
		display = fmt.Sprintf("%s %s", trackConfig.TrackName, trackConfig.ConfigName)
	}
	insertQuery := "INSERT INTO track_config (track_name, config_name, display_name, country, city, length) VALUES (?, ?, ?, ?, ?, ?)"
	result, err := w.executeSQL(insertQuery, trackConfig.TrackName, trackConfig.ConfigName, display, country, city, length)
	if err != nil {
		return 0, fmt.Errorf("创建赛道配置失败: %v", err)
	}

	newConfigID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("获取赛道配置ID失败: %v", err)
	}

	trackConfig.ID = int(newConfigID)
	trackConfig.DisplayName = display
	trackConfig.Country = country
	trackConfig.City = city
	trackConfig.Length = length
	cacheTrackConfig(trackConfig)

	return int(newConfigID), nil
}

// FindOrCreateCarID 查找或创建车辆ID
func (w *DBWriter) FindOrCreateCarID(config *Car) (int, error) {
	// 模拟模式：按车型去重后分配稳定模拟ID
	if w.SimulateSQL {
		cacheMutex.Lock()
		for _, car := range globalCars {
			if car != nil && car.Name == config.Name {
				id := car.ID
				cacheMutex.Unlock()
				cacheCar(config, id)
				return id, nil
			}
		}
		cacheMutex.Unlock()
		cacheCar(config, int(nextSimulatedID()))
		return config.ID, nil
	}

	// 先查找车辆
	var carID int
	query := "SELECT car_id FROM car WHERE name = ?"
	err := w.DB.QueryRow(query, config.Name).Scan(&carID)
	if err == nil {
		// 车辆已存在，返回ID并补缓存（供主循环按车型反查 car_id）
		cacheCar(config, carID)
		return carID, nil
	}

	if err != sql.ErrNoRows {
		// 非"未找到"错误，返回错误
		return 0, fmt.Errorf("查询车辆失败: %v", err)
	}

	// 车辆不存在，创建新车辆（display_name/manufacturer 为 NOT NULL 约束，必须提供值）
	displayName := config.DisplayName
	if displayName == "" {
		displayName = config.Name
	}
	manufacturer := config.Manufacturer
	if manufacturer == "" {
		manufacturer = "N/A"
	}
	carClass := config.CarClass
	if carClass == "" {
		carClass = "N/A"
	}
	insertQuery := "INSERT INTO car (name, display_name, manufacturer, car_class) VALUES (?, ?, ?, ?)"
	result, err := w.executeSQL(insertQuery, config.Name, displayName, manufacturer, carClass)
	if err != nil {
		return 0, fmt.Errorf("创建车辆失败: %v", err)
	}

	newCarID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("获取车辆ID失败: %v", err)
	}
	cacheCar(config, int(newCarID))

	return int(newCarID), nil
}

// FindOrCreateTeam 查找或创建团队
func (w *DBWriter) FindOrCreateTeam(team *Team) (int, error) {
	// 模拟模式：按(event_id,name)去重后分配稳定模拟ID
	if w.SimulateSQL {
		cacheMutex.Lock()
		for _, t := range globalTeams {
			if t != nil && t.EventID == team.EventID && t.Name == team.Name {
				id := t.ID
				cacheMutex.Unlock()
				team.ID = id
				return id, nil
			}
		}
		cacheMutex.Unlock()
		team.ID = int(nextSimulatedID())
		return team.ID, nil
	}

	// 先查找团队
	var teamID int
	query := "SELECT team_id FROM team WHERE event_id = ? AND name = ?"
	err := w.DB.QueryRow(query, team.EventID, team.Name).Scan(&teamID)
	if err == nil {
		// 团队已存在，返回ID
		team.ID = teamID
		return teamID, nil
	}

	if err != sql.ErrNoRows {
		// 非"未找到"错误，返回错误
		return 0, fmt.Errorf("查询团队失败: %v", err)
	}

	// 团队不存在，创建新团队（created_at 为 NOT NULL，team_no 可空）
	createdAt := team.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	insertQuery := "INSERT INTO team (event_id, name, team_no, car_id, livery_name, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)"
	result, err := w.executeSQL(insertQuery, team.EventID, team.Name, optionalInt(team.TeamNo), team.CarID, team.LiveryName, team.Active, createdAt)
	if err != nil {
		return 0, fmt.Errorf("创建团队失败: %v", err)
	}

	newTeamID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("获取团队ID失败: %v", err)
	}
	team.ID = int(newTeamID)

	return int(newTeamID), nil
}

// FindOrCreateTeamMember 查找或创建团队成员
func (w *DBWriter) FindOrCreateTeamMember(teamMember *TeamMember) (int, error) {
	// 模拟模式：按(team_id,user_id)去重后分配稳定模拟ID
	if w.SimulateSQL {
		cacheMutex.Lock()
		for _, m := range globalTeamMembers {
			if m != nil && m.TeamID == teamMember.TeamID && m.UserID == teamMember.UserID {
				id := m.ID
				cacheMutex.Unlock()
				teamMember.ID = id
				return id, nil
			}
		}
		cacheMutex.Unlock()
		teamMember.ID = int(nextSimulatedID())
		return teamMember.ID, nil
	}

	// 先查找团队成员
	var teamMemberID int
	query := "SELECT team_member_id FROM team_member WHERE team_id = ? AND user_id = ?"
	err := w.DB.QueryRow(query, teamMember.TeamID, teamMember.UserID).Scan(&teamMemberID)
	if err == nil {
		// 团队成员已存在，返回ID
		teamMember.ID = teamMemberID
		return teamMemberID, nil
	}

	if err != sql.ErrNoRows {
		// 非"未找到"错误，返回错误
		return 0, fmt.Errorf("查询团队成员失败: %v", err)
	}

	// 团队成员不存在，创建新团队成员（created_at 为 NOT NULL 约束）
	createdAt := teamMember.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	insertQuery := "INSERT INTO team_member (team_id, user_id, role, active, created_at) VALUES (?, ?, ?, ?, ?)"
	result, err := w.executeSQL(insertQuery, teamMember.TeamID, teamMember.UserID, teamMember.Role, teamMember.Active, createdAt)
	if err != nil {
		return 0, fmt.Errorf("创建团队成员失败: %v", err)
	}

	newTeamMemberID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("获取团队成员ID失败: %v", err)
	}
	teamMember.ID = int(newTeamMemberID)

	return int(newTeamMemberID), nil
}

// FindEventByName 根据名称查找事件
func (w *DBWriter) FindEventByName(name string) (*Event, error) {
	// 模拟模式不读真实 event 表：命中已模拟创建的 event，保证 event_id 稳定（前端 cookie 依赖它）
	if w.SimulateSQL {
		if globalEvent != nil && globalEvent.ID > 0 && globalEvent.Name == name {
			return globalEvent, nil
		}
		if ev := lookupSimEvent(name); ev != nil {
			return ev, nil
		}
		return nil, sql.ErrNoRows
	}

	event := &Event{}
	query := `SELECT event_id, name, server_name, track_config_id, practice_duration, quali_duration, race_duration, race_duration_type, race_extra_laps, reverse_grid_positions, team_event, active 
			 FROM event 
			 WHERE name = ?`
	var (
		eventID     sql.NullInt64
		eventName   sql.NullString
		serverName  sql.NullString
		trackConfID sql.NullInt64
		practiceDur sql.NullInt64
		qualiDur    sql.NullInt64
		raceDur     sql.NullInt64
		raceDurType sql.NullInt64
		raceExtra   sql.NullInt64
		reverseGrid sql.NullInt64
		teamEvent   sql.NullInt64
		active      sql.NullInt64
	)
	err := w.DB.QueryRow(query, name).Scan(
		&eventID, &eventName, &serverName, &trackConfID,
		&practiceDur, &qualiDur, &raceDur,
		&raceDurType, &raceExtra, &reverseGrid,
		&teamEvent, &active,
	)
	if err != nil {
		// 保留 sql.ErrNoRows 原样返回，调用方依赖这个判断
		return nil, err
	}
	event.ID = nint(eventID)
	event.Name = nstr(eventName)
	event.ServerName = nstr(serverName)
	event.TrackConfigID = nint(trackConfID)
	event.PracticeDuration = nint(practiceDur)
	event.QualiDuration = nint(qualiDur)
	event.RaceDuration = nint(raceDur)
	event.RaceDurationType = nint(raceDurType)
	event.RaceExtraLaps = nint(raceExtra)
	event.ReverseGridPositions = nint(reverseGrid)
	event.TeamEvent = nint(teamEvent)
	event.Active = nint(active)
	return event, nil
}

// InsertEvent 插入事件记录（包装CreateEvent）
func (w *DBWriter) InsertEvent(event *Event) error {
	_, err := w.CreateEvent(event)
	return err
}

// UpdateEvent 更新事件信息（含各节时长、发车等待与倒序发车位置，供前端/结果页读取）
func (w *DBWriter) UpdateEvent(event *Event) error {
	query := `UPDATE event SET name = ?, server_name = ?, track_config_id = ?, team_event = ?, active = ?, livery_preview = ?, use_number = ?, practice_duration = ?, quali_duration = ?, race_duration = ?, race_duration_type = ?, race_wait_time = ?, race_extra_laps = ?, reverse_grid_positions = ?
			 WHERE event_id = ?`
	_, err := w.executeSQL(query, event.Name, event.ServerName, event.TrackConfigID, event.TeamEvent, event.Active, event.LiveryPreview, event.UseNumber, event.PracticeDuration, event.QualiDuration, event.RaceDuration, event.RaceDurationType, event.RaceWaitTime, event.RaceExtraLaps, event.ReverseGridPositions, event.ID)
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
