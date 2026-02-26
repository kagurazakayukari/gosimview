// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gosimview/config"

	_ "github.com/go-sql-driver/mysql"
)

// 全局变量定义
var db *sql.DB
var (
	logger      *log.Logger
	fatalLogger *log.Logger
)

// 全局配置变量
var cfg *config.Config // 全局配置实例

// 静态文件服务器实例
var fs http.Handler

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
		log.Fatalf("创建日志目录失败: %v", err)
	}

	logFile, err := os.OpenFile(
		filepath.Join(logDir, fmt.Sprintf("simview-http-%s.log", time.Now().Format("2006-01-02-15-04-05"))),
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		0644,
	)
	if err != nil {
		log.Fatalf("打开日志文件失败: %v", err)
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

	// 执行SQL schema文件（如果存在）
	executeSQLSchema(db, logger)

	return nil
}

// executeSQLSchema 执行SQL schema文件
func executeSQLSchema(db *sql.DB, logger *log.Logger) {
	// 只在data目录下查找SQL文件
	dataSQLFiles, err := filepath.Glob("data/*.sql")
	if err != nil {
		logger.Printf("查找data目录下的SQL文件失败: %v", err)
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
		logger.Println("未找到data目录下的SQL schema文件")
		logger.Println("请将原版SimView的SQL文件放置在data目录下")
		return
	}

	// 读取SQL文件内容
	sqlContent, err := os.ReadFile(sqlFile)
	if err != nil {
		logger.Printf("读取SQL文件失败: %v", err)
		return
	}

	// 从SQL文件中提取版本号
	fileVersion := extractSQLVersion(sqlContent)

	// 从schema_info表获取数据库版本
	dbVersion := getDatabaseVersion(db)

	// 比较版本并决定是否执行
	if dbVersion == "" || isNewerVersion(fileVersion, dbVersion) {
		// 执行SQL语句
		logger.Printf("执行SQL schema: %s", sqlFile)
		sqlStatements := strings.Split(string(sqlContent), ";")
		for i, stmt := range sqlStatements {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" || strings.HasPrefix(stmt, "--") {
				continue
			}

			stmt += ";"
			if _, err := db.Exec(stmt); err != nil {
				logger.Printf("SQL执行失败 (行 %d): %v", i+1, err)
				continue
			}
		}
		logger.Println("SQL schema执行完成")
	} else {
		logger.Printf("数据库版本已更新，跳过SQL schema执行")
	}
}

// extractSQLVersion 从SQL文件中提取版本号
func extractSQLVersion(sqlContent []byte) string {
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

// tableExists 检查表是否存在
func tableExists(db *sql.DB, tableName string) bool {
	query := "SHOW TABLES LIKE ?"
	row := db.QueryRow(query, tableName)
	var name string
	err := row.Scan(&name)
	return err == nil
}

// getDatabaseVersion 获取数据库版本
func getDatabaseVersion(db *sql.DB) string {
	// 检查是否存在schema_info表
	if !tableExists(db, "schema_info") {
		return ""
	}

	// 查询版本号
	query := "SELECT version FROM schema_info LIMIT 1"
	row := db.QueryRow(query)
	var version string
	err := row.Scan(&version)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Printf("查询数据库版本失败: %v", err)
		}
		return ""
	}
	return version
}

// isNewerVersion 比较两个版本号，判断fileVersion是否比dbVersion新
func isNewerVersion(fileVersion, dbVersion string) bool {
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
