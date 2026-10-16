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

// 静态文件根目录（启动时解析）
var docRoot = "html"

func main() {
	// 初始化日志系统
	initLogger()
	logger.Println("应用程序启动")

	// 配置路径入口：只接受一个可选参数（os.Args[1]），多参数直接报错。
	// 无参数时由 config.ResolvePath 按约定顺序查找：config.toml 优先于 config/config.toml。
	var explicitConfig string
	switch len(os.Args) {
	case 1:
	case 2:
		explicitConfig = os.Args[1]
	default:
		fatalLogger.Fatalf("参数过多: 只接受一个可选的配置文件路径，实际收到 %d 个", len(os.Args)-1)
	}

	configPath, err := config.ResolvePath(explicitConfig)
	if err != nil {
		fatalLogger.Fatalf("配置解析失败: %v", err)
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
	// 解析静态文件目录：优先 config 的 server.doc.root，其次 simview/html，最后 html
	docRoot = firstExistingPath(cfg.App.Server.DocRoot, "simview/html", "html")
	if docRoot == "" {
		docRoot = cfg.App.Server.DocRoot
	}
	logger.Printf("静态文件目录: %s", docRoot)
	// 连接数据库
	if err := connectDB(cfg); err != nil {
		fatalLogger.Fatalf("数据库连接失败: %v", err)
	}
	defer db.Close()

	// 设置HTTP路由
	setupRoutes()

	// 启动HTTP服务器：监听地址遵循 app.server.host（为空则监听所有网卡）
	port := cfg.App.Server.Port
	host := cfg.App.Server.Host
	logger.Printf("HTTP服务器监听地址: %s", listenAddr(host, port))

	// HTTPS 配置尚未接入：明确报告，避免误以为已启用 TLS
	if cfg.App.SSL.CertFile != "" || cfg.App.SSL.PrivateKeyFile != "" {
		logger.Printf("警告: 已配置 ssl.cert.file/private.key.file，但当前未启用 HTTPS（仍以明文 HTTP 监听）")
	}

	// 直播 WebSocket 端口：writer.http.port，为 0 时退回 ac.server.udp.local.port
	lbPort := cfg.Writer.HTTP.Port
	if lbPort == 0 {
		lbPort = cfg.ACServer.UDP.LocalPort
	}

	// 主 HTTP 端口已通过 setupRoutes 提供 /live；两者相同则不重复绑定
	if lbPort > 0 && lbPort != port {
		wsMux := http.NewServeMux()
		wsMux.HandleFunc("/live", handleACEventWebSocket)
		addr := listenAddr(host, lbPort)
		go func() {
			logger.Printf("WebSocket服务器监听地址: %s", addr)
			if err := http.ListenAndServe(addr, wsMux); err != nil {
				logger.Printf("WebSocket服务器启动失败: %v", err)
			}
		}()
	}

	// 启动主HTTP服务器
	if err := http.ListenAndServe(listenAddr(host, port), nil); err != nil {
		fatalLogger.Fatalf("服务器启动失败: %v", err)
	}
}

// listenAddr 依据配置的 host 组装监听地址；host 为空时监听所有网卡
func listenAddr(host string, port int) string {
	if host == "" {
		return fmt.Sprintf(":%d", port)
	}
	return fmt.Sprintf("%s:%d", host, port)
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

	// 注意：HTTP 进程不再执行 schema（原版 schema 含 DROP TABLE，会在启动时破坏数据）。
	// 建表/迁移由 Writer 侧的显式 ApplySchema 负责。
	return nil
}
