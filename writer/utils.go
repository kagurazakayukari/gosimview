// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// boolToInt 将布尔值转换为整数，true 转换为 1，false 转换为 0
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// calculateElapsedMs 计算会话已用时间（毫秒）
func calculateElapsedMs(sessionID int64) (int64, error) {
	// 查询会话开始时间
	var startTime time.Time
	query := "SELECT start_time FROM session WHERE session_id = ?"
	err := globalDBWriter.DB.QueryRow(query, sessionID).Scan(&startTime)
	if err != nil {
		return 0, err
	}

	// 计算已用时间（毫秒）
	elapsed := time.Since(startTime).Milliseconds()
	return elapsed, nil
}

// findLatestSessionLogFile 查找最新的会话日志文件
func findLatestSessionLogFile(logDir string) (os.DirEntry, error) {
	// 读取日志目录
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return nil, err
	}

	var latestFile os.DirEntry
	var latestTime time.Time

	// 遍历所有文件，找到最新的会话日志文件
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// 检查文件名是否符合会话日志格式
		if strings.HasPrefix(entry.Name(), "acServer_") && strings.HasSuffix(entry.Name(), ".log") {
			// 获取文件修改时间
			info, err := entry.Info()
			if err != nil {
				continue
			}

			// 比较时间，找出最新的文件
			if latestFile == nil || info.ModTime().After(latestTime) {
				latestFile = entry
				latestTime = info.ModTime()
			}
		}
	}

	if latestFile == nil {
		return nil, os.ErrNotExist
	}

	return latestFile, nil
}

// initLogger 初始化日志系统
func initLogger() {
	logDir := "logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatalf("创建日志目录失败: %v", err)
	}

	logFile, err := os.OpenFile(
		filepath.Join(logDir, fmt.Sprintf("simview-writer-%s.log", time.Now().Format("2006-01-02-15-04-05"))),
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		0644,
	)
	if err != nil {
		log.Fatalf("打开日志文件失败: %v", err)
	}

	// 创建多输出写入器，同时输出到文件和控制台
	multiWriter := io.MultiWriter(os.Stdout, logFile)

	// 设置全局日志器
	logger = log.New(multiWriter, "[SimView] ", log.Ldate|log.Ltime)
	fatalLogger = log.New(multiWriter, "[SimView][FATAL] ", log.Ldate|log.Ltime|log.Lshortfile)
}

// getLogFileName 获取日志文件名
func getLogFileName(prefix string) string {
	return fmt.Sprintf("%s_%s.log", prefix, time.Now().Format("2006-01-02-15-04-05"))
}
