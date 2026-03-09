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
	return "" // 未找到时返回空字符串
}
