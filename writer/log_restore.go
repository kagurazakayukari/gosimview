// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
// See LICENSE.md for license information.

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var restoredSessionLogPath string

// restoreConnectionsFromSessionLog 恢复原 handleSessionInfoMessage 的启动职责。
// 玩家状态行确定姓名，再与该文件的连接记录匹配；槽位取连接记录中的
// DRIVER ACCEPTED FOR CAR，不能用状态行的编号或 SesID 代替。
// 本函数由已持有 stateMutex 的会话处理器调用，只恢复身份，不重放圈和 feed。
func restoreConnectionsFromSessionLog() error {
	file, err := findLatestSessionLogFile(logDir)
	if err != nil {
		return fmt.Errorf("查找最新会话日志: %w", err)
	}
	path := filepath.Join(logDir, file.Name())
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("读取会话日志: %w", err)
	}
	// Windows 日志为 CRLF；统一换行后沿用原版正则和姓名匹配规则。
	content := strings.ReplaceAll(string(data), "\r\n", "\n")
	lines := strings.Split(content, "\n")
	playerPattern := regexp.MustCompile(`^(\d+)\) (.*?) BEST: (.*?) TOTAL: (.*?) Laps:(\d+) SesID:(\d+) HasFinished:(\w+)$`)
	connections := connectionRegex.FindAllStringSubmatch(content, -1)
	for lineIndex, line := range lines {
		player := playerPattern.FindStringSubmatch(line)
		if len(player) < 8 {
			continue
		}
		name := strings.TrimSpace(player[2])
		if name == "" {
			continue
		}
		// 与原版相同：按文件顺序选中第一条姓名一致的连接记录。
		for _, match := range connections {
			if len(match) < 6 || strings.TrimSpace(match[2]) != name {
				continue
			}
			slot, err := strconv.Atoi(match[5])
			if err != nil {
				logger.Printf("日志中的车辆槽位无效: %s", match[5])
				continue
			}
			entry := globalEntryList[slot]
			conn := &ConnectionInfo{
				CarID: slot, DriverName: name, DriverGUID: match[4],
				CarModel: strings.TrimSpace(match[3]), CarSkin: entry.Skin,
				CarName: entry.Name, ConnectionStatus: 0, TrackStatus: 3,
				TrackStatusLastChangedTime: time.Now(), LastConnectionLine: lineIndex + 1,
			}
			initializeConnectionIdentity(conn)
			if conn.UserID == 0 || conn.CarDBID == 0 {
				logger.Printf("无法从日志解析车手身份: %s", name)
				break
			}
			globalConnections[slot] = conn
			break
		}
	}
	latestFile = file
	// 恢复读取到的历史快照后，再继续读取文件新增的字节。
	// 不用 Stat().Size()，避免跳过 ReadFile 之后才追加的新内容。
	if restoredSessionLogPath != path {
		lastFileSize = int64(len(data))
		lastLineIdx = strings.Count(content, "\n")
		partialLine = ""
		restoredSessionLogPath = path
	}
	return nil
}
