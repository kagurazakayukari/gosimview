// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
// See LICENSE.md for license information.

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var restoredSessionLogPath string
var recoveredLogSessionID int64
var restoredSessionLogSize int64

// restoreConnectionsFromSessionLog 恢复原 handleSessionInfoMessage 的启动职责。
// 连接记录按文件顺序恢复各槽位最后一位车手；槽位取 DRIVER ACCEPTED FOR CAR，
// 不能用状态行的编号或 SesID 代替。无完圈的已连接车手同样需要恢复。
// 本函数由已持有 stateMutex 的会话处理器调用。
// 当前节成绩仅恢复到内存，不把历史圈或 feed 再次写入数据库。
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
	// 同一份日志、同一节的重复探活仅消费新增行，不能用历史快照覆盖实时状态。
	newSnapshot := restoredSessionLogPath != path || int64(len(data)) < restoredSessionLogSize
	if !newSnapshot && globalSession != nil && recoveredLogSessionID == globalSession.ID {
		collectCarUpdates()
		return nil
	}
	connections := connectionRegex.FindAllStringSubmatchIndex(content, -1)
	latestConnections := make(map[int][]int)
	for _, indices := range connections {
		slotText := content[indices[10]:indices[11]]
		slot, err := strconv.Atoi(slotText)
		if err != nil {
			logger.Printf("日志中的车辆槽位无效: %s", slotText)
			continue
		}
		latestConnections[slot] = indices
	}
	for slot, indices := range latestConnections {
		name := strings.TrimSpace(content[indices[4]:indices[5]])
		if name == "" {
			continue
		}
		entry := globalEntryList[slot]
		conn := &ConnectionInfo{
			CarID: slot, DriverName: name, DriverGUID: content[indices[8]:indices[9]],
			CarModel: cleanRequestedCarModel(content[indices[6]:indices[7]]), CarSkin: entry.Skin,
			CarName: entry.Name, ConnectionStatus: 0, TrackStatus: 3,
			TrackStatusLastChangedTime: time.Now(), LastConnectionLine: strings.Count(content[:indices[0]], "\n"),
		}
		// 没有日志时间戳，不编造 ConnectedAt；只恢复可确认的最后连接状态。
		for _, line := range strings.Split(content[indices[1]:], "\n") {
			if match := disconnectRegex.FindStringSubmatch(line); len(match) == 2 && strings.TrimSpace(match[1]) == name {
				conn.ConnectionStatus = 2
			}
		}
		initializeConnectionIdentity(conn)
		if conn.UserID == 0 || conn.CarDBID == 0 {
			return fmt.Errorf("无法从日志解析车手身份: %s", name)
		}
		globalConnections[slot] = conn
	}
	latestFile = file
	// 原 collectCarUpdates 按最后一条 SESSION: 定位当前节。
	// 先回放该节已生成的成绩，再接着处理新增字节。
	if globalSession != nil && globalSession.ID > 0 && (newSnapshot || recoveredLogSessionID != globalSession.ID) {
		start := 0
		for i, line := range lines {
			if strings.Contains(line, "SESSION:") {
				start = i + 1
			}
		}
		recoveredScores = make(map[int]*recoveredScore)
		stintLapData[int(globalSession.ID)] = make(map[int][]StintLap)
		for _, conn := range globalConnections {
			userStintIDs[conn.DriverName] = 1
			userTyres[conn.DriverName] = ""
		}
		// 原解析先读取分节标记之前的轮胎记录，当前节未再次换胎时沿用最后配方。
		for _, line := range lines[:start] {
			if match := tyreRegex.FindStringSubmatch(line); len(match) == 3 {
				name := strings.TrimSpace(match[1])
				if slotForLogName(name) >= 0 {
					userTyres[name] = match[2]
					userStintIDs[name]++
				}
			}
		}
		// 文件最后未换行的片段留给增量读取，不能提前当完整圈解析。
		complete := lines
		if !strings.HasSuffix(content, "\n") {
			complete = lines[:len(lines)-1]
		}
		if start <= len(complete) {
			processSessionLogLinesAt(complete[start:], true, start)
		}
		recoveredLogSessionID = globalSession.ID
	}
	// 恢复读取到的历史快照后，再继续读取文件新增的字节。
	// 不用 Stat().Size()，避免跳过 ReadFile 之后才追加的新内容。
	if newSnapshot {
		lastFileSize = int64(len(data))
		lastLineIdx = strings.Count(content, "\n")
		partialLine = ""
		if !strings.HasSuffix(content, "\n") {
			partialLine = lines[len(lines)-1]
		}
		restoredSessionLogPath = path
	}
	restoredSessionLogSize = int64(len(data))
	return nil
}
