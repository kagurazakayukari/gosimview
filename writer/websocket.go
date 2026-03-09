// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"sort"
	"time"

	"gosimview/udp"

	"github.com/gorilla/websocket"
)

// WebSocket连接和相关变量
var (
	wsConn         *websocket.Conn
	reconnectDelay = 5 * time.Second
)

// initWebSocket 初始化WebSocket连接
func initWebSocket() error {
	logger.Println("尝试连接WebSocket服务器...")

	// 构建WebSocket URL
	wssURL := url.URL{Scheme: "ws", Host: fmt.Sprintf("localhost:%d", cfg.App.Server.Port), Path: "/live"}
	url := wssURL.String()

	// 建立WebSocket连接
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return fmt.Errorf("WebSocket连接失败: %v", err)
	}

	// 更新全局WebSocket连接
	wsMutex.Lock()
	wsConn = conn
	wsMutex.Unlock()

	logger.Println("WebSocket连接成功")
	return nil
}

// startWebSocketReconnect 启动WebSocket重连机制
func startWebSocketReconnect() {
	go func() {
		for {
			wsMutex.Lock()
			conn := wsConn
			wsMutex.Unlock()

			if conn == nil {
				// 连接已关闭，尝试重连
				time.Sleep(reconnectDelay)
				if err := initWebSocket(); err != nil {
					logger.Printf("WebSocket重连失败: %v", err)
				}
				continue
			}

			// 读取WebSocket消息（主要是为了检测连接断开）
			_, _, err := conn.ReadMessage()
			if err != nil {
				logger.Printf("WebSocket连接断开: %v, 尝试重连...", err)

				// 关闭连接并重置为nil
				conn.Close()
				wsMutex.Lock()
				wsConn = nil
				wsMutex.Unlock()
			}
		}
	}()
}

// buildAndSendWebSocketMessage 构建并发送WebSocket消息
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
		latestSessionID, err := globalDBWriter.GetLatestSessionID(globalEvent.ID)
		if err != nil {
			logger.Printf("获取最新会话ID失败: %v", err)
			return
		}

		// 查询会话详情
		if latestSessionID > 0 {
			globalSession, err = globalDBWriter.GetSessionDetails(latestSessionID)
			if err != nil {
				logger.Printf("获取会话信息失败: %v", err)
				return
			}
			session.SessionID = globalSession.ID
			session.Type = globalSession.Type
			session.DurationMin = globalSession.DurationMin
			session.StartGrip = globalSession.StartGrip
			session.CurrentGrip = globalSession.CurrentGrip
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
		EventID           int       `json:"event_id"`
		Session           ACSession `json:"session"`
		Entries           []Entry   `json:"entries"`
	}{Version: 10, BroadcastInterval: int32(cfg.Writer.HTTP.Leaderboard.Broadcast.Interval.MS), ViewerCount: 0, EventID: globalEvent.ID, Session: session, Entries: entries}

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

// waitForWebSocketServer 等待WebSocket服务器启动
func waitForWebSocketServer(port int) error {
	maxAttempts := 30
	attempt := 0

	for {
		// 检查服务器是否已启动
		conn, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", port), 500*time.Millisecond)
		if err == nil {
			conn.Close()
			logger.Println("WebSocket服务器已启动")
			return nil
		}

		attempt++
		if attempt > maxAttempts {
			return fmt.Errorf("超时: WebSocket服务器在%d秒内未启动", maxAttempts/2)
		}

		logger.Printf("等待WebSocket服务器启动...")
		time.Sleep(1 * time.Second)
	}
}

// parseLeaderboardEntries 解析排行榜条目
func parseLeaderboardEntries(sessionID int, carUpdates map[udp.CarID]udp.CarUpdate) ([]Entry, error) {
	var entries []Entry

	// 遍历最新车辆更新数据
	for carID, update := range carUpdates {
		ServerCarID := int(carID)
		var laps, validLaps int = 0, 0
		var currentS1, currentS2, currentS3, currentTime int = 0, 0, 0, 0
		var tyre string = ""
		var bestS1, bestS2, bestS3 int = 0, 0, 0

		// 获取圈速数据
		lapDataMutex.Lock()
		if lapData, ok := carLapDataMap[ServerCarID]; ok && len(lapData) > 0 {
			latestLap := lapData[len(lapData)-1]
			laps = int(latestLap.Laps)
			validLaps = int(latestLap.Completed) // 使用completed作为有效圈数
			currentTime = latestLap.LapTime
			if dataCollectionEnabled {
				currentS1 = latestLap.Sector1
				currentS2 = latestLap.Sector2
				currentS3 = latestLap.Sector3
				tyre = latestLap.Tyre
			}
		}
		lapDataMutex.Unlock()

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

		// 构建Entry对象
		entry := Entry{
			TeamID:        0, // 团队赛事中会覆盖此值
			UserID:        0, // 根据赛事类型在后续逻辑中设置
			CarID:         uint16(ServerCarID),
			GameCarID:     0, // 默认值，实际应从数据库获取
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
			NSP:           float32(update.NormalisedSplinePos),
			PosX:          float32(update.Pos.X),
			PosZ:          float32(update.Pos.Z),
			TelemetryMask: 0,
			RPM:           uint16(update.EngineRPM),
			BestLapS1:     0,
			BestLapS2:     0,
			BestLapS3:     0,
			BestLapTime:   0,
			DriverName:    "",
			TeamName:      "",
		}

		// 根据赛事类型设置TeamID和UserID
		if globalEvent.TeamEvent == 1 {
			// 团队赛事: 通过团队获取用户ID
			entry.TeamID = int64(getTeamIDByCarID(ServerCarID))
			// 团队赛事中UserID可以设为0或特定值
		} else {
			// 非团队赛事: 直接通过车辆ID获取用户ID
			entry.UserID = int64(getUserIDByCarID(ServerCarID))
		}

		// 计算个人最佳赛段
		if dataCollectionEnabled {
			lapDataMutex.Lock()
			if stintData, ok := stintLapData[int(sessionID)]; ok && len(stintData[ServerCarID]) > 0 {
				for _, lap := range stintData[ServerCarID] {
					if lap.Sector1 > 0 && (bestS1 == 0 || lap.Sector1 < bestS1) {
						bestS1 = lap.Sector1
						entry.BestLapS1 = uint32(bestS1)
					}
					if lap.Sector2 > 0 && (bestS2 == 0 || lap.Sector2 < bestS2) {
						bestS2 = lap.Sector2
						entry.BestLapS2 = uint32(bestS2)
					}
					if lap.Sector3 > 0 && (bestS3 == 0 || lap.Sector3 < bestS3) {
						bestS3 = lap.Sector3
						entry.BestLapS3 = uint32(bestS3)
					}
					if lap.Time > 0 && (entry.BestLapTime == 0 || lap.Time < int(entry.BestLapTime)) {
						entry.BestLapTime = uint32(lap.Time)
					}
				}
			}
			lapDataMutex.Unlock()
		}

		// 设置赛段掩码
		sectorMask := uint8(0)
		if currentS1 > 0 {
			sectorMask |= 0x01 // 赛段1已完成
		}
		if currentS2 > 0 {
			sectorMask |= 0x02 // 赛段2已完成
		}
		if currentS3 > 0 {
			sectorMask |= 0x04 // 赛段3已完成
		}
		entry.SectorMask = sectorMask

		// 设置遥测掩码
		entry.TelemetryMask = 0xFFFF // 简化处理，实际应根据配置和权限设置

		// 获取驾驶员名称和团队名称
		connectionsMutex.Lock()
		if conn, ok := globalConnections[ServerCarID]; ok {
			entry.DriverName = conn.DriverName
		}
		connectionsMutex.Unlock()

		if entry.TeamID > 0 {
			entry.TeamName = getTeamNameByID(int(entry.TeamID))
		}

		entries = append(entries, entry)
	}

	// 计算车手间的差距和间隔
	if len(entries) > 0 {
		// 按NSP排序，确定当前领先车手
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].NSP > entries[j].NSP
		})

		// 计算各车手与领先车手的差距和间隔
		for i := range entries {
			if i == 0 {
				entries[i].Gap = 0
				entries[i].Interval = 0
			} else {
				// 简化处理：使用NSP差距计算时间差距
				nspDiff := entries[0].NSP - entries[i].NSP
				gapTime := int32(nspDiff * float32(entries[0].LastLapTime))
				entries[i].Gap = gapTime

				// 计算与前一名车手的间隔
				prevNspDiff := entries[i-1].NSP - entries[i].NSP
				intervalTime := int32(prevNspDiff * float32(entries[i-1].LastLapTime))
				entries[i].Interval = intervalTime
			}
		}
	}

	return entries, nil
}
