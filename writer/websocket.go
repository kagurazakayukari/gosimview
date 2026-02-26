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
	"sync"
	"time"

	"gosimview/udp"

	"github.com/gorilla/websocket"
)

// WebSocket连接和相关变量
var (
	wsConn         *websocket.Conn
	wsMutex        sync.Mutex
	reconnectDelay = 5 * time.Second
	wsReconnecting = false
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
func parseLeaderboardEntries(sessionID int, carUpdates map[int64]udp.CarUpdate) ([]Entry, error) {
	// 构建entries列表
	var entries []Entry

	// 遍历carUpdates，构建Entry对象
	for carID, update := range carUpdates {
		entry := Entry{
			CarID: uint16(carID),
			NSP:   float32(update.NormalisedSplinePos),
			PosX:  float32(update.Pos.X),
			PosZ:  float32(update.Pos.Z),
			RPM:   uint16(update.EngineRPM),
			// 使用默认值填充其他字段，因为CarUpdate结构体中没有这些字段
			TeamID:        0,
			UserID:        0,
			StatusMask:    0,
			Laps:          0,
			ValidLaps:     0,
			TelemetryMask: 0,
		}

		entries = append(entries, entry)
	}

	return entries, nil
}
