// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
// See LICENSE.md for license information.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"gosimview/udp"
	"net"
	"net/http"
	"net/url"
	"sort"
	"time"
)

var wsConn *websocket.Conn
var wsEventID int
var reconnectDelay = 5 * time.Second

// 名次字段及 feed 编码有原版证据；相邻快照名次差是 Go 侧采用的计算规则。
// 基线只由广播快照更新，受 stateMutex 保护；槽位换人、换车或换队须建立新基线。
type positionIdentity struct {
	slot   int
	userID int64
	carID  uint16
	teamID int64
}

var prevPositions = make(map[positionIdentity]int)
var prevPositionsSession int

// applyPositionChanges 按上一次广播快照计算正升负降，首次出现的身份不制造变化。
// 调用者持有 stateMutex 写锁；单纯读取排行榜不会消费名次基线。
func applyPositionChanges(entries []Entry, sessionID int) {
	if prevPositionsSession != sessionID {
		prevPositions = make(map[positionIdentity]int, len(entries))
		prevPositionsSession = sessionID
	}
	current := make(map[positionIdentity]int, len(entries))
	for i := range entries {
		entries[i].PosChange = 0
		id := positionIdentity{slot: entries[i].GameCarID, userID: entries[i].UserID, carID: entries[i].CarID, teamID: entries[i].TeamID}
		current[id] = i
		prev, ok := prevPositions[id]
		if !ok || prev == i {
			continue
		}
		delta := prev - i
		if delta > 127 {
			delta = 127
		}
		if delta < -128 {
			delta = -128
		}
		entries[i].PosChange = int8(delta)
	}
	prevPositions = current
}

// recordPositionChangeFeeds 给发生名次变动的车写入 type=10 feed；
// detail 字段与 live.js getPositionChangeMsg 一致（final_pos 为 0 基名次）。
// 调用者持有 stateMutex，身份和 session 与本次名次快照一致。
func recordPositionChangeFeeds(sessionID int64, entries []Entry) {
	for i := range entries {
		if entries[i].PosChange == 0 {
			continue
		}
		detail := feedDetail(entries[i].GameCarID, map[string]interface{}{
			"pos_change": int(entries[i].PosChange),
			"final_pos":  i,
		})
		if err := globalDBWriter.InsertSessionFeed(&SessionFeed{SessionID: sessionID, Type: 10, Detail: detail, Time: time.Now()}); err != nil {
			logger.Printf("插入名次变化 feed 失败: %v", err)
		}
	}
}

// initWebSocket 建立 writer 专用发布连接；事件切换后必须更新 cookie。
func initWebSocket(ctx context.Context) error {
	stateMutex.RLock()
	eventID := globalEvent.ID
	stateMutex.RUnlock()
	if eventID <= 0 {
		return fmt.Errorf("事件尚未初始化")
	}
	host := cfg.App.Server.Host
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	u := url.URL{Scheme: "ws", Host: net.JoinHostPort(host, fmt.Sprint(cfg.App.Server.Port)), Path: "/live"}
	header := http.Header{}
	header.Set("Cookie", fmt.Sprintf("eventId=%d", eventID))
	header.Set("X-SimView-Writer", "1")
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 5 * time.Second
	conn, _, err := dialer.DialContext(ctx, u.String(), header)
	if err != nil {
		return err
	}
	wsMutex.Lock()
	if ctx.Err() != nil {
		wsMutex.Unlock()
		conn.Close()
		return ctx.Err()
	}
	if wsConn != nil {
		wsConn.Close()
	}
	wsConn, wsEventID = conn, eventID
	wsMutex.Unlock()
	return nil
}

func closeWebSocket() {
	wsMutex.Lock()
	defer wsMutex.Unlock()
	if wsConn != nil {
		wsConn.Close()
		wsConn = nil
	}
}

func startWebSocketReconnect(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			wsMutex.Lock()
			conn := wsConn
			wsMutex.Unlock()
			if conn == nil {
				if err := initWebSocket(ctx); err != nil {
					if ctx.Err() != nil {
						return
					}
					logger.Printf("WS重连失败: %v", err)
					if !waitWriterContext(ctx, reconnectDelay) {
						return
					}
				}
				continue
			}
			_, _, err := conn.ReadMessage()
			if err != nil {
				conn.Close()
				wsMutex.Lock()
				if wsConn == conn {
					wsConn = nil
				}
				wsMutex.Unlock()
			}
		}
	}()
	return done
}

// 广播只读已恢复的内存状态；接入已有 session 由探活恢复路径完成，广播不改写 globalSession。
func buildAndSendWebSocketMessage() {
	stateMutex.Lock()
	if !recoveryReady || globalSession == nil || globalSession.ID == 0 {
		stateMutex.Unlock()
		return
	}
	s := *globalSession
	eventID := globalEvent.ID
	collecting := sessionCollecting()
	entries, _ := parseLeaderboardEntries(int(s.ID), latestCarUpdates)
	applyPositionChanges(entries, int(s.ID))
	if collecting {
		recordPositionChangeFeeds(s.ID, entries)
	}
	stateMutex.Unlock()
	interval := cfg.Writer.HTTP.Leaderboard.Broadcast.Interval.MS
	if interval <= 0 {
		interval = 2000
	}
	session := ACSession{SessionID: s.ID, EventID: s.EventID, Type: s.Type, Name: s.Name, TrackTime: s.TrackTime, Laps: s.Laps, Weather: s.Weather, DurationMin: s.DurationMin, ElapsedMs: time.Since(s.StartTime).Milliseconds(), StartGrip: s.StartGrip, CurrentGrip: s.CurrentGrip, HttpPort: s.HTTPPort, StartTime: s.StartTime.UnixMicro(), LastActivity: s.LastActivity.UnixMicro(), AirTemp: s.AirTemp, RoadTemp: s.RoadTemp, IsFinished: s.IsFinished}
	if !s.FinishTime.IsZero() {
		session.FinishTime = s.FinishTime.UnixMicro()
	}
	if s.IsFinished != 0 {
		session.ElapsedMs = int64(s.ElapsedMs)
	}
	msg := struct {
		Version           uint8     `json:"version"`
		BroadcastInterval int32     `json:"broadcastInterval"`
		ViewerCount       int32     `json:"viewerCount"`
		EventID           int       `json:"event_id"`
		Session           ACSession `json:"session"`
		Entries           []Entry   `json:"entries"`
	}{10, int32(interval), 0, eventID, session, entries}
	data, err := json.Marshal(msg)
	if err != nil {
		logger.Printf("序列化WS失败: %v", err)
		return
	}
	wsMutex.Lock()
	defer wsMutex.Unlock()
	if wsConn == nil {
		return
	}
	if wsEventID != eventID {
		wsConn.Close()
		wsConn = nil
		return
	}
	wsConn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := wsConn.WriteMessage(websocket.TextMessage, data); err != nil {
		logger.Printf("发布WS失败: %v", err)
		wsConn.Close()
		wsConn = nil
	}
}

// 调用者持有 stateMutex.RLock；列表按 globalConnections 建立，
// 已连接/加载中/断开的车辆都会投影，不依赖是否收到过 CarUpdate。
func parseLeaderboardEntries(sessionID int, updates map[udp.CarID]udp.CarUpdate) ([]Entry, error) {
	slots := make([]int, 0, len(globalConnections))
	for slot := range globalConnections {
		slots = append(slots, slot)
	}
	sort.Ints(slots)
	projections := make([]*slotProjection, 0, len(slots))
	for _, slot := range slots {
		conn := globalConnections[slot]
		if conn == nil || conn.UserID == 0 || conn.CarDBID == 0 {
			continue
		}
		update, hasUpdate := updates[udp.CarID(slot)]
		projections = append(projections, buildSlotProjection(sessionID, slot, conn, update, hasUpdate))
	}
	// 第二遍：按车组汇总同类全局最快圈/分段后再写 SectorMask。
	applySectorMasks(projections)
	entries := make([]Entry, 0, len(projections))
	for _, p := range projections {
		e := p.entry
		// 官方 board 的圈数可能领先于 DB/恢复基线，用已锚定的观测圈数补齐，避免名次迟滞。
		if t := raceTiming[e.GameCarID]; t != nil && t.anchored && t.drivingLap-1 > int(e.Laps) {
			e.Laps = uint16(t.drivingLap - 1)
		}
		entries = append(entries, e)
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if globalSession.Type == 2 {
			if entries[i].Laps != entries[j].Laps {
				return entries[i].Laps > entries[j].Laps
			}
			if entries[i].NSP != entries[j].NSP {
				return entries[i].NSP > entries[j].NSP
			}
		} else {
			a, b := entries[i].BestLapTime, entries[j].BestLapTime
			if a != b {
				if a == 0 {
					return false
				}
				if b == 0 {
					return true
				}
				return a < b
			}
		}
		// 同键时按槽位号，消除 map 迭代顺序对名次的影响。
		return entries[i].GameCarID < entries[j].GameCarID
	})
	if globalSession.Type == 2 && len(entries) > 0 {
		// Race Gap 用计时点：Gap 对领跑车、Interval 对前车；偶数=毫秒*2，奇数=落后圈数*2+1，无记录 -1。
		gap, interval := raceGaps(entries)
		for i := range entries {
			entries[i].Gap, entries[i].Interval = gap[i], interval[i]
		}
	} else if len(entries) > 0 && entries[0].BestLapTime > 0 {
		// Quali/Practice 不做位图解码，gap/interval 直接是毫秒差。
		for i := range entries {
			if entries[i].BestLapTime > 0 {
				entries[i].Gap = int32(entries[i].BestLapTime - entries[0].BestLapTime)
				if i > 0 && entries[i-1].BestLapTime > 0 {
					entries[i].Interval = int32(entries[i].BestLapTime - entries[i-1].BestLapTime)
				}
			}
		}
	}
	return entries, nil
}

func maxZero(v int) int {
	if v < 0 {
		return 0
	}
	return v
}
