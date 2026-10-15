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
	"math"
	"net"
	"net/http"
	"net/url"
	"sort"
	"time"
)

var wsConn *websocket.Conn
var wsEventID int
var reconnectDelay = 5 * time.Second

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

// 广播读取已恢复的内存状态；接入已有 session 的查询不持连接锁。
func buildAndSendWebSocketMessage() {
	// 保留原广播逻辑：启动接入已有比赛时从数据库取得最新 session，
	// SESSION_INFO 恢复本身不创建或替换 session。
	stateMutex.RLock()
	needsSession := globalSession == nil || globalSession.ID == 0
	eventIDForLookup := 0
	if globalEvent != nil {
		eventIDForLookup = globalEvent.ID
	}
	stateMutex.RUnlock()
	if needsSession && eventIDForLookup > 0 && !globalDBWriter.SimulateSQL {
		latest, err := globalDBWriter.FindLatestSessionByEventID(eventIDForLookup)
		if err != nil {
			logger.Printf("读取已有 session 失败: %v", err)
			return
		}
		if latest != nil {
			stateMutex.Lock()
			if globalEvent != nil && globalEvent.ID == eventIDForLookup && (globalSession == nil || globalSession.ID == 0) {
				globalSession = latest
			}
			stateMutex.Unlock()
		}
	}
	stateMutex.RLock()
	if !recoveryReady || globalSession == nil || globalSession.ID == 0 {
		stateMutex.RUnlock()
		return
	}
	s := *globalSession
	eventID := globalEvent.ID
	entries, _ := parseLeaderboardEntries(int(s.ID), latestCarUpdates)
	stateMutex.RUnlock()
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

// 调用者持有 stateMutex.RLock；所有身份来自接入时建立的缓存。
func parseLeaderboardEntries(sessionID int, updates map[udp.CarID]udp.CarUpdate) ([]Entry, error) {
	entries := make([]Entry, 0, len(updates))
	for slot, update := range updates {
		conn := globalConnections[int(slot)]
		if conn == nil || conn.UserID == 0 || conn.CarDBID == 0 {
			continue
		}
		entry := Entry{UserID: int64(conn.UserID), TeamID: int64(conn.TeamID), CarID: uint16(conn.CarDBID), GameCarID: int(slot), DriverName: conn.DriverName, StatusMask: (conn.TrackStatus << 1) | (conn.ConnectionStatus << 5), NSP: update.NormalisedSplinePos, PosX: update.Pos.X, PosZ: update.Pos.Z, RPM: update.EngineRPM, Gear: update.Gear, Gap: -1, Interval: -1}
		speed := math.Sqrt(float64(update.Velocity.X*update.Velocity.X+update.Velocity.Y*update.Velocity.Y+update.Velocity.Z*update.Velocity.Z)) * 3.6
		if speed > 1023 {
			speed = 1023
		}
		if speed < 0 {
			speed = 0
		}
		entry.Speed = uint16(speed)
		entry.Gear &= 63
		if score := recoveredScores[int(slot)]; score != nil {
			entry.Laps = uint16(score.Laps)
			entry.LastLapTime = int32(score.LastTime)
			entry.ValidLaps = uint16(score.ValidLaps)
			if score.Finished {
				entry.StatusMask |= 1
			}
			if score.BestTime > 0 {
				entry.BestLapTime = uint32(score.BestTime)
				best := score.BestLap
				if best.Time == score.BestTime {
					entry.BestLapS1 = uint32(maxZero(best.Sector1))
					entry.BestLapS2 = uint32(maxZero(best.Sector2))
					entry.BestLapS3 = uint32(maxZero(best.Sector3))
				}
				// 只有 BEST 汇总而没有该圈分段时，只传已知总圈时。
				if entry.BestLapS1+entry.BestLapS2+entry.BestLapS3 == 0 {
					entry.BestLapS3 = uint32(score.BestTime)
				}
			}
		}
		if values := carLapDataMap[int(slot)]; len(values) > 0 {
			last := values[len(values)-1]
			entry.Laps = last.Laps
			entry.LastLapTime = int32(last.LapTime)
			if last.Completed != 0 {
				entry.StatusMask |= 1
			}
		}
		validLaps := 0
		for _, stint := range sessionStints {
			if stint.GameCarID == int(slot) {
				validLaps += stint.ValidLaps
			}
		}
		if uint16(validLaps) > entry.ValidLaps {
			entry.ValidLaps = uint16(validLaps)
		}
		// 无已有 stint 时，恢复基线与接入后新采集计数相加；已有 stint 则避免双计数。
		if score := recoveredScores[int(slot)]; score != nil {
			known := false
			for _, stint := range sessionStints {
				if stint.GameCarID == int(slot) && restoredStintIDs[stint.ID] {
					known = true
					break
				}
			}
			if !known {
				entry.ValidLaps = uint16(score.ValidLaps + validLaps)
			}
		}
		entry.Tyre = userTyres[conn.DriverName]
		// 最快圈的三段必须属于同一圈，不能拼接三个不同圈的最佳分段。
		for _, lap := range stintLapData[sessionID][int(slot)] {
			if lap.Time > 0 && lap.Cuts == 0 && lap.Crashes == 0 && lap.CarCrashes == 0 && (entry.BestLapTime == 0 || uint32(lap.Time) < entry.BestLapTime) {
				entry.BestLapTime = uint32(lap.Time)
				entry.BestLapS1 = uint32(maxZero(lap.Sector1))
				entry.BestLapS2 = uint32(maxZero(lap.Sector2))
				entry.BestLapS3 = uint32(maxZero(lap.Sector3))
			}
		}
		// 没有日志分段时保留 UDP 圈时，放在第三段使原版 Lap 求和得到总圈时。
		for _, lap := range carLapDataMap[int(slot)] {
			if lap.LapTime > 0 && lap.Cuts == 0 && lap.Crashes == 0 && lap.CarCrashes == 0 && (entry.BestLapTime == 0 || uint32(lap.LapTime) < entry.BestLapTime) {
				entry.BestLapTime = uint32(lap.LapTime)
				entry.BestLapS1 = uint32(maxZero(lap.Sector1))
				entry.BestLapS2 = uint32(maxZero(lap.Sector2))
				entry.BestLapS3 = uint32(maxZero(lap.Sector3))
				if entry.BestLapS1+entry.BestLapS2+entry.BestLapS3 == 0 {
					entry.BestLapS3 = uint32(lap.LapTime)
				}
			}
		}
		values := stintLapData[sessionID][int(slot)]
		if len(values) > 0 {
			v := values[len(values)-1]
			if v.Time == 0 {
				entry.CurrentLapS1 = uint32(maxZero(v.Sector1))
				entry.CurrentLapS2 = uint32(maxZero(v.Sector2))
				entry.CurrentLapS3 = uint32(maxZero(v.Sector3))
			}
		}
		entry.TyreLength = uint8(len(entry.Tyre))
		entries = append(entries, entry)
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if globalSession.Type == 2 {
			if entries[i].Laps != entries[j].Laps {
				return entries[i].Laps > entries[j].Laps
			}
			return entries[i].NSP > entries[j].NSP
		}
		a, b := entries[i].BestLapTime, entries[j].BestLapTime
		if a == 0 {
			return false
		}
		if b == 0 {
			return true
		}
		return a < b
	})
	if globalSession.Type == 2 && len(entries) > 0 {
		for i := 1; i < len(entries); i++ {
			diff := int(entries[0].Laps) - int(entries[i].Laps)
			if diff > 0 {
				entries[i].Gap = int32(diff*2 + 1)
			}
			prev := int(entries[i-1].Laps) - int(entries[i].Laps)
			if prev > 0 {
				entries[i].Interval = int32(prev*2 + 1)
			}
		}
	} else if len(entries) > 0 && entries[0].BestLapTime > 0 {
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
