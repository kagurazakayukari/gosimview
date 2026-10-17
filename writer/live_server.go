// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

// Writer 自己提供 /live：与原版一致，页面按 session.http_port 直连 Writer，
// http 只发页面与 /api。这里负责 v10 二进制帧的构建、观众计数与每连接的 feed 游标。
//
// 帧顺序与前端 LeaderBoardDeserialiser v10 一致，改字段必须同步页面解析。

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	leaderboardVersion = 10
	liveFeedLimit      = 255
	liveWriteWait      = 10 * time.Second
)

var liveUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// liveFeed 是一条待下发的 session_feed。
type liveFeed struct {
	typ    uint8
	at     int64 // 微秒，与页面 feed.time/1e3 口径一致
	detail string
}

// liveFrame 是一次广播的输入快照，由广播 tick 覆盖写。
type liveFrame struct {
	session  *Session
	entries  []Entry
	interval int32
}

var liveState = struct {
	mu      sync.Mutex
	frame   *liveFrame
	viewers int32
	ctx     context.Context
}{}

var liveStartedAt = time.Now()

func liveBroadcastInterval() int32 {
	if cfg != nil && cfg.Writer.HTTP.Leaderboard.Broadcast.Interval.MS > 0 {
		return int32(cfg.Writer.HTTP.Leaderboard.Broadcast.Interval.MS)
	}
	return 2000
}

func emptyLiveFrame() *liveFrame {
	return &liveFrame{
		session:  &Session{StartGrip: -1, CurrentGrip: -1},
		interval: liveBroadcastInterval(),
	}
}

func liveContext() context.Context {
	liveState.mu.Lock()
	defer liveState.mu.Unlock()
	if liveState.ctx != nil {
		return liveState.ctx
	}
	return context.Background()
}

// setLiveFrame 保存最新快照；调用者不必持锁。
func setLiveFrame(s *Session, entries []Entry, interval int32) {
	liveState.mu.Lock()
	liveState.frame = &liveFrame{session: s, entries: entries, interval: interval}
	liveState.mu.Unlock()
}

// snapshotFrame 取出当前快照与观众数。
func snapshotFrame() (*liveFrame, int32) {
	liveState.mu.Lock()
	defer liveState.mu.Unlock()
	return liveState.frame, liveState.viewers
}

func addViewer(delta int32) {
	liveState.mu.Lock()
	liveState.viewers += delta
	if liveState.viewers < 0 {
		liveState.viewers = 0
	}
	liveState.mu.Unlock()
}

// latestLiveFeedID 取得会话当前最后一条 feed，避免新观众从历史开头重放。
func latestLiveFeedID(sessionID int64) (int64, bool) {
	if sessionID <= 0 || globalDBWriter == nil || globalDBWriter.DB == nil {
		return 0, true
	}
	var id int64
	if err := globalDBWriter.DB.QueryRow("SELECT COALESCE(MAX(session_feed_id), 0) FROM session_feed WHERE session_id = ?", sessionID).Scan(&id); err != nil {
		logger.Printf("查询最新 feed 游标失败: %v", err)
		return 0, false
	}
	return id, true
}

// loadLiveFeeds 增量读取 session_feed，cursor 为上次已发送的 session_feed_id。
func loadLiveFeeds(sessionID, cursor int64) ([]liveFeed, int64) {
	if sessionID <= 0 || globalDBWriter == nil || globalDBWriter.DB == nil {
		return nil, cursor
	}
	rows, err := globalDBWriter.DB.Query(`SELECT session_feed_id, type, COALESCE(detail, ''),
		CAST(UNIX_TIMESTAMP(time) * 1000000 AS SIGNED)
		FROM session_feed WHERE session_id = ? AND session_feed_id > ?
		ORDER BY session_feed_id ASC LIMIT ?`, sessionID, cursor, liveFeedLimit)
	if err != nil {
		logger.Printf("查询 feed 失败: %v", err)
		return nil, cursor
	}
	defer rows.Close()

	feeds := make([]liveFeed, 0, liveFeedLimit)
	next := cursor
	for rows.Next() {
		var (
			id  int64
			typ int
			det string
			at  int64
		)
		err := rows.Scan(&id, &typ, &det, &at)
		// id 是结果集第一列；即使后续列转换失败，数据库驱动通常已经写入它，
		// 先推进游标，避免坏行在每帧重复阻塞后续 feed。
		if id > next {
			next = id
		}
		if err != nil {
			logger.Printf("扫描 feed 行失败，跳过当前行: %v", err)
			continue
		}
		if len(det) > int(^uint16(0)) {
			logger.Printf("跳过超长 feed 详情(session=%d, feed=%d, bytes=%d)", sessionID, id, len(det))
			continue
		}
		feeds = append(feeds, liveFeed{typ: uint8(typ), at: at, detail: det})
	}
	return feeds, next
}

// buildLivePayload 按 v10 顺序拼一帧二进制。
func buildLivePayload(f *liveFrame, feeds []liveFeed, viewers int32) []byte {
	if f == nil {
		f = emptyLiveFrame()
	}
	buf := &bytes.Buffer{}
	s := f.session
	if s == nil {
		s = &Session{StartGrip: -1, CurrentGrip: -1}
	}

	interval := f.interval
	if interval <= 0 {
		interval = 2000
	}
	binary.Write(buf, binary.LittleEndian, uint8(leaderboardVersion))
	binary.Write(buf, binary.LittleEndian, interval)
	binary.Write(buf, binary.LittleEndian, viewers)
	binary.Write(buf, binary.LittleEndian, int64(s.ID))
	binary.Write(buf, binary.LittleEndian, int32(s.ElapsedMs))
	binary.Write(buf, binary.LittleEndian, uint16(s.DurationMin))
	sessionFlag := uint8(0)
	if s.Type == 2 {
		sessionFlag = 1
	}
	binary.Write(buf, binary.LittleEndian, sessionFlag)
	binary.Write(buf, binary.LittleEndian, float32(s.StartGrip))
	binary.Write(buf, binary.LittleEndian, float32(s.CurrentGrip))

	entries := f.entries
	if len(entries) > 255 {
		entries = entries[:255]
	}
	binary.Write(buf, binary.LittleEndian, uint8(len(entries)))
	writeEntries(buf, entries)

	if len(feeds) > 255 {
		feeds = feeds[:255]
	}
	binary.Write(buf, binary.LittleEndian, uint8(len(feeds)))
	for _, fd := range feeds {
		detail := []byte(fd.detail)
		binary.Write(buf, binary.LittleEndian, fd.typ)
		binary.Write(buf, binary.LittleEndian, fd.at)
		binary.Write(buf, binary.LittleEndian, uint16(len(detail)))
		buf.Write(detail)
	}
	return buf.Bytes()
}

// writeEntries 写条目区，字段顺序与页面反序列化严格一致。
// team/user 在线上是 uint32（http 那边的 Entry 就是 uint32），
// writer 侧的 Entry 用 int64 存库值，写帧必须截断，否则每条多 8 字节、后面全错位。
func writeEntries(buf *bytes.Buffer, entries []Entry) {
	for _, e := range entries {
		binary.Write(buf, binary.LittleEndian, uint32(e.TeamID))
		binary.Write(buf, binary.LittleEndian, uint32(e.UserID))
		binary.Write(buf, binary.LittleEndian, e.CarID)
		binary.Write(buf, binary.LittleEndian, e.StatusMask)
		binary.Write(buf, binary.LittleEndian, e.Laps)
		binary.Write(buf, binary.LittleEndian, e.ValidLaps)
		binary.Write(buf, binary.LittleEndian, e.NSP)
		binary.Write(buf, binary.LittleEndian, e.PosX)
		binary.Write(buf, binary.LittleEndian, e.PosZ)
		gear := int(e.Gear)
		if gear > 63 {
			gear = 63
		}
		speed := int(e.Speed)
		if speed > 1023 {
			speed = 1023
		}
		binary.Write(buf, binary.LittleEndian, uint16(gear)|uint16(speed)<<6)
		binary.Write(buf, binary.LittleEndian, e.RPM)
		tyre := []byte(e.Tyre)
		if len(tyre) > 255 {
			tyre = tyre[:255]
		}
		binary.Write(buf, binary.LittleEndian, uint8(len(tyre)))
		buf.Write(tyre)
		binary.Write(buf, binary.LittleEndian, e.BestLapS1)
		binary.Write(buf, binary.LittleEndian, e.BestLapS2)
		binary.Write(buf, binary.LittleEndian, e.BestLapS3)
		binary.Write(buf, binary.LittleEndian, e.CurrentLapS1)
		binary.Write(buf, binary.LittleEndian, e.CurrentLapS2)
		binary.Write(buf, binary.LittleEndian, e.CurrentLapS3)
		binary.Write(buf, binary.LittleEndian, e.SectorMask)
		binary.Write(buf, binary.LittleEndian, e.LastLapTime)
		binary.Write(buf, binary.LittleEndian, e.Gap)
		binary.Write(buf, binary.LittleEndian, e.Interval)
		binary.Write(buf, binary.LittleEndian, e.PosChange)
	}
}

// startLiveServer 在 writer.http.port 上提供 /live，退出时关闭监听。
// 返回的 channel 在监听协程退出后关闭，供主流程等待。
func startLiveServer(ctx context.Context, port int) (<-chan struct{}, error) {
	setLiveFrame(nil, nil, int32(cfg.Writer.HTTP.Leaderboard.Broadcast.Interval.MS))
	mux := http.NewServeMux()
	mux.HandleFunc("/live", handleLiveWebSocket)
	srv := &http.Server{Addr: net.JoinHostPort("", strconv.Itoa(port)), Handler: mux}

	if port <= 0 || port > 65535 {
		return nil, fmt.Errorf("live 端口无效: %d", port)
	}
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return nil, err
	}
	liveState.mu.Lock()
	liveState.ctx = ctx
	liveState.mu.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Printf("live 服务退出: %v", err)
		}
	}()
	logger.Printf("Live WebSocket 服务监听: %s/live", srv.Addr)
	return done, nil
}

// handleLiveWebSocket 单连接：按帧间隔下发，feed 按本连接游标增量补。
func handleLiveWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := liveUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	addViewer(1)
	ctx := liveContext()
	done := make(chan struct{})
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			close(done)
			_ = conn.Close()
		})
	}
	defer func() {
		stop()
		addViewer(-1)
	}()

	// 读协程只负责感知断开；页面不发业务消息。
	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				stop()
				return
			}
		}
	}()

	lastSend := time.Time{}
	lastInterval := int32(0)
	feedSession := int64(-1)
	feedCursor := int64(0)
	for {
		frame, viewers := snapshotFrame()
		if frame == nil {
			frame = emptyLiveFrame()
		}
		interval := frame.interval
		if interval <= 0 {
			interval = liveBroadcastInterval()
		}
		if lastInterval != interval {
			lastInterval = interval
			lastSend = time.Time{}
		}
		if !lastSend.IsZero() && time.Since(lastSend) < time.Duration(interval)*time.Millisecond {
			timer := time.NewTimer(50 * time.Millisecond)
			select {
			case <-timer.C:
			case <-done:
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			}
			continue
		}
		sessionID := int64(0)
		if frame.session != nil {
			sessionID = frame.session.ID
		}
		if feedSession != sessionID {
			feedSession = sessionID
			feedCursor = 0
			if sessionID > 0 {
				var ok bool
				feedCursor, ok = latestLiveFeedID(sessionID)
				if !ok {
					timer := time.NewTimer(200 * time.Millisecond)
					select {
					case <-timer.C:
					case <-done:
						if !timer.Stop() {
							<-timer.C
						}
						return
					case <-ctx.Done():
						if !timer.Stop() {
							<-timer.C
						}
						return
					}
					feedSession = -1
					continue
				}
			}
		}
		var feeds []liveFeed
		next := feedCursor
		if sessionID > 0 {
			feeds, next = loadLiveFeeds(sessionID, feedCursor)
		}
		payload := buildLivePayload(frame, feeds, viewers)

		conn.SetWriteDeadline(time.Now().Add(liveWriteWait))
		if err := conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
			return
		}
		feedCursor = next
		lastSend = time.Now()
	}
}
