// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocketMessage 定义 Writer 进程推送的排行榜消息结构。
// 字段名（json tag）必须与 writer 端一致。
type WebSocketMessage struct {
	Version           uint8     `json:"version"`
	BroadcastInterval int32     `json:"broadcastInterval"`
	ViewerCount       int32     `json:"viewerCount"`
	EventID           int       `json:"event_id"`
	Session           ACSession `json:"session"`
	Entries           []Entry   `json:"entries"`
}

// leaderboardVersion 原版前端 LeaderBoardDeserialiser.VERSION
const leaderboardVersion = 10

// WebSocket 读写超时（原版前端与 gorilla 默认一致）
const (
	pongWait   = 60 * time.Second // 读超时：超过此时长未收到 pong 即判定断开
	pingPeriod = 15 * time.Second // 服务端发 ping 的间隔，需小于 pongWait
	writeWait  = 10 * time.Second // 单次写的超时
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// eventLiveState 单个 event 的实时状态：完整保存推送内容（含 Version/EventID），按 event 隔离。
type eventLiveState struct {
	mu      sync.Mutex
	message WebSocketMessage
	hasData bool
	viewers int32
}

func (st *eventLiveState) addViewer(delta int32) {
	st.mu.Lock()
	st.viewers += delta
	if st.viewers < 0 {
		st.viewers = 0
	}
	st.mu.Unlock()
}

// liveHub 以 event_id 为键隔离各事件的实时数据
var liveHub = struct {
	mu     sync.Mutex
	events map[int]*eventLiveState
}{events: make(map[int]*eventLiveState)}

func hubState(eventID int) *eventLiveState {
	liveHub.mu.Lock()
	defer liveHub.mu.Unlock()
	st := liveHub.events[eventID]
	if st == nil {
		st = &eventLiveState{}
		liveHub.events[eventID] = st
	}
	return st
}

// hubStore 保存 Writer 推送的完整消息（含 Version/EventID/ViewerCount）
func hubStore(msg WebSocketMessage) {
	if msg.EventID <= 0 {
		return
	}
	if msg.Version == 0 {
		msg.Version = leaderboardVersion
	}
	if msg.Session.EventID == 0 {
		msg.Session.EventID = msg.EventID
	}
	st := hubState(msg.EventID)
	st.mu.Lock()
	st.message = msg
	st.hasData = true
	st.mu.Unlock()
}

// hubSnapshot 读取某事件的实时快照
func hubSnapshot(eventID int) (WebSocketMessage, bool, int32) {
	liveHub.mu.Lock()
	st := liveHub.events[eventID]
	liveHub.mu.Unlock()
	if st == nil {
		return WebSocketMessage{}, false, 0
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.message, st.hasData, st.viewers
}

// hubLiveSession 返回某事件收到的实时会话（若有）
func hubLiveSession(eventID int) *ACSession {
	msg, ok, _ := hubSnapshot(eventID)
	if !ok {
		return nil
	}
	s := msg.Session
	return &s
}

// hubLiveEventIDs 返回当前有未结束会话的事件 ID
func hubLiveEventIDs() []int {
	liveHub.mu.Lock()
	states := make(map[int]*eventLiveState, len(liveHub.events))
	for k, v := range liveHub.events {
		states[k] = v
	}
	liveHub.mu.Unlock()

	ids := []int{}
	for id, st := range states {
		st.mu.Lock()
		live := st.hasData && st.message.Session.IsFinished == 0
		st.mu.Unlock()
		if live {
			ids = append(ids, id)
		}
	}
	return ids
}

// liveEventIDFromRequest 从查询参数或 eventId cookie 解析事件 ID
func liveEventIDFromRequest(r *http.Request) int {
	if r == nil {
		return 0
	}
	if v := r.URL.Query().Get("event_id"); v != "" {
		if id, err := strconv.Atoi(v); err == nil && id > 0 {
			return id
		}
	}
	if c, err := r.Cookie("eventId"); err == nil && c.Value != "" {
		if id, err := strconv.Atoi(c.Value); err == nil && id > 0 {
			return id
		}
	}
	return 0
}

// feedItem 一条会话 feed
type feedItem struct {
	typ       uint8
	timestamp TimeWithTimezone
	detail    string
}

// loadSessionFeeds 增量读取 feed：cursor 为 0 时也会从会话开头开始（初始 feed 0 也可增量）
func loadSessionFeeds(sessionID, cursor int64, limit int) ([]feedItem, int64) {
	if sessionID <= 0 || limit <= 0 {
		return nil, cursor
	}
	rows, err := db.Query(`SELECT session_feed_id, type, COALESCE(detail, ''), CAST(UNIX_TIMESTAMP(time) * 1000000 AS SIGNED)
		FROM session_feed WHERE session_id = ? AND session_feed_id > ? ORDER BY session_feed_id ASC LIMIT ?`, sessionID, cursor, limit)
	if err != nil {
		logger.Printf("查询feed数据失败: %v", err)
		return nil, cursor
	}
	defer rows.Close()

	feeds := make([]feedItem, 0, limit)
	newCursor := cursor
	for rows.Next() {
		var id int64
		var typ int
		var detail string
		var ts TimeWithTimezone
		if err := rows.Scan(&id, &typ, &detail, &ts); err != nil {
			continue
		}
		feeds = append(feeds, feedItem{typ: uint8(typ), timestamp: ts, detail: detail})
		if id > newCursor {
			newCursor = id
		}
	}
	return feeds, newCursor
}

// liveConn 单个已升级连接的状态
type liveConn struct {
	conn          *websocket.Conn
	eventID       int
	producer      bool
	viewerCounted bool
	feedSession   int64
	feedCursor    int64
}

// writeBinary 写入二进制数据并统一处理错误
func writeBinary(buf *bytes.Buffer, order binary.ByteOrder, data interface{}, field string) error {
	if err := binary.Write(buf, order, data); err != nil {
		return err
	}
	return nil
}

// writeEntriesToBuffer 按前端 LeaderBoardDeserialiser 的顺序写入条目
func writeEntriesToBuffer(entries []Entry, buf *bytes.Buffer) error {
	for _, e := range entries {
		if err := writeBinary(buf, binary.LittleEndian, e.TeamID, "TeamID"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.UserID, "UserID"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.CarID, "CarID"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.StatusMask, "StatusMask"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.Laps, "Laps"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.ValidLaps, "ValidLaps"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.NSP, "NSP"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.PosX, "PosX"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.PosZ, "PosZ"); err != nil {
			return err
		}
		// gear 6bit、speed 10bit：先各自截断再拼合，避免相互溢出（前端 mask2&63 / mask2>>6）
		gear := int(e.Gear)
		if gear > 63 {
			gear = 63
		}
		speed := int(e.Speed)
		if speed > 1023 {
			speed = 1023
		}
		mask2 := uint16(gear) | (uint16(speed) << 6)
		if err := writeBinary(buf, binary.LittleEndian, mask2, "GearSpeed"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.RPM, "RPM"); err != nil {
			return err
		}
		// TyreLength 必须按实际字节数计算（上限 255），否则前端解析会错位
		tyreBytes := []byte(e.Tyre)
		if len(tyreBytes) > 255 {
			tyreBytes = tyreBytes[:255]
		}
		if err := writeBinary(buf, binary.LittleEndian, uint8(len(tyreBytes)), "TyreLength"); err != nil {
			return err
		}
		buf.Write(tyreBytes)
		if err := writeBinary(buf, binary.LittleEndian, e.BestLapS1, "BestLapS1"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.BestLapS2, "BestLapS2"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.BestLapS3, "BestLapS3"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.CurrentLapS1, "CurrentLapS1"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.CurrentLapS2, "CurrentLapS2"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.CurrentLapS3, "CurrentLapS3"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.SectorMask, "SectorMask"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.LastLapTime, "LastLapTime"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.Gap, "Gap"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.Interval, "Interval"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.PosChange, "PosChange"); err != nil {
			return err
		}
	}
	return nil
}

// buildLeaderboardMessage 依据前端 LeaderBoardDeserialiser v10 的字段顺序构建二进制消息
func buildLeaderboardMessage(msg WebSocketMessage, feeds []feedItem, viewers int32) ([]byte, error) {
	buf := &bytes.Buffer{}

	version := msg.Version
	if version == 0 {
		version = leaderboardVersion
	}
	if err := writeBinary(buf, binary.LittleEndian, version, "版本号"); err != nil {
		return nil, err
	}
	if err := writeBinary(buf, binary.LittleEndian, msg.BroadcastInterval, "广播间隔"); err != nil {
		return nil, err
	}
	if err := writeBinary(buf, binary.LittleEndian, viewers, "观众数量"); err != nil {
		return nil, err
	}
	if err := writeBinary(buf, binary.LittleEndian, int64(msg.Session.SessionID), "会话ID"); err != nil {
		return nil, err
	}
	if err := writeBinary(buf, binary.LittleEndian, int32(msg.Session.ElapsedMs), "已用时间"); err != nil {
		return nil, err
	}
	if err := writeBinary(buf, binary.LittleEndian, uint16(msg.Session.DurationMin), "持续时间"); err != nil {
		return nil, err
	}
	sessionType := uint8(0)
	if msg.Session.Type == 2 {
		sessionType = 1
	}
	if err := writeBinary(buf, binary.LittleEndian, sessionType, "会话类型"); err != nil {
		return nil, err
	}
	if err := writeBinary(buf, binary.LittleEndian, float32(msg.Session.StartGrip), "抓地力值"); err != nil {
		return nil, err
	}
	if err := writeBinary(buf, binary.LittleEndian, float32(msg.Session.CurrentGrip), "赛道状况"); err != nil {
		return nil, err
	}

	entries := msg.Entries
	if len(entries) > 255 {
		entries = entries[:255]
	}
	if err := binary.Write(buf, binary.LittleEndian, uint8(len(entries))); err != nil {
		return nil, err
	}
	if err := writeEntriesToBuffer(entries, buf); err != nil {
		return nil, err
	}

	if len(feeds) > 255 {
		feeds = feeds[:255]
	}
	if err := binary.Write(buf, binary.LittleEndian, uint8(len(feeds))); err != nil {
		return nil, err
	}
	for _, f := range feeds {
		if err := binary.Write(buf, binary.LittleEndian, f.typ); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, int64(f.timestamp)); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(f.detail))); err != nil {
			return nil, err
		}
		buf.WriteString(f.detail)
	}
	return buf.Bytes(), nil
}

// handleACEventWebSocket 同时服务于两类连接：
//   - Writer 生产者（请求头 X-SimView-Writer: 1）：只 ReadMessage 更新完整消息
//     （全 Version/interval/EventID/session/entries），不接收 viewer 二进制、不计入观众数，并响应 ping；
//   - 浏览器观众：按 eventId 隔离，接收二进制排行榜数据。
//
// 每个连接独立管理生命周期：任一端断开即退出全部协程；同一连接的写操作串行执行。
func handleACEventWebSocket(w http.ResponseWriter, r *http.Request) {
	eventID := liveEventIDFromRequest(r)
	isWriter := strings.EqualFold(strings.TrimSpace(r.Header.Get("X-SimView-Writer")), "1")

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Printf("WebSocket升级失败: %v", err)
		return
	}

	c := &liveConn{conn: conn, eventID: eventID, producer: isWriter}
	if !isWriter && eventID > 0 {
		hubState(eventID).addViewer(1)
		c.viewerCounted = true
	}

	// 所有写操作（数据帧 + 控制帧）共用这把锁，保证串行；ping 的 pong 也必须复用写锁
	var writeMu sync.Mutex
	conn.SetPingHandler(func(appData string) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(writeWait))
	})

	// 读超时与 pong 处理必须在读循环启动前设置好：
	// 若 pongWait 内收不到 pong（客户端断开），ReadMessage 会超时返回 → 触发退出
	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	var doneOnce sync.Once
	done := make(chan struct{})
	stop := func() { doneOnce.Do(func() { close(done) }) }
	defer stop()
	defer conn.Close()
	defer func() {
		if c.viewerCounted {
			hubState(c.eventID).addViewer(-1)
			c.viewerCounted = false
		}
	}()

	// 读循环：唯一的读取者，解析出的消息交给主循环处理（避免并发访问连接状态）
	msgCh := make(chan WebSocketMessage, 32)
	go func() {
		defer close(msgCh)
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var msg WebSocketMessage
			if json.Unmarshal(data, &msg) != nil {
				continue
			}
			select {
			case msgCh <- msg:
			case <-done:
				return
			}
		}
	}()

	pingTicker := time.NewTicker(pingPeriod)
	defer pingTicker.Stop()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()

	var lastSend time.Time
	var lastInterval int32

	for {
		select {
		case msg, ok := <-msgCh:
			if !ok {
				return
			}
			if msg.EventID <= 0 {
				msg.EventID = c.eventID
			}
			if msg.EventID <= 0 {
				continue
			}
			if !c.producer {
				// 未声明为 Writer 的连接一旦推送排行榜消息，也视作生产者：不再向它发送二进制、不计入观众
				c.producer = true
				if c.viewerCounted {
					hubState(c.eventID).addViewer(-1)
					c.viewerCounted = false
				}
			}
			hubStore(msg)
		case <-pingTicker.C:
			writeMu.Lock()
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			werr := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait))
			writeMu.Unlock()
			if werr != nil {
				return
			}
		case <-tick.C:
			if c.producer {
				continue
			}
			if c.eventID == 0 {
				// 无 cookie/参数时，若只有一个实时事件则自动归入
				if ids := hubLiveEventIDs(); len(ids) == 1 {
					c.eventID = ids[0]
					hubState(c.eventID).addViewer(1)
					c.viewerCounted = true
				} else {
					continue
				}
			}
			msg, ok, viewers := hubSnapshot(c.eventID)
			if !ok {
				continue
			}
			interval := msg.BroadcastInterval
			if interval <= 0 {
				interval = 2000
			}
			if lastInterval != interval {
				lastInterval = interval
				lastSend = time.Time{}
			}
			if !lastSend.IsZero() && time.Since(lastSend) < time.Duration(interval)*time.Millisecond {
				continue
			}
			// 切换会话时重置 feed 游标，重发新会话的 feed
			sessionID := msg.Session.SessionID
			if c.feedSession != sessionID {
				c.feedSession = sessionID
				c.feedCursor = 0
				lastSend = time.Time{}
			}
			feeds, newCursor := loadSessionFeeds(sessionID, c.feedCursor, 255)
			payload, err := buildLeaderboardMessage(msg, feeds, viewers)
			if err != nil {
				logger.Printf("构建排行榜消息失败: %v", err)
				continue
			}
			writeMu.Lock()
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			werr := conn.WriteMessage(websocket.BinaryMessage, payload)
			writeMu.Unlock()
			if werr != nil {
				return
			}
			c.feedCursor = newCursor
			lastSend = time.Now()
		}
	}
}
