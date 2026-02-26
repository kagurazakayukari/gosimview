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
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"gosimview/udp"

	"github.com/gorilla/websocket"
)

// WebSocketMessage 定义从WebSocket接收的消息结构
type WebSocketMessage struct {
	Version           uint8     `json:"version"`
	BroadcastInterval int32     `json:"broadcastInterval"`
	ViewerCount       int32     `json:"viewerCount"`
	EventID           int       `json:"event_id"`
	Session           ACSession `json:"session"`
	Entries           []Entry   `json:"entries"`
}

// 存储从WebSocket接收的事件数据
var liveEventsData WebSocketMessage
var liveEventsMutex sync.Mutex

// WebSocket相关变量
var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	wsConnectionCount int32
	carUpdates        = make(map[int64]udp.CarUpdate)
	carUpdatesMutex   sync.Mutex
)

// 缓冲区池，用于减少内存分配
var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// 写入二进制数据并统一处理错误
func writeBinary(buf *bytes.Buffer, order binary.ByteOrder, data interface{}, field string) error {
	if err := binary.Write(buf, order, data); err != nil {
		return fmt.Errorf("写入%s失败: %v", field, err)
	}
	return nil
}

// 写入条目数据到缓冲区
func writeEntriesToBuffer(entries []Entry, buf *bytes.Buffer) error {
	for _, e := range entries {
		// 逐个字段写入基础数据（每个writeBinary单独成行）
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
		if err := writeBinary(buf, binary.LittleEndian, e.TelemetryMask, "TelemetryMask"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.RPM, "RPM"); err != nil {
			return err
		}
		if err := writeBinary(buf, binary.LittleEndian, e.TyreLength, "TyreLength"); err != nil {
			return err
		}

		// 写入Tyre字符串
		buf.Write([]byte(e.Tyre))

		// 逐个字段写入圈速数据（每个writeBinary单独成行）
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

// 构建WebSocket消息
func buildWebSocketMessage(data WebSocketMessage, buf *bytes.Buffer) error {
	buf.Reset()

	if err := writeBinary(buf, binary.LittleEndian, data.Version, "版本号"); err != nil {
		return err
	}
	if err := writeBinary(buf, binary.LittleEndian, data.BroadcastInterval, "广播间隔"); err != nil {
		return err
	}
	if err := writeBinary(buf, binary.LittleEndian, data.ViewerCount, "观众数量"); err != nil {
		return err
	}
	if err := writeBinary(buf, binary.LittleEndian, int64(data.Session.SessionID), "会话ID"); err != nil {
		return err
	}
	if err := writeBinary(buf, binary.LittleEndian, int32(data.Session.ElapsedMs), "已用时间"); err != nil {
		return err
	}
	if err := writeBinary(buf, binary.LittleEndian, uint16(data.Session.DurationMin), "持续时间"); err != nil {
		return err
	}

	// 写入会话类型
	sessionType := uint8(0)
	if data.Session.Type == 2 {
		sessionType = 1
	}
	if err := writeBinary(buf, binary.LittleEndian, sessionType, "会话类型"); err != nil {
		return err
	}

	// 写入抓地力值和赛道状况（每个writeBinary单独成行）
	if err := writeBinary(buf, binary.LittleEndian, float32(data.Session.StartGrip), "抓地力值"); err != nil {
		return err
	}
	if err := writeBinary(buf, binary.LittleEndian, float32(data.Session.CurrentGrip), "赛道状况"); err != nil {
		return err
	}
	return nil
}

// WebSocket处理器，发送符合前端LeaderBoardDeserialiser v10格式的二进制数据
func handleACEventWebSocket(w http.ResponseWriter, r *http.Request) {
	// 升级HTTP连接为WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Printf("WebSocket升级失败: %v", err)
		return
	}
	defer conn.Close()

	// 从cookie获取eventId
	cookie, err := r.Cookie("eventId")
	if err != nil || cookie.Value == "" {
		logger.Printf("eventId cookie缺失")
		return
	}
	eventId := cookie.Value

	atomic.AddInt32(&wsConnectionCount, 1)
	defer func() {
		atomic.AddInt32(&wsConnectionCount, -1)
		conn.Close()
	}()

	// 读取WebSocket消息
	go func() {
		defer conn.Close()
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				logger.Printf("WebSocket读取错误: %v", err)
				break
			}
			// 解析WebSocket消息
			var msg WebSocketMessage
			if err := json.Unmarshal(message, &msg); err == nil {
				// 验证消息中的eventId是否与cookie匹配
				if msg.EventID > 0 && strconv.Itoa(msg.EventID) == eventId {
					// 更新活跃会话和条目数据
					liveEventsMutex.Lock()
					liveEventsData.Session = msg.Session
					liveEventsData.Entries = msg.Entries
					liveEventsMutex.Unlock()
				} else {
					logger.Printf("消息eventId不匹配: 消息=%d, Cookie=%s", msg.Session.EventID, eventId)
				}
			}
		}
	}()

	// 获取最新会话ID
	latestSessionID := liveEventsData.Session.SessionID

	// 获取会话详情
	session := liveEventsData.Session

	// 查询session feed数据并记录最后一个feed_id
	var lastFeedID int64
	feedRows, err := db.Query(`SELECT session_feed_id, type, detail, CAST(UNIX_TIMESTAMP(time) * 1000000 AS SIGNED) AS time FROM session_feed WHERE session_id = ? ORDER BY session_feed_id ASC`, session.SessionID)
	if err != nil {
		logger.Printf("查询feed数据失败: %v", err)
	} else {
		defer feedRows.Close()
		for feedRows.Next() {
			var feedID int64
			var typ int
			var detail string
			var t TimeWithTimezone
			if err := feedRows.Scan(&feedID, &typ, &detail, &t); err != nil {
				logger.Printf("解析feed数据失败: %v", err)
				continue
			} else {
				lastFeedID = feedID
			}
		}
	}

	// 数据更新和心跳检测循环
	pingTicker := time.NewTicker(15 * time.Second) // 缩短ping间隔至15秒
	// Ensure non-negative interval with default fallback
	interval := liveEventsData.BroadcastInterval

	updateTicker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	defer func() {
		pingTicker.Stop()
		updateTicker.Stop()
	}()

	for {
		select {
		case <-pingTicker.C:
			// 发送ping帧并等待pong响应
			if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(5*time.Second)); err != nil {
				logger.Printf("Ping错误: %v，连接已停止，正在关闭连接", err)
				conn.Close()
				return
			}
			// 设置pong处理
			conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			conn.SetPongHandler(func(string) error {
				conn.SetReadDeadline(time.Now().Add(30 * time.Second))
				return nil
			})
		case <-updateTicker.C:
			// 使用goroutine异步处理数据查询和发送，避免阻塞ticker
			go func() {
				// 增量查询新的session feed数据
				var newFeeds []struct {
					feedType  uint8
					detail    string
					timestamp TimeWithTimezone
				}
				if lastFeedID > 0 {
					feedRows, err := db.Query(`SELECT type, detail, CAST(UNIX_TIMESTAMP(time) * 1000000 AS SIGNED) AS time FROM session_feed WHERE session_id = ? AND session_feed_id > ? ORDER BY session_feed_id ASC`, latestSessionID, lastFeedID)
					if err == nil {
						defer feedRows.Close()
						for feedRows.Next() {
							var typ int
							var detail string
							var t TimeWithTimezone
							if err := feedRows.Scan(&typ, &detail, &t); err == nil {
								newFeeds = append(newFeeds, struct {
									feedType  uint8
									detail    string
									timestamp TimeWithTimezone
								}{uint8(typ), detail, TimeWithTimezone(t)})
							}
						}
						// 更新最后一个feed_id
						if len(newFeeds) > 0 {
							lastFeedIDQuery := db.QueryRow(`SELECT MAX(session_feed_id) FROM session_feed WHERE session_id = ?`, latestSessionID)
							lastFeedIDQuery.Scan(&lastFeedID)
						}
					}
				}
				if err != nil {
					logger.Printf("查询feed更新数据失败: %v", err)
					return
				}

				// 解析排行榜数据
				entries := liveEventsData.Entries

				// 从池获取缓冲区
				buf := bufferPool.Get().(*bytes.Buffer)
				defer bufferPool.Put(buf)

				// 构建WebSocket消息
				if err := buildWebSocketMessage(liveEventsData, buf); err != nil {
					logger.Printf("构建消息失败: %v", err)
					return
				}

				// 写入条目数量
				binary.Write(buf, binary.LittleEndian, uint8(len(entries)))

				// 复制carUpdates数据
				carUpdatesMutex.Lock()
				copiedCarUpdates := make(map[int64]udp.CarUpdate)
				for k, v := range carUpdates {
					copiedCarUpdates[k] = v
				}
				carUpdatesMutex.Unlock()

				if err := writeEntriesToBuffer(entries, buf); err != nil {
					logger.Printf("写入条目数据失败: %v", err)
					return
				}

				// 写入feed数量
				binary.Write(buf, binary.LittleEndian, uint8(len(newFeeds)))
				// 写入每个feed
				for _, f := range newFeeds {
					binary.Write(buf, binary.LittleEndian, f.feedType)
					binary.Write(buf, binary.LittleEndian, f.timestamp)
					binary.Write(buf, binary.LittleEndian, uint16(len(f.detail)))
					buf.Write([]byte(f.detail))
				}

				// 写入结束标识
				binary.Write(buf, binary.LittleEndian, uint8(0))

				// 发送消息
				if err := conn.WriteMessage(websocket.BinaryMessage, buf.Bytes()); err != nil {
					logger.Printf("发送更新数据失败: %v", err)
				}
			}()
		}
	}
}

// 从WebSocket接收的事件数据中获取活跃事件
func getLiveEventsFromWebSocket() (WebSocketMessage, error) {
	liveEventsMutex.Lock()
	defer liveEventsMutex.Unlock()
	return liveEventsData, nil
}
