// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

// 待处理圈队列：官方完圈一旦被观测，就不因为日志临时读不到或核心事务失败而丢失。
// 每条记录冻结观测到的会话、身份、时间、整圈样本与绑定的段对象；入队后采集继续下一圈，
// 提交成功前不改动段统计与内存计数。重试只用冻结的观测与本条样本，不再切当前采集缓存，
// 也不依赖 UDP 重发。队列与其它 writer 状态共用 stateMutex，由会话处理器、结束路径与后台重试协程串行驱动。

import (
	"context"
	"fmt"
	"time"

	"gosimview/udp"
)

// pendingLap 是一条已观测但尚未落库的完整圈。
type pendingLap struct {
	stint       *SessionStint   // 绑定的段对象，可能是尚未落库的 ID0 占位段
	sessionID   int64           // 观测时所属会话
	slot        int             // UDP 槽位，仅用于日志与遥测
	record      StintLap        // 观测到的整圈；StintID 在提交时按段 ID 填充
	carLap      CarLapData      // 提交成功后并入 carLapDataMap
	samples     []udp.CarUpdate // 整圈遥测样本，重试复用
	telemetryOn bool            // 当前事件是否启用圈遥测
	trackLength int32           // 观测时赛道长度，重试不读取后续赛事缓存
	carClass    string          // 观测时的车辆分级，用于组别最快圈判定
	feedDetail  string          // 冻结的 feed detail（含观测时的身份）
	timestamp   time.Time
	needsLog    bool // 观测时日志读取失败，提交前须补齐日志分段
}

var pendingLaps []*pendingLap

// enqueuePendingLap 追加一条待处理圈。调用者持有 stateMutex。
func enqueuePendingLap(entry *pendingLap) {
	pendingLaps = append(pendingLaps, entry)
}

// flushPendingLaps 按入队顺序提交待处理圈，遇到失败即停止并保留其余条目。
// 调用者持有 stateMutex。返回 nil 表示队列已清空。
func flushPendingLaps() error {
	for len(pendingLaps) > 0 {
		entry := pendingLaps[0]
		if entry.needsLog {
			// 日志尚未恢复：重读增量日志，成功后再补分段；仍失败则整队保留等下次重试。
			if err := collectCarUpdates(); err != nil {
				return fmt.Errorf("待处理圈等待日志恢复: %w", err)
			}
			supplementPendingLapFromLog(entry)
			if entry.needsLog {
				// 官方完圈优先落库；日志只有补充分段/碰撞，缺失时不阻塞成绩。
				logger.Printf("待处理圈未匹配到日志分段，按官方完圈保存(slot=%d time=%d)", entry.slot, entry.record.Time)
				entry.needsLog = false
			}
		}
		if err := submitPendingLap(entry); err != nil {
			return err
		}
		pendingLaps = pendingLaps[1:]
	}
	return nil
}

// submitPendingLap 先把绑定的段落库，再原子提交单圈与统计，最后写辅助数据并发布内存。
// 段已结束时仍可补旧圈：updateFinishedAt=false，保留真实结束区间。
// 调用者持有 stateMutex。
func submitPendingLap(entry *pendingLap) error {
	if err := persistStint(entry.stint); err != nil {
		return fmt.Errorf("待处理圈的段尚未落库(slot=%d): %w", entry.slot, err)
	}
	record := entry.record
	record.StintID = entry.stint.ID
	valid := record.Cuts == 0 && record.Crashes == 0 && record.CarCrashes == 0 && record.Time > 0
	updated := *entry.stint
	updated.Laps++
	if updated.IsFinished == 0 {
		updated.FinishedAt = entry.timestamp
	}
	// 最快圈与组别最快圈在提交时按已落库状态重算，避免同段多条待处理圈互相覆盖。
	newBest := valid && (entry.stint.BestLapID == 0 || record.Time < bestLapTimes[entry.stint.ID])
	newClassBest := valid && (classBestTimes[entry.carClass] == 0 || record.Time < classBestTimes[entry.carClass])
	if valid {
		updated.ValidLaps++
		if newBest {
			updated.BestLapID = sentinelNewBestLapID
		}
	}
	lapID, err := globalDBWriter.PersistLap(&record, &updated, entry.stint.IsFinished == 0)
	if err != nil {
		return fmt.Errorf("提交单圈失败(slot=%d time=%d): %w", entry.slot, record.Time, err)
	}
	*entry.stint = updated
	// 核心已提交：以下辅助数据与内存发布失败只记录，不回滚成绩。
	if newBest {
		bestLapTimes[entry.stint.ID] = record.Time
		if err := globalDBWriter.InsertSessionFeed(&SessionFeed{SessionID: entry.sessionID, Type: 4, Detail: entry.feedDetail, Time: entry.timestamp}); err != nil {
			logger.Printf("写入最快圈消息失败(slot=%d): %v", entry.slot, err)
		}
	}
	if newClassBest {
		classBestTimes[entry.carClass] = record.Time
		if err := globalDBWriter.InsertSessionFeed(&SessionFeed{SessionID: entry.sessionID, Type: 5, Detail: entry.feedDetail, Time: entry.timestamp}); err != nil {
			logger.Printf("写入组别最快圈消息失败(slot=%d): %v", entry.slot, err)
		}
	}
	if entry.telemetryOn && valid && len(entry.samples) > 0 {
		telemetry, err := generateLapTelemetry(entry.slot, entry.trackLength, entry.samples)
		if err != nil {
			logger.Printf("生成单圈遥测失败，单圈已保存(slot=%d): %v", entry.slot, err)
		} else if _, err := globalDBWriter.CreateLapTelemetry(&LapTelemetry{LapID: int64(lapID), Telemetry: telemetry}); err != nil {
			logger.Printf("写入单圈遥测失败，单圈已保存(slot=%d): %v", entry.slot, err)
		}
	}
	carLapDataMap[entry.slot] = append(carLapDataMap[entry.slot], entry.carLap)
	if globalSession != nil && globalSession.ID == entry.sessionID {
		if globalSession.LastActivity.Before(entry.timestamp) {
			globalSession.LastActivity = entry.timestamp
		}
		if err := globalDBWriter.UpdateSessionGrip(entry.sessionID, entry.record.Grip); err != nil {
			logger.Printf("更新圈抓地力失败，成绩已保存(session=%d): %v", entry.sessionID, err)
		}
	}
	return nil
}

// supplementPendingLapFromLog 用已读到内存的日志分段补齐待处理圈的分段/轮胎/碰撞。
// 只按冻结的槽位与圈时匹配，不重新依赖 globalConnections 归属。调用者持有 stateMutex。
func supplementPendingLapFromLog(entry *pendingLap) {
	values := stintLapData[int(entry.sessionID)][entry.slot]
	matched := false
	for i := len(values) - 1; i >= 0; i-- {
		v := values[i]
		if v.Time <= 0 || v.Time != entry.record.Time {
			continue
		}
		entry.record.Sector1, entry.record.Sector2, entry.record.Sector3 = v.Sector1, v.Sector2, v.Sector3
		entry.record.Crashes, entry.record.CarCrashes = v.Crashes, v.CarCrashes
		entry.carLap.Sector1, entry.carLap.Sector2, entry.carLap.Sector3 = v.Sector1, v.Sector2, v.Sector3
		entry.carLap.Crashes, entry.carLap.CarCrashes = uint8(v.Crashes), uint8(v.CarCrashes)
		if v.Tyre != "" {
			entry.record.Tyre = v.Tyre
			entry.carLap.Tyre = v.Tyre
		}
		matched = true
		break
	}
	if matched {
		entry.needsLog = false
	}
}

// startPendingLapRetry 周期性在后台重试待处理圈，不依赖 UDP 重发或探活节奏。
// 与采集/探活共用 stateMutex，避免和车辆事件并发改动会话状态。
func startPendingLapRetry(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				stateMutex.Lock()
				if err := flushPendingLaps(); err != nil {
					logger.Printf("后台重试待处理圈失败: %v", err)
				}
				stateMutex.Unlock()
			}
		}
	}()
	return done
}
