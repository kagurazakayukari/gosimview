// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
// See LICENSE.md for license information.

package main

import (
	"math"
	"time"

	"gosimview/udp"
)

// 判停只容忍速度浮点抖动，不以停留时长或轮胎配方猜测进站用途。
const pitStoppedSpeedMS = 0.1

// hasPitDetection 统一分段条件：KN5 或 AI 判据可用时，以区内停车为准；
// 两者都不可用才允许日志换胎分段。
func hasPitDetection() bool {
	return len(pitBoundary) >= 3 || pitLaneFallback != nil
}

type pitObservation struct {
	inPit         bool
	stopped       bool
	split         bool
	resumePending bool
	entryTime     time.Time
}

var pitObservations = make(map[int]*pitObservation)

// observePitVisit 由持有 stateMutex 的车辆更新处理器调用。
// 驶过维修区不分段；观察到区内停车时结束旧段，同次访问只结束一次，起步时建立新段。
// 第一个采样只建立基线，不能将接入前的停站冒充本次新事件。
func observePitVisit(slot int, inPit bool, update udp.CarUpdate, now time.Time) {
	conn := globalConnections[slot]
	if conn == nil {
		return
	}
	speed := math.Sqrt(float64(update.Velocity.X)*float64(update.Velocity.X) +
		float64(update.Velocity.Y)*float64(update.Velocity.Y) + float64(update.Velocity.Z)*float64(update.Velocity.Z))
	if math.IsNaN(speed) || math.IsInf(speed, 0) {
		return
	}
	stopped := speed <= pitStoppedSpeedMS
	obs := pitObservations[slot]
	if obs == nil {
		pitObservations[slot] = &pitObservation{inPit: inPit, stopped: stopped}
		if inPit {
			conn.TrackStatus = 3
		} else {
			conn.TrackStatus = 0
		}
		return
	}
	entered := inPit && !obs.inPit
	if entered {
		obs.inPit = true
		obs.stopped = false
		obs.entryTime = now
		obs.split = false
		conn.PitVisits = append(conn.PitVisits, PitVisit{EntryTime: now})
		conn.PitEntryTime = now
		// 传送候选由独立的连续 UDP 观测确认并发 type=15，不能先误报正常驶入。
		jump := positionObservations[slot]
		if jump == nil || !jump.pending || !stopped {
			insertPitFeed(slot, 11, nil, now)
		}
	}
	if inPit && stopped && !obs.split && (entered || !obs.stopped) {
		if err := finishStint(slot); err != nil {
			return // 写库未成功，不吞掉停车跃迁，下一条静止采样继续处理。
		}
		obs.split = true
		obs.resumePending = true
		conn.StintStartedAt = time.Time{}
		// 停车跨越了分段边界，站内当前半圈不能保存为下一整圈遥测。
		resetLapSamples(slot)
	}
	if obs.resumePending && !stopped {
		if conn.StintStartedAt.IsZero() {
			conn.StintStartedAt = now
		}
		if _, err := ensureCurrentStint(slot, now); err != nil {
			logger.Printf("进站停车后建立新 stint 失败(slot=%d): %v", slot, err)
			return
		}
		obs.resumePending = false
	}
	if !inPit && obs.inPit {
		if !obs.entryTime.IsZero() {
			for i := len(conn.PitVisits) - 1; i >= 0; i-- {
				if conn.PitVisits[i].ExitTime.IsZero() {
					conn.PitVisits[i].ExitTime = now
					break
				}
			}
			insertPitFeed(slot, 12, map[string]interface{}{"pit_time": now.Sub(obs.entryTime).Milliseconds()}, now)
		}
		conn.PitEntryTime = time.Time{}
		obs.entryTime = time.Time{}
		conn.TrackStatus = 4
	} else if inPit {
		if stopped {
			conn.TrackStatus = 3
		} else {
			conn.TrackStatus = 2
		}
	} else {
		conn.TrackStatus = 0
	}
	obs.inPit, obs.stopped = inPit, stopped
}

func insertPitFeed(slot, typ int, detail map[string]interface{}, now time.Time) {
	if err := globalDBWriter.InsertSessionFeed(&SessionFeed{SessionID: globalSession.ID, Type: typ, Detail: feedDetail(slot, detail), Time: now}); err != nil {
		logger.Printf("保存维修区事件失败(slot=%d, type=%d): %v", slot, typ, err)
	}
}
