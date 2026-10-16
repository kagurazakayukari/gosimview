// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
// See LICENSE.md for license information.

package main

import (
	"math"
	"time"

	"gosimview/udp"
)

// 用户指定的传送候选条件：连续 UDP 包间移动超过 5 米，移动后连续两帧速度近零。
// 与维修区几何无关；缺帧太久只重建基线，不能用累计位移证明传送。
const positionJumpM = 5.0

type positionObservation struct {
	x, z          float32
	at            time.Time
	pending       bool
	stoppedFrames int
	teleported    bool // 仅确认当帧为 true，避免重复发事件
}

var positionObservations = make(map[int]*positionObservation)

func positionSampleLimit() time.Duration {
	interval := 200 * time.Millisecond
	if cfg != nil && cfg.ACServer.UDP.Realtime.Update.Interval.MS > 0 {
		interval = time.Duration(cfg.ACServer.UDP.Realtime.Update.Interval.MS) * time.Millisecond
	}
	return 3 * interval
}

// observePositionJump 只识别 teleport feed 的触发条件，不修改计时点或遥测缓存。
// 返回 true 仅表示本帧确认传送，调用者持有 stateMutex。
func observePositionJump(update udp.CarUpdate, now time.Time) bool {
	slot := int(update.CarID)
	if conn := globalConnections[slot]; conn == nil || conn.UserID == 0 || conn.CarDBID == 0 {
		delete(positionObservations, slot)
		return false
	}
	x, z := float64(update.Pos.X), float64(update.Pos.Z)
	speed := math.Sqrt(float64(update.Velocity.X)*float64(update.Velocity.X) +
		float64(update.Velocity.Y)*float64(update.Velocity.Y) + float64(update.Velocity.Z)*float64(update.Velocity.Z))
	if math.IsNaN(x) || math.IsInf(x, 0) || math.IsNaN(z) || math.IsInf(z, 0) || math.IsNaN(speed) || math.IsInf(speed, 0) {
		delete(positionObservations, slot)
		return false
	}
	previous := positionObservations[slot]
	current := &positionObservation{x: update.Pos.X, z: update.Pos.Z, at: now}
	positionObservations[slot] = current
	if previous == nil {
		return false
	}
	dt := now.Sub(previous.at)
	if dt <= 0 || dt > positionSampleLimit() {
		return false
	}
	dx, dz := x-float64(previous.x), z-float64(previous.z)
	jump := dx*dx+dz*dz > positionJumpM*positionJumpM
	stopped := speed <= pitStoppedSpeedMS
	if jump {
		// 跳变包可能仍带落地前的速度；由后续连续静止包确认。
		current.pending = true
		return false
	}
	if previous.pending {
		if stopped {
			current.pending = true
			current.stoppedFrames = previous.stoppedFrames + 1
			if current.stoppedFrames >= 2 {
				current.teleported = true
				current.pending = false
			}
		} else {
			// 停止序列被打断，等待下一次新的位置突变候选。
			current.pending = false
			current.stoppedFrames = 0
		}
	}
	return current.teleported
}
