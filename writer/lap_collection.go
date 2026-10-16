// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
// See LICENSE.md for license information.

package main

import (
	"time"

	"gosimview/udp"
)

// 完圈报文没有唯一序号。仅拦截相同内容在一个圈时之内的重复报文；
// 下一次跑出相同圈时和相同官方圈数（例如连续无效圈）仍能正常保存。
// 这里不使用接入前的排行榜圈数作为新事件过滤条件。
type lapCompletion struct {
	Time       uint32
	Cuts       uint8
	Laps       uint16
	Completed  uint8
	ReceivedAt time.Time
}

var lastLapCompletions = make(map[int]lapCompletion)

func acceptLapCompletion(msg udp.LapCompleted, car udp.LapCompletedCar, now time.Time) bool {
	if msg.LapTime == 0 {
		return false
	}
	slot := int(msg.CarID)
	previous, exists := lastLapCompletions[slot]
	if exists && previous.Time == msg.LapTime && previous.Cuts == msg.Cuts &&
		previous.Laps == car.Laps && previous.Completed == car.Completed &&
		now.Sub(previous.ReceivedAt) < time.Duration(msg.LapTime)*time.Millisecond {
		return false
	}
	return true
}

// collectLapSample 只缓存完圈之后的新圈；最新车辆状态另由 Live Timing 使用。
// 调用者持有 stateMutex，避免会话切换和车辆重连混入旧样本。
func collectLapSample(update udp.CarUpdate) {
	slot := int(update.CarID)
	if !lapTelemetryReady[slot] {
		return
	}
	lapDataMutex.Lock()
	defer lapDataMutex.Unlock()
	if len(carLapData[slot]) >= 100000 {
		// 超限后整圈作废，不能把剩余半圈当完整遥测写入。
		delete(carLapData, slot)
		lapTelemetryReady[slot] = false
		logger.Printf("车辆 %d 圈遥测样本超限，等待下一次完圈重新采集", slot)
		return
	}
	carLapData[slot] = append(carLapData[slot], update)
}

// completeLapSamples 使用官方完圈事件划分圈，不用稍后到达的 NSP 跨线更新。
// 中途接入的第一次完圈仅开启采集，不能保存接入前已开始的半圈。
func completeLapSamples(slot int) []udp.CarUpdate {
	lapDataMutex.Lock()
	defer lapDataMutex.Unlock()
	var samples []udp.CarUpdate
	if lapTelemetryReady[slot] {
		samples = carLapData[slot]
	}
	delete(carLapData, slot)
	lapTelemetryReady[slot] = true
	return samples
}

// resetLapSamples 在断开、重连时丢弃未完成圈，防止跨车手或跨连接拼接遥测。
func resetLapSamples(slot int) {
	resetRaceObservations(slot)
	lapDataMutex.Lock()
	defer lapDataMutex.Unlock()
	delete(carLapData, slot)
	delete(lapTelemetryReady, slot)
	delete(lastLapCompletions, slot)
}
