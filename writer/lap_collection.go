// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
// See LICENSE.md for license information.

package main

import "gosimview/udp"

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
	lapDataMutex.Lock()
	defer lapDataMutex.Unlock()
	delete(carLapData, slot)
	delete(lapTelemetryReady, slot)
}
