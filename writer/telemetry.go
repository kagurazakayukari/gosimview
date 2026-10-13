// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"gosimview/udp"
)

// 遥测二进制布局（必须与 simview/html/js/analysis.js 的解析一致）：
//
//	头部 8 字节：version(int32) + trackLength(int32)
//	每条记录 17 字节：nsp(float32) + gear(uint8) + speed(float32) + x(float32) + z(float32)
//
// 不写 Y 坐标、不写转速；速度单位为 km/h。
const (
	lapTelemetryVersion = int32(1)
	lapTelemetryHeader  = 8  // version + trackLength
	lapTelemetryRecord  = 17 // nsp + gear + speed + x + z
)

// velocityKMH 由速度向量模长(m/s)换算为 km/h
func velocityKMH(v udp.Vec) float64 {
	magnitude := math.Sqrt(float64(v.X*v.X + v.Y*v.Y + v.Z*v.Z))
	return magnitude * 3.6
}

// calculateMaxSpeed 计算单圈最大速度（km/h）
func calculateMaxSpeed(laps []udp.CarUpdate) float64 {
	var maxSpeed float64 = 0
	for _, lap := range laps {
		currentSpeed := velocityKMH(lap.Velocity)
		if currentSpeed > maxSpeed {
			maxSpeed = currentSpeed
		}
	}
	return maxSpeed
}

// calculateAverageSpeed 计算平均速度（km/h）
func calculateAverageSpeed(laps []udp.CarUpdate) float64 {
	if len(laps) == 0 {
		return 0
	}

	var totalSpeed float64 = 0
	for _, lap := range laps {
		totalSpeed += velocityKMH(lap.Velocity)
	}

	return totalSpeed / float64(len(laps))
}

// generateLapTelemetry 生成与前端分析页一致的圈速遥测二进制
func generateLapTelemetry(carID int, trackLength int32, telemetryData []udp.CarUpdate) ([]byte, error) {
	// 预分配：头部 + 每条记录
	buffer := bytes.NewBuffer(make([]byte, 0, lapTelemetryHeader+lapTelemetryRecord*len(telemetryData)))

	// 写入版本号 (1)，小端序
	if err := binary.Write(buffer, binary.LittleEndian, lapTelemetryVersion); err != nil {
		return nil, fmt.Errorf("写入版本号失败: %v", err)
	}

	// 写入赛道长度（米），小端序
	if err := binary.Write(buffer, binary.LittleEndian, trackLength); err != nil {
		return nil, fmt.Errorf("写入赛道长度失败: %v", err)
	}

	for _, data := range telemetryData {
		// NSP (NormalisedSplinePos) - float32
		if err := binary.Write(buffer, binary.LittleEndian, data.NormalisedSplinePos); err != nil {
			return nil, fmt.Errorf("写入NSP失败: %v", err)
		}

		// 档位 - uint8（加1后存储，0 表示无效档位，与前端解析对应）
		if err := binary.Write(buffer, binary.LittleEndian, uint8(data.Gear+1)); err != nil {
			return nil, fmt.Errorf("写入档位失败: %v", err)
		}

		// 速度 - float32（km/h）
		if err := binary.Write(buffer, binary.LittleEndian, float32(velocityKMH(data.Velocity))); err != nil {
			return nil, fmt.Errorf("写入速度失败: %v", err)
		}

		// X 坐标 - float32
		if err := binary.Write(buffer, binary.LittleEndian, data.Pos.X); err != nil {
			return nil, fmt.Errorf("写入X坐标失败: %v", err)
		}

		// Z 坐标 - float32
		if err := binary.Write(buffer, binary.LittleEndian, data.Pos.Z); err != nil {
			return nil, fmt.Errorf("写入Z坐标失败: %v", err)
		}
	}

	return buffer.Bytes(), nil
}
