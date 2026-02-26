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

// calculateMaxSpeed 计算单圈最大速度
func calculateMaxSpeed(laps []udp.CarUpdate) float64 {
	var maxSpeed float64 = 0
	for _, lap := range laps {
		// 将Velocity从m/s转换为km/h
		// 计算速度向量模长(m/s)并转换为km/h
		velocityMagnitude := math.Sqrt(float64(lap.Velocity.X*lap.Velocity.X + lap.Velocity.Y*lap.Velocity.Y + lap.Velocity.Z*lap.Velocity.Z))
		currentSpeed := velocityMagnitude * 3.6
		if currentSpeed > maxSpeed {
			maxSpeed = currentSpeed
		}
	}
	return maxSpeed
}

// generateLapTelemetry 创建符合LapTelemetry格式的二进制文件
func generateLapTelemetry(carID int, trackLength int32, telemetryData []udp.CarUpdate) ([]byte, error) {
	// 使用内存缓冲区替代文件存储
	var buffer bytes.Buffer

	// 写入版本号 (1)，小端序
	version := int32(1)
	if err := binary.Write(&buffer, binary.LittleEndian, version); err != nil {
		return nil, fmt.Errorf("写入版本号失败: %v", err)
	}

	// 写入赛道长度，使用小端序
	length := trackLength
	if err := binary.Write(&buffer, binary.LittleEndian, length); err != nil {
		return nil, fmt.Errorf("写入赛道长度失败: %v", err)
	}

	// 写入每条遥测记录
	for _, data := range telemetryData {
		// 写入NSP (NormalisedSplinePos) - float32
		if err := binary.Write(&buffer, binary.LittleEndian, data.NormalisedSplinePos); err != nil {
			return nil, fmt.Errorf("写入NSP失败: %v", err)
		}

		// 写入档位 (加1后存储，与前端解析对应)
		gear := uint8(data.Gear + 1)
		if err := binary.Write(&buffer, binary.LittleEndian, gear); err != nil {
			return nil, fmt.Errorf("写入档位失败: %v", err)
		}

		// 写入速度 - float32
		if err := binary.Write(&buffer, binary.LittleEndian, data.Velocity); err != nil {
			return nil, fmt.Errorf("写入速度失败: %v", err)
		}

		// 写入X坐标 - float32
		if err := binary.Write(&buffer, binary.LittleEndian, data.Pos.X); err != nil {
			return nil, fmt.Errorf("写入X坐标失败: %v", err)
		}

		// 写入Y坐标 - float32
		if err := binary.Write(&buffer, binary.LittleEndian, data.Pos.Y); err != nil {
			return nil, fmt.Errorf("写入Y坐标失败: %v", err)
		}

		// 写入Z坐标 - float32
		if err := binary.Write(&buffer, binary.LittleEndian, data.Pos.Z); err != nil {
			return nil, fmt.Errorf("写入Z坐标失败: %v", err)
		}

		// 写入转速 - float32
		if err := binary.Write(&buffer, binary.LittleEndian, float32(data.EngineRPM)); err != nil {
			return nil, fmt.Errorf("写入转速失败: %v", err)
		}
	}

	return buffer.Bytes(), nil
}

// calculateAverageSpeed 计算平均速度
func calculateAverageSpeed(laps []udp.CarUpdate) float64 {
	if len(laps) == 0 {
		return 0
	}

	var totalSpeed float64 = 0
	for _, lap := range laps {
		// 计算速度向量模长(m/s)并转换为km/h
		velocityMagnitude := math.Sqrt(float64(lap.Velocity.X*lap.Velocity.X + lap.Velocity.Y*lap.Velocity.Y + lap.Velocity.Z*lap.Velocity.Z))
		totalSpeed += velocityMagnitude * 3.6
	}

	return totalSpeed / float64(len(laps))
}
