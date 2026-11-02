// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

// Live 排行榜投影：把已恢复/已提交的内存状态（连接表、日志分段、已提交 UDP 历史、恢复基线）
// 折算成原版 v10 帧的 entry，并生成 SectorMask。
// 只读已提交数据：未提交的待处理圈不参与，避免发布尚未落库的统计。
// 证据：原版 live.js LeaderBoardDeserialiser（sectorMask 低 2 位=上一完成圈状态，
// 位 2-3/4-5/6-7 分别为 S3/S2/S1 分段状态）与 udp-injection-result.json。

import (
	"math"

	"gosimview/udp"
)

// 分段/圈状态颜色取值（原版 live.js LapAndSectorStatus）：0 黄、1 绿、2 紫，低 2 位另用 3 表示红。
const (
	sectorYellow = 0 // 无最佳或分段缺失
	sectorGreen  = 1 // 个人最佳
	sectorPurple = 2 // 同类最佳
	sectorRed    = 3 // 仅上一完成圈：无效圈
)

// lapSectors 是一个已完成的圈及其同圈分段；valid 表示可用于最快圈统计。
type lapSectors struct {
	time       int
	segments   [3]int
	cuts       int
	crashes    int
	carCrashes int
}

func (l lapSectors) valid() bool {
	return l.time > 0 && l.cuts == 0 && l.crashes == 0 && l.carCrashes == 0
}

// slotProjection 是单个槽位的投影结果，供第二遍按车组汇总后计算 SectorMask。
type slotProjection struct {
	entry       Entry
	carClass    string
	bestLapTime int    // 个人最快圈时间，用于同类比较
	bestSector  [3]int // 个人最佳分段（各段独立最优，仅统计已提交圈）
	lastLap     lapSectors
	hasLastLap  bool
	displayed   [3]int // 参与上色的分段：Race=当前日志圈，Quali/Practice=最快圈同圈
}

// buildSlotProjection 汇总单车投影。调用者持有 stateMutex.RLock。
func buildSlotProjection(sessionID, slot int, conn *ConnectionInfo, update udp.CarUpdate, hasUpdate bool) *slotProjection {
	p := &slotProjection{entry: Entry{
		UserID:     int64(conn.UserID),
		TeamID:     int64(conn.TeamID),
		CarID:      uint16(conn.CarDBID),
		GameCarID:  slot,
		DriverName: conn.DriverName,
		StatusMask: (conn.TrackStatus << 1) | (conn.ConnectionStatus << 5),
		Gap:        -1,
		Interval:   -1,
	}}
	if car := globalCars[conn.CarDBID]; car != nil {
		p.carClass = car.CarClass
	}
	// 没有 CarUpdate 的加载中车辆保留零位置，不伪造轨迹。
	if hasUpdate {
		p.entry.NSP = update.NormalisedSplinePos
		p.entry.PosX = update.Pos.X
		p.entry.PosZ = update.Pos.Z
		p.entry.RPM = update.EngineRPM
		p.entry.Gear = update.Gear & 63
		speed := math.Sqrt(float64(update.Velocity.X*update.Velocity.X+update.Velocity.Y*update.Velocity.Y+update.Velocity.Z*update.Velocity.Z)) * 3.6
		if speed > 1023 {
			speed = 1023
		}
		if speed < 0 {
			speed = 0
		}
		p.entry.Speed = uint16(speed)
	}

	// 个人最快圈与个人最佳分段：只取已提交的单圈，绝不拼接不同圈的分段。
	var best lapSectors
	var bestSector [3]int
	hasBest := false
	consider := func(cand lapSectors) {
		if !cand.valid() {
			return
		}
		if !hasBest || cand.time < best.time {
			best, hasBest = cand, true
		}
		for i := 0; i < 3; i++ {
			if cand.segments[i] > 0 && (bestSector[i] == 0 || cand.segments[i] < bestSector[i]) {
				bestSector[i] = cand.segments[i]
			}
		}
	}

	var recoveredBest *lapSectors
	// 1) 日志恢复基线（接入前的历史成绩）
	if score := recoveredScores[slot]; score != nil {
		p.entry.Laps = uint16(score.Laps)
		p.entry.ValidLaps = uint16(score.ValidLaps)
		if score.Finished {
			p.entry.StatusMask |= 1
		}
		if score.BestTime > 0 {
			base := lapSectors{time: score.BestTime}
			if score.BestLap.Time == score.BestTime {
				base.segments = [3]int{score.BestLap.Sector1, score.BestLap.Sector2, score.BestLap.Sector3}
			}
			recoveredBest = &base
		}
		if score.LastTime > 0 {
			p.lastLap, p.hasLastLap = lapSectors{time: score.LastTime}, true
		}
	}
	// 历史回放圈：接入前已经完成的圈逐条参与个人最快圈/最佳分段。
	// 在线阶段不再写 Time，因此这里只会拿到回放历史，不会与已提交圈重复计入。
	historyLaps := 0
	for _, lap := range stintLapData[sessionID][slot] {
		if lap.Time <= 0 {
			continue
		}
		historyLaps++
		consider(lapSectors{time: lap.Time, segments: [3]int{lap.Sector1, lap.Sector2, lap.Sector3}, cuts: lap.Cuts, crashes: lap.Crashes, carCrashes: lap.CarCrashes})
	}
	// 恢复基线只在日志窗口内没有任何完成圈时兑底（官方 BEST 口径），
	// 已有完成历史时以历史圈自身为准，不再按圈时判重。
	if recoveredBest != nil && historyLaps == 0 {
		consider(*recoveredBest)
	}
	// 已提交 UDP 历史：每条圈只用自己的冻结字段。
	if values := carLapDataMap[slot]; len(values) > 0 {
		last := values[len(values)-1]
		if last.Completed != 0 {
			p.entry.StatusMask |= 1
		}
		for _, lap := range values {
			consider(lapSectors{time: lap.LapTime, segments: [3]int{lap.Sector1, lap.Sector2, lap.Sector3}, cuts: int(lap.Cuts), crashes: int(lap.Crashes), carCrashes: int(lap.CarCrashes)})
		}
		p.lastLap = lapSectors{time: last.LapTime, segments: [3]int{last.Sector1, last.Sector2, last.Sector3}, cuts: int(last.Cuts), crashes: int(last.Crashes), carCrashes: int(last.CarCrashes)}
		p.hasLastLap = true
	}

	// 最快圈字段只写这一条圈自己的三段；没有分段就是 0（页面据此不显示总圈时）。
	if hasBest {
		p.bestLapTime = best.time
		p.entry.BestLapTime = uint32(best.time)
		p.entry.BestLapS1 = uint32(maxZero(best.segments[0]))
		p.entry.BestLapS2 = uint32(maxZero(best.segments[1]))
		p.entry.BestLapS3 = uint32(maxZero(best.segments[2]))
	}
	p.bestSector = bestSector
	if p.hasLastLap {
		p.entry.LastLapTime = int32(p.lastLap.time)
	}

	// 当前圈（未被 UDP 完圈消费的尾记录）的分段：Race 展示用；Quali/Practice 用最快圈分段。
	// 在线阶段日志只写当前圈的 S1/S2，S3 在完圈时才由官方圈时推导，这里不伪造。
	if v := currentLapForDisplay(sessionID, slot); v != nil {
		p.entry.CurrentLapS1 = uint32(maxZero(v.Sector1))
		p.entry.CurrentLapS2 = uint32(maxZero(v.Sector2))
		p.entry.CurrentLapS3 = uint32(maxZero(v.Sector3))
	}
	if globalSession != nil && globalSession.Type == 2 {
		p.displayed = [3]int{int(p.entry.CurrentLapS1), int(p.entry.CurrentLapS2), int(p.entry.CurrentLapS3)}
	} else {
		p.displayed = best.segments
	}

	// Laps / ValidLaps 按本会话已提交的圈自己数，和原版一致：
	// 每完成一圈都算 laps（含无效圈），只有干净圈算 validLaps。
	// 不采用 UDP 报文里的 laps 字段：原版也不采用（实测报文写 57/3/3/0，
	// 帧内仍然 1/2/3/4），而且 AC 那边的字段口径与我们不一致。
	stintLaps, validLaps := 0, 0
	for _, stint := range sessionStints {
		if stint.GameCarID == slot && stint.UserID == conn.UserID {
			stintLaps += stint.Laps
			validLaps += stint.ValidLaps
		}
	}
	if uint16(stintLaps) > p.entry.Laps {
		p.entry.Laps = uint16(stintLaps)
	}
	if uint16(validLaps) > p.entry.ValidLaps {
		p.entry.ValidLaps = uint16(validLaps)
	}
	if score := recoveredScores[slot]; score != nil {
		known := false
		for _, stint := range sessionStints {
			if stint.GameCarID == slot && stint.UserID == conn.UserID && restoredStintIDs[stint.ID] {
				known = true
				break
			}
		}
		if !known {
			p.entry.Laps = uint16(score.Laps + stintLaps)
			p.entry.ValidLaps = uint16(score.ValidLaps + validLaps)
		}
	}
	p.entry.Tyre = userTyres[conn.DriverName]
	p.entry.TyreLength = uint8(len(p.entry.Tyre))
	return p
}

// applySectorMasks 第二遍：按 carClass 汇总同类全局最快圈与分段，再写每条 entry 的 SectorMask。
// 低位是上一完成圈状态，高位是展示分段的个人/同类最佳比较（完成圈状态与当前分段状态分开）。
func applySectorMasks(projections []*slotProjection) {
	classBestLap := make(map[string]int)
	classBestSector := make(map[string][3]int)
	for _, p := range projections {
		if p.bestLapTime > 0 {
			if cur, ok := classBestLap[p.carClass]; !ok || p.bestLapTime < cur {
				classBestLap[p.carClass] = p.bestLapTime
			}
		}
		agg := classBestSector[p.carClass]
		for i := 0; i < 3; i++ {
			if p.bestSector[i] > 0 && (agg[i] == 0 || p.bestSector[i] < agg[i]) {
				agg[i] = p.bestSector[i]
			}
		}
		classBestSector[p.carClass] = agg
	}
	for _, p := range projections {
		status := sectorYellow
		if p.hasLastLap {
			status = lastLapStatus(p.lastLap, p.bestLapTime, classBestLap[p.carClass])
		}
		best := classBestSector[p.carClass]
		mask := uint8(status)
		mask |= uint8(sectorStatus(p.displayed[2], p.bestSector[2], best[2])) << 2 // S3
		mask |= uint8(sectorStatus(p.displayed[1], p.bestSector[1], best[1])) << 4 // S2
		mask |= uint8(sectorStatus(p.displayed[0], p.bestSector[0], best[0])) << 6 // S1
		p.entry.SectorMask = mask
	}
}

// lastLapStatus 判定上一完成圈的状态：无效 3 / 同类最快 2 / 个人最快 1 / 否则 0。
func lastLapStatus(last lapSectors, personalBest, classBest int) int {
	if !last.valid() {
		return sectorRed
	}
	if classBest > 0 && last.time == classBest {
		return sectorPurple
	}
	if personalBest > 0 && last.time == personalBest {
		return sectorGreen
	}
	return sectorYellow
}

// sectorStatus 比较展示分段与同类/个人最佳分段：同类最快 2 / 个人最快 1 / 缺失或未刷新 0。
func sectorStatus(displayed, personalBest, classBest int) int {
	if displayed <= 0 {
		return sectorYellow
	}
	if classBest > 0 && displayed <= classBest {
		return sectorPurple
	}
	if personalBest > 0 && displayed <= personalBest {
		return sectorGreen
	}
	return sectorYellow
}
