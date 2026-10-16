// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

// Race Gap：20 个等距 NSP 计时点（.00,.05,...,.95），记录每车首次被观测越过该点的时刻。
// 只用连续前向采样，不插值、不外推速度。圈号只由官方圈数（board/recovered/已提交圈）派生，
// NSP 回卷不自行写回官方圈：过线按 lap+1 记账，官方追平后一致；官方先到时按 lap-1 直到观测到过线。
// 起跑线顺序不明（官方未追平）时丢弃该次 point0/低侧点，宁可少一个点不算错。
// 观测状态与其它 writer 状态同受 stateMutex 保护，不查数据库。

import (
	"math"
	"time"

	"gosimview/udp"
)

const (
	raceCheckpointCount = 20
	raceCheckpointStep  = 1.0 / float64(raceCheckpointCount)
	raceMaxForwardNSP   = 0.10                    // 单次采样最大前向位移，超出视为大跳
	raceMaxJitterNSP    = 0.005                   // 允许的静止抖动，超出且非过线视为倒车
	raceMaxSampleGap    = 3 * time.Second         // 长时间缺帧阈值，超出重建基线
	raceMaxCrossings    = 3 * raceCheckpointCount // 每车历史略多于两圈，供落后车匹配 (lap,point)
)

// raceCrossing 是“某车在某圈的某计时点首次被观测越过”的记录。
type raceCrossing struct {
	lap   int
	point int
	at    time.Time
}

// carRaceTiming 是单车的计时观测状态。调用者持有 stateMutex。
type carRaceTiming struct {
	hasLast    bool
	lastNSP    float32
	lastAt     time.Time
	anchored   bool // 圈号是否已由官方圈数锚定
	drivingLap int  // 官方圈号对应的当前圈序号（1 起），只随官方圈数推进
	// 观测与官方的偏差：>0 表示观测到过线但官方还没追平，<0 表示官方已推进而过线还没观测到。
	skew      int
	crossings []raceCrossing
}

var raceTiming = make(map[int]*carRaceTiming)

// seedDrivingLap 用已提交的内存圈数锚定圈号；未知时返回 0（宁缺不猜，不查数据库）。
func seedDrivingLap(slot int) int {
	if values := carLapDataMap[slot]; len(values) > 0 {
		return int(values[len(values)-1].Laps) + 1
	}
	if score := recoveredScores[slot]; score != nil && score.Laps > 0 {
		return score.Laps + 1
	}
	return 0
}

// currentLap 是当前实际所处圈：叠加观测与官方的偏差，过线两侧不互相误判。
func (t *carRaceTiming) currentLap() int {
	return t.drivingLap + t.skew
}

// progress 是车辆的连续进度（圈，含圈内小数）。
func (t *carRaceTiming) progress() float64 {
	return float64(t.currentLap()) + float64(t.lastNSP)
}

// append 记录一个计时点；同一 (lap,point) 只保留首次越过，重跑/断点重连不覆盖最早时刻。
func (t *carRaceTiming) append(c raceCrossing) {
	for _, existing := range t.crossings {
		if existing.lap == c.lap && existing.point == c.point {
			return
		}
	}
	t.crossings = append(t.crossings, c)
	if len(t.crossings) > raceMaxCrossings {
		n := copy(t.crossings, t.crossings[len(t.crossings)-raceMaxCrossings:])
		t.crossings = t.crossings[:n]
	}
}

// recordSpan 记录 (from,to] 区间内的计时点到指定圈；未锚定圈号时不记，避免写出 lap 0 伪点。
func (t *carRaceTiming) recordSpan(from, to float64, lap int, at time.Time) {
	if !t.anchored {
		return
	}
	for p := 0; p < raceCheckpointCount; p++ {
		point := float64(p) * raceCheckpointStep
		if from < point && point <= to {
			t.append(raceCrossing{lap: lap, point: p, at: at})
		}
	}
}

// recordWrap 处理一次有效过线：高侧点归刚跑完的那圈。只有官方已经为这次过线推进（偏差归零）时，
// 起跑线与低侧点才归属明确、可以记；否则丢弃本次低侧点，等官方追平后按新 baseline 继续。
func (t *carRaceTiming) recordWrap(from, to float32, now time.Time) {
	if !t.anchored {
		return
	}
	t.recordSpan(float64(from), 1, t.currentLap(), now)
	t.skew++
	if t.skew > 1 {
		// 连续两次回卷都没有官方确认，圈号不可再用于跨车比较。
		t.discardObservations()
		return
	}
	if t.skew == 0 {
		t.recordSpan(-1, float64(to), t.currentLap(), now)
	}
}

// discardObservations 传送、倒车或缺帧后重建位置计时，保留已知官方圈号。
// 旧计时点不得跨越位置中断继续配对，下一有效位置只建立基线。
func (t *carRaceTiming) discardObservations() {
	t.crossings = nil
	t.hasLast = false
	t.skew = 0
}

// observeRacePosition 观测一帧车辆位置，识别过点。调用者持有 stateMutex。
func observeRacePosition(update udp.CarUpdate, now time.Time) {
	if globalSession == nil || globalSession.Type != 2 {
		return
	}
	slot := int(update.CarID)
	conn := globalConnections[slot]
	if conn == nil || conn.UserID == 0 {
		return
	}
	nsp := update.NormalisedSplinePos
	if math.IsNaN(float64(nsp)) || math.IsInf(float64(nsp), 0) || nsp < 0 || nsp > 1 {
		resetRaceObservations(slot)
		return
	}
	t := raceTiming[slot]
	if t == nil {
		t = &carRaceTiming{}
		raceTiming[slot] = t
	}
	if !t.anchored && !t.hasLast {
		if lap := seedDrivingLap(slot); lap > 0 {
			t.drivingLap, t.anchored = lap, true
		}
	}
	from := t.lastNSP
	if !t.hasLast {
		// 首个样本（可能落在边界）：只建基线，不伪造过点，也不清刚锚定的圈号。
		t.hasLast, t.lastNSP, t.lastAt = true, nsp, now
		return
	}
	if now.Sub(t.lastAt) > raceMaxSampleGap {
		// 长时间缺帧：清掉旧过点，保留官方圈号并重建位置基线。
		t.discardObservations()
		t.hasLast, t.lastNSP, t.lastAt = true, nsp, now
		return
	}
	delta := nsp - from
	switch {
	case delta >= 0 && delta <= raceMaxForwardNSP:
		t.recordSpan(float64(from), float64(nsp), t.currentLap(), now)
	case delta < 0 && from > 0.5 && nsp < 0.5 && (1-float64(from))+float64(nsp) <= raceMaxForwardNSP:
		t.recordWrap(from, nsp, now)
	case delta < 0 && -delta <= raceMaxJitterNSP:
		// 静止抖动：保留基线，不产生过点。
	default:
		// 大跳或倒车：不跨缺口补点，丢弃旧记录。
		t.discardObservations()
	}
	t.hasLast, t.lastNSP, t.lastAt = true, nsp, now
}

// observeRaceLapCounts 用官方完成圈报文推进每车官方圈号。调用者持有 stateMutex。
func observeRaceLapCounts(cars []*udp.LapCompletedCar, now time.Time) {
	if globalSession == nil || globalSession.Type != 2 {
		return
	}
	for _, car := range cars {
		if car == nil {
			continue
		}
		slot := int(car.CarID)
		conn := globalConnections[slot]
		if conn == nil || conn.UserID == 0 {
			continue
		}
		t := raceTiming[slot]
		if t == nil {
			t = &carRaceTiming{}
			raceTiming[slot] = t
		}
		next := int(car.Laps) + 1
		if !t.anchored || !t.hasLast {
			t.drivingLap, t.anchored = next, true
			t.skew = 0
			continue
		}
		if next <= t.drivingLap {
			// 重复或回退的 board 圈数不伪造推进。
			continue
		}
		if next-t.drivingLap > 1 {
			// 缺失多圈时无法配对跨线顺序，清掉旧过点并从当前官方圈重新开始。
			t.discardObservations()
			t.hasLast = false
			t.drivingLap, t.anchored = next, true
			continue
		}
		t.drivingLap = next
		t.skew--
	}
}

// resetRaceObservations 只重置位置计时，进站停车或失联不能回退官方圈数。
func resetRaceObservations(slot int) {
	if t := raceTiming[slot]; t != nil {
		t.discardObservations()
	}
}

// resetRaceTiming 换人或换车时删除槽位全部计时身份。调用者持有 stateMutex。
func resetRaceTiming(slot int) {
	delete(raceTiming, slot)
}

// resetAllRaceTiming 会话切换时清空全部 Race 计时观测。调用者持有 stateMutex。
func resetAllRaceTiming() {
	raceTiming = make(map[int]*carRaceTiming)
}

// raceGaps 按名次算 Gap（相对领跑车）与 Interval（相对前车）的位图值；无可用记录为 -1。
// 调用者持有 stateMutex。
func raceGaps(entries []Entry) ([]int32, []int32) {
	gap := make([]int32, len(entries))
	interval := make([]int32, len(entries))
	progress := make([]float64, len(entries))
	index := make([]map[int]time.Time, len(entries))
	usable := make([]bool, len(entries))
	for i, e := range entries {
		gap[i], interval[i] = -1, -1
		t := raceTiming[e.GameCarID]
		if t == nil || !t.anchored || !t.hasLast || t.skew != 0 || len(t.crossings) == 0 || time.Since(t.lastAt) > raceMaxSampleGap {
			// 缺帧过久的残留时间点不再参与，避免用陈旧位置算 gap。
			continue
		}
		progress[i], usable[i] = t.progress(), true
		m := make(map[int]time.Time, len(t.crossings))
		for _, c := range t.crossings {
			key := c.lap*raceCheckpointCount + c.point
			if _, ok := m[key]; !ok {
				m[key] = c.at
			}
		}
		index[i] = m
	}
	for i := 1; i < len(entries); i++ {
		if !usable[i] {
			continue
		}
		if usable[0] {
			gap[i] = raceGapBitmap(progress[0]-progress[i], index[i], index[0])
		}
		if usable[i-1] {
			interval[i] = raceGapBitmap(progress[i-1]-progress[i], index[i], index[i-1])
		}
	}
	return gap, interval
}

// raceGapBitmap 先取两车共同计时的最新 (lap,point) 求时间差，再看相对进度是否确实落后整圈：
// 不足整圈用计时差（偶数=毫秒*2），整圈用奇数（圈数*2+1）；缺共同记录或计时倒退返回 -1。
func raceGapBitmap(progressDiff float64, behind, ahead map[int]time.Time) int32 {
	if progressDiff < 0 {
		return -1
	}
	if progressDiff >= 1 {
		return int32(int(math.Floor(progressDiff))*2 + 1)
	}
	ms, ok := raceTimeDelta(behind, ahead)
	if !ok || ms < 0 {
		return -1
	}
	if ms > math.MaxInt32/2 {
		return -1
	}
	return int32(ms * 2)
}

// raceTimeDelta 取两车都已越过的、最新的相同 (lap,point) 记录求时间差（毫秒）。
func raceTimeDelta(behind, ahead map[int]time.Time) (int64, bool) {
	bestKey := -1
	var bestTime time.Time
	for key, at := range behind {
		if key <= bestKey {
			continue
		}
		if _, ok := ahead[key]; ok {
			bestKey, bestTime = key, at
		}
	}
	if bestKey < 0 {
		return 0, false
	}
	return bestTime.Sub(ahead[bestKey]).Milliseconds(), true
}
