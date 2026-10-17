// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
// See LICENSE.md for license information.

package main

import (
	"sort"
	"time"

	"gosimview/udp"
)

// 名次字段及 feed 编码有原版证据；相邻快照名次差是 Go 侧采用的计算规则。
// 已用双车换位抓帧对照原版：posChange 只在顺序变化的那一次广播出现 ±1，
// 下一帧即归零，正为升位；圈数反超与 NSP 反超走同一计算，不区分原因。
// 基线与 session 绑定；槽位换人、换车或换队须建立新基线。
type positionIdentity struct {
	slot   int
	userID int64
	carID  uint16
	teamID int64
}

var prevPositions = make(map[positionIdentity]int)
var prevPositionsSession int

// applyPositionChanges 按上一次广播快照计算正升负降，首次出现的身份不制造变化。
// 调用者持有 stateMutex 写锁；单纯读取排行榜不会消费名次基线。
func applyPositionChanges(entries []Entry, sessionID int) {
	if prevPositionsSession != sessionID {
		prevPositions = make(map[positionIdentity]int, len(entries))
		prevPositionsSession = sessionID
	}
	current := make(map[positionIdentity]int, len(entries))
	for i := range entries {
		entries[i].PosChange = 0
		id := positionIdentity{slot: entries[i].GameCarID, userID: entries[i].UserID, carID: entries[i].CarID, teamID: entries[i].TeamID}
		current[id] = i
		prev, ok := prevPositions[id]
		if !ok || prev == i {
			continue
		}
		delta := prev - i
		if delta > 127 {
			delta = 127
		}
		if delta < -128 {
			delta = -128
		}
		entries[i].PosChange = int8(delta)
	}
	prevPositions = current
}

// recordPositionChangeFeeds 已移除：原版不产 type=10 feed。
// 依据（原版直接证据）：反编译原版 SimViewWriter 时，11 个 session_feed 插入点
// 的 type 常量只有 0/1/2/3/4/5/6/7/11/12/16 加一个动态值（传送 15），没有 10；
// 运行时也验证过：双车换位产生 posChange 的 107 帧里，feed 只出现 2 和 5。
// 名次变化只走二进制帧字段，页面据此显示升降位提示。

// maxZero 把负值归零，写帧字段时不输出负数。
func maxZero(v int) int {
	if v < 0 {
		return 0
	}
	return v
}

// publishLiveFrame 把当前排行榜快照交给 writer 自己的 /live 服务。
// 原版就是 Writer 直接对浏览器发帧，http 只负责页面与 /api，这里不再有生产者链路。
// 广播只读已恢复的内存状态；接入已有 session 由探活恢复路径完成，广播不改写 globalSession。
func publishLiveFrame() {
	stateMutex.Lock()
	if !recoveryReady || globalSession == nil || globalSession.ID == 0 || globalSession.IsFinished != 0 {
		stateMutex.Unlock()
		interval := cfg.Writer.HTTP.Leaderboard.Broadcast.Interval.MS
		setLiveFrame(nil, nil, int32(interval))
		return
	}
	s := *globalSession
	entries, _ := parseLeaderboardEntries(int(s.ID), latestCarUpdates)
	applyPositionChanges(entries, int(s.ID))
	stateMutex.Unlock()

	interval := cfg.Writer.HTTP.Leaderboard.Broadcast.Interval.MS
	if interval <= 0 {
		interval = 2000
	}
	if s.IsFinished == 0 {
		s.ElapsedMs = int(time.Since(s.StartTime).Milliseconds())
	}
	// 已结束的会话沿用落库的 ElapsedMs，页面倒计时不再继续跑。
	setLiveFrame(&s, entries, int32(interval))
}

// 调用者持有 stateMutex.RLock；列表按 globalConnections 建立，
// 已连接/加载中/断开的车辆都会投影，不依赖是否收到过 CarUpdate。
func parseLeaderboardEntries(sessionID int, updates map[udp.CarID]udp.CarUpdate) ([]Entry, error) {
	slots := make([]int, 0, len(globalConnections))
	for slot := range globalConnections {
		slots = append(slots, slot)
	}
	sort.Ints(slots)
	projections := make([]*slotProjection, 0, len(slots))
	for _, slot := range slots {
		conn := globalConnections[slot]
		if conn == nil || conn.UserID == 0 || conn.CarDBID == 0 {
			continue
		}
		update, hasUpdate := updates[udp.CarID(slot)]
		projections = append(projections, buildSlotProjection(sessionID, slot, conn, update, hasUpdate))
	}
	// 第二遍：按车组汇总同类全局最快圈/分段后再写 SectorMask。
	applySectorMasks(projections)
	entries := make([]Entry, 0, len(projections))
	for _, p := range projections {
		e := p.entry
		// 官方 board 的圈数可能领先于 DB/恢复基线，用已锚定的观测圈数补齐，避免名次迟滞。
		if t := raceTiming[e.GameCarID]; t != nil && t.anchored && t.drivingLap-1 > int(e.Laps) {
			e.Laps = uint16(t.drivingLap - 1)
		}
		entries = append(entries, e)
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if globalSession.Type == 2 {
			if entries[i].Laps != entries[j].Laps {
				return entries[i].Laps > entries[j].Laps
			}
			if entries[i].NSP != entries[j].NSP {
				return entries[i].NSP > entries[j].NSP
			}
		} else {
			if entries[i].BestLapTime != entries[j].BestLapTime {
				return entries[i].BestLapTime < entries[j].BestLapTime
			}
			if entries[i].LastLapTime != entries[j].LastLapTime {
				return entries[i].LastLapTime < entries[j].LastLapTime
			}
		}
		return entries[i].GameCarID < entries[j].GameCarID
	})
	return entries, nil
}
