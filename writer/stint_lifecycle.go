// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
// See LICENSE.md for license information.

package main

import (
	"fmt"
	"time"
)

// stint 生命周期集中在本文件。
// 按进站信息观察到区内停车时结束旧段，重新起步开启新段；驶过维修区不分段。
// 断开与会话切换同样收尾，换胎日志本身不证明发生了停车。
// 本文件不做完圈接收判定（那由 handleLapCompletedMessage 的调用方负责）。

// connStintStartedAt 返回新建 stint 的 started_at。
// 真实连接时刻优先；同一连接可能跨 session（例如练习连接到正赛），连接时刻早于本 session 起点时
// 取 session 起点。连接时刻缺失（中途接入/日志恢复）时用 observedAt：它只是该段首次被观测到的
// 完圈时间，作为区间开口，不宣称是真实加入时刻。两者都缺失时记录日志并返回零值，由调用方跳过。
// 由主在 handleLapCompletedMessage 创建 stint 时调用。
func connStintStartedAt(conn *ConnectionInfo, observedAt time.Time) time.Time {
	var sessionStart time.Time
	if globalSession != nil {
		sessionStart = globalSession.StartTime
	}
	if conn != nil && !conn.StintStartedAt.IsZero() {
		if !sessionStart.IsZero() && conn.StintStartedAt.Before(sessionStart) {
			return sessionStart
		}
		return conn.StintStartedAt
	}
	if conn != nil && !conn.ConnectedAt.IsZero() {
		if !sessionStart.IsZero() && conn.ConnectedAt.Before(sessionStart) {
			return sessionStart
		}
		return conn.ConnectedAt
	}
	if !observedAt.IsZero() {
		return observedAt
	}
	logger.Printf("stint 缺少连接时刻与首次观测时间，无法确定 started_at")
	return time.Time{}
}

// ensureCurrentStint 让完圈与进站后起步使用同一建段路径。
// 按真实连接或本次起步时间建立区间，不把进站新段退回旧连接时间。
func ensureCurrentStint(slot int, observedAt time.Time) (*SessionStint, error) {
	conn := globalConnections[slot]
	if !sessionCollecting() || conn == nil || conn.UserID == 0 || conn.CarDBID == 0 {
		return nil, fmt.Errorf("车辆 %d 身份未恢复或当前会话已停止采集", slot)
	}
	stint := activeStints[slot]
	if stint != nil && (stint.UserID != conn.UserID || stint.CarID != conn.CarDBID) {
		if err := finishStint(slot); err != nil {
			return nil, err
		}
		stint = nil
	}
	if stint != nil && stint.IsFinished == 0 {
		return stint, persistStint(stint)
	}
	startedAt := connStintStartedAt(conn, observedAt)
	if startedAt.IsZero() {
		return nil, fmt.Errorf("车辆 %d 的 stint 起点缺失", slot)
	}
	stint = &SessionStint{UserID: conn.UserID, DriverIdentityID: conn.DriverIdentityID, SessionID: int(globalSession.ID), CarID: conn.CarDBID, GameCarID: slot, StartedAt: startedAt}
	if globalEvent.TeamEvent == 1 {
		for _, member := range globalTeamMembers {
			if member.UserID == conn.UserID && member.TeamID == conn.TeamID {
				stint.TeamMemberID = member.ID
				break
			}
		}
	}
	// 先保留已观测段的身份与起点；建段失败时完圈仍能绑定同一对象重试。
	activeStints[slot] = stint
	sessionStints = append(sessionStints, stint)
	return stint, persistStint(stint)
}

// persistStint 将已观测的内存段写库，失败不丢弃其身份或原始起点。
// 调用者持有 stateMutex。
func persistStint(stint *SessionStint) error {
	if stint.ID == 0 {
		id, err := globalDBWriter.CreateSessionStint(stint)
		if err != nil {
			return err
		}
		stint.ID = int(id)
	}
	if stint.DriverIdentityID > 0 {
		return globalDBWriter.LinkSessionStintDriverIdentity(stint.ID, stint.DriverIdentityID)
	}
	return nil
}

// finishStint 结束槽位当前未完成的 stint：先写库成功，再改内存；写库失败时返回错误并保留
// activeStints，调用方不得继续覆盖同槽位的连接或 stint。调用者需持有 stateMutex。
func finishStint(slot int) error {
	stint := activeStints[slot]
	if stint == nil || stint.IsFinished != 0 {
		return nil
	}
	if err := persistStint(stint); err != nil {
		return err
	}
	finishedAt := time.Now()
	if _, err := globalDBWriter.executeSQL("UPDATE session_stint SET is_finished=1,finished_at=? WHERE session_stint_id=?", finishedAt, stint.ID); err != nil {
		logger.Printf("结束 stint 失败，保留内存状态待重试(slot=%d, stint=%d): %v", slot, stint.ID, err)
		return err
	}
	stint.IsFinished = 1
	stint.FinishedAt = finishedAt
	delete(activeStints, slot)
	return nil
}

// stintForResultLap 按 session_stint 的 started_at/finished_at 区间，把成绩 JSON 里的一圈
// 归属到具体 stint。成绩 JSON 的 Timestamp 是相对 session.start_time 的毫秒数；JSON 里的
// StintID 语义未证实，不作为数据库 session_stint_id 使用。
// 区间为闭区间 [started_at, finished_at]，且只有 is_finished=1 的段才用 finished_at 截止：
// 未结束段的 finished_at 是随每次完圈刷新的“最后活跃时间”，不能当作区间上界。
// 归属不唯一（区间重叠、结束段无结束时间、落在空隙）时返回 nil，由调用方记录并跳过，不做猜测。
func stintForResultLap(slot, userID int, when time.Time) *SessionStint {
	if when.IsZero() {
		return nil
	}
	var matched *SessionStint
	for _, st := range sessionStints {
		if st == nil || st.GameCarID != slot || st.UserID != userID || st.StartedAt.IsZero() {
			continue
		}
		if when.Before(st.StartedAt) {
			continue
		}
		if st.IsFinished != 0 {
			if st.FinishedAt.IsZero() {
				continue // 结束段没有结束时间，无法界定区间，不猜测
			}
			if when.After(st.FinishedAt) {
				continue
			}
		}
		if matched != nil {
			return nil // 区间重叠，无法唯一归属
		}
		matched = st
	}
	return matched
}
