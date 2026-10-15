// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

// 单圈原子持久化。把 stint_lap / lap_telemetry / session_feed / session_stint 统计放进同一个事务，
// 消除“圈已写入但 stint 计数未跟上”“feed 已声明最快圈而 stint 未记录”这类中间态。
// 只复用既有表结构与列，不新建表、不改 schema；失败不做自动重试，由调用方保留原内存状态。

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

// 单圈持久化与 db.go 的 Create* 共用同一份 SQL 定义，避免两处列顺序或列名漂移。
const (
	sqlInsertStintLap             = `INSERT INTO stint_lap (stint_id, time, sector_1, sector_2, sector_3, grip, tyre, avg_speed, max_speed, cuts, crashes, car_crashes, finished_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	sqlInsertLapTelemetry         = "INSERT INTO lap_telemetry (lap_id, telemetry) VALUES (?, ?)"
	sqlInsertSessionFeed          = "INSERT INTO session_feed (session_id, type, detail, time) VALUES (?, ?, ?, ?)"
	sqlUpdateStintStats           = "UPDATE session_stint SET laps = ?, valid_laps = ?, best_lap_id = ? WHERE session_stint_id = ?"
	sqlUpdateStintStatsFinishedAt = "UPDATE session_stint SET laps = ?, valid_laps = ?, best_lap_id = ?, finished_at = ? WHERE session_stint_id = ?"
)

// sessionFeedDetailLimit 对应 session_feed.detail 的 VARCHAR(200)（按字符计，不是字节）。
const sessionFeedDetailLimit = 200

// sentinelNewBestLapID 是调用方在“本圈刷新最快圈”时写的哨兵：事务内替换为真实插入的 stint_lap_id。
const sentinelNewBestLapID = -1

// sqlExecer 由 *sql.DB 与 *sql.Tx 同时满足，让同一份 SQL 在直连与事务下复用。
type sqlExecer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// execOn 在指定执行器上执行 SQL，日志格式与 executeSQL 一致（不含模拟分支）。
func (w *DBWriter) execOn(execer sqlExecer, query string, args ...interface{}) (sql.Result, error) {
	w.Logger.Printf("执行SQL: %s, 参数: %v", query, args)
	result, err := execer.Exec(query, args...)
	if err != nil {
		w.Logger.Printf("SQL执行失败: %v", err)
		return nil, err
	}
	affected, _ := result.RowsAffected()
	lastID, _ := result.LastInsertId()
	w.Logger.Printf("SQL执行成功，受影响行数: %d, 最后插入ID: %d", affected, lastID)
	return result, nil
}

// PersistLap 原子保存一个完整圈，返回新写入的 stint_lap_id。
//
// 参数约定（由调用方保证）：
//   - record      待写入的圈；record.StintID 必须指向已存在的 session_stint。
//   - updated     调用方持有的 stint 拷贝，计数已按本圈调整（laps / valid_laps）；
//     若本圈刷新最快圈，把 updated.BestLapID 置为 sentinelNewBestLapID(-1)，
//     事务内替换为新插入的 stint_lap_id。updated.ID 必须非 0。
//   - telemetry   为 nil 时不写 lap_telemetry；非 nil（含空切片）按原样写入。
//   - feeds       同一事务内写入的 session_feed，detail 超过列容量时按 normalizeFeedDetail 处理。
//   - updateFinishedAt 为 true 时用 updated.FinishedAt 更新 stint 的 finished_at；
//     成绩回填传 false，不把“最后活跃时间”当区间上界写回。
//
// 事务内顺序：插 stint_lap → 替换 -1 哨兵 → 插遥测 → 插 feeds → UPDATE stint 统计与 finished_at。
// 只有全部成功提交后才把新 lap id 写回 updated.BestLapID；失败时 updated 与数据库都保持原状。
func (w *DBWriter) PersistLap(record *StintLap, updated *SessionStint, telemetry []byte, feeds []*SessionFeed, updateFinishedAt bool) (int, error) {
	if record == nil || updated == nil {
		return 0, fmt.Errorf("PersistLap 缺少单圈或 stint 参数")
	}
	if record.StintID == 0 {
		return 0, fmt.Errorf("PersistLap 缺少 stint_id")
	}
	if updated.ID == 0 {
		return 0, fmt.Errorf("PersistLap 缺少 session_stint_id")
	}

	// 模拟模式不开启事务：沿用 executeSQL 的 SQL 日志与 nextSimulatedID 语义。
	if w.SimulateSQL {
		return w.persistLapSimulated(record, updated, telemetry, feeds, updateFinishedAt)
	}

	tx, err := w.DB.Begin()
	if err != nil {
		return 0, fmt.Errorf("开启单圈事务失败: %v", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		if rollbackErr := tx.Rollback(); rollbackErr != nil && rollbackErr != sql.ErrTxDone {
			w.Logger.Printf("回滚单圈事务失败: %v", rollbackErr)
		}
	}()

	// 1) 先落 stint_lap，最快圈外键需要真实圈ID
	result, err := w.execOn(tx, sqlInsertStintLap, record.StintID, record.Time, record.Sector1, record.Sector2, record.Sector3,
		record.Grip, record.Tyre, record.AvgSpeed, record.MaxSpeed, record.Cuts, record.Crashes, record.CarCrashes, record.FinishedAt)
	if err != nil {
		return 0, fmt.Errorf("写入单圈失败: %v", err)
	}
	lapID64, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("获取单圈ID失败: %v", err)
	}
	lapID := int(lapID64)

	// 2) 哨兵替换：本圈刷新最快圈时 best_lap_id 用新圈ID
	bestLapID := resolvePersistedBestLapID(updated.BestLapID, lapID)

	// 3) 只在调用方提供遥测时写 lap_telemetry
	if telemetry != nil {
		if _, err := w.execOn(tx, sqlInsertLapTelemetry, lapID, telemetry); err != nil {
			return 0, fmt.Errorf("插入遥测数据失败: %v", err)
		}
	}

	// 4) feeds 与 stint 统计同事务提交
	for _, feed := range feeds {
		normalized, err := normalizeSessionFeed(feed)
		if err != nil {
			return 0, err
		}
		if _, err := w.execOn(tx, sqlInsertSessionFeed, normalized.SessionID, normalized.Type, normalized.Detail, normalized.Time); err != nil {
			return 0, fmt.Errorf("写入session_feed失败: %v", err)
		}
	}
	if err := w.updateStintStatsTx(tx, updated, bestLapID, updateFinishedAt); err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("提交单圈事务失败: %v", err)
	}
	committed = true

	// 只在此处（提交成功后）回写哨兵，调用方随后用它覆盖内存 stint
	updated.BestLapID = bestLapID
	return lapID, nil
}

// persistLapSimulated 模拟模式下按原语义逐步执行（不事务），保持 SQL 日志与模拟ID不变。
func (w *DBWriter) persistLapSimulated(record *StintLap, updated *SessionStint, telemetry []byte, feeds []*SessionFeed, updateFinishedAt bool) (int, error) {
	lapID, err := w.CreateStintLap(record)
	if err != nil {
		return 0, fmt.Errorf("写入单圈失败: %v", err)
	}
	bestLapID := resolvePersistedBestLapID(updated.BestLapID, int(lapID))
	if telemetry != nil {
		if _, err := w.CreateLapTelemetry(&LapTelemetry{LapID: lapID, Telemetry: telemetry}); err != nil {
			return 0, fmt.Errorf("插入遥测数据失败: %v", err)
		}
	}
	for _, feed := range feeds {
		normalized, err := normalizeSessionFeed(feed)
		if err != nil {
			return 0, err
		}
		if _, err := w.CreateSessionFeed(normalized); err != nil {
			return 0, fmt.Errorf("写入session_feed失败: %v", err)
		}
	}
	if updateFinishedAt {
		if _, err := w.executeSQL(sqlUpdateStintStatsFinishedAt, updated.Laps, updated.ValidLaps, nullableLapID(bestLapID), updated.FinishedAt, updated.ID); err != nil {
			return 0, fmt.Errorf("更新 stint 统计失败: %v", err)
		}
	} else {
		if _, err := w.executeSQL(sqlUpdateStintStats, updated.Laps, updated.ValidLaps, nullableLapID(bestLapID), updated.ID); err != nil {
			return 0, fmt.Errorf("更新 stint 统计失败: %v", err)
		}
	}
	updated.BestLapID = bestLapID
	return int(lapID), nil
}

// updateStintStatsTx 在同一事务内更新 stint 的圈数/有效圈/最快圈外键，按调用方要求决定是否写 finished_at。
func (w *DBWriter) updateStintStatsTx(tx *sql.Tx, updated *SessionStint, bestLapID int, updateFinishedAt bool) error {
	if updateFinishedAt {
		if _, err := w.execOn(tx, sqlUpdateStintStatsFinishedAt, updated.Laps, updated.ValidLaps, nullableLapID(bestLapID), updated.FinishedAt, updated.ID); err != nil {
			return fmt.Errorf("更新 stint 统计失败: %v", err)
		}
		return nil
	}
	if _, err := w.execOn(tx, sqlUpdateStintStats, updated.Laps, updated.ValidLaps, nullableLapID(bestLapID), updated.ID); err != nil {
		return fmt.Errorf("更新 stint 统计失败: %v", err)
	}
	return nil
}

// resolvePersistedBestLapID 把“本圈刷新最快圈”的哨兵替换为新圈ID，其余情况原样保留（0 表示当前无最快圈）。
func resolvePersistedBestLapID(current int, lapID int) int {
	if current < 0 {
		return lapID
	}
	return current
}

// normalizeSessionFeed 校验并规范化 feed 的 detail 容量；未超限时原样返回入参。
func normalizeSessionFeed(feed *SessionFeed) (*SessionFeed, error) {
	if feed == nil {
		return nil, fmt.Errorf("session_feed 为空")
	}
	detail, err := normalizeFeedDetail(feed.Detail)
	if err != nil {
		return nil, err
	}
	if detail == feed.Detail {
		return feed, nil
	}
	normalized := *feed
	normalized.Detail = detail
	return &normalized, nil
}

// normalizeFeedDetail 处理 session_feed.detail 的 VARCHAR(200) 容量：
// 聊天 detail 保留合法 JSON，把 msg 按 Unicode 字符截短，使整个 JSON 不超过 200 字符；
// 其它 detail 超长时返回明确错误，不截断结构、不写半截 JSON。
func normalizeFeedDetail(detail string) (string, error) {
	if utf8.RuneCountInString(detail) <= sessionFeedDetailLimit {
		return detail, nil
	}
	if trimmed, ok := trimChatFeedDetail(detail); ok {
		return trimmed, nil
	}
	return "", fmt.Errorf("session_feed.detail 超过 %d 字符且不是可截短的聊天消息(当前 %d 字符)",
		sessionFeedDetailLimit, utf8.RuneCountInString(detail))
}

// trimChatFeedDetail 只对含 msg 字符串字段的聊天 detail 生效；成功时返回仍是合法 JSON 的短消息。
func trimChatFeedDetail(detail string) (string, bool) {
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(detail), &obj); err != nil {
		return "", false
	}
	msg, ok := obj["msg"].(string)
	if !ok {
		return "", false
	}
	runes := []rune(msg)
	// 逐字符收缩 msg，直到整个 JSON 满足列容量；结构（键与类型）保持不变。
	for n := len(runes) - 1; n >= 0; n-- {
		obj["msg"] = string(runes[:n])
		data, err := json.Marshal(obj)
		if err != nil {
			return "", false
		}
		if utf8.RuneCountInString(string(data)) <= sessionFeedDetailLimit {
			return string(data), true
		}
	}
	return "", false
}
