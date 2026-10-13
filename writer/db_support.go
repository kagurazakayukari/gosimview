// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

// 本文件集中放置三类支撑代码（仅服务于 db.go / utils.go / telemetry.go）：
//  1. 模拟执行结果：executeSQL 在 SimulateSQL 下返回稳定且非零的ID，避免下游把 0 当成“无ID”
//  2. 可空列扫描工具：数据库里大量列允许 NULL，直接扫进 string/int/time.Time 会报 Scan 错误
//  3. SQL 脚本切分与安全过滤：只执行非破坏性语句，且不自动执行

import (
	"database/sql"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// simResult 模拟模式下的 sql.Result 实现。
// 调用方会对 executeSQL 的返回值直接调 LastInsertId/RowsAffected，
// 因此必须给出稳定、递增且非零的ID。
type simResult struct{ id int64 }

func (r simResult) LastInsertId() (int64, error) { return r.id, nil }
func (r simResult) RowsAffected() (int64, error) { return 1, nil }

// 模拟ID从 0 起递增：首次分配即 1，保持小整数，避免被 uint16 等窄类型截断
var simIDCounter int64 = 0

// nextSimulatedID 返回下一个模拟ID（1,2,3,...）
func nextSimulatedID() int64 {
	return atomic.AddInt64(&simIDCounter, 1)
}

// optionalInt 把可选外键/可选数值列折叠为 nil（0 视为未设置），写入 NULL 而不是 0
func optionalInt(v int) interface{} {
	if v == 0 {
		return nil
	}
	return v
}

// nstr / nint / nf64 / ntime 把可空列扫描结果折叠成零值
func nstr(v sql.NullString) string {
	if v.Valid {
		return v.String
	}
	return ""
}

func nint(v sql.NullInt64) int {
	if v.Valid {
		return int(v.Int64)
	}
	return 0
}

func nf64(v sql.NullFloat64) float64 {
	if v.Valid {
		return v.Float64
	}
	return 0
}

func ntime(v sql.NullTime) time.Time {
	if v.Valid {
		return v.Time
	}
	return time.Time{}
}

// splitSQLStatements 按分号切分 SQL 脚本，
// 忽略单引号/双引号/反引号字符串以及 -- 行注释内部的分号。
func splitSQLStatements(script string) []string {
	var (
		statements []string
		buf        strings.Builder
		quote      rune
		inLineNote bool
	)
	runes := []rune(script)
	for i := 0; i < len(runes); i++ {
		ch := runes[i]

		if inLineNote {
			if ch == '\n' {
				inLineNote = false
				buf.WriteRune(ch)
			}
			continue
		}

		if quote != 0 {
			buf.WriteRune(ch)
			if ch == quote && (i == 0 || runes[i-1] != '\\') {
				quote = 0
			}
			continue
		}

		switch ch {
		case '\'', '"', '`':
			quote = ch
			buf.WriteRune(ch)
		case '-':
			if i+1 < len(runes) && runes[i+1] == '-' {
				inLineNote = true
				i++
			} else {
				buf.WriteRune(ch)
			}
		case ';':
			statements = append(statements, buf.String())
			buf.Reset()
		default:
			buf.WriteRune(ch)
		}
	}
	if strings.TrimSpace(buf.String()) != "" {
		statements = append(statements, buf.String())
	}
	return statements
}

// isDestructiveSQL 判断语句是否会改动既有数据或结构。
// DROP/TRUNCATE/DELETE 直接删数据；ALTER/UPDATE/REPLACE 改动既有表，同样跳过。
func isDestructiveSQL(stmt string) bool {
	upper := strings.ToUpper(strings.TrimSpace(stmt))
	upper = strings.TrimSpace(stripLeadingSQLComments(upper))
	for _, prefix := range []string{"DROP ", "TRUNCATE ", "DELETE ", "ALTER ", "UPDATE ", "REPLACE ", "RENAME "} {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}
	return false
}

// stripLeadingSQLComments 去掉语句开头的 -- 注释行（保留后续语句内容）
func stripLeadingSQLComments(stmt string) string {
	rest := stmt
	for {
		rest = strings.TrimSpace(rest)
		if !strings.HasPrefix(rest, "--") {
			return rest
		}
		idx := strings.IndexAny(rest, "\r\n")
		if idx < 0 {
			return ""
		}
		rest = rest[idx+1:]
	}
}

// cacheMutex 只服务下面的本地缓存读写（车/赛道/模拟event）。
// 刻意不复用 main 的 globalMutex / stateMutex，避免与主循环的锁形成循环等待。
var cacheMutex sync.Mutex

// simEvents 模拟模式下按名称记住已创建的 event，保证 FindEventByName 返回稳定的 event_id
var simEvents = make(map[string]*Event)

// rememberSimEvent 记录模拟创建的 event
func rememberSimEvent(event *Event) {
	if event == nil || event.Name == "" || event.ID <= 0 {
		return
	}
	cacheMutex.Lock()
	simEvents[event.Name] = event
	cacheMutex.Unlock()
}

// lookupSimEvent 按名称取回模拟创建的 event（不存在返回 nil）
func lookupSimEvent(name string) *Event {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	if ev, ok := simEvents[name]; ok {
		return ev
	}
	return nil
}

// cacheTrackConfig 把赛道配置写入 globalTrackConfigs（供主循环 getTrackLength 直接命中）
func cacheTrackConfig(tc *TrackConfig) {
	if tc == nil || tc.ID <= 0 {
		return
	}
	cacheMutex.Lock()
	if globalTrackConfigs == nil {
		globalTrackConfigs = make(map[int]*TrackConfig)
	}
	globalTrackConfigs[tc.ID] = tc
	cacheMutex.Unlock()
}

// cacheTeam 把车队写入 globalTeams
func cacheTeam(team *Team) {
	if team == nil || team.ID <= 0 {
		return
	}
	cacheMutex.Lock()
	if globalTeams == nil {
		globalTeams = make(map[int]*Team)
	}
	globalTeams[team.ID] = team
	cacheMutex.Unlock()
}

// cacheTeamMember 把车队成员写入 globalTeamMembers
func cacheTeamMember(member *TeamMember) {
	if member == nil || member.ID <= 0 {
		return
	}
	cacheMutex.Lock()
	if globalTeamMembers == nil {
		globalTeamMembers = make(map[int]*TeamMember)
	}
	globalTeamMembers[member.ID] = member
	cacheMutex.Unlock()
}

// cacheCar 把车辆写入 globalCars（供主循环按车型反查 car_id）
func cacheCar(car *Car, carID int) {
	if car == nil || carID <= 0 {
		return
	}
	car.ID = carID
	cacheMutex.Lock()
	if globalCars == nil {
		globalCars = make(map[int]*Car)
	}
	globalCars[carID] = car
	cacheMutex.Unlock()
}

// firstLine 取语句首行，仅用于日志
func firstLine(stmt string) string {
	if idx := strings.IndexAny(stmt, "\r\n"); idx >= 0 {
		return strings.TrimSpace(stmt[:idx])
	}
	return strings.TrimSpace(stmt)
}

// cleanSQLStatement 去掉空行与纯注释行，返回可执行语句（无内容时返回空串）
func cleanSQLStatement(stmt string) string {
	lines := strings.Split(stmt, "\n")
	kept := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		kept = append(kept, line)
	}
	return strings.TrimSpace(strings.Join(kept, "\n"))
}
