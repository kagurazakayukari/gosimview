// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
// See LICENSE.md for license information.

package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gosimview/udp"
)

// recoveredScore 保存日志里已经发生的当前节成绩；它不代表新采集的一圈。
type recoveredScore struct {
	Laps      int
	ValidLaps int
	BestTime  int
	TotalTime int
	LastTime  int
	Finished  bool
	BestLap   StintLap
	// OfficialBest 是玩家行给出的非哨兵 BEST，与本次回放的临时观测分开保存；
	// 只有在日志窗口内没有任何完成圈可判定时才用它兜底，哨兵会使它归 0。
	OfficialBest int
}

// AC 玩家行在“尚无有效圈”时给出的 BEST 哨兵 (16666:39:999)，等于 999999999 毫秒。
// 它必须被丢弃，不能当成真实圈时参与最快圈比较。
const logSentinelBestTime = 16666*60000 + 39*1000 + 999

var recoveredScores = make(map[int]*recoveredScore)
var restoredStintIDs = make(map[int]bool)
var playerLineRegex = regexp.MustCompile(`^(\d+)\) (.*?) BEST: (.*?) TOTAL: (.*?) Laps:(\d+) SesID:(\d+) HasFinished:(\w+)$`)
var splitLineRegex = regexp.MustCompile(`Car\.onSplitCompleted (\d+) (\d+) (\d+)`)
var lapLineRegex = regexp.MustCompile(`LAP (.*?) (\d+):(\d+):(\d+)`)

// LAP WITH CUTS 的冒号后可能无空格（LAP WITH CUTS:Player），laptime 也可能缺省。
var cutsLineRegex = regexp.MustCompile(`LAP WITH CUTS:\s*(.*?),\s*(\d+) cuts(?:,?\s*laptime\s*(\d+))?`)
var environmentLineRegex = regexp.MustCompile(`COLLISION BETWEEN: (.*?) \[\] AND ENVIRONMENT`)
var collisionLineRegex = regexp.MustCompile(`COLLISION BETWEEN: (.*?) \[\] AND (.*?) \[\]`)

// cleanRequestedCarModel 规范化日志 REQUESTED CAR 字段：AC 在按皮肤通配选择时
// 会追加尾随 '*'（如 rss_formula_hybrid_2022_s*）。该值用于匹配 entry_list 建立的
// 车辆，必须先去掉空白再去掉尾随 '*'，否则会另建一台带星号的重复车辆。
func cleanRequestedCarModel(raw string) string {
	return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(raw), "*"))
}

func slotForLogName(name string) int {
	name = strings.TrimSpace(name)
	for slot, conn := range globalConnections {
		if conn.DriverName == name {
			return slot
		}
	}
	return -1
}

// 使用原 LAP 日志的 m:s:ms 单位；纯整数按同一毫秒单位读取。
func parseLogMilliseconds(value string) int {
	value = strings.TrimSpace(value)
	if n, err := strconv.Atoi(value); err == nil && n >= 0 {
		return n
	}
	parts := strings.Split(value, ":")
	if len(parts) != 3 {
		return 0
	}
	min, e1 := strconv.Atoi(parts[0])
	sec, e2 := strconv.Atoi(parts[1])
	ms, e3 := strconv.Atoi(parts[2])
	if e1 != nil || e2 != nil || e3 != nil || min < 0 || sec < 0 || sec >= 60 || ms < 0 || ms >= 1000 {
		return 0
	}
	return min*60000 + sec*1000 + ms
}

// applyLogCuts 把日志里的切弯数落到“按 laptime 匹配的最近一条已完成圈”。
// LAP 行之后紧跟的 LAP WITH CUTS 行会落在随后新建的空圈之后，这里必须回退查找
// 已完成圈（Time>0），不能写到下圈的空记录上；匹配只依赖已累积的 stintLapData，
// 因此 LAP 与 LAP WITH CUTS 允许落在不同的增量批次里。
func applyLogCuts(sid, slot, cuts, lapTime int) {
	values := stintLapData[sid][slot]
	for i := len(values) - 1; i >= 0; i-- {
		if values[i].Time <= 0 {
			continue
		}
		if lapTime > 0 && values[i].Time != lapTime {
			continue
		}
		values[i].Cuts = cuts
		return
	}
}

// refreshRecoveredScores 在回放结束后按实际记录下来的已完成圈重算有效圈与最快圈。
// LAP WITH CUTS 可能晚于 LAP 到达，有效性只能在全部行处理完后统一判定；
// BestTime 始终由当前观测重算，不会保留已被 cuts/碰撞否定的旧值。
func refreshRecoveredScores() {
	if globalSession == nil {
		return
	}
	sid := int(globalSession.ID)
	for slot, score := range recoveredScores {
		if score == nil {
			continue
		}
		completed, valid, best := 0, 0, 0
		var bestLap StintLap
		for _, lap := range stintLapData[sid][slot] {
			if lap.Time <= 0 {
				continue
			}
			completed++
			if lap.Cuts != 0 || lap.Crashes != 0 || lap.CarCrashes != 0 {
				continue
			}
			valid++
			if best == 0 || lap.Time < best {
				best = lap.Time
				bestLap = lap
			}
		}
		score.ValidLaps = valid
		if completed == 0 {
			// 日志窗口内没有任何完成圈：只能采用官方 BEST（哨兵已归 0）。
			score.BestTime = score.OfficialBest
			score.BestLap = StintLap{}
			continue
		}
		// 有完成圈时以回放观测为准：被 cuts/碰撞否定的圈一律不计，
		// 既保证“无有效圈即 0”，也不会把被否定的记录当成最快圈。
		score.BestTime = best
		if best > 0 {
			score.BestLap = bestLap
		} else {
			score.BestLap = StintLap{}
		}
	}
}

// replay=true 仅回放内存状态；不结束 stint，不产生历史 feed 或写圈。
// 增量与恢复共用原代码里的日志字段规则，保证两条路径一致。
func processSessionLogLines(lines []string, replay bool) {
	_, _ = processSessionLogLinesAt(lines, replay, 0)
}

// 返回已消费行数；失败行及其后内容由调用方保留游标，下次继续处理。
func processSessionLogLinesAt(lines []string, replay bool, lineStart int) (int, error) {
	if globalSession == nil || globalSession.ID == 0 {
		return 0, fmt.Errorf("当前会话尚未恢复")
	}
	sid := int(globalSession.ID)
	if stintLapData[sid] == nil {
		stintLapData[sid] = make(map[int][]StintLap)
	}
	currentLap := func(slot int) *StintLap {
		values := stintLapData[sid][slot]
		if len(values) == 0 || values[len(values)-1].Time > 0 {
			conn := globalConnections[slot]
			if conn == nil {
				return nil
			}
			values = append(values, StintLap{Tyre: userTyres[conn.DriverName], StintID: userStintIDs[conn.DriverName]})
			stintLapData[sid][slot] = values
		}
		return &stintLapData[sid][slot][len(values)-1]
	}
	if !replay {
		// 原连接日志是多行块；按整批匹配更新原断线判定使用的连接行号。
		chunk := strings.ReplaceAll(strings.Join(lines, "\n"), "\r\n", "\n")
		for _, indices := range connectionRegex.FindAllStringSubmatchIndex(chunk, -1) {
			name := strings.TrimSpace(chunk[indices[4]:indices[5]])
			if slot := slotForLogName(name); slot >= 0 {
				globalConnections[slot].LastConnectionLine = lineStart + strings.Count(chunk[:indices[0]], "\n")
			}
		}
	}
	for index, raw := range lines {
		line := strings.TrimSuffix(raw, "\r")
		for _, conn := range globalConnections {
			if _, ok := userStintIDs[conn.DriverName]; !ok {
				userStintIDs[conn.DriverName] = 1
			}
		}
		if m := tyreRegex.FindStringSubmatch(line); len(m) == 3 {
			name := strings.TrimSpace(m[1])
			old := userTyres[name]
			slot := slotForLogName(name)
			if !replay && slot >= 0 && len(pitBoundary) < 3 && old != "" && old != m[2] {
				// 用户指定无边界时沿用原方式：已知配方发生变化才结束段。
				// 日志回放不触发写库；有边界时仅由区内停车处理，避免重复切段。
				if err := finishStint(slot); err != nil {
					return index, err
				}
				globalConnections[slot].StintStartedAt = time.Now()
				// collectCarUpdates 已持有 lapDataMutex，不在此递归加锁。
				delete(carLapData, slot)
				delete(lapTelemetryReady, slot)
			}
			userTyres[name] = m[2]
			userStintIDs[name]++
			if slot >= 0 {
				if !replay {
					globalDBWriter.InsertSessionFeed(&SessionFeed{SessionID: globalSession.ID, Type: 16, Detail: feedDetail(slot, map[string]interface{}{"old_t": old, "new_t": m[2]}), Time: time.Now()})
				}
				if lap := currentLap(slot); lap != nil {
					lap.Tyre = m[2]
				}
			}
			continue
		}
		if m := splitLineRegex.FindStringSubmatch(line); len(m) == 4 {
			slot, _ := strconv.Atoi(m[1])
			split, _ := strconv.Atoi(m[2])
			ms, _ := strconv.Atoi(m[3])
			lap := currentLap(slot)
			if lap == nil {
				continue
			}
			if split == 0 {
				lap.Sector1 = ms
			}
			if split == 1 {
				lap.Sector2 = ms
			}
			continue
		}
		if m := cutsLineRegex.FindStringSubmatch(line); len(m) >= 4 {
			slot := slotForLogName(m[1])
			if slot >= 0 {
				cuts, _ := strconv.Atoi(m[2])
				applyLogCuts(sid, slot, cuts, parseLogMilliseconds(m[3]))
			}
			continue
		}
		if m := environmentLineRegex.FindStringSubmatch(line); len(m) == 2 {
			slot := slotForLogName(m[1])
			if slot >= 0 {
				if lap := currentLap(slot); lap != nil {
					lap.Crashes++
				}
			}
			continue
		}
		if m := collisionLineRegex.FindStringSubmatch(line); len(m) == 3 {
			slot := slotForLogName(m[1])
			if slot >= 0 {
				if lap := currentLap(slot); lap != nil {
					lap.CarCrashes++
				}
			}
			continue
		}
		if m := lapLineRegex.FindStringSubmatch(line); len(m) == 5 {
			slot := slotForLogName(m[1])
			if slot < 0 {
				continue
			}
			lap := currentLap(slot)
			if lap == nil {
				continue
			}
			ms := parseLogMilliseconds(strings.Join(m[2:], ":"))
			if ms <= 0 {
				continue
			}
			lap.Time = ms
			lap.Sector3 = ms - lap.Sector1 - lap.Sector2
			if lap.Sector3 < 0 {
				lap.Sector3 = 0
			}
			if replay {
				score := recoveredScores[slot]
				if score == nil {
					score = &recoveredScore{}
					recoveredScores[slot] = score
				}
				score.Laps++
				score.LastTime = ms
				score.TotalTime += ms
				// 有效圈与最快圈在回放结束后由 refreshRecoveredScores 统一重算：
				// LAP WITH CUTS 行晚于 LAP 到达，这里无法判定本圈是否有效。
			}
			// 原日志解析在 LAP 后保留下一圈的空记录。
			conn := globalConnections[slot]
			stintLapData[sid][slot] = append(stintLapData[sid][slot], StintLap{Tyre: userTyres[conn.DriverName], StintID: userStintIDs[conn.DriverName]})
			continue
		}
		if replay {
			if m := playerLineRegex.FindStringSubmatch(line); len(m) == 8 {
				slot := slotForLogName(m[2])
				if slot < 0 {
					continue
				}
				score := recoveredScores[slot]
				if score == nil {
					score = &recoveredScore{}
					recoveredScores[slot] = score
				}
				if n, err := strconv.Atoi(m[5]); err == nil && n >= 0 {
					score.Laps = n
				}
				// 官方 BEST：哨兵 16666:39:999 代表“尚无有效圈”，必须归 0，不能泄漏为最快圈。
				if best := parseLogMilliseconds(m[3]); best > 0 && best < logSentinelBestTime {
					score.OfficialBest = best
				} else {
					score.OfficialBest = 0
				}
				// 官方 TOTAL 即使为 0 也要覆盖此前按圈累加出的无效总计。
				score.TotalTime = parseLogMilliseconds(m[4])
				score.Finished = strings.EqualFold(m[7], "true") || m[7] == "1"
			}
		} else {
			if m := disconnectRegex.FindStringSubmatch(line); len(m) == 2 {
				slot := slotForLogName(m[1])
				if conn := globalConnections[slot]; conn != nil && lineStart+index > conn.LastConnectionLine {
					conn.ConnectionStatus = 2
				}
			}
		}
	}
	if replay {
		refreshRecoveredScores()
	}
	return len(lines), nil
}

// 接入当前会话只查已有 session，不向数据库创建新 session。
func adoptCurrentSession(msg udp.SessionInfo) error {
	if globalSession != nil && globalSession.ID > 0 {
		dataCollectionEnabled = globalSession.IsFinished == 0
		return nil
	}
	var session *Session
	if !globalDBWriter.SimulateSQL {
		var err error
		session, err = globalDBWriter.FindLatestSessionByEventID(globalEvent.ID)
		if err != nil {
			return err
		}
		if session == nil {
			return fmt.Errorf("事件 %d 无已有 session，等待服务器新开 session", globalEvent.ID)
		}
	} else {
		// 只有内存的模拟 ID，不执行 INSERT，不伪装成已写入数据库。
		port := cfg.Writer.HTTP.Port
		if port == 0 {
			port = cfg.ACServer.UDP.LocalPort
		}
		session = &Session{ID: nextSimulatedID(), EventID: globalEvent.ID, Type: databaseSessionType(msg), Name: msg.Name, TrackTime: "0", StartTime: time.Now().Add(-time.Duration(msg.ElapsedMilliseconds) * time.Millisecond), DurationMin: int(msg.Time), ElapsedMs: int(msg.ElapsedMilliseconds), Laps: int(msg.Laps), Weather: msg.WeatherGraphics, AirTemp: float64(msg.AmbientTemp), RoadTemp: float64(msg.RoadTemp), StartGrip: -1, CurrentGrip: -1, HTTPPort: port, LastActivity: time.Now()}
	}
	// 完整读取后才发布 session，查询失败后的下一次恢复不能跳过未完成的步骤。
	var stints []*SessionStint
	restoredBestTimes := make(map[int]int)
	if !globalDBWriter.SimulateSQL {
		var err error
		stints, err = globalDBWriter.FindSessionStintsBySessionID(session.ID)
		if err != nil {
			return err
		}
		for _, stint := range stints {
			if stint.BestLapID > 0 {
				lapTime, err := getLapTimeByID(stint.BestLapID)
				if err != nil {
					return fmt.Errorf("恢复 stint %d 最快圈: %w", stint.ID, err)
				}
				restoredBestTimes[stint.ID] = lapTime
			}
		}
	}
	globalSession = session
	dataCollectionEnabled = session.IsFinished == 0
	// 续用已有 stint，避免接入后另起一条并丢失已有计数。
	if !globalDBWriter.SimulateSQL {
		sessionStints = stints
		restoredStintIDs = make(map[int]bool)
		for _, stint := range stints {
			restoredStintIDs[stint.ID] = true
		}
		for _, stint := range stints {
			if stint.IsFinished == 0 {
				old := activeStints[stint.GameCarID]
				if old == nil || old.ID < stint.ID {
					activeStints[stint.GameCarID] = stint
				}
			}
			if stint.BestLapID > 0 {
				bestLapTimes[stint.ID] = restoredBestTimes[stint.ID]
			}
		}
	}
	return nil
}
