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
}

var recoveredScores = make(map[int]*recoveredScore)
var restoredStintIDs = make(map[int]bool)
var playerLineRegex = regexp.MustCompile(`^(\d+)\) (.*?) BEST: (.*?) TOTAL: (.*?) Laps:(\d+) SesID:(\d+) HasFinished:(\w+)$`)
var splitLineRegex = regexp.MustCompile(`Car\.onSplitCompleted (\d+) (\d+) (\d+)`)
var lapLineRegex = regexp.MustCompile(`LAP (.*?) (\d+):(\d+):(\d+)`)
var cutsLineRegex = regexp.MustCompile(`LAP WITH CUTS: (.*?), (\d+) cuts`)
var environmentLineRegex = regexp.MustCompile(`COLLISION BETWEEN: (.*?) \[\] AND ENVIRONMENT`)
var collisionLineRegex = regexp.MustCompile(`COLLISION BETWEEN: (.*?) \[\] AND (.*?) \[\]`)

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

// replay=true 仅回放内存状态；不结束 stint，不产生历史 feed 或写圈。
// 增量与恢复共用原代码里的日志字段规则，保证两条路径一致。
func processSessionLogLines(lines []string, replay bool) { processSessionLogLinesAt(lines, replay, 0) }

func processSessionLogLinesAt(lines []string, replay bool, lineStart int) {
	if globalSession == nil || globalSession.ID == 0 {
		return
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
			userTyres[name] = m[2]
			userStintIDs[name]++
			slot := slotForLogName(name)
			if slot >= 0 {
				if !replay {
					if old != "" && old != m[2] {
						finishStint(slot)
					}
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
		if m := cutsLineRegex.FindStringSubmatch(line); len(m) == 3 {
			slot := slotForLogName(m[1])
			if slot >= 0 {
				if lap := currentLap(slot); lap != nil {
					lap.Cuts, _ = strconv.Atoi(m[2])
				}
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
				if lap.Cuts == 0 && lap.Crashes == 0 && lap.CarCrashes == 0 {
					score.ValidLaps++
					if score.BestTime == 0 || ms < score.BestTime {
						score.BestTime = ms
						score.BestLap = *lap
					}
				}
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
				if best := parseLogMilliseconds(m[3]); best > 0 {
					score.BestTime = best
				}
				if total := parseLogMilliseconds(m[4]); total > 0 {
					score.TotalTime = total
				}
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
	globalSession = session
	dataCollectionEnabled = session.IsFinished == 0
	// 续用已有 stint，避免接入后另起一条并丢失已有计数。
	if !globalDBWriter.SimulateSQL {
		stints, err := globalDBWriter.FindSessionStintsBySessionID(session.ID)
		if err != nil {
			return err
		}
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
				bestLapTimes[stint.ID] = int(getLapTimeByID(stint.BestLapID))
			}
		}
	}
	return nil
}
