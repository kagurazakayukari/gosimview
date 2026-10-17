// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// 说明：本文件的接口契约来自原版 SimView 前端（result.js/analysis.js）。
// 会话类子资源统一走 /api/ac/session/{id}/{type}/...，
// 其中 {type} 为 practice/qualifying/race（原版前端由 session.type 小写得到）。
// 所有可空列都在 SQL 里 COALESCE，避免驱动把 NULL 扫进 int 报错导致 500 或整行丢失。

// sessionSelectColumns 会话统一查询列（调用方须使用 `FROM session s`）。
// 时间戳统一转成微秒；可空列给默认值。
const sessionSelectColumns = `s.session_id, s.event_id, s.type, COALESCE(s.track_time, ''), COALESCE(s.name, ''),
	COALESCE(CAST(UNIX_TIMESTAMP(s.start_time) * 1000000 AS SIGNED), 0), COALESCE(s.duration_min, 0),
	COALESCE(s.elapsed_ms, 0), COALESCE(s.laps, 0), COALESCE(s.weather, ''), COALESCE(s.air_temp, 0),
	COALESCE(s.road_temp, 0), COALESCE(s.start_grip, -1), COALESCE(s.current_grip, -1),
	COALESCE(s.is_finished, 0), COALESCE(CAST(UNIX_TIMESTAMP(s.finish_time) * 1000000 AS SIGNED), 0),
	COALESCE(CAST(UNIX_TIMESTAMP(s.last_activity) * 1000000 AS SIGNED), 0), COALESCE(s.http_port, 0)`

// rowScanner 同时适配 *sql.Row 与 *sql.Rows
type rowScanner interface {
	Scan(dest ...interface{}) error
}

// defaultHTTPPort 会话未记录 http_port 时用于连接实时排行榜 WebSocket 的端口：
// writer.http.port -> ac.server.udp.local.port -> app.server.port
func defaultHTTPPort() int {
	if cfg == nil {
		return 0
	}
	if cfg.Writer.HTTP.Port > 0 {
		return cfg.Writer.HTTP.Port
	}
	if cfg.ACServer.UDP.LocalPort > 0 {
		return cfg.ACServer.UDP.LocalPort
	}
	return cfg.App.Server.Port
}

// scanSessionRow 扫描一行会话数据
func scanSessionRow(sc rowScanner) (ACSession, error) {
	var s ACSession
	err := sc.Scan(&s.SessionID, &s.EventID, &s.Type, &s.TrackTime, &s.Name, &s.StartTime,
		&s.DurationMin, &s.ElapsedMs, &s.Laps, &s.Weather, &s.AirTemp, &s.RoadTemp,
		&s.StartGrip, &s.CurrentGrip, &s.IsFinished, &s.FinishTime, &s.LastActivity, &s.HttpPort)
	if err != nil {
		return s, err
	}
	if s.HttpPort == 0 {
		s.HttpPort = defaultHTTPPort()
	}
	return s, nil
}

// latestSessionForEvent 取某事件最新的一个会话
func latestSessionForEvent(eventID int) (ACSession, bool, error) {
	row := db.QueryRow(`SELECT `+sessionSelectColumns+` FROM session s WHERE s.event_id = ? ORDER BY s.session_id DESC LIMIT 1`, eventID)
	s, err := scanSessionRow(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return ACSession{}, false, nil
		}
		return ACSession{}, false, err
	}
	return s, true, nil
}

// sessionIsTeamEvent 判断会话所属事件是否为团队赛
func sessionIsTeamEvent(sessionID int) (bool, error) {
	var teamEvent sql.NullInt64
	err := db.QueryRow(`SELECT e.team_event FROM session s JOIN event e ON s.event_id = e.event_id WHERE s.session_id = ?`, sessionID).Scan(&teamEvent)
	if err != nil {
		return false, err
	}
	return teamEvent.Valid && teamEvent.Int64 == 1, nil
}

// dispatchSessionResource 分发 /api/ac/session/{id}/... 的各子资源
func dispatchSessionResource(w http.ResponseWriter, r *http.Request, sessionID int, resourceType string) {
	switch {
	case resourceType == "":
		sessionDetail(w, sessionID)
	case resourceType == "live":
		sendErrorResponse(w, http.StatusNotFound, "实时数据不可用", "sessionLive")
	case resourceType == "live/positions":
		sendSuccessResponse(w, "位置数据获取成功", "positions", []ACLivePosition{})
	case resourceType == "result":
		sendErrorResponse(w, http.StatusNotFound, "结果数据不可用", "sessionResult")
	case strings.HasSuffix(resourceType, "result/standings"):
		handleSessionStandings(w, sessionID, strings.HasPrefix(resourceType, "race/"))
	case resourceType == "result/sectors":
		handleSessionSectors(w, sessionID)
	case strings.HasSuffix(resourceType, "race/result/laps") || resourceType == "result/laps":
		handleSessionConsistency(w, sessionID)
	case resourceType == "race/pitstops" || resourceType == "result/pitstops":
		handleSessionPitstops(w, sessionID)
	case strings.HasPrefix(resourceType, "result/stints/team/"):
		handleSessionStints(w, sessionID, 0, 0, parseTrailingInt(resourceType))
	case strings.HasPrefix(resourceType, "result/stints/user/"):
		uid, cid := parseUserCar(resourceType)
		handleSessionStints(w, sessionID, uid, cid, 0)
	case resourceType == "cars":
		// 原版 /api/ac/session/{id}/cars：该会话用过的车
		handleSessionCars(w, sessionID)
	case resourceType == "stints":
		// 原版 /api/ac/session/{id}/stints：该会话全部 stint（不带 result/ 前缀）
		handleSessionAllStints(w, sessionID)
	default:
		sendErrorResponse(w, http.StatusNotFound, "未知的会话资源", "session")
	}
}

// sessionDetail 返回单个会话详情
func sessionDetail(w http.ResponseWriter, sessionID int) {
	row := db.QueryRow(`SELECT `+sessionSelectColumns+` FROM session s WHERE s.session_id = ?`, sessionID)
	session, err := scanSessionRow(row)
	if err != nil {
		if err == sql.ErrNoRows {
			sendErrorResponse(w, http.StatusNotFound, "会话不存在", "sessionDetails")
			return
		}
		logger.Printf("查询会话失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "查询会话失败", "sessionDetails")
		return
	}
	sendSuccessResponse(w, "会话详情获取成功", "session", session)
}

// sessionGroup 榜单分组键；团队赛只按 teamID 归并
type sessionGroup struct {
	teamID int
	userID int
	carID  int
}

type groupAgg struct {
	laps      int
	validLaps int
	stintIDs  []int
	carID     int
	userID    int
}

// fetchSessionGroups 返回该会话下分组及聚合数据。
// 团队赛按 team_id 归并（队友的多个 user/car 合成一队），个人赛按 (team,user,car)。
func fetchSessionGroups(sessionID int, teamEvent bool) (map[sessionGroup]*groupAgg, []sessionGroup, error) {
	rows, err := db.Query(`
		SELECT COALESCE(tm.team_id, 0) AS team_id, COALESCE(ss.user_id, 0) AS user_id,
		       COALESCE(ss.car_id, 0) AS car_id, ss.session_stint_id,
		       COALESCE(ss.laps, 0), COALESCE(ss.valid_laps, 0)
		FROM session_stint ss
		LEFT JOIN team_member tm ON ss.team_member_id = tm.team_member_id
		WHERE ss.session_id = ?
		ORDER BY ss.session_stint_id ASC`, sessionID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	aggs := map[sessionGroup]*groupAgg{}
	var order []sessionGroup
	for rows.Next() {
		var teamID, userID, carID, stintID, laps, validLaps int
		if err := rows.Scan(&teamID, &userID, &carID, &stintID, &laps, &validLaps); err != nil {
			return nil, nil, err
		}
		key := sessionGroup{teamID: teamID, userID: userID, carID: carID}
		if teamEvent {
			key = sessionGroup{teamID: teamID}
		}
		a := aggs[key]
		if a == nil {
			a = &groupAgg{carID: carID, userID: userID}
			if teamEvent {
				a.userID = 0
			}
			aggs[key] = a
			order = append(order, key)
		} else if a.carID == 0 && carID != 0 {
			a.carID = carID
		}
		a.laps += laps
		a.validLaps += validLaps
		a.stintIDs = append(a.stintIDs, stintID)
	}
	return aggs, order, rows.Err()
}

// handleSessionStandings 处理 {type}/result/standings
func handleSessionStandings(w http.ResponseWriter, sessionID int, isRace bool) {
	if isRace {
		handleRaceStandings(w, sessionID)
		return
	}
	handleQualiStandings(w, sessionID)
}

type standingRaceEntry struct {
	TeamID    int   `json:"team_id"`
	UserID    int   `json:"user_id"`
	CarID     int   `json:"car_id"`
	Laps      int   `json:"laps"`
	ValidLaps int   `json:"valid_laps"`
	Gap       int   `json:"gap"`
	Interval  int   `json:"interval"`
	TotalTime int64 `json:"total_time"`
}

type standingQualiEntry struct {
	TeamID      int   `json:"team_id"`
	UserID      int   `json:"user_id"`
	CarID       int   `json:"car_id"`
	LapID       int   `json:"lap_id"`
	BestLapTime int64 `json:"best_lap_time"`
	Sector1     int64 `json:"sector_1"`
	Sector2     int64 `json:"sector_2"`
	Sector3     int64 `json:"sector_3"`
	ValidLaps   int   `json:"valid_laps"`
	Gap         int64 `json:"gap"`
	Interval    int64 `json:"interval"`
}

// raceGapBitmap 依据原版前端 Util.getGapFromBitmap 的约定编码差距：
// 偶数 = 同圈内的时间差（毫秒 × 2），奇数 = (落后圈数 × 2 + 1) 表示 “N L”。
func raceGapBitmap(baseLaps int, baseTime int64, laps int, totalTime int64) int {
	if laps == baseLaps {
		return int((totalTime - baseTime) * 2)
	}
	return (baseLaps-laps)*2 + 1
}

func handleRaceStandings(w http.ResponseWriter, sessionID int) {
	teamEvent, err := sessionIsTeamEvent(sessionID)
	if err != nil {
		logger.Printf("查询团队赛属性失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "查询失败", "standings")
		return
	}
	aggs, order, err := fetchSessionGroups(sessionID, teamEvent)
	if err != nil {
		logger.Printf("查询standings失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "查询失败", "standings")
		return
	}
	entries := make([]standingRaceEntry, 0, len(order))
	for _, g := range order {
		a := aggs[g]
		var totalTime sql.NullInt64
		_ = db.QueryRow(`SELECT SUM(time) FROM stint_lap WHERE stint_id IN (`+placeholders(len(a.stintIDs))+`)`, intsToArgs(a.stintIDs)...).Scan(&totalTime)
		carID := a.carID
		if !teamEvent {
			carID = g.carID
		}
		entries = append(entries, standingRaceEntry{
			TeamID:    g.teamID,
			UserID:    a.userID,
			CarID:     carID,
			Laps:      a.laps,
			ValidLaps: a.validLaps,
			TotalTime: totalTime.Int64,
		})
	}
	// 按圈数降序、总时间升序排名
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].Laps != entries[j].Laps {
			return entries[i].Laps > entries[j].Laps
		}
		return entries[i].TotalTime < entries[j].TotalTime
	})
	// gap/interval 位图编码（偶数=同圈时间差，奇数=落后圈数）
	for i := range entries {
		if i == 0 {
			continue
		}
		entries[i].Gap = raceGapBitmap(entries[0].Laps, entries[0].TotalTime, entries[i].Laps, entries[i].TotalTime)
		entries[i].Interval = raceGapBitmap(entries[i-1].Laps, entries[i-1].TotalTime, entries[i].Laps, entries[i].TotalTime)
	}
	sendSuccessResponse(w, "standings获取成功", "standings", entries)
}

func handleQualiStandings(w http.ResponseWriter, sessionID int) {
	teamEvent, err := sessionIsTeamEvent(sessionID)
	if err != nil {
		logger.Printf("查询团队赛属性失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "查询失败", "standings")
		return
	}
	rows, err := db.Query(`
		SELECT COALESCE(tm.team_id, 0) AS team_id, COALESCE(ss.user_id, 0) AS user_id,
		       COALESCE(ss.car_id, 0) AS car_id, sl.stint_lap_id,
		       COALESCE(sl.time, 0), COALESCE(sl.sector_1, 0), COALESCE(sl.sector_2, 0), COALESCE(sl.sector_3, 0)
		FROM stint_lap sl
		JOIN session_stint ss ON sl.stint_id = ss.session_stint_id
		LEFT JOIN team_member tm ON ss.team_member_id = tm.team_member_id
		WHERE ss.session_id = ? AND sl.cuts = 0 AND sl.crashes = 0 AND sl.car_crashes = 0 AND sl.time > 0
		ORDER BY sl.stint_lap_id ASC`, sessionID)
	if err != nil {
		logger.Printf("查询quali standings失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "查询失败", "standings")
		return
	}
	defer rows.Close()

	type key struct{ teamID, userID, carID int }
	type best struct {
		entry     standingQualiEntry
		validLaps int
	}
	bestByGroup := map[key]*best{}
	var order []key
	for rows.Next() {
		var teamID, userID, carID int
		var e standingQualiEntry
		if err := rows.Scan(&teamID, &userID, &carID, &e.LapID, &e.BestLapTime, &e.Sector1, &e.Sector2, &e.Sector3); err != nil {
			continue
		}
		k := key{teamID, userID, carID}
		if teamEvent {
			k = key{teamID: teamID}
		}
		e.TeamID, e.UserID, e.CarID = teamID, userID, carID
		if teamEvent {
			e.UserID = 0
		}
		b := bestByGroup[k]
		if b == nil {
			b = &best{entry: e}
			bestByGroup[k] = b
			order = append(order, k)
		}
		b.validLaps++
		if e.BestLapTime < b.entry.BestLapTime || b.entry.BestLapTime == 0 {
			b.entry = e
		} else if e.CarID != 0 && b.entry.CarID == 0 {
			b.entry.CarID = e.CarID
		}
	}

	entries := make([]standingQualiEntry, 0, len(order))
	for _, k := range order {
		e := bestByGroup[k].entry
		e.ValidLaps = bestByGroup[k].validLaps
		entries = append(entries, e)
	}
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].BestLapTime < entries[j].BestLapTime })
	if len(entries) > 0 {
		sessionBest := entries[0].BestLapTime
		for i := range entries {
			entries[i].Gap = entries[i].BestLapTime - sessionBest
			if i == 0 {
				entries[i].Interval = 0
			} else {
				entries[i].Interval = entries[i].BestLapTime - entries[i-1].BestLapTime
			}
		}
	}
	sendSuccessResponse(w, "standings获取成功", "standings", entries)
}

type sectorResult struct {
	TeamID         int   `json:"team_id"`
	UserID         int   `json:"user_id"`
	CarID          int   `json:"car_id"`
	BestSectorTime int64 `json:"best_sector_time"`
	Gap            int64 `json:"gap"`
	Interval       int64 `json:"interval"`
}

type sectorResponse struct {
	Sector1 []sectorResult `json:"sector1"`
	Sector2 []sectorResult `json:"sector2"`
	Sector3 []sectorResult `json:"sector3"`
}

// mergeSectorMin 把 possibly-null 的扇区值并入当前最小值（0 视为无成绩）
func mergeSectorMin(dst *int64, v sql.NullInt64) {
	if !v.Valid || v.Int64 <= 0 {
		return
	}
	if *dst == 0 || v.Int64 < *dst {
		*dst = v.Int64
	}
}

func handleSessionSectors(w http.ResponseWriter, sessionID int) {
	teamEvent, err := sessionIsTeamEvent(sessionID)
	if err != nil {
		logger.Printf("查询团队赛属性失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "查询失败", "sectors")
		return
	}
	rows, err := db.Query(`
		SELECT COALESCE(tm.team_id, 0) AS team_id, COALESCE(ss.user_id, 0) AS user_id,
		       COALESCE(ss.car_id, 0) AS car_id,
		       MIN(NULLIF(sl.sector_1, 0)) AS s1,
		       MIN(NULLIF(sl.sector_2, 0)) AS s2,
		       MIN(NULLIF(sl.sector_3, 0)) AS s3
		FROM stint_lap sl
		JOIN session_stint ss ON sl.stint_id = ss.session_stint_id
		LEFT JOIN team_member tm ON ss.team_member_id = tm.team_member_id
		WHERE ss.session_id = ?
		GROUP BY tm.team_id, ss.user_id, ss.car_id`, sessionID)
	if err != nil {
		logger.Printf("查询sectors失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "查询失败", "sectors")
		return
	}
	defer rows.Close()

	type key struct{ teamID, userID, carID int }
	type acc struct {
		carID, userID int
		s1, s2, s3    int64
	}
	byKey := map[key]*acc{}
	var order []key
	for rows.Next() {
		var teamID, userID, carID int
		var s1, s2, s3 sql.NullInt64
		if err := rows.Scan(&teamID, &userID, &carID, &s1, &s2, &s3); err != nil {
			continue
		}
		k := key{teamID, userID, carID}
		if teamEvent {
			k = key{teamID: teamID}
		}
		a := byKey[k]
		if a == nil {
			a = &acc{carID: carID, userID: userID}
			if teamEvent {
				a.userID = 0
			}
			byKey[k] = a
			order = append(order, k)
		} else if a.carID == 0 && carID != 0 {
			a.carID = carID
		}
		mergeSectorMin(&a.s1, s1)
		mergeSectorMin(&a.s2, s2)
		mergeSectorMin(&a.s3, s3)
	}

	resp := sectorResponse{Sector1: []sectorResult{}, Sector2: []sectorResult{}, Sector3: []sectorResult{}}
	for _, k := range order {
		a := byKey[k]
		if a.s1 > 0 {
			resp.Sector1 = append(resp.Sector1, sectorResult{TeamID: k.teamID, UserID: a.userID, CarID: a.carID, BestSectorTime: a.s1})
		}
		if a.s2 > 0 {
			resp.Sector2 = append(resp.Sector2, sectorResult{TeamID: k.teamID, UserID: a.userID, CarID: a.carID, BestSectorTime: a.s2})
		}
		if a.s3 > 0 {
			resp.Sector3 = append(resp.Sector3, sectorResult{TeamID: k.teamID, UserID: a.userID, CarID: a.carID, BestSectorTime: a.s3})
		}
	}
	fillSectorGaps(resp.Sector1)
	fillSectorGaps(resp.Sector2)
	fillSectorGaps(resp.Sector3)
	sendSuccessResponse(w, "sectors获取成功", "sectors", resp)
}

func fillSectorGaps(list []sectorResult) {
	sort.SliceStable(list, func(i, j int) bool { return list[i].BestSectorTime < list[j].BestSectorTime })
	for i := range list {
		if i == 0 {
			continue
		}
		list[i].Gap = list[i].BestSectorTime - list[0].BestSectorTime
		list[i].Interval = list[i].BestSectorTime - list[i-1].BestSectorTime
	}
}

type consistencyEntry struct {
	TeamID   int     `json:"team_id"`
	UserID   int     `json:"user_id"`
	CarID    int     `json:"car_id"`
	LapTimes []int64 `json:"lap_times"`
}

func handleSessionConsistency(w http.ResponseWriter, sessionID int) {
	teamEvent, err := sessionIsTeamEvent(sessionID)
	if err != nil {
		logger.Printf("查询团队赛属性失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "查询失败", "laps")
		return
	}
	rows, err := db.Query(`
		SELECT COALESCE(tm.team_id, 0) AS team_id, COALESCE(ss.user_id, 0) AS user_id,
		       COALESCE(ss.car_id, 0) AS car_id, COALESCE(sl.time, 0)
		FROM stint_lap sl
		JOIN session_stint ss ON sl.stint_id = ss.session_stint_id
		LEFT JOIN team_member tm ON ss.team_member_id = tm.team_member_id
		WHERE ss.session_id = ? AND sl.time > 0
		ORDER BY sl.stint_lap_id ASC`, sessionID)
	if err != nil {
		logger.Printf("查询laps失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "查询失败", "laps")
		return
	}
	defer rows.Close()

	type key struct{ teamID, userID, carID int }
	byKey := map[key]*consistencyEntry{}
	var order []key
	for rows.Next() {
		var teamID, userID, carID int
		var t int64
		if err := rows.Scan(&teamID, &userID, &carID, &t); err != nil {
			continue
		}
		k := key{teamID, userID, carID}
		if teamEvent {
			k = key{teamID: teamID}
		}
		e := byKey[k]
		if e == nil {
			e = &consistencyEntry{TeamID: teamID, UserID: userID, CarID: carID, LapTimes: []int64{}}
			if teamEvent {
				e.UserID = 0
			}
			byKey[k] = e
			order = append(order, k)
		} else if e.CarID == 0 && carID != 0 {
			e.CarID = carID
		}
		e.LapTimes = append(e.LapTimes, t)
	}
	entries := make([]consistencyEntry, 0, len(order))
	for _, k := range order {
		entries = append(entries, *byKey[k])
	}
	sendSuccessResponse(w, "laps获取成功", "laps", entries)
}

type pitstopEntry struct {
	TeamID  int   `json:"team_id"`
	UserID  int   `json:"user_id"`
	CarID   int   `json:"car_id"`
	PitTime int64 `json:"pit_time"`
}

func handleSessionPitstops(w http.ResponseWriter, sessionID int) {
	teamEvent, _ := sessionIsTeamEvent(sessionID)

	// 团队赛需要 team_id：用会话内用户的 team_member 关系补全
	teamByUser := map[int]int{}
	if teamEvent {
		relRows, err := db.Query(`
			SELECT DISTINCT tm.user_id, tm.team_id
			FROM session_stint ss
			JOIN team_member tm ON ss.team_member_id = tm.team_member_id
			WHERE ss.session_id = ?`, sessionID)
		if err == nil {
			for relRows.Next() {
				var uid, tid int
				if err := relRows.Scan(&uid, &tid); err == nil {
					teamByUser[uid] = tid
				}
			}
			relRows.Close()
		}
	}

	rows, err := db.Query("SELECT detail FROM session_feed WHERE session_id = ? AND type = 12 ORDER BY session_feed_id ASC", sessionID)
	if err != nil {
		logger.Printf("查询pitstops失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "查询失败", "pitstops")
		return
	}
	defer rows.Close()

	pitstops := []pitstopEntry{}
	for rows.Next() {
		var detail sql.NullString
		if err := rows.Scan(&detail); err != nil {
			continue
		}
		var d struct {
			UserID  int   `json:"user_id"`
			CarID   int   `json:"car_id"`
			PitTime int64 `json:"pit_time"`
		}
		if err := json.Unmarshal([]byte(detail.String), &d); err != nil {
			continue
		}
		entry := pitstopEntry{UserID: d.UserID, CarID: d.CarID, PitTime: d.PitTime}
		if teamEvent {
			entry.TeamID = teamByUser[d.UserID]
		}
		pitstops = append(pitstops, entry)
	}
	sendSuccessResponse(w, "pitstops获取成功", "pitstops", pitstops)
}

type stintLapEntry struct {
	LapID      int     `json:"lap_id"`
	LapTime    int64   `json:"lap_time"`
	Sector1    int64   `json:"sector_1"`
	Sector2    int64   `json:"sector_2"`
	Sector3    int64   `json:"sector_3"`
	Grip       float64 `json:"grip"`
	Tyre       string  `json:"tyre"`
	AvgSpeed   int     `json:"avg_speed"`
	MaxSpeed   int     `json:"max_speed"`
	Cuts       int     `json:"cuts"`
	Crashes    int     `json:"crashes"`
	CarCrashes int     `json:"car_crashes"`
	FinishAt   int64   `json:"finish_at"`
	BestLap    bool    `json:"best_lap"`
}

type singleStintEntry struct {
	UserID      int             `json:"user_id"`
	TotalLaps   int             `json:"total_laps"`
	ValidLaps   int             `json:"valid_laps"`
	BestLapTime int64           `json:"best_lap_time"`
	AvgLapTime  int64           `json:"avg_lap_time"`
	AvgLapGap   int64           `json:"avg_lap_gap"`
	Laps        []stintLapEntry `json:"laps"`
}

type stintsWrapper struct {
	Stints []singleStintEntry `json:"stints"`
}

// handleSessionStints 处理 result/stints/{team|user}/...
func handleSessionStints(w http.ResponseWriter, sessionID, userID, carID, teamID int) {
	teamMode := teamID > 0
	var rows *sql.Rows
	var err error
	if teamMode {
		// 兼容 writer 可能把 team_member_id 记为 0 的情况，同时按 user_id 归属团队
		rows, err = db.Query(`SELECT session_stint_id, user_id, car_id, COALESCE(laps, 0), COALESCE(valid_laps, 0), best_lap_id
			FROM session_stint
			WHERE session_id = ? AND (
				team_member_id IN (SELECT team_member_id FROM team_member WHERE team_id = ?)
				OR user_id IN (SELECT user_id FROM team_member WHERE team_id = ?)
			)
			ORDER BY session_stint_id ASC`, sessionID, teamID, teamID)
	} else {
		rows, err = db.Query(`SELECT session_stint_id, user_id, car_id, COALESCE(laps, 0), COALESCE(valid_laps, 0), best_lap_id
			FROM session_stint
			WHERE session_id = ? AND COALESCE(user_id, 0) = ? AND COALESCE(car_id, 0) = ?
			ORDER BY session_stint_id ASC`, sessionID, userID, carID)
	}
	if err != nil {
		logger.Printf("查询stints失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "查询失败", "stints")
		return
	}
	defer rows.Close()

	out := stintsWrapper{Stints: []singleStintEntry{}}
	for rows.Next() {
		var stintID int
		var uid, cid sql.NullInt64
		var laps, validLaps int
		var bestLapID sql.NullInt64
		if err := rows.Scan(&stintID, &uid, &cid, &laps, &validLaps, &bestLapID); err != nil {
			continue
		}
		entry := singleStintEntry{UserID: int(uid.Int64), TotalLaps: laps, ValidLaps: validLaps, Laps: []stintLapEntry{}}

		lapRows, err := db.Query(`SELECT stint_lap_id, COALESCE(time, 0), COALESCE(sector_1, 0), COALESCE(sector_2, 0), COALESCE(sector_3, 0),
			COALESCE(grip, -1), COALESCE(tyre, ''), COALESCE(avg_speed, 0), COALESCE(max_speed, 0),
			COALESCE(cuts, 0), COALESCE(crashes, 0), COALESCE(car_crashes, 0),
			CAST(UNIX_TIMESTAMP(finished_at) * 1000000 AS SIGNED)
			FROM stint_lap WHERE stint_id = ? ORDER BY stint_lap_id ASC`, stintID)
		if err != nil {
			continue
		}
		var sumTime int64
		var count int
		for lapRows.Next() {
			var l stintLapEntry
			var fin sql.NullInt64
			if err := lapRows.Scan(&l.LapID, &l.LapTime, &l.Sector1, &l.Sector2, &l.Sector3, &l.Grip, &l.Tyre,
				&l.AvgSpeed, &l.MaxSpeed, &l.Cuts, &l.Crashes, &l.CarCrashes, &fin); err != nil {
				continue
			}
			l.FinishAt = fin.Int64
			if l.LapTime > 0 {
				if entry.BestLapTime == 0 || l.LapTime < entry.BestLapTime {
					entry.BestLapTime = l.LapTime
				}
				sumTime += l.LapTime
				count++
			}
			if bestLapID.Valid && int64(l.LapID) == bestLapID.Int64 {
				l.BestLap = true
			}
			entry.Laps = append(entry.Laps, l)
		}
		lapRows.Close()
		if count > 0 {
			entry.AvgLapTime = sumTime / int64(count)
			entry.AvgLapGap = entry.AvgLapTime - entry.BestLapTime
		}
		out.Stints = append(out.Stints, entry)
	}
	sendSuccessResponse(w, "stints获取成功", "stints", out)
}

// ---- 小工具 ----

func placeholders(n int) string {
	if n <= 0 {
		return "NULL"
	}
	parts := make([]string, n)
	for i := range parts {
		parts[i] = "?"
	}
	return strings.Join(parts, ",")
}

func intsToArgs(vals []int) []interface{} {
	args := make([]interface{}, len(vals))
	for i, v := range vals {
		args[i] = v
	}
	return args
}

func parseTrailingInt(s string) int {
	parts := strings.Split(strings.Trim(s, "/"), "/")
	for i := len(parts) - 1; i >= 0; i-- {
		if n, err := strconv.Atoi(parts[i]); err == nil {
			return n
		}
	}
	return 0
}

// parseUserCar 解析 .../user/{uid}/car/{cid}
func parseUserCar(s string) (int, int) {
	parts := strings.Split(strings.Trim(s, "/"), "/")
	uid, cid := 0, 0
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == "user" {
			uid, _ = strconv.Atoi(parts[i+1])
		} else if parts[i] == "car" {
			cid, _ = strconv.Atoi(parts[i+1])
		}
	}
	return uid, cid
}

// handleSessionCars 返回该会话用过的车（原版 /api/ac/session/{id}/cars）。
func handleSessionCars(w http.ResponseWriter, sessionID int) {
	rows, err := db.Query(`SELECT DISTINCT c.car_id, c.name, COALESCE(c.display_name, ''),
		COALESCE(c.manufacturer, ''), COALESCE(c.car_class, '')
		FROM session_stint ss JOIN car c ON ss.car_id = c.car_id
		WHERE ss.session_id = ?`, sessionID)
	if err != nil {
		logger.Printf("会话车辆查询失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "车辆数据查询失败", "cars")
		return
	}
	defer rows.Close()

	cars := []ACCar{}
	for rows.Next() {
		var c ACCar
		if err := rows.Scan(&c.CarID, &c.Name, &c.DisplayName, &c.Manufacturer, &c.CarClass); err != nil {
			logger.Printf("会话车辆行扫描失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "车辆数据解析失败", "cars")
			return
		}
		cars = append(cars, c)
	}
	if err := rows.Err(); err != nil {
		logger.Printf("会话车辆行迭代错误: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "车辆数据处理失败", "cars")
		return
	}
	sendSuccessResponse(w, "会话车辆数据获取成功", "cars", cars)
}

// sessionStintRow 是原版 /api/ac/session/{id}/stints 的单条记录。
// 字段顺序与 session_stint 表一致，时间列可空。
type sessionStintRow struct {
	SessionStintID int    `json:"session_stint_id"`
	UserID         int    `json:"user_id"`
	TeamMemberID   int    `json:"team_member_id"`
	SessionID      int    `json:"session_id"`
	CarID          int    `json:"car_id"`
	GameCarID      int    `json:"game_car_id"`
	Laps           int    `json:"laps"`
	ValidLaps      int    `json:"valid_laps"`
	BestLapID      int    `json:"best_lap_id"`
	IsFinished     int    `json:"is_finished"`
	StartedAt      string `json:"started_at"`
	FinishedAt     string `json:"finished_at"`
}

// handleSessionAllStints 返回该会话全部 stint（原版 /api/ac/session/{id}/stints，无 result/ 前缀）。
func handleSessionAllStints(w http.ResponseWriter, sessionID int) {
	rows, err := db.Query(`SELECT session_stint_id, COALESCE(user_id, 0), COALESCE(team_member_id, 0),
		COALESCE(session_id, 0), COALESCE(car_id, 0), COALESCE(game_car_id, 0),
		COALESCE(laps, 0), COALESCE(valid_laps, 0), COALESCE(best_lap_id, 0),
		COALESCE(is_finished, 0),
		COALESCE(DATE_FORMAT(started_at, '%Y-%m-%d %H:%i:%s'), ''),
		COALESCE(DATE_FORMAT(finished_at, '%Y-%m-%d %H:%i:%s'), '')
		FROM session_stint WHERE session_id = ? ORDER BY session_stint_id ASC`, sessionID)
	if err != nil {
		logger.Printf("会话 stint 查询失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "stint 查询失败", "session_stints")
		return
	}
	defer rows.Close()

	stints := []sessionStintRow{}
	for rows.Next() {
		var s sessionStintRow
		if err := rows.Scan(&s.SessionStintID, &s.UserID, &s.TeamMemberID, &s.SessionID, &s.CarID,
			&s.GameCarID, &s.Laps, &s.ValidLaps, &s.BestLapID, &s.IsFinished, &s.StartedAt, &s.FinishedAt); err != nil {
			logger.Printf("会话 stint 行扫描失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "stint 数据解析失败", "session_stints")
			return
		}
		stints = append(stints, s)
	}
	if err := rows.Err(); err != nil {
		logger.Printf("会话 stint 行迭代错误: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "stint 数据处理失败", "session_stints")
		return
	}
	sendSuccessResponse(w, "会话 stint 数据获取成功", "session_stints", stints)
}
