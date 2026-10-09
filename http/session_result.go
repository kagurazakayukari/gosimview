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
// 会话类子资源统一走 /api/ac/session/{id}/{resource}。

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
	default:
		sendErrorResponse(w, http.StatusNotFound, "未知的会话资源", "session")
	}
}

// sessionDetail 返回单个会话详情
func sessionDetail(w http.ResponseWriter, sessionID int) {
	session := ACSession{}
	err := db.QueryRow("SELECT session_id, event_id, type, track_time, name, CAST(UNIX_TIMESTAMP(start_time) * 1000000 AS SIGNED) AS start_time, duration_min, elapsed_ms, laps, weather, air_temp, road_temp, start_grip, current_grip, is_finished, CAST(UNIX_TIMESTAMP(finish_time) * 1000000 AS SIGNED) AS finish_time, CAST(UNIX_TIMESTAMP(last_activity) * 1000000 AS SIGNED) AS last_activity FROM session WHERE session_id = ?", sessionID).Scan(
		&session.SessionID, &session.EventID, &session.Type, &session.TrackTime, &session.Name, &session.StartTime, &session.DurationMin, &session.ElapsedMs, &session.Laps, &session.Weather, &session.AirTemp, &session.RoadTemp, &session.StartGrip, &session.CurrentGrip, &session.IsFinished, &session.FinishTime, &session.LastActivity)
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

type sessionGroup struct {
	teamID int
	userID int
	carID  int
}

// fetchSessionGroups 返回该会话下 (team_id, user_id, car_id) 分组及聚合数据
func fetchSessionGroups(sessionID int) (map[sessionGroup]*groupAgg, []sessionGroup, error) {
	rows, err := db.Query(`
		SELECT COALESCE(tm.team_id, 0) AS team_id, ss.user_id, ss.car_id, ss.session_stint_id, ss.laps, ss.valid_laps
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
		var g sessionGroup
		var stintID, laps, validLaps int
		if err := rows.Scan(&g.teamID, &g.userID, &g.carID, &stintID, &laps, &validLaps); err != nil {
			return nil, nil, err
		}
		a := aggs[g]
		if a == nil {
			a = &groupAgg{}
			aggs[g] = a
			order = append(order, g)
		}
		a.laps += laps
		a.validLaps += validLaps
		a.stintIDs = append(a.stintIDs, stintID)
	}
	return aggs, order, rows.Err()
}

type groupAgg struct {
	laps      int
	validLaps int
	stintIDs  []int
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

func handleRaceStandings(w http.ResponseWriter, sessionID int) {
	aggs, order, err := fetchSessionGroups(sessionID)
	if err != nil {
		logger.Printf("查询standings失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "查询失败", "standings")
		return
	}
	entries := make([]standingRaceEntry, 0, len(order))
	for _, g := range order {
		a := aggs[g]
		var totalTime sql.NullInt64
		db.QueryRow(`SELECT SUM(time) FROM stint_lap WHERE stint_id IN (`+placeholders(len(a.stintIDs))+`)`, intsToArgs(a.stintIDs)...).Scan(&totalTime)
		entries = append(entries, standingRaceEntry{
			TeamID: g.teamID, UserID: g.userID, CarID: g.carID,
			Laps: a.laps, ValidLaps: a.validLaps, TotalTime: totalTime.Int64,
		})
	}
	// 按圈数降序、总时间升序排名
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].Laps != entries[j].Laps {
			return entries[i].Laps > entries[j].Laps
		}
		return entries[i].TotalTime < entries[j].TotalTime
	})
	// gap/interval 用位图编码（偶数=同圈，奇数=落后 N 圈）
	leaderLaps := 0
	if len(entries) > 0 {
		leaderLaps = entries[0].Laps
	}
	for i := range entries {
		if i == 0 {
			continue
		}
		lapDiff := leaderLaps - entries[i].Laps
		if lapDiff == 0 {
			entries[i].Gap = 0
		} else {
			entries[i].Gap = (lapDiff << 1) | 1
		}
		prevDiff := entries[i-1].Laps - entries[i].Laps
		if prevDiff == 0 {
			entries[i].Interval = 0
		} else {
			entries[i].Interval = (prevDiff << 1) | 1
		}
	}
	sendSuccessResponse(w, "standings获取成功", "standings", entries)
}

func handleQualiStandings(w http.ResponseWriter, sessionID int) {
	rows, err := db.Query(`
		SELECT COALESCE(tm.team_id, 0) AS team_id, ss.user_id, ss.car_id,
		       sl.stint_lap_id, sl.time, sl.sector_1, sl.sector_2, sl.sector_3
		FROM stint_lap sl
		JOIN session_stint ss ON sl.stint_id = ss.session_stint_id
		LEFT JOIN team_member tm ON ss.team_member_id = tm.team_member_id
		WHERE ss.session_id = ? AND sl.cuts = 0 AND sl.crashes = 0 AND sl.time > 0
		ORDER BY sl.stint_lap_id ASC`, sessionID)
	if err != nil {
		logger.Printf("查询quali standings失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "查询失败", "standings")
		return
	}
	defer rows.Close()

	type best struct {
		entry     standingQualiEntry
		validLaps int
	}
	bestByGroup := map[sessionGroup]*best{}
	var order []sessionGroup
	for rows.Next() {
		var g sessionGroup
		var e standingQualiEntry
		if err := rows.Scan(&g.teamID, &g.userID, &g.carID, &e.LapID, &e.BestLapTime, &e.Sector1, &e.Sector2, &e.Sector3); err != nil {
			continue
		}
		b := bestByGroup[g]
		if b == nil {
			e.TeamID, e.UserID, e.CarID = g.teamID, g.userID, g.carID
			b = &best{entry: e}
			bestByGroup[g] = b
			order = append(order, g)
		}
		b.validLaps++
		if e.BestLapTime < b.entry.BestLapTime || b.entry.BestLapTime == 0 {
			e.TeamID, e.UserID, e.CarID = g.teamID, g.userID, g.carID
			b.entry = e
		}
	}

	entries := make([]standingQualiEntry, 0, len(order))
	for _, g := range order {
		e := bestByGroup[g].entry
		e.ValidLaps = bestByGroup[g].validLaps
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

func handleSessionSectors(w http.ResponseWriter, sessionID int) {
	rows, err := db.Query(`
		SELECT COALESCE(tm.team_id, 0) AS team_id, ss.user_id, ss.car_id,
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

	resp := sectorResponse{Sector1: []sectorResult{}, Sector2: []sectorResult{}, Sector3: []sectorResult{}}
	for rows.Next() {
		var g sessionGroup
		var s1, s2, s3 sql.NullInt64
		if err := rows.Scan(&g.teamID, &g.userID, &g.carID, &s1, &s2, &s3); err != nil {
			continue
		}
		if s1.Valid {
			resp.Sector1 = append(resp.Sector1, sectorResult{TeamID: g.teamID, UserID: g.userID, CarID: g.carID, BestSectorTime: s1.Int64})
		}
		if s2.Valid {
			resp.Sector2 = append(resp.Sector2, sectorResult{TeamID: g.teamID, UserID: g.userID, CarID: g.carID, BestSectorTime: s2.Int64})
		}
		if s3.Valid {
			resp.Sector3 = append(resp.Sector3, sectorResult{TeamID: g.teamID, UserID: g.userID, CarID: g.carID, BestSectorTime: s3.Int64})
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
	rows, err := db.Query(`
		SELECT COALESCE(tm.team_id, 0) AS team_id, ss.user_id, ss.car_id, sl.time
		FROM stint_lap sl
		JOIN session_stint ss ON sl.stint_id = ss.session_stint_id
		LEFT JOIN team_member tm ON ss.team_member_id = tm.team_member_id
		WHERE ss.session_id = ? AND sl.time > 0
		ORDER BY ss.user_id ASC, ss.car_id ASC, sl.stint_lap_id ASC`, sessionID)
	if err != nil {
		logger.Printf("查询laps失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "查询失败", "laps")
		return
	}
	defer rows.Close()

	byGroup := map[sessionGroup]*consistencyEntry{}
	var order []sessionGroup
	for rows.Next() {
		var g sessionGroup
		var t int64
		if err := rows.Scan(&g.teamID, &g.userID, &g.carID, &t); err != nil {
			continue
		}
		e := byGroup[g]
		if e == nil {
			e = &consistencyEntry{TeamID: g.teamID, UserID: g.userID, CarID: g.carID, LapTimes: []int64{}}
			byGroup[g] = e
			order = append(order, g)
		}
		e.LapTimes = append(e.LapTimes, t)
	}
	entries := make([]consistencyEntry, 0, len(order))
	for _, g := range order {
		entries = append(entries, *byGroup[g])
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
	rows, err := db.Query("SELECT detail FROM session_feed WHERE session_id = ? AND type = 12 ORDER BY session_feed_id ASC", sessionID)
	if err != nil {
		logger.Printf("查询pitstops失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "查询失败", "pitstops")
		return
	}
	defer rows.Close()

	pitstops := []pitstopEntry{}
	for rows.Next() {
		var detail string
		if err := rows.Scan(&detail); err != nil {
			continue
		}
		var d struct {
			UserID  int   `json:"user_id"`
			CarID   int   `json:"car_id"`
			PitTime int64 `json:"pit_time"`
		}
		if err := json.Unmarshal([]byte(detail), &d); err != nil {
			continue
		}
		pitstops = append(pitstops, pitstopEntry{UserID: d.UserID, CarID: d.CarID, PitTime: d.PitTime})
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
	IsBestLap  bool    `json:"is_best_lap"`
}

type singleStintEntry struct {
	UserID      int            `json:"user_id"`
	TotalLaps   int            `json:"total_laps"`
	ValidLaps   int            `json:"valid_laps"`
	BestLapTime int64          `json:"best_lap_time"`
	AvgLapTime  int64          `json:"avg_lap_time"`
	AvgLapGap   int64          `json:"avg_lap_gap"`
	Laps        []stintLapEntry `json:"laps"`
}

type stintsWrapper struct {
	Stints []singleStintEntry `json:"stints"`
}

// handleSessionStints 处理 result/stints/{team|user}/...；teamMode 为 true 时按 teamID 过滤
func handleSessionStints(w http.ResponseWriter, sessionID, userID, carID, teamID int) {
	teamMode := teamID > 0
	var rows *sql.Rows
	var err error
	if teamMode {
		rows, err = db.Query(`SELECT session_stint_id, user_id, car_id, laps, valid_laps, best_lap_id
			FROM session_stint
			WHERE session_id = ? AND team_member_id IN (SELECT team_member_id FROM team_member WHERE team_id = ?)
			ORDER BY session_stint_id ASC`, sessionID, teamID)
	} else {
		rows, err = db.Query(`SELECT session_stint_id, user_id, car_id, laps, valid_laps, best_lap_id
			FROM session_stint
			WHERE session_id = ? AND user_id = ? AND car_id = ?
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
		var stintID, uid, cid, laps, validLaps int
		var bestLapID sql.NullInt64
		if err := rows.Scan(&stintID, &uid, &cid, &laps, &validLaps, &bestLapID); err != nil {
			continue
		}
		entry := singleStintEntry{UserID: uid, TotalLaps: laps, ValidLaps: validLaps, Laps: []stintLapEntry{}}

		lapRows, err := db.Query(`SELECT stint_lap_id, time, sector_1, sector_2, sector_3, grip, tyre,
			avg_speed, max_speed, cuts, crashes, car_crashes,
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
				l.IsBestLap = true
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
