// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// sanitize 清理字符串，移除文件名中不允许的字符
func sanitize(s string) string {
	// 替换文件名中不允许的字符为下划线
	reg := regexp.MustCompile(`[<>:"/\\|?*]`)
	return reg.ReplaceAllString(s, "_")
}

// 处理AC事件列表请求
func handleACEvents(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) >= 7 && pathParts[5] == "session" && pathParts[6] == "latest" {
		eventIDsStr := pathParts[4]
		eventIDs := strings.Split(eventIDsStr, ",")

		if len(eventIDs) == 0 || eventIDs[0] == "" {
			sendErrorResponse(w, http.StatusBadRequest, "事件ID不能为空", "events")
			return
		}

		for _, id := range eventIDs {
			if _, err := strconv.Atoi(id); err != nil {
				sendErrorResponse(w, http.StatusBadRequest, "无效的事件ID格式", "events")
				return
			}
		}
		// 从WebSocket获取实时会话数据
		liveEventsMutex.Lock()
		defer liveEventsMutex.Unlock()

		var sessions []ACSession
		// 过滤匹配当前eventIDs的会话
		if liveEventsData.Session.EventID > 0 {
			for _, idStr := range eventIDs {
				id, _ := strconv.Atoi(idStr)
				if liveEventsData.Session.EventID == id {
					sessions = append(sessions, liveEventsData.Session)
					break
				}
			}
		}
		sendSuccessResponse(w, "最新事件会话获取成功", "sessions", sessions)
		return
	}

	// 原handleACEvents函数逻辑

	rows, err := db.Query(`SELECT event_id, name, server_name, track_config_id, practice_duration, quali_duration, race_duration, race_duration_type, race_extra_laps, reverse_grid_positions FROM event ORDER BY event_id DESC`)
	if err != nil {
		logger.Printf("AC事件查询SQL错误: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "数据库查询失败", "events")
		return
	}
	defer rows.Close()

	events := []ACEvent{}
	for rows.Next() {
		e := ACEvent{}
		if err := rows.Scan(&e.EventID, &e.Name, &e.ServerName, &e.TrackConfigID, &e.PracticeDuration, &e.QualiDuration, &e.RaceDuration, &e.RaceDurationType, &e.RaceExtraLaps, &e.ReverseGridPositions); err != nil {
			logger.Printf("数据库行扫描失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "数据解析失败", "events")
			return
		}
		events = append(events, e)
	}

	if err = rows.Err(); err != nil {
		logger.Printf("行迭代错误: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "行迭代错误", "events")
		return
	}

	sendSuccessResponse(w, "AC事件数据获取成功", "events", events)
}

// 处理AC活跃事件请求
func handleACEventsLive(w http.ResponseWriter, r *http.Request) {
	// 从WebSocket获取活跃事件数据
	events, err := getLiveEventsFromWebSocket()
	if err != nil {
		logger.Printf("获取活跃事件失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "获取事件数据失败", "events")
		return
	}

	// 直接返回WebSocket接收的事件数据
	sendSuccessResponse(w, "AC活跃事件数据获取成功", "events", events)
}

// 处理AC团队列表请求
func handleACTeams(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 4 || pathParts[2] != "ac" || pathParts[3] != "teams" {
		sendErrorResponse(w, http.StatusBadRequest, "无效的URL格式", "teams")
		return
	}

	// 检查是否提供了ID参数
	var query string
	var args []interface{}
	if len(pathParts) >= 5 && pathParts[4] != "" {
		idStr := pathParts[4]
		ids := strings.Split(idStr, ",")

		placeholders := make([]string, len(ids))
		args = make([]interface{}, len(ids))
		for i, id := range ids {
			// Convert string ID to integer
			idInt, err := strconv.Atoi(id)
			if err != nil {
				sendErrorResponse(w, http.StatusBadRequest, "Invalid team ID format: "+id, "teams")
				return
			}
			placeholders[i] = "?"
			args[i] = idInt
		}

		query = fmt.Sprintf("SELECT team_id, name, car_id, team_no, livery_name FROM team WHERE team_id IN (%s)", strings.Join(placeholders, ","))
	} else {
		// 如果没有提供ID参数，返回所有团队
		query = "SELECT team_id, name, car_id, team_no, livery_name FROM team"
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		logger.Printf("数据库查询失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "数据库查询失败", "teams")
		return
	}
	defer rows.Close()

	var teams []ACTeam
	for rows.Next() {
		var t ACTeam = ACTeam{}
		if err := rows.Scan(&t.TeamID, &t.Name, &t.CarID, &t.TeamNo, &t.LiveryName); err != nil {
			logger.Printf("数据库行扫描失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "数据解析失败", "teams")
			return
		}
		teams = append(teams, t)
	}

	if err = rows.Err(); err != nil {
		logger.Printf("行迭代错误: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "数据处理失败", "teams")
		return
	}

	sendSuccessResponse(w, "团队数据获取成功", "teams", teams)
}

// 处理AC车辆列表请求
func handleACCars(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 4 || pathParts[2] != "ac" || (pathParts[3] != "cars" && pathParts[3] != "car") {
		sendErrorResponse(w, http.StatusBadRequest, "无效的URL格式", "cars")
		return
	}

	// 检查是否提供了ID参数
	var query string
	var args []interface{}
	if len(pathParts) >= 5 && pathParts[4] != "" {
		idStr := pathParts[4]
		ids := strings.Split(idStr, ",")

		placeholders := make([]string, len(ids))
		args = make([]interface{}, len(ids))
		for i, id := range ids {
			placeholders[i] = "?"
			args[i] = id
		}

		query = fmt.Sprintf("SELECT car_id, name, display_name, manufacturer, car_class FROM car WHERE car_id IN (%s)", strings.Join(placeholders, ","))
		// Validate single ID format for /api/ac/car/
		if pathParts[3] == "car" && len(ids) > 1 {
			sendErrorResponse(w, http.StatusBadRequest, "单个车辆查询不支持多个ID", "car")
			return
		}
	} else {
		// 如果没有提供ID参数，返回所有车辆
		query = "SELECT car_id, name, display_name, manufacturer, car_class FROM car"
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		logger.Printf("数据库查询失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "数据库查询失败", "cars")
		return
	}
	defer rows.Close()

	var cars []ACCar
	for rows.Next() {
		var c ACCar = ACCar{}
		if err := rows.Scan(&c.CarID, &c.Name, &c.DisplayName, &c.Manufacturer, &c.CarClass); err != nil {
			logger.Printf("数据库行扫描失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "数据解析失败", "cars")
			return
		}
		cars = append(cars, c)
	}

	if err = rows.Err(); err != nil {
		logger.Printf("行迭代错误: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "数据处理失败", "cars")
		return
	}

	// Return single car object for /api/ac/car/ endpoint
	if pathParts[3] == "car" {
		if len(cars) > 0 {
			sendSuccessResponse(w, "车辆数据获取成功", "car", cars[0])
		} else {
			sendErrorResponse(w, http.StatusNotFound, "车辆数据未找到", "car")
		}
	} else {
		sendSuccessResponse(w, "车辆数据获取成功", "cars", cars)
	}
}

// 处理AC用户列表
func handleACUsers(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 4 || pathParts[2] != "ac" {
		sendErrorResponse(w, http.StatusBadRequest, "无效的URL格式", "users")
		return
	}

	// 处理单个用户详情请求
	if pathParts[3] == "user" && len(pathParts) >= 5 && pathParts[4] != "" {
		handleACUserDetails(w, r)
		return
	}

	// 处理用户列表请求
	if pathParts[3] != "users" {
		sendErrorResponse(w, http.StatusBadRequest, "无效的URL格式", "users")
		return
	}

	// 检查是否提供了ID参数
	var query string
	var args []interface{}
	if len(pathParts) >= 5 && pathParts[4] != "" {
		idStr := pathParts[4]
		ids := strings.Split(idStr, ",")

		placeholders := make([]string, len(ids))
		args = make([]interface{}, len(ids))
		for i, id := range ids {
			placeholders[i] = "?"
			args[i] = id
		}

		query = fmt.Sprintf("SELECT user_id, name FROM user WHERE user_id IN (%s) ORDER BY user_id ASC", strings.Join(placeholders, ","))
	} else {
		// 如果没有提供ID参数，返回所有用户
		query = "SELECT user_id, name FROM user ORDER BY user_id ASC"
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		logger.Printf("数据库查询失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "数据库查询失败", "users")
		return
	}
	defer rows.Close()

	var users []ACUser
	for rows.Next() {
		u := ACUser{}
		if err := rows.Scan(&u.UserID, &u.Name); err != nil {
			logger.Printf("数据库行扫描失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "数据解析失败", "users")
			return
		}
		users = append(users, u)
	}

	if err = rows.Err(); err != nil {
		logger.Printf("行迭代错误: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "数据处理失败", "users")
		return
	}

	sendSuccessResponse(w, "用户数据获取成功", "users", users)
}

// 处理单个用户详情请求
func handleACUserDetails(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")
	driverId := pathParts[4]
	hasSummary := false
	if len(pathParts) > 5 && strings.ToLower(pathParts[5]) == "summary" {
		hasSummary = true
	}

	// 基础用户信息
	user := ACUser{}
	err := db.QueryRow("SELECT user_id, name, prev_name, country FROM user WHERE user_id = ?", driverId).Scan(
		&user.UserID, &user.Name, &user.PrevName, &user.Country)

	if err != nil {
		if err == sql.ErrNoRows {
			sendErrorResponse(w, http.StatusNotFound, "用户数据未找到", "user")
		} else {
			logger.Printf("数据库查询失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "数据库查询失败", "user")
		}
		return
	}

	if !hasSummary {
		sendSuccessResponse(w, "用户详情数据获取成功", "user", user)
		return
	}

	// 定义所需结构体

	// 查询用户参与的事件列表及统计
	eventsQuery := `
		SELECT 
			e.event_id, e.name as event_name, COALESCE(tc.track_name, 'Unknown') as track_name, 
			COALESCE(MAX(t.name), 'N/A') as team_name, 
			COALESCE(SUM(l.time * l.avg_speed / 3600000), 0) as distance_driven_km,
			COALESCE(COUNT(l.stint_lap_id), 0) as total_laps,
			COALESCE(SUM(CASE WHEN l.cuts = 0 AND l.crashes = 0 THEN 1 ELSE 0 END), 0) as total_valid_laps, 
			CAST(UNIX_TIMESTAMP(COALESCE(MAX(s.last_activity), FROM_UNIXTIME(0))) * 1000000 AS UNSIGNED) as event_time
		FROM event e
		LEFT JOIN session s ON e.event_id = s.event_id
		LEFT JOIN session_stint st ON s.session_id = st.session_id AND st.user_id = ?
		LEFT JOIN stint_lap l ON st.session_stint_id = l.stint_id
		LEFT JOIN track_config tc ON e.track_config_id = tc.track_config_id
		LEFT JOIN team_member tm ON st.team_member_id = tm.team_member_id
		LEFT JOIN team t ON tm.team_id = t.team_id
		GROUP BY e.event_id, e.name, tc.track_name
		ORDER BY event_time DESC
		`

	rows, err := db.Query(eventsQuery, driverId)
	if err != nil && err != sql.ErrNoRows {
		logger.Printf("事件数据查询失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "事件数据查询失败", "userDetails")
		return
	}

	defer rows.Close()

	var eventMap = make(map[int]*DriverEvent)
	var totalDistance float64
	var totalLaps int
	var totalValidLaps int
	now := time.Now().Unix()

	for rows.Next() {
		var event DriverEvent = DriverEvent{}
		var eventTime int64
		var distance float64
		var laps, validLaps int

		err := rows.Scan(
			&event.EventID, &event.EventName, &event.TrackName, &event.TeamName,
			&distance, &laps, &validLaps, &eventTime,
		)
		if err != nil {
			logger.Printf("事件数据扫描失败: %v", err)
			continue
		}

		event.DistanceDrivenKm = distance
		event.TotalLaps = laps
		event.TotalValidLaps = validLaps
		eventTimeSec := eventTime / 1000
		event.EventTime = TimeWithTimezone(eventTimeSec)
		event.TimeAgoSec = now - int64(eventTimeSec)
		if event.TimeAgoSec < 0 {
			event.TimeAgoSec = 0 // 防止未来时间导致的负数值
		}

		// 合并相同event_id的记录
		if existingEvent, exists := eventMap[event.EventID]; exists {
			existingEvent.DistanceDrivenKm += event.DistanceDrivenKm
			existingEvent.TotalLaps += event.TotalLaps
			existingEvent.TotalValidLaps += event.TotalValidLaps
			// 保留最新的事件时间
			if event.EventTime > existingEvent.EventTime {
				existingEvent.EventTime = event.EventTime
				existingEvent.TimeAgoSec = now - int64(event.EventTime)
			}
		} else {
			eventMap[event.EventID] = &event
		}
	}

	// 转换map为切片并过滤公里数为0的事件
	events := []DriverEvent{}
	totalEvents := 0
	totalDistance = 0
	totalLaps = 0
	totalValidLaps = 0
	for _, e := range eventMap {
		if e.DistanceDrivenKm > 0 {
			e.DistanceDrivenKm = math.Floor(e.DistanceDrivenKm)
			events = append(events, *e)
			totalEvents++
			totalDistance += e.DistanceDrivenKm
			totalLaps += e.TotalLaps
			totalValidLaps += e.TotalValidLaps
		}
	}

	// 按事件时间排序（最新的在前）
	sort.Slice(events, func(i, j int) bool {
		return events[i].TimeAgoSec < events[j].TimeAgoSec
	})

	totalDistance = math.Floor(totalDistance)

	// 查询顶级赛道
	topTracksQuery := `
		SELECT tc.track_name, SUM(l.time * l.avg_speed / 3600000) as distance_driven
		FROM session s
		JOIN session_stint st ON s.session_id = st.session_id
		JOIN stint_lap l ON st.session_stint_id = l.stint_id
		JOIN event ev ON s.event_id = ev.event_id
		JOIN track_config tc ON ev.track_config_id = tc.track_config_id
		WHERE st.user_id = ?
		GROUP BY tc.track_name
		ORDER BY distance_driven DESC
		`

	rows, err = db.Query(topTracksQuery, driverId)
	if err != nil && err != sql.ErrNoRows {
		logger.Printf("赛道数据查询失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "赛道数据查询失败", "userDetails")
		return
	}

	defer rows.Close()

	topTracks := []TrackSummary{}
	for rows.Next() {
		var track TrackSummary = TrackSummary{}
		err := rows.Scan(&track.TrackName, &track.DistanceDriven)
		if err != nil {
			logger.Printf("赛道数据扫描失败: %v", err)
			continue
		}
		track.DistanceDriven = math.Floor(track.DistanceDriven)
		topTracks = append(topTracks, track)
	}

	// 查询顶级车辆
	topCarsQuery := `
		SELECT c.name as car_name, SUM(l.time * l.avg_speed / 3600000) as distance_driven
		FROM session s
		JOIN session_stint st ON s.session_id = st.session_id
		JOIN stint_lap l ON st.session_stint_id = l.stint_id
		JOIN car c ON st.car_id = c.car_id
		WHERE st.user_id = ?
		GROUP BY c.name
		ORDER BY distance_driven DESC
		`

	rows, err = db.Query(topCarsQuery, driverId)
	if err != nil && err != sql.ErrNoRows {
		logger.Printf("车辆数据查询失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "车辆数据查询失败", "userDetails")
		return
	}

	defer rows.Close()

	topCars := []CarSummary{}
	for rows.Next() {
		car := CarSummary{}
		err := rows.Scan(&car.CarName, &car.DistanceDriven)
		if err != nil {
			logger.Printf("车辆数据扫描失败: %v", err)
			continue
		}
		car.DistanceDriven = math.Floor(car.DistanceDriven)
		topCars = append(topCars, car)
	}

	// 构建响应结果
	driver := DriverSummary{
		UserID:                user.UserID,
		TotalEvents:           totalEvents,
		TotalDistanceDrivenKm: totalDistance,
		TotalLaps:             totalLaps,
		TotalValidLaps:        totalValidLaps,
		Events:                events,
		TopTracks:             topTracks,
		TopCars:               topCars,
	}
	sendSuccessResponse(w, "用户详情数据获取成功", "driver", driver)
}

// 处理AC团队成员请求
func handleACTeamMembers(w http.ResponseWriter, r *http.Request) {
	teamID := strings.TrimPrefix(r.URL.Path, "/api/ac/team/")
	teamID = strings.TrimSuffix(teamID, "/members")

	rows, err := db.Query("SELECT user_id FROM team_member WHERE team_id = ?", teamID)
	if err != nil {
		logger.Printf("数据库查询失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "数据库查询失败", "teamMembers")
		return
	}
	defer rows.Close()

	var members []ACTeamMember

	for rows.Next() {
		m := ACTeamMember{}
		if err := rows.Scan(&m.UserID); err != nil {
			logger.Printf("数据解析失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "数据解析失败", "teamMembers")
			return
		}
		members = append(members, m)
	}

	if err := rows.Err(); err != nil {
		logger.Printf("行迭代错误: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "行迭代错误", "teamMembers")
		return
	}

	sendSuccessResponse(w, "团队成员数据获取成功", "teamMembers", members)
}

// 处理AC赛道请求
func handleACTrack(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(pathParts) < 4 || pathParts[2] != "track" {
		http.NotFound(w, r)
		return
	}

	// 提取track_config_id和资源类型
	trackID := pathParts[3]
	var resourceType string
	if len(pathParts) >= 5 {
		resourceType = pathParts[4]
	}

	// 根据资源类型分发到不同的处理函数
	switch resourceType {
	case "map":
		handleTrackSVG(w, r)
	case "preview":
		handleTrackImage(w, r)
	default:
		// 原始赛道信息查询逻辑
		track := ACTrackConfig{}
		err := db.QueryRow("SELECT track_config_id, track_name, config_name, display_name, country, city, length FROM track_config WHERE track_config_id = ?", trackID).Scan(
			&track.TrackConfigID, &track.TrackName, &track.ConfigName, &track.DisplayName, &track.Country, &track.City, &track.Length)
		if err != nil {
			if err == sql.ErrNoRows {
				logger.Printf("赛道未找到: %s", trackID)
				sendErrorResponse(w, http.StatusNotFound, "赛道未找到", "track")
			} else {
				logger.Printf("数据库查询失败: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, "数据库查询失败", "track")
			}
			return
		}
		sendSuccessResponse(w, "赛道信息获取成功", "track", track)
	}
}

// 处理AC赛道列表请求
func handleACTracks(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT track_config_id, track_name, config_name, display_name, country, city, length FROM track_config")
	if err != nil {
		logger.Printf("数据库查询失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "数据库查询失败", "tracks")
		return
	}
	defer rows.Close()

	var tracks []ACTrackConfig

	for rows.Next() {
		t := ACTrackConfig{}
		if err := rows.Scan(&t.TrackConfigID, &t.TrackName, &t.ConfigName, &t.DisplayName, &t.Country, &t.City, &t.Length); err != nil {
			logger.Printf("数据解析失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "数据解析失败", "tracks")
			return
		}
		tracks = append(tracks, t)
	}

	if err := rows.Err(); err != nil {
		logger.Printf("行迭代错误: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "行迭代错误", "tracks")
		return
	}

	sendSuccessResponse(w, "赛道数据获取成功", "tracks", tracks)
}

// 处理AC圈速摘要请求
func handleACLapSummary(w http.ResponseWriter, r *http.Request) {
	lapID := strings.TrimPrefix(r.URL.Path, "/api/ac/lap/summary/")

	type Response struct {
		Status  string  `json:"status"`
		Summary Summary `json:"summary"`
	}

	response := Response{}

	err := db.QueryRow(`
		SELECT 
			l.stint_lap_id, l.stint_id, l.sector_1, l.sector_2, l.sector_3, l.grip, l.tyre, 
			l.time, l.cuts, l.crashes, l.car_crashes, l.max_speed, l.avg_speed, 
			CAST(UNIX_TIMESTAMP(l.finished_at) * 1000000 AS SIGNED) AS finished_at, ss.user_id, ss.car_id, 
			e.event_id, e.name, t.track_name, t.length, t.track_config_id, 
			s.type, s.air_temp, s.road_temp, s.weather
		FROM stint_lap l
		JOIN session_stint ss ON l.stint_id = ss.session_stint_id
		JOIN session s ON ss.session_id = s.session_id
		JOIN event e ON s.event_id = e.event_id
		JOIN user u ON ss.user_id = u.user_id
		JOIN car c ON ss.car_id = c.car_id
		JOIN track_config t ON e.track_config_id = t.track_config_id
		WHERE l.stint_lap_id = ?`, lapID).Scan(
		&response.Summary.Lap.StintLapID, &response.Summary.Lap.StintID,
		&response.Summary.Lap.Sector1, &response.Summary.Lap.Sector2, &response.Summary.Lap.Sector3,
		&response.Summary.Lap.Grip, &response.Summary.Lap.Tyre, &response.Summary.Lap.Time,
		&response.Summary.Lap.Cuts, &response.Summary.Lap.Crashes, &response.Summary.Lap.CarCrashes,
		&response.Summary.Lap.MaxSpeed, &response.Summary.Lap.AvgSpeed, &response.Summary.Lap.FinishedAt,
		&response.Summary.UserID, &response.Summary.CarID, &response.Summary.EventID,
		&response.Summary.EventName, &response.Summary.TrackName, &response.Summary.TrackLength,
		&response.Summary.TrackConfigID, &response.Summary.SessionType, &response.Summary.AirTemp,
		&response.Summary.RoadTemp, &response.Summary.Weather)
	if err != nil {
		if err == sql.ErrNoRows {
			logger.Printf("圈速摘要未找到")
			sendErrorResponse(w, http.StatusNotFound, "圈速摘要未找到", "summary")
		} else {
			logger.Printf("数据库查询失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "数据库查询失败", "summary")
		}
		return
	}
	sendSuccessResponse(w, "圈速摘要获取成功", "summary", response.Summary)
}

// 处理AC单圈遥测数据请求
func handleACLapTelemetry(w http.ResponseWriter, r *http.Request) {
	// 解析URL路径获取lap ID
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(pathParts) < 5 || pathParts[1] != "ac" || pathParts[2] != "lap" || pathParts[3] != "telemetry" {
		logger.Printf("无效的URL格式: %s", r.URL.Path)
		sendErrorResponse(w, http.StatusBadRequest, "无效的URL格式", "telemetry")
		return
	}

	if pathParts[4] == "" {
		logger.Printf("缺少lap_id参数")
		sendErrorResponse(w, http.StatusBadRequest, "缺少lap_id参数", "telemetry")
		return
	}
	lapID := pathParts[4]

	// 转换lapID为整数
	id, err := strconv.Atoi(lapID)
	if err != nil {
		logger.Printf("无效的lap ID: %v", err)
		sendErrorResponse(w, http.StatusBadRequest, "无效的lap ID格式", "telemetry")
		return
	}

	// 从lap_telemetry表查询telemetry二进制数据
	var telemetryData []byte
	// 查询telemetry数据
	err = db.QueryRow("SELECT telemetry FROM lap_telemetry WHERE lap_id = ?", id).Scan(&telemetryData)
	if err != nil {
		if err == sql.ErrNoRows {
			logger.Printf("遥测数据未找到: %d", id)
			sendErrorResponse(w, http.StatusNotFound, "遥测数据未找到", "telemetry")
		} else {
			logger.Printf("查询遥测数据失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "数据库查询失败", "telemetry")
		}
		return
	}

	// 创建包含版本和赛道长度的二进制数据
	buffer := new(bytes.Buffer)
	buffer.Write(telemetryData)

	// 设置响应头为二进制数据
	w.Header().Set("Content-Type", "application/octet-stream")

	// 写入响应数据
	_, err = w.Write(buffer.Bytes())
	if err != nil {
		logger.Printf("写入响应数据失败: %v", err)
	}
}

// 通用最佳圈速处理函数
func handleACBestLap(w http.ResponseWriter, r *http.Request) {
	// 解析URL路径参数
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	var allLaps []ACBestLap
	var page, entries int = 1, 10
	var err error

	// 判断是按事件还是按赛道查询
	switch pathParts[3] {
	case "event":
		// 按事件查询逻辑
		// 验证URL格式
		if len(pathParts) < 6 || pathParts[3] != "event" || pathParts[5] != "cars" {
			logger.Printf("无效的URL路径格式")
			sendErrorResponse(w, http.StatusBadRequest, "无效的URL路径格式", "bestlap")
			return
		}

		// 提取参数
		eventIDStr := pathParts[4]
		carIDsStr := pathParts[6]

		// 解析分页参数
		if len(pathParts) >= 8 && pathParts[7] == "page" && len(pathParts) >= 9 {
			pageStr := pathParts[8]
			page, err = strconv.Atoi(pageStr)
			if err != nil || page < 1 {
				logger.Printf("无效的页码: %s", pageStr)
				sendErrorResponse(w, http.StatusBadRequest, "无效的页码参数", "bestlap")
				return
			}

			if len(pathParts) >= 10 && pathParts[9] == "entries" && len(pathParts) >= 11 {
				entriesStr := pathParts[10]
				entries, err = strconv.Atoi(entriesStr)
				if err != nil || entries < 1 || entries > 100 {
					logger.Printf("无效的条目数: %s", entriesStr)
					sendErrorResponse(w, http.StatusBadRequest, "条目数必须为1-100之间的整数", "bestlap")
					return
				}
			}
		}

		// 验证参数
		if eventIDStr == "" {
			logger.Printf("缺少必要参数:eventID")
			sendErrorResponse(w, http.StatusBadRequest, "缺少必要参数:eventID", "bestlap")
			return
		}

		// 转换为整数
		eventID, err := strconv.Atoi(eventIDStr)
		if err != nil || eventID <= 0 {
			logger.Printf("无效的event_id参数: %v", err)
			sendErrorResponse(w, http.StatusBadRequest, "无效的event_id参数", "bestlap")
			return
		}

		// 处理车辆ID（可选）
		var carFilter string
		var args []interface{}
		carIDs := strings.Split(carIDsStr, ",")
		if len(carIDs) > 0 && carIDs[0] != "" {
			placeholders := make([]string, len(carIDs))
			for i := range carIDs {
				placeholders[i] = "?"
			}
			carFilter = fmt.Sprintf("AND ss.car_id IN (%s)", strings.Join(placeholders, ","))
		}

		// 验证并处理车辆ID
		if carIDsStr != "" {
			carIDs = strings.Split(carIDsStr, ",")
			for _, id := range carIDs {
				if _, err := strconv.Atoi(id); err != nil {
					logger.Printf("无效的车辆ID: %s", id)
					sendErrorResponse(w, http.StatusBadRequest, "无效的车辆ID格式", "bestlap")
					return
				}
			}
		}
		placeholders := make([]string, len(carIDs))
		args = make([]interface{}, 0, len(carIDs)+1)
		for i, id := range carIDs {
			placeholders[i] = "?"
			args = append(args, id)
		}

		// 查询最佳圈速数据
		query := fmt.Sprintf(`
            WITH all_laps AS (
                SELECT l.stint_lap_id AS lap_id, ss.user_id, ss.car_id, l.time, l.sector_1, l.sector_2, l.sector_3, l.grip, l.tyre, l.max_speed, CAST(UNIX_TIMESTAMP(l.finished_at) * 1000000 AS SIGNED) AS finished_at, e.event_id
                FROM stint_lap l
                JOIN session_stint ss ON l.stint_id = ss.session_stint_id
                JOIN user u ON ss.user_id = u.user_id
                JOIN car c ON ss.car_id = c.car_id
                JOIN session s ON ss.session_id = s.session_id
                JOIN event e ON s.event_id = e.event_id
                WHERE e.event_id = ? AND l.cuts = 0 AND l.crashes = 0 %s
            ),
            ranked_laps AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id, car_id ORDER BY time ASC) as rn
                FROM all_laps
            )
            SELECT lap_id, user_id, car_id, time, sector_1, sector_2, sector_3, grip, tyre, max_speed, finished_at, event_id
            FROM ranked_laps
            WHERE rn = 1
            ORDER BY time ASC
       `, carFilter)

		// 插入eventID作为第一个参数
		args = append([]interface{}{eventID}, args...)
		rows, err := db.Query(query, args...)
		if err != nil {
			logger.Printf("查询最佳圈速数据失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "查询数据失败", "bestlap")
			return
		}
		defer rows.Close()

		// 收集所有数据
		allLaps = []ACBestLap{}
		for rows.Next() {
			lap := ACBestLap{}
			if err := rows.Scan(&lap.LapID, &lap.User_id, &lap.Car_id, &lap.Time, &lap.Sector1, &lap.Sector2, &lap.Sector3, &lap.Grip, &lap.Tyre, &lap.MaxSpeed, &lap.FinishedAt, &lap.EventID); err != nil {
				logger.Printf("数据解析失败: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, "数据解析失败", "bestlap")
				return
			}
			allLaps = append(allLaps, lap)
		}

		if err = rows.Err(); err != nil {
			logger.Printf("行迭代错误: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "行迭代错误", "bestlap")
			return
		}

	case "track":
		// 按赛道查询逻辑
		// 验证URL格式
		if len(pathParts) < 11 {
			logger.Printf("无效的URL路径格式")
			sendErrorResponse(w, http.StatusBadRequest, "无效的URL路径格式", "bestlap")
			return
		}

		// 提取参数
		trackIDStr := pathParts[4]
		carIDStr := pathParts[6]
		pageStr := pathParts[8]
		entriesStr := pathParts[10]

		// 转换为整数
		trackID, err := strconv.Atoi(trackIDStr)
		if err != nil || trackID <= 0 {
			logger.Printf("无效的track_id参数: %v", err)
			sendErrorResponse(w, http.StatusBadRequest, "无效的track_id参数", "bestlap")
			return
		}

		carID, err := strconv.Atoi(carIDStr)
		if err != nil || carID <= 0 {
			logger.Printf("无效的car_id参数: %v", err)
			sendErrorResponse(w, http.StatusBadRequest, "无效的car_id参数", "bestlap")
			return
		}

		page, err = strconv.Atoi(pageStr)
		if err != nil || page <= 0 {
			logger.Printf("无效的page参数: %v", err)
			sendErrorResponse(w, http.StatusBadRequest, "无效的page参数", "bestlap")
			return
		}

		entries, err = strconv.Atoi(entriesStr)
		if err != nil || entries <= 0 || entries > 100 {
			logger.Printf("无效的entries参数: %v", err)
			sendErrorResponse(w, http.StatusBadRequest, "无效的entries参数", "bestlap")
			return
		}

		// 查询所有数据（不使用LIMIT/OFFSET，后续在应用层分页）
		query := `
            WITH ranked_laps AS (
                SELECT 
                    l.stint_lap_id AS lap_id, 
                    ss.user_id, 
                    ss.car_id, 
                    l.time, 
                    l.sector_1, 
                    l.sector_2, 
                    l.sector_3, 
                    l.grip, 
                    l.tyre, 
                    l.max_speed, 
                    CAST(UNIX_TIMESTAMP(l.finished_at) * 1000000 AS SIGNED) AS finished_at,
                    ROW_NUMBER() OVER (PARTITION BY ss.user_id, ss.car_id ORDER BY l.time ASC) AS rn
                FROM stint_lap l
                JOIN session_stint ss ON l.stint_id = ss.session_stint_id
                JOIN session s ON ss.session_id = s.session_id
                JOIN event e ON s.event_id = e.event_id
                JOIN user u ON ss.user_id = u.user_id
                JOIN car c ON ss.car_id = c.car_id
                WHERE e.track_config_id = ? AND ss.car_id = ? AND l.cuts = 0 AND l.crashes = 0 
            )
            SELECT lap_id, user_id, car_id, time, sector_1, sector_2, sector_3, grip, tyre, max_speed, finished_at 
            FROM ranked_laps 
            WHERE rn = 1
            ORDER BY time ASC
        `

		rows, err := db.Query(query, trackID, carID)
		if err != nil {
			logger.Printf("查询最佳圈速数据失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "查询数据失败", "bestlap")
			return
		}
		defer rows.Close()

		// 收集所有数据
		allLaps = []ACBestLap{}
		for rows.Next() {
			lap := ACBestLap{}
			if err := rows.Scan(&lap.LapID, &lap.User_id, &lap.Car_id, &lap.Time, &lap.Sector1, &lap.Sector2, &lap.Sector3, &lap.Grip, &lap.Tyre, &lap.MaxSpeed, &lap.FinishedAt); err != nil {
				logger.Printf("数据解析失败: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, "数据解析失败", "bestlap")
				return
			}
			allLaps = append(allLaps, lap)
		}

		if err = rows.Err(); err != nil {
			logger.Printf("行迭代错误: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "行迭代错误", "bestlap")
			return
		}

	default:
		logger.Printf("无效的查询类型")
		sendErrorResponse(w, http.StatusBadRequest, "无效的查询类型", "bestlap")
		return
	}

	// 按时间升序排序
	sort.Slice(allLaps, func(i, j int) bool {
		return allLaps[i].Time < allLaps[j].Time
	})

	// 计算差距和最佳圈速标记
	var classBestTime int64 = math.MaxInt64
	var sector1Best, sector2Best, sector3Best int = math.MaxInt32, math.MaxInt32, math.MaxInt32

	// 1. 先计算每个车的最佳圈速和全局最佳
	carBestTimes := make(map[int]int64)
	carBestSector1 := make(map[int]int)
	carBestSector2 := make(map[int]int)
	carBestSector3 := make(map[int]int)

	// 第一遍：找出全局最佳和每个车的最佳圈速和扇区
	for _, lap := range allLaps {
		carID := lap.Car_id
		// 全局最佳圈速
		if lap.Time < float64(classBestTime) {
			classBestTime = int64(lap.Time)
		}
		// 全局最佳扇区1
		if lap.Sector1 > 0 && lap.Sector1 < sector1Best {
			sector1Best = lap.Sector1
		}
		// 全局最佳扇区2
		if lap.Sector2 > 0 && lap.Sector2 < sector2Best {
			sector2Best = lap.Sector2
		}
		// 全局最佳扇区3
		if lap.Sector3 > 0 && lap.Sector3 < sector3Best {
			sector3Best = lap.Sector3
		}
		// 圈速最佳
		if currentBest, exists := carBestTimes[carID]; !exists || float64(lap.Time) < float64(currentBest) {
			carBestTimes[carID] = int64(lap.Time)
		}
		// 扇区1最佳
		if currentBest, exists := carBestSector1[carID]; !exists || lap.Sector1 < currentBest {
			carBestSector1[carID] = lap.Sector1
		}
		// 扇区2最佳
		if currentBest, exists := carBestSector2[carID]; !exists || lap.Sector2 < currentBest {
			carBestSector2[carID] = lap.Sector2
		}
		// 扇区3最佳
		if currentBest, exists := carBestSector3[carID]; !exists || lap.Sector3 < currentBest {
			carBestSector3[carID] = lap.Sector3
		}
	}

	// 第二遍：设置最佳标记
	for i := range allLaps {
		lap := &allLaps[i]
		carID := lap.Car_id

		// 车辆最佳标记 (每个车只有1个)
		lap.CarBest = 0
		lap.Sector1CarBest = 0
		lap.Sector2CarBest = 0
		lap.Sector3CarBest = 0

		if lap.Time == float64(carBestTimes[carID]) {
			lap.CarBest = 1
		}
		if lap.Sector1 > 0 && lap.Sector1 == carBestSector1[carID] {
			lap.Sector1CarBest = 1
		}
		if lap.Sector2 > 0 && lap.Sector2 == carBestSector2[carID] {
			lap.Sector2CarBest = 1
		}
		if lap.Sector3 > 0 && lap.Sector3 == carBestSector3[carID] {
			lap.Sector3CarBest = 1
		}

		// 计算与全局最佳的差距
		lap.Gap = int(lap.Time - float64(classBestTime))
		if classBestTime > 0 {
			lap.GapPer = float64(lap.Gap) / float64(classBestTime)
		}

		// 全局最佳标记
		lap.ClassBest = 0
		lap.Sector1ClassBest = 0
		lap.Sector2ClassBest = 0
		lap.Sector3ClassBest = 0

		if lap.Time == float64(classBestTime) {
			lap.ClassBest = 1
		}
		if lap.Sector1 == int(sector1Best) && lap.Sector1 > 0 {
			lap.Sector1ClassBest = 1
		}
		if lap.Sector2 == int(sector2Best) && lap.Sector2 > 0 {
			lap.Sector2ClassBest = 1
		}
		if lap.Sector3 == int(sector3Best) && lap.Sector3 > 0 {
			lap.Sector3ClassBest = 1
		}
	}

	// 应用层分页
	start := (page - 1) * entries
	end := start + entries
	if end > len(allLaps) {
		end = len(allLaps)
	}
	if start > len(allLaps) {
		start = len(allLaps)
	}

	// 处理查询结果
	var response struct {
		Status   string `json:"status"`
		Bestlaps struct {
			Laps    []ACBestLap `json:"laps"`
			First   int         `json:"first"`
			Prev    int         `json:"prev"`
			Next    int         `json:"next"`
			Last    int         `json:"last"`
			Page    int         `json:"page"`
			PerPage int         `json:"per_page"`
		} `json:"bestlaps"`
	}
	response.Bestlaps.Laps = allLaps[start:end]
	response.Bestlaps.Page = page
	response.Bestlaps.PerPage = entries
	response.Bestlaps.Last = (len(allLaps) + entries - 1) / entries
	response.Bestlaps.First = 1
	response.Bestlaps.Prev = 0
	if page > 1 {
		response.Bestlaps.Prev = page - 1
	}
	response.Bestlaps.Next = 0
	if page < response.Bestlaps.Last {
		response.Bestlaps.Next = page + 1
	}

	// 返回JSON响应
	sendSuccessResponse(w, "最佳圈速数据获取成功", "bestlaps", response.Bestlaps)
}

// 处理AC会话详情请求
func handleACSession(w http.ResponseWriter, r *http.Request) {
	// 解析URL路径获取sessionId和资源类型
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(pathParts) < 4 || pathParts[2] != "session" {
		sendErrorResponse(w, http.StatusBadRequest, "无效的URL格式", "sessionDetails")
		return
	}

	sessionID := pathParts[3]
	var resourceType string
	if len(pathParts) >= 5 {
		resourceType = strings.Join(pathParts[4:], "/")
	}

	// 验证sessionID
	if sessionID == "" {
		sendErrorResponse(w, http.StatusBadRequest, "缺少sessionId参数", "sessionDetails")
		return
	}

	// 验证sessionID是否为有效整数
	sessionIDInt, err := strconv.Atoi(sessionID)
	if err != nil {
		sendErrorResponse(w, http.StatusBadRequest, "无效的sessionId格式", "sessionDetails")
		return
	}

	// 根据资源类型处理不同请求
	switch resourceType {
	case "":
		// 单个会话详情
		session := ACSession{}
		err := db.QueryRow("SELECT session_id, event_id, type, track_time, name, CAST(UNIX_TIMESTAMP(start_time) * 1000000 AS SIGNED) AS start_time, duration_min, elapsed_ms, laps, weather, air_temp, road_temp, start_grip, current_grip, is_finished, CAST(UNIX_TIMESTAMP(finish_time) * 1000000 AS SIGNED) AS finish_time, CAST(UNIX_TIMESTAMP(last_activity) * 1000000 AS SIGNED) AS last_activity FROM session WHERE session_id = ?", sessionIDInt).Scan(
			&session.SessionID, &session.EventID, &session.Type, &session.TrackTime, &session.Name, &session.StartTime, &session.DurationMin, &session.ElapsedMs, &session.Laps, &session.Weather, &session.AirTemp, &session.RoadTemp, &session.StartGrip, &session.CurrentGrip, &session.IsFinished, &session.FinishTime, &session.LastActivity)
		if err != nil {
			if err == sql.ErrNoRows {
				sendErrorResponse(w, http.StatusNotFound, "会话不存在", "sessionDetails")
				return
			}
			logger.Printf("查询会话失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("查询会话失败: %v", err), "sessionDetails")
			return
		}
		sendSuccessResponse(w, "会话详情获取成功", "session", session)

	case "live":
		// 单个会话实时数据
		liveData := ACLiveData{}
		err := db.QueryRow("SELECT session_id, timestamp, status FROM live_data WHERE session_id = ? ORDER BY timestamp DESC LIMIT 1", sessionID).Scan(
			&liveData.SessionID, &liveData.Timestamp, &liveData.Status)
		if err != nil {
			if err == sql.ErrNoRows {
				sendErrorResponse(w, http.StatusNotFound, "实时数据不存在", "sessionLive")
				return
			}
			logger.Printf("查询实时数据失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("查询实时数据失败: %v", err), "sessionLive")
			return
		}
		sendSuccessResponse(w, "实时数据获取成功", "liveData", liveData)

	case "live/positions":
		// 单个会话实时位置
		rows, err := db.Query("SELECT driver_id, driver_name, position, x, y, z FROM session_feed WHERE session_id = ? ORDER BY position", sessionID)
		if err != nil {
			logger.Printf("查询位置数据失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("查询位置数据失败: %v", err), "sessionPositions")
			return
		}
		defer rows.Close()

		var positions []ACLivePosition
		for rows.Next() {
			pos := ACLivePosition{}
			if err := rows.Scan(&pos.DriverID, &pos.DriverName, &pos.Position, &pos.X, &pos.Y, &pos.Z); err != nil {
				logger.Printf("解析位置数据失败: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, "解析位置数据失败", "sessionPositions")
				return
			}
			positions = append(positions, pos)
		}
		sendSuccessResponse(w, "位置数据获取成功", "positions", positions)

	case "result":
		// 单个会话结果
		result := ACResult{}
		err := db.QueryRow("SELECT session_id, winner_driver, winner_team, laps_completed, fastest_lap FROM results WHERE session_id = ?", sessionID).Scan(
			&result.SessionID, &result.WinnerDriver, &result.WinnerTeam, &result.LapsCompleted, &result.FastestLap)
		if err != nil {
			if err == sql.ErrNoRows {
				sendErrorResponse(w, http.StatusNotFound, "结果数据不存在", "sessionResult")
				return
			}
			logger.Printf("查询结果数据失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("查询结果数据失败: %v", err), "sessionResult")
			return
		}
		sendSuccessResponse(w, "结果数据获取成功", "result", result)
	}
}

// 处理AC事件详情请求
func handleACEventDetails(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 5 || pathParts[2] != "ac" || pathParts[3] != "event" {
		sendErrorResponse(w, http.StatusBadRequest, "无效的URL格式", "eventDetails")
		return
	}

	eventIDStr := pathParts[4]
	eventID, err := strconv.Atoi(eventIDStr)
	if err != nil {
		sendErrorResponse(w, http.StatusBadRequest, "无效的事件ID格式", "eventDetails")
		return
	}

	// 处理特定资源请求（sessions, cars）
	if len(pathParts) >= 6 {
		resource := pathParts[5]
		switch resource {
		case "sessions":
			// 查询事件会话数据
			rows, err := db.Query(`SELECT s.session_id, s.event_id, s.type, s.track_time, s.name, CAST(UNIX_TIMESTAMP(s.start_time) * 1000000 AS SIGNED) AS start_time, s.duration_min, s.elapsed_ms, s.laps, s.weather, s.air_temp, s.road_temp, s.start_grip, s.current_grip, s.is_finished, CAST(UNIX_TIMESTAMP(s.finish_time) * 1000000 AS SIGNED) AS finish_time, CAST(UNIX_TIMESTAMP(s.last_activity) * 1000000 AS SIGNED) AS last_activity, ? AS http_port FROM session s WHERE s.event_id = ? ORDER BY s.start_time ASC`, cfg.App.Server.Port, eventID)
			if err != nil {
				logger.Printf("会话查询失败: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, "会话数据查询失败", "sessions")
				return
			}
			defer rows.Close()

			sessions := []ACSession{}
			for rows.Next() {
				s := ACSession{}
				if err := rows.Scan(&s.SessionID, &s.EventID, &s.Type, &s.TrackTime, &s.Name, &s.StartTime, &s.DurationMin, &s.ElapsedMs, &s.Laps, &s.Weather, &s.AirTemp, &s.RoadTemp, &s.StartGrip, &s.CurrentGrip, &s.IsFinished, &s.FinishTime, &s.LastActivity, &s.HttpPort); err != nil {
					logger.Printf("会话行扫描失败: %v", err)
					sendErrorResponse(w, http.StatusInternalServerError, "会话数据解析失败", "sessions")
					return
				}
				sessions = append(sessions, s)
			}

			if err = rows.Err(); err != nil {
				logger.Printf("会话行迭代错误: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, "会话数据处理失败", "sessions")
				return
			}

			sendSuccessResponse(w, "事件会话数据获取成功", "sessions", sessions)
			return
		case "cars":
			// 查询事件车辆数据
			carRows, err := db.Query("SELECT DISTINCT c.car_id, c.name, c.display_name, c.manufacturer, c.car_class FROM session_stint ss JOIN session s ON ss.session_id = s.session_id JOIN car c ON ss.car_id = c.car_id WHERE s.event_id = ?", eventID)
			if err != nil {
				logger.Printf("车辆查询失败: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, "车辆数据查询失败", "vehicles")
				return
			}
			defer carRows.Close()

			var cars []ACCar
			for carRows.Next() {
				c := ACCar{}
				if err := carRows.Scan(&c.CarID, &c.Name, &c.DisplayName, &c.Manufacturer, &c.CarClass); err != nil {
					logger.Printf("车辆行扫描失败: %v", err)
					sendErrorResponse(w, http.StatusInternalServerError, "车辆数据解析失败", "vehicles")
					return
				}
				cars = append(cars, c)
			}

			if err = carRows.Err(); err != nil {
				logger.Printf("车辆行迭代错误: %v", err)
				sendErrorResponse(w, http.StatusInternalServerError, "车辆数据处理失败", "vehicles")
				return
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{"status": "success", "cars": cars})
			return
		}
	}

	// 查询事件基本信息（包含完整元数据）
	event := ACEventResponse{}
	row := db.QueryRow(`SELECT event_id, name, server_name, track_config_id, team_event, active, livery_preview, use_number, practice_duration, quali_duration, race_duration, race_duration_type, race_wait_time, race_extra_laps, reverse_grid_positions FROM event WHERE event_id = ?`, eventID)
	err = row.Scan(&event.EventID, &event.Name, &event.ServerName, &event.TrackConfigID, &event.TeamEvent, &event.Active, &event.LiveryPreview, &event.UseNumber, &event.PracticeDuration, &event.QualiDuration, &event.RaceDuration, &event.RaceDurationType, &event.RaceWaitTime, &event.RaceExtraLaps, &event.ReverseGridPositions)
	if err != nil {
		if err == sql.ErrNoRows {
			logger.Printf("事件不存在: %v", err)
			sendErrorResponse(w, http.StatusNotFound, "事件不存在", "events")
			return
		} else {
			logger.Printf("扫描事件详情失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "扫描事件详情失败", "events")
			return
		}
	}

	sendSuccessResponse(w, "事件详情获取成功", "event", event)
}

// 处理赛道资源请求
func handleTrackResource(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// 路径格式应为: images/ac/track/{track_config_id}/{resource_type}
	if len(pathParts) != 5 || pathParts[2] != "track" {
		logger.Printf("无效的请求路径: %s, 路径部分: %v", r.URL.Path, pathParts)
		http.NotFound(w, r)
		return
	}

	// 提取track_config_id和资源类型
	trackConfigID := pathParts[3]
	resourceType := pathParts[4]

	// 设置URL参数以便后续处理函数获取
	q := r.URL.Query()
	q.Set("track_config_id", trackConfigID)
	r.URL.RawQuery = q.Encode()

	// 根据资源类型分发请求
	switch resourceType {
	case "map":
		handleTrackSVG(w, r)
	case "preview":
		handleTrackImage(w, r)
	default:
		logger.Printf("不支持的资源类型: %s", resourceType)
		http.NotFound(w, r)
	}
}

// 处理赛道SVG地图请求
func handleTrackSVG(w http.ResponseWriter, r *http.Request) {
	// 从查询参数获取track_config_id
	trackConfigID := r.URL.Query().Get("track_config_id")
	if trackConfigID == "" {
		sendErrorResponse(w, http.StatusBadRequest, "缺少赛道ID参数", "trackID")
		return
	}

	// 查询赛道名称
	var trackName, configName string
	query := `SELECT track_name, config_name FROM track_config
		WHERE track_config_id = ?`
	err := db.QueryRow(query, trackConfigID).Scan(&trackName, &configName)
	if err != nil {
		logger.Printf("查询赛道失败: %v", err)
		sendErrorResponse(w, http.StatusNotFound, "未找到赛道", "track")
		return
	}

	// 清理文件名并构建路径
	// 将赛道名称转换为小写并替换空格为下划线
	sanitizedTrack := strings.ToLower(strings.ReplaceAll(trackName, " ", "_"))
	sanitizedConfig := strings.ToLower(strings.ReplaceAll(configName, " ", "_"))
	fileName := fmt.Sprintf("map_%s_%s.svg", sanitizedTrack, sanitizedConfig)
	filePath := filepath.Join(cfg.App.Server.CachePath, "live-track-maps", fileName)

	// 读取并返回SVG文件
	file, err := os.Open(filePath)
	if err != nil {
		logger.Printf("打开地图文件失败: %v", err)
		w.WriteHeader(http.StatusNotFound)
		// 返回默认SVG以避免前端错误
		w.Header().Set("Content-Type", "image/svg+xml")
		w.Write([]byte(`<svg xmlns="http://www.w3.org/2000/svg" width="200" height="100"><text x="100" y="50" text-anchor="middle">地图未找到</text></svg>`))
		return
	}
	defer file.Close()

	fi, _ := file.Stat()
	w.Header().Set("Content-Type", "image/svg+xml")
	w.Header().Set("Content-Length", strconv.FormatInt(fi.Size(), 10))
	io.Copy(w, file)
}

// 处理赛道图片请求
func handleTrackImage(w http.ResponseWriter, r *http.Request) {
	// 从查询参数获取track_config_id
	trackConfigID := r.URL.Query().Get("track_config_id")
	if trackConfigID == "" {
		sendErrorResponse(w, http.StatusBadRequest, "缺少赛道ID参数", "trackID")
		return
	}

	// 查询数据库获取赛道名称和配置名称
	var trackName, configName string
	err := db.QueryRow("SELECT track_name, config_name FROM track_config WHERE track_config_id = ?", trackConfigID).Scan(&trackName, &configName)
	if err != nil {
		if err == sql.ErrNoRows {
			http.NotFound(w, r)
		} else {
			logger.Printf("数据库查询失败: %v", err)
			sendErrorResponse(w, http.StatusInternalServerError, "数据库错误", "database")
		}
		return
	}

	// 清理名称以创建有效的文件名
	sanitizedTrack := sanitize(trackName)
	sanitizedConfig := sanitize(configName)
	filename := fmt.Sprintf("preview_%s_%s.png", sanitizedTrack, sanitizedConfig)

	// 构建完整文件路径
	filePath := filepath.Join(cfg.App.Server.CachePath, "tracks", filename)

	// 提供文件，先尝试从缓存路径加载
	file, err := os.Open(filePath)
	if err != nil {
		logger.Printf("缓存赛道图片不存在: %s，尝试备选路径", filePath)
		// 构建GameConfig.Path父目录下的备选路径
		gameConfigParentDir := filepath.Dir(cfg.Game.Path)
		fallbackPath := filepath.Join(gameConfigParentDir, "content", "tracks", trackName, configName, "ui", "outline.png")
		file, err = os.Open(fallbackPath)
		if err != nil {
			// 尝试新的路径组合: trackName/ui/configName
			fallbackPath2 := filepath.Join(gameConfigParentDir, "content", "tracks", trackName, "ui", configName, "outline.png")
			file, err = os.Open(fallbackPath2)
			if err != nil {
				// 尝试新的路径组合: trackName/ui
				fallbackPath2 := filepath.Join(gameConfigParentDir, "content", "tracks", trackName, "ui", "outline.png")
				file, err = os.Open(fallbackPath2)
				if err != nil {
					logger.Printf("赛道图片路径加载失败: %v", err)
					http.NotFound(w, r)
					return
				}
			}
		}

		// 确保缓存目录存在
		cacheDir := filepath.Dir(filePath)
		if err := os.MkdirAll(cacheDir, 0755); err != nil {
			logger.Printf("创建缓存目录失败: %v", err)
		}

		// 从备选路径复制文件到缓存目录
		fallbackContent, err := io.ReadAll(file)
		if err != nil {
			logger.Printf("读取备选文件失败: %v", err)
		} else {
			if err := os.WriteFile(filePath, fallbackContent, 0644); err != nil {
				logger.Printf("复制文件到缓存目录失败: %v", err)
			} else {
				logger.Printf("成功将赛道图片复制到缓存目录: %s", filePath)
			}
			// 重新打开文件以读取
			file, _ = os.Open(filePath)
		}
	}
	defer file.Close()

	fi, _ := file.Stat()
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Content-Length", strconv.FormatInt(fi.Size(), 10))
	io.Copy(w, file)
}

// 处理车辆资源请求
func handleCarResource(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// 路径格式应为: images/ac/car/{car_id}/badge
	if len(pathParts) != 5 || pathParts[2] != "car" || pathParts[4] != "badge" {
		logger.Printf("无效的车辆徽章请求路径: %s, 路径部分: %v", r.URL.Path, pathParts)
		http.NotFound(w, r)
		return
	}

	// 提取car_id
	carID := pathParts[3]

	// 查询车辆名称
	var carName string
	query := `SELECT name FROM car WHERE car_id = ?`
	err := db.QueryRow(query, carID).Scan(&carName)
	if err != nil {
		logger.Printf("查询车辆名称失败: %v", err)
		http.NotFound(w, r)
		return
	}

	// 清理文件名并构建路径
	sanitizedName := strings.ToLower(strings.ReplaceAll(carName, " ", "_"))
	fileName := fmt.Sprintf("badge_%s.png", sanitizedName)
	filePath := filepath.Join(cfg.App.Server.CachePath, "cars", fileName)

	// 读取并返回文件
	file, err := os.Open(filePath)
	if err != nil {
		// 构建GameConfig.Path父目录下的备选路径
		gameConfigParentDir := filepath.Dir(cfg.Game.Path)
		fallbackPath := filepath.Join(gameConfigParentDir, "content", "cars", carName, "ui", "badge.png")
		file, err = os.Open(fallbackPath)
		if err != nil {
			logger.Printf("车辆徽章文件不存在: %s，尝试备选路径: %s", filePath, fallbackPath)
			// 尝试另一种路径格式
			fallbackPath2 := filepath.Join(gameConfigParentDir, "content", "cars", carName, "ui", "badge.dds")
			file, err = os.Open(fallbackPath2)
			if err != nil {
				logger.Printf("所有车辆徽章路径都不存在: %s, %s", filePath, fallbackPath2)
				http.NotFound(w, r)
				return
			}
		}

		// 确保缓存目录存在
		cacheDir := filepath.Dir(filePath)
		if err := os.MkdirAll(cacheDir, 0755); err != nil {
			logger.Printf("创建缓存目录失败: %v", err)
		}

		// 从备选路径复制文件到缓存目录
		fallbackContent, err := io.ReadAll(file)
		if err != nil {
			logger.Printf("读取备选文件失败: %v", err)
		} else {
			if err := os.WriteFile(filePath, fallbackContent, 0644); err != nil {
				logger.Printf("复制文件到缓存目录失败: %v", err)
			} else {
				logger.Printf("成功将车辆徽章复制到缓存目录: %s", filePath)
			}
			// 重新打开文件以读取
			file, _ = os.Open(filePath)
		}
	}
	defer file.Close()

	fi, _ := file.Stat()
	// 设置正确的Content-Type
	if strings.HasSuffix(filePath, ".dds") {
		w.Header().Set("Content-Type", "image/vnd.ms-dds")
	} else {
		w.Header().Set("Content-Type", "image/png")
	}
	w.Header().Set("Content-Length", strconv.FormatInt(fi.Size(), 10))
	io.Copy(w, file)
}

// 根路径处理器
func rootHandler(w http.ResponseWriter, r *http.Request) {
	// 处理根路径和支持的文件类型
	isRootPath := r.URL.Path == "/"
	ext := strings.ToLower(filepath.Ext(r.URL.Path))
	supportedExts := map[string]bool{
		".html": true,
		".js":   true,
		".css":  true,
		".png":  true,
		".jpg":  true,
		".jpeg": true,
		".svg":  true,
	}

	if r.URL.Path == "/events" {
		serveFileWithGzip(w, r, "index.html")
		return
	}

	if r.URL.Path == "/driver" {
		serveFileWithGzip(w, r, "driver.html")
		return
	}

	if r.URL.Path == "/result" {
		// 获取eventId cookie
		cookie, err := r.Cookie("eventId")
		if err != nil {
			// 重定向到事件列表页要求选择事件
			http.Redirect(w, r, "/events", http.StatusSeeOther)
			return
		}
		// 重定向到带eventId的URL路径
		http.Redirect(w, r, "/ac/event/"+cookie.Value+"/result", http.StatusSeeOther)
		return
	}

	if r.URL.Path == "/bestlap" {
		serveFileWithGzip(w, r, "bestlap.html")
		return
	}

	if regexp.MustCompile(`^/ac/event/([0-9]+)/live$`).MatchString(r.URL.Path) {
		serveFileWithGzip(w, r, "live.html")
		return
	}

	if isRootPath {
		// 根路径指向index.html
		serveFileWithGzip(w, r, "index.html")
		return
	} else if ext == ".html" {
		// HTML文件使用统一处理
		relativePath := strings.TrimPrefix(r.URL.Path, "/")
		serveFileWithGzip(w, r, relativePath)
		return
	} else if ext == ".js" || ext == ".css" {
		// JavaScript和CSS文件使用serveFileWithGzip处理，确保正确设置Content-Type和处理压缩
		relativePath := strings.TrimPrefix(r.URL.Path, "/")
		serveFileWithGzip(w, r, relativePath)
		return
	} else if supportedExts[ext] || ext == "" {
		// 其他静态文件由文件服务器处理
		fs.ServeHTTP(w, r)
		return
	}

	// 非HTML文件由文件服务器处理
	fs.ServeHTTP(w, r)
}

// 分析页面处理器
func analysisHandler(w http.ResponseWriter, r *http.Request) {
	// 解析URL路径
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")

	// 处理 /analysis/lap/{id} 格式
	if len(parts) >= 2 && parts[1] == "lap" && len(parts) >= 3 && parts[2] != "" {
		lapID := parts[2]
		if _, err := strconv.Atoi(lapID); err == nil {
			http.SetCookie(w, &http.Cookie{
				Name:  "lapId",
				Value: lapID,
				Path:  "/",
			})
			serveFileWithGzip(w, r, "analysis.html")
			return
		}
		logger.Printf("无效的lap ID: %s", lapID)
		sendErrorResponse(w, http.StatusBadRequest, "无效的lap ID", "lapID")
		return
	}

	// 处理 /analysis/compare/lap1/{id1}/lap2/{id2} 格式
	if len(parts) >= 6 && parts[1] == "compare" && parts[2] == "lap1" && parts[4] == "lap2" && parts[3] != "" && parts[5] != "" {
		lap1ID := parts[3]
		lap2ID := parts[5]
		if _, err1 := strconv.Atoi(lap1ID); err1 == nil {
			if _, err2 := strconv.Atoi(lap2ID); err2 == nil {
				http.SetCookie(w, &http.Cookie{
					Name:  "lap1Id",
					Value: lap1ID,
					Path:  "/",
				})
				http.SetCookie(w, &http.Cookie{
					Name:  "lap2Id",
					Value: lap2ID,
					Path:  "/",
				})
			}
			serveFileWithGzip(w, r, "analysis.html")
			return
		}
		logger.Printf("无效的lap ID: lap1=%s, lap2=%s", lap1ID, lap2ID)
		sendErrorResponse(w, http.StatusBadRequest, "无效的lap ID", "lapID")
		return
	}

	if r.URL.Path == "/analysis/" {
		serveFileWithGzip(w, r, "analysis.html")
		return
	}
	// 未找到匹配的路径
	http.NotFound(w, r)
}

// AC事件页面处理器
func acEventHandler(w http.ResponseWriter, r *http.Request) {
	// 解析URL路径获取事件ID
	parts := strings.Split(r.URL.Path, "/")
	// 路径格式应为: ac/event/{event_id}/result
	if len(parts) >= 5 && parts[4] == "result" && len(parts[3]) > 0 {
		eventID := parts[3]
		if _, err := strconv.Atoi(eventID); err == nil {
			http.SetCookie(w, &http.Cookie{
				Name:  "eventId",
				Value: eventID,
				Path:  "/",
			})
			serveFileWithGzip(w, r, "result.html")
			return
		}
		logger.Printf("无效的事件ID: %s", eventID)
		sendErrorResponse(w, http.StatusBadRequest, "无效的事件ID", "eventID")
		return
	}

	if len(parts) >= 5 && parts[4] == "live" && len(parts[3]) > 0 {
		eventId := parts[3]
		// 验证eventId是数字
		if _, err := strconv.Atoi(eventId); err == nil {
			http.SetCookie(w, &http.Cookie{
				Name:  "eventId",
				Value: eventId,
				Path:  "/",
			})
			serveFileWithGzip(w, r, "live.html")
			return
		}
		logger.Printf("无效的事件ID: %s", eventId)
		sendErrorResponse(w, http.StatusBadRequest, "无效的事件ID", "eventID")
		return
	}
	// 未找到匹配的路径
	http.NotFound(w, r)
}
