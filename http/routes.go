// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

import (
	"net/http"
)

// 设置HTTP路由
func setupRoutes() {
	// 静态文件服务
	fs = http.FileServer(http.Dir("html"))
	// 创建自定义处理器处理所有HTML文件和静态资源
	http.HandleFunc("/", methodMiddleware(rootHandler, "GET"))
	http.HandleFunc("/analysis/", methodMiddleware(analysisHandler, "GET"))
	http.HandleFunc("/ac/event/", methodMiddleware(acEventHandler, "GET"))
	http.HandleFunc("/api/ac/events", methodMiddleware(handleACEvents, "GET"))
	http.HandleFunc("/api/ac/events/", methodMiddleware(handleACEvents, "GET"))
	http.HandleFunc("/api/ac/events/live", methodMiddleware(handleACEventsLive, "GET"))
	http.HandleFunc("/api/ac/teams/", methodMiddleware(handleACTeams, "GET"))
	http.HandleFunc("/api/ac/car/", methodMiddleware(handleACCars, "GET"))
	http.HandleFunc("/api/ac/cars", methodMiddleware(handleACCars, "GET"))
	http.HandleFunc("/api/ac/cars/", methodMiddleware(handleACCars, "GET"))
	http.HandleFunc("/api/ac/users", methodMiddleware(handleACUsers, "GET"))
	http.HandleFunc("/api/ac/users/", methodMiddleware(handleACUsers, "GET"))
	http.HandleFunc("/api/ac/user", methodMiddleware(handleACUserDetails, "GET"))
	http.HandleFunc("/api/ac/user/", methodMiddleware(handleACUserDetails, "GET"))
	http.HandleFunc("/api/ac/team/", methodMiddleware(handleACTeamMembers, "GET"))
	http.HandleFunc("/api/ac/track/", methodMiddleware(handleACTrack, "GET"))
	http.HandleFunc("/api/ac/lap/summary/", methodMiddleware(handleACLapSummary, "GET"))
	http.HandleFunc("/api/ac/lap/telemetry/", methodMiddleware(handleACLapTelemetry, "GET"))
	http.HandleFunc("/api/ac/session/", methodMiddleware(handleACSession, "GET"))
	http.HandleFunc("/api/ac/event", methodMiddleware(handleACEventDetails, "GET"))
	http.HandleFunc("/api/ac/event/", methodMiddleware(handleACEventDetails, "GET"))
	http.HandleFunc("/api/ac/tracks", methodMiddleware(handleACTracks, "GET"))
	http.HandleFunc("/api/ac/bestlap/", methodMiddleware(handleACBestLap, "GET"))
	http.HandleFunc("/images/ac/track/", handleTrackResource)
	http.HandleFunc("/images/ac/car/", handleCarResource)
	http.HandleFunc("/live", handleACEventWebSocket)

	logger.Println("路由配置完成")
}
