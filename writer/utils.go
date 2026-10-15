// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// boolToInt 将布尔值转换为整数，true 转换为 1，false 转换为 0
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// firstExistingPath 返回第一个存在的路径，都没有时返回空字符串
func firstExistingPath(paths ...string) string {
	for _, p := range paths {
		if p == "" {
			continue
		}
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

// calculateElapsedMs 计算会话已用时间（毫秒）
func calculateElapsedMs(sessionID int64) (int64, error) {
	// 查询会话开始时间（start_time 允许为 NULL）
	var startTime sql.NullTime
	query := "SELECT start_time FROM session WHERE session_id = ?"
	if err := globalDBWriter.DB.QueryRow(query, sessionID).Scan(&startTime); err != nil {
		return 0, err
	}
	if !startTime.Valid {
		return 0, nil
	}

	// 计算已用时间（毫秒）
	return time.Since(startTime.Time).Milliseconds(), nil
}

// findLatestSessionLogFile 查找最新的会话日志文件
func findLatestSessionLogFile(logDir string) (os.DirEntry, error) {
	// 读取日志目录
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return nil, err
	}

	var latestFile os.DirEntry
	var latestTime time.Time

	// 遍历所有文件，找到最新的会话日志文件
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// AC 启动脚本生成 output<日期>_<时间>.log；WMIC 无结果时为 output_.log。
		// 按最后修改时间选当前会话日志，不使用原代码误设的 acServer_ 前缀。
		name := entry.Name()
		if strings.HasPrefix(name, "output") && strings.HasSuffix(name, ".log") {
			// 获取文件修改时间
			info, err := entry.Info()
			if err != nil {
				continue
			}

			// 比较时间，找出最新的文件
			if latestFile == nil || info.ModTime().After(latestTime) {
				latestFile = entry
				latestTime = info.ModTime()
			}
		}
	}

	if latestFile == nil {
		return nil, os.ErrNotExist
	}

	return latestFile, nil
}

var writerLogFile *os.File

// initLogger 初始化日志系统
func initLogger() {
	logDir := "logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatalf("创建日志目录失败: %v", err)
	}

	logFile, err := os.OpenFile(
		filepath.Join(logDir, fmt.Sprintf("simview-writer-%s.log", time.Now().Format("2006-01-02-15-04-05"))),
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		0644,
	)
	if err != nil {
		log.Fatalf("打开日志文件失败: %v", err)
	}

	writerLogFile = logFile
	// 创建多输出写入器，同时输出到文件和控制台
	multiWriter := io.MultiWriter(os.Stdout, logFile)

	// 设置全局日志器
	logger = log.New(multiWriter, "[SimView] ", log.Ldate|log.Ltime)
	fatalLogger = log.New(multiWriter, "[SimView][FATAL] ", log.Ldate|log.Ltime|log.Lshortfile)
}

// pointInPolygon 判断点是否在多边形内（射线法）
func pointInPolygon(point [2]float32, polygon [][3]float32) bool {
	if len(polygon) < 3 {
		return false
	}
	inside := false
	n := len(polygon)

	for i, j := 0, n-1; i < n; j, i = i, i+1 {
		iPoint := polygon[i]
		jPoint := polygon[j]

		// 检查点是否在多边形顶点上
		if (iPoint[0] == point[0] && iPoint[2] == point[1]) ||
			(jPoint[0] == point[0] && jPoint[2] == point[1]) {
			return true
		}

		// 检查边是否与射线相交
		if (iPoint[2] > point[1]) != (jPoint[2] > point[1]) {
			xIntersect := (point[1]-iPoint[2])*(jPoint[0]-iPoint[0])/(jPoint[2]-iPoint[2]) + iPoint[0]
			if point[0] < xIntersect {
				inside = !inside
			}
		}
	}
	return inside
}

// 槽位 -> 用户ID/GUID 缓存：避免同一辆车反复查库或建出重复用户
var (
	carUserCacheMutex sync.Mutex
	carUserIDCache    = make(map[int]uint32)
	carGUIDCache      = make(map[int]string)
)

// peekConnection 读取槽位对应的连接信息。
// 刻意不加 connectionsMutex：调用方（连接/车辆更新处理器）可能已持有该锁，
// 重复加锁会死锁；连接表由主代理引入的 stateMutex 统一保护。
func peekConnection(carID int) *ConnectionInfo {
	if conn, ok := globalConnections[carID]; ok {
		return conn
	}
	return nil
}

// firstGUID 从 entry_list 的 GUID 字段取第一个可用值（车队赛事为分号分隔的多 GUID）
func firstGUID(guids string) string {
	for _, g := range strings.Split(guids, ";") {
		g = strings.TrimSpace(g)
		if g != "" {
			return g
		}
	}
	return ""
}

// parseEntryTeam 解析 entry_list 的 TEAM 字段。
// useNumber 为 true 时格式是 "编号|车队名"（例如 "12|Red Bull"），否则整串即车队名。
// 返回车队名与车号（没有合法编号时车号为 0）。
func parseEntryTeam(raw string, useNumber bool) (name string, no int) {
	raw = strings.TrimSpace(raw)
	if !useNumber {
		return raw, 0
	}
	parts := strings.SplitN(raw, "|", 2)
	if len(parts) != 2 {
		return "", 0
	}
	no, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	teamName := strings.TrimSpace(parts[1])
	if err != nil || no <= 0 {
		return teamName, 0
	}
	return teamName, no
}

// getUserIDByCarID 根据车辆槽位获取用户ID（非团队赛事）。
// 取值优先级：连接上缓存的 UserID > 本地缓存 > 按 GUID 查询/创建。
// 不再用 Steam64ID=0 去查/造用户，避免所有车都落到同一个错误的 user 上。
func getUserIDByCarID(carID int) uint32 {
	conn := peekConnection(carID)
	if conn != nil && conn.UserID > 0 {
		return uint32(conn.UserID)
	}

	carUserCacheMutex.Lock()
	if id, ok := carUserIDCache[carID]; ok && id > 0 {
		carUserCacheMutex.Unlock()
		return id
	}
	carUserCacheMutex.Unlock()

	// GUID 优先取连接信息，其次取 entry_list（启动后只读）
	guid := ""
	name := ""
	if conn != nil {
		guid = conn.DriverGUID
		name = conn.DriverName
	}
	if guid == "" {
		entryListMutex.Lock()
		if driver, ok := globalEntryList[carID]; ok {
			guid = firstGUID(driver.GUID)
			if name == "" {
				name = driver.Name
			}
		}
		entryListMutex.Unlock()
	}
	if guid == "" {
		// 连接与 entry_list 都拿不到时，回退最近一次解析到的 GUID
		carUserCacheMutex.Lock()
		guid = carGUIDCache[carID]
		carUserCacheMutex.Unlock()
	}

	steam64ID, err := strconv.ParseInt(strings.TrimSpace(guid), 10, 64)
	if err != nil || steam64ID <= 0 {
		// 没有有效 GUID 时返回 0，交由前端按“未知用户”展示
		return 0
	}

	userID, _, err := globalDBWriter.FindOrCreateUser(&User{Steam64ID: steam64ID, Name: name})
	if err != nil || userID <= 0 {
		if err != nil {
			logger.Printf("按GUID解析用户失败(carID=%d, guid=%s): %v", carID, guid, err)
		}
		return 0
	}

	carUserCacheMutex.Lock()
	carUserIDCache[carID] = uint32(userID)
	carGUIDCache[carID] = guid
	carUserCacheMutex.Unlock()
	return uint32(userID)
}

// getTeamIDByCarID 根据车辆槽位获取团队ID（团队赛事）。
// 匹配规则：同一 event 下先按 entry_list 的车队名匹配，再退回按 entry_list 车型匹配团队车辆。
func getTeamIDByCarID(carID int) uint32 {
	conn := peekConnection(carID)
	if conn != nil && conn.TeamID > 0 {
		return uint32(conn.TeamID)
	}

	entryListMutex.Lock()
	entry, hasEntry := globalEntryList[carID]
	entryListMutex.Unlock()
	if !hasEntry {
		return 0
	}

	useNumber := cfg != nil && cfg.ACServer.Event.Team.UseNumber
	teamName, _ := parseEntryTeam(entry.Team, useNumber)
	model := entry.Car

	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	for _, team := range globalTeams {
		if team == nil || team.Active != 1 || team.EventID != globalEvent.ID {
			continue
		}
		// 车队名直接命中
		if teamName != "" && team.Name == teamName {
			return uint32(team.ID)
		}
		// 退回到按车型匹配（team.CarID 是数据库 car_id）
		if teamName == "" && model != "" {
			if car, ok := globalCars[team.CarID]; ok && car != nil && car.Name == model {
				return uint32(team.ID)
			}
		}
	}
	return 0
}

// getTeamNameByID 根据团队ID获取团队名称
func getTeamNameByID(teamID int) string {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	for _, team := range globalTeams {
		if team.ID == teamID && team.Active == 1 {
			return team.Name
		}
	}
	return "" // 未找到时返回空字符串
}

// getUserNameByID 根据用户ID获取用户名
func getUserNameByID(userID int) string {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	for _, user := range globalUsers {
		if user.UserID == userID {
			return user.Name
		}
	}
	return "" // 未找到时返回空字符串
}
