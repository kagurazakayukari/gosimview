// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

import (
	"database/sql"
)

// 最佳圈速数据结构
type ACBestLap struct {
	LapID            int              `json:"lap_id"`
	User_id          int              `json:"user_id"`
	Car_id           int              `json:"car_id"`
	Time             float64          `json:"time"`
	Gap              int              `json:"gap,omitempty"`
	GapPer           float64          `json:"gap_per,omitempty"`
	Sector1          int              `json:"sector_1"`
	Sector2          int              `json:"sector_2"`
	Sector3          int              `json:"sector_3"`
	Grip             int              `json:"grip"`
	Tyre             string           `json:"tyre"`
	MaxSpeed         int              `json:"max_speed"`
	CarBest          int              `json:"car_best,omitempty"`
	ClassBest        int              `json:"class_best,omitempty"`
	Sector1CarBest   int              `json:"sector_1_car_best,omitempty"`
	Sector2CarBest   int              `json:"sector_2_car_best,omitempty"`
	Sector3CarBest   int              `json:"sector_3_car_best,omitempty"`
	Sector1ClassBest int              `json:"sector_1_class_best,omitempty"`
	Sector2ClassBest int              `json:"sector_2_class_best,omitempty"`
	Sector3ClassBest int              `json:"sector_3_class_best,omitempty"`
	FinishedAt       TimeWithTimezone `json:"finished_at"`
	EventID          int              `json:"event_id,omitempty"`
}

// 数据库表对应的数据结构定义
// 赛道配置
type ACTrackConfig struct {
	TrackConfigID int    `json:"track_config_id"`
	TrackName     string `json:"track_name"`
	ConfigName    string `json:"config_name"`
	DisplayName   string `json:"display_name"`
	Country       string `json:"country"`
	City          string `json:"city"`
	Length        int    `json:"length"`
}

// 赛事事件
type ACEvent struct {
	EventID              int    `json:"event_id"`
	ServerName           string `json:"server_name"`
	TrackConfigID        int    `json:"track_config_id"`
	Name                 string `json:"name"`
	TeamEvent            int    `json:"team_event"`
	Active               int    `json:"active"`
	PracticeDuration     int    `json:"practice_duration"`
	QualiDuration        int    `json:"quali_duration"`
	RaceDuration         int    `json:"race_duration"`
	RaceDurationType     int    `json:"race_duration_type"`
	RaceExtraLaps        int    `json:"race_extra_laps"`
	ReverseGridPositions int    `json:"reverse_grid_positions"`
}

// 团队成员
type ACTeamMember struct {
	TeamMemberID int    `json:"team_member_id"`
	TeamID       int    `json:"team_id"`
	UserID       int    `json:"user_id"`
	Role         string `json:"role"`
}

// 团队数据结构定义
type ACTeam struct {
	TeamID     int    `json:"team_id"`
	Name       string `json:"name"`
	Country    string `json:"country,omitempty"`
	CarID      int    `json:"car_id,omitempty"`
	TeamNo     int    `json:"team_no,omitempty"`
	LiveryName string `json:"livery_name,omitempty"`
}

// 车辆数据结构定义
type ACCar struct {
	CarID        int    `json:"car_id"`
	Name         string `json:"name"`
	DisplayName  string `json:"display_name"`
	Manufacturer string `json:"manufacturer"`
	CarClass     string `json:"car_class"`
	Model        string `json:"model"`
	TeamID       int    `json:"team_id,omitempty"`
}

// 用户数据结构定义
type ACUser struct {
	UserID   int    `json:"user_id"`
	Name     string `json:"name"`
	PrevName string `json:"prev_name"`
	Country  string `json:"country"`
}

// 会话数据结构定义
type ACSession struct {
	SessionID    int64            `json:"session_id"`
	EventID      int              `json:"event_id"`
	Type         int              `json:"type"`
	TrackTime    string           `json:"track_time"`
	Name         string           `json:"name"`
	StartTime    TimeWithTimezone `json:"start_time"`
	DurationMin  int              `json:"duration_min"`
	ElapsedMs    int64            `json:"elapsed_ms"`
	Laps         int              `json:"laps"`
	Weather      string           `json:"weather"`
	AirTemp      int              `json:"air_temp"`
	RoadTemp     int              `json:"road_temp"`
	StartGrip    float64          `json:"start_grip"`
	CurrentGrip  float64          `json:"current_grip"`
	IsFinished   int              `json:"is_finished"`
	FinishTime   TimeWithTimezone `json:"finish_time"`
	LastActivity TimeWithTimezone `json:"last_activity"`
	HttpPort     int              `json:"http_port"`
}

type ACLiveData struct {
	SessionID string `json:"session_id"`
	Timestamp string `json:"timestamp"`
	Status    string `json:"status"`
}

type ACLivePosition struct {
	DriverID   string  `json:"driver_id"`
	DriverName string  `json:"driver_name"`
	Position   int     `json:"position"`
	X          float64 `json:"x"`
	Y          float64 `json:"y"`
	Z          float64 `json:"z"`
}

type ACResult struct {
	SessionID     string `json:"session_id"`
	WinnerDriver  string `json:"winner_driver"`
	WinnerTeam    string `json:"winner_team"`
	LapsCompleted int    `json:"laps_completed"`
	FastestLap    string `json:"fastest_lap"`
}

type ACStint struct {
	ID        string `json:"id"`
	DriverID  string `json:"driver_id"`
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time"`
	Duration  string `json:"duration"`
}

type ACDriverStint struct {
	ID         string `json:"id"`
	DriverName string `json:"driver_name"`
	StartTime  string `json:"start_time"`
	EndTime    string `json:"end_time"`
	Duration   string `json:"duration"`
}

type Lap struct {
	LapID      int              `json:"lap_id"`
	LapTime    int64            `json:"lap_time"`
	Sector1    int64            `json:"sector_1"`
	Sector2    int64            `json:"sector_2"`
	Sector3    int64            `json:"sector_3"`
	Grip       int              `json:"grip"`
	Tyre       string           `json:"tyre"`
	AvgSpeed   int              `json:"avg_speed"`
	MaxSpeed   int              `json:"max_speed"`
	Cuts       int              `json:"cuts"`
	Crashes    int              `json:"crashes"`
	CarCrashes int              `json:"car_crashes"`
	FinishAt   TimeWithTimezone `json:"finish_at"`
	BestLap    int              `json:"best_lap,omitempty"`
}

type ACTeamStint struct {
	UserID      int   `json:"user_id"`
	TotalLaps   int   `json:"total_laps"`
	ValidLaps   int   `json:"valid_laps"`
	BestLapTime int64 `json:"best_lap_time"`
	AvgLapTime  int64 `json:"avg_lap_time"`
	AvgLapGap   int64 `json:"avg_lap_gap"`
	Laps        []Lap `json:"laps"`
}

type ACUserCarStint struct {
	ID        string `json:"id"`
	UserName  string `json:"user_name"`
	CarModel  string `json:"car_model"`
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time"`
	Duration  string `json:"duration"`
}

type ACStanding struct {
	SessionStintID int `json:"session_stint_id,omitempty"`
	TeamID         int `json:"team_id,omitempty"`
	UserID         int `json:"user_id,omitempty"`
	CarID          int `json:"car_id"`
	LapID          int `json:"lap_id,omitempty"`
	Laps           int `json:"laps,omitempty"`
	ValidLaps      int `json:"valid_laps"`
	BestLapTime    int `json:"best_lap_time,omitempty"`
	Sector1        int `json:"sector_1,omitempty"`
	Sector2        int `json:"sector_2,omitempty"`
	Sector3        int `json:"sector_3,omitempty"`
	TotalTime      int `json:"total_time,omitempty"`
	Gap            int `json:"gap,omitempty"`
	Interval       int `json:"interval,omitempty"`
}

// 从数据库获取赛事数据
type Entry struct {
	SessionStintID uint32
	TeamID         uint32
	UserID         uint32
	CarID          uint16
	GameCarID      int
	DriverName     string
	TeamName       string
	StatusMask     uint8
	Laps           uint16
	ValidLaps      uint16
	NSP            float32
	PosX           float32
	PosZ           float32
	TelemetryMask  uint16
	RPM            uint16
	TyreLength     uint8
	Tyre           string
	BestLapS1      uint32
	BestLapS2      uint32
	BestLapS3      uint32
	BestLapTime    uint32
	CurrentLapS1   uint32
	CurrentLapS2   uint32
	CurrentLapS3   uint32
	SectorMask     uint8
	LastLapTime    int32
	Gap            int32
	Interval       int32
	PosChange      int8
}

type LapInfo struct {
	StintLapID int64            `json:"stint_lap_id"`
	StintID    int64            `json:"stint_id"`
	Sector1    int64            `json:"sector_1"`
	Sector2    int64            `json:"sector_2"`
	Sector3    int64            `json:"sector_3"`
	Grip       int64            `json:"grip"`
	Tyre       string           `json:"tyre"`
	Time       int64            `json:"time"`
	Cuts       int64            `json:"cuts"`
	Crashes    int64            `json:"crashes"`
	CarCrashes int64            `json:"car_crashes"`
	MaxSpeed   int64            `json:"max_speed"`
	AvgSpeed   int64            `json:"avg_speed"`
	FinishedAt TimeWithTimezone `json:"finished_at"`
}

type Summary struct {
	EventID       int64   `json:"event_id"`
	EventName     string  `json:"event_name"`
	TrackName     string  `json:"track_name"`
	TrackLength   int64   `json:"track_length"`
	TrackConfigID int64   `json:"track_config_id"`
	UserID        int64   `json:"user_id"`
	CarID         int64   `json:"car_id"`
	SessionType   string  `json:"session_type"`
	AirTemp       string  `json:"air_temp"`
	RoadTemp      string  `json:"road_temp"`
	Weather       string  `json:"weather"`
	Lap           LapInfo `json:"lap"`
}

// 事件详情响应结构体
type ACEventResponse struct {
	EventID              int    `json:"event_id"`
	Name                 string `json:"name"`
	ServerName           string `json:"server_name"`
	TrackConfigID        int    `json:"track_config_id"`
	TeamEvent            int    `json:"team_event"`
	Active               int    `json:"active"`
	LiveryPreview        int    `json:"livery_preview"`
	UseNumber            int    `json:"use_number"`
	PracticeDuration     int    `json:"practice_duration"`
	QualiDuration        int    `json:"quali_duration"`
	RaceDuration         int    `json:"race_duration"`
	RaceDurationType     int    `json:"race_duration_type"`
	RaceWaitTime         int    `json:"race_wait_time"`
	RaceExtraLaps        int    `json:"race_extra_laps"`
	ReverseGridPositions int    `json:"reverse_grid_positions"`
}

type SectorResult struct {
	UserID         int  `json:"user_id"`
	CarID          int  `json:"car_id"`
	BestSectorTime int  `json:"best_sector_time"`
	Gap            *int `json:"gap,omitempty"`
	Interval       *int `json:"interval,omitempty"`
}

type SectorResponse struct {
	Sector1 []SectorResult `json:"sector1"`
	Sector2 []SectorResult `json:"sector2"`
	Sector3 []SectorResult `json:"sector3"`
}

type StintInfo struct {
	UserID     int64
	CarID      int64
	TeamID     sql.NullInt64
	StartedAt  TimeWithTimezone
	FinishedAt TimeWithTimezone
}

type DriverEvent struct {
	EventID          int              `json:"event_id"`
	EventName        string           `json:"event_name"`
	TrackName        string           `json:"track_name"`
	TeamName         string           `json:"team_name"`
	DistanceDrivenKm float64          `json:"distance_driven_km"`
	TotalLaps        int              `json:"total_laps"`
	TotalValidLaps   int              `json:"total_valid_laps"`
	TimeAgoSec       int64            `json:"time_ago_sec"`
	EventTime        TimeWithTimezone `json:"event_time"` // 事件时间戳(秒级)
}

type TrackSummary struct {
	TrackName      string  `json:"track_name"`
	DistanceDriven float64 `json:"distance_driven"`
}

type CarSummary struct {
	CarName        string  `json:"car_name"`
	DistanceDriven float64 `json:"distance_driven"`
}

type DriverSummary struct {
	UserID                int            `json:"user_id"`
	TotalEvents           int            `json:"total_events"`
	TotalDistanceDrivenKm float64        `json:"total_distance_driven_km"`
	TotalLaps             int            `json:"total_laps"`
	TotalValidLaps        int            `json:"total_valid_laps"`
	Events                []DriverEvent  `json:"events"`
	TopTracks             []TrackSummary `json:"top_tracks"`
	TopCars               []CarSummary   `json:"top_cars"`
}

type TeamLapTimes struct {
	TeamID   int   `json:"team_id"`
	CarID    int   `json:"car_id"`
	LapTimes []int `json:"lap_times"`
}
type UserLapTimes struct {
	UserID   int   `json:"user_id"`
	CarID    int   `json:"car_id"`
	LapTimes []int `json:"lap_times"`
}

type rankedTeamLapData struct {
	TeamLapTimes
	Stability   float64
	bestLapTime int
}

type rankedLapData struct {
	UserLapTimes
	Stability   float64
	bestLapTime int
}

type Pitstop struct {
	UserID  *int64 `json:"user_id,omitempty"`
	TeamID  *int64 `json:"team_id,omitempty"`
	CarID   int64  `json:"car_id"`
	PitTime int64  `json:"pit_time"`
}
