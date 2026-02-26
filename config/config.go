// This file is part of SimView Extension
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
)

// TimeWithTimezone 带时区的时间戳类型
type TimeWithTimezone int64

func (t TimeWithTimezone) MarshalJSON() ([]byte, error) {
	// 获取本地时区偏移（秒）
	_, offset := time.Now().Zone()
	// 转换为微秒并添加到时间戳
	adjustedTimestamp := int64(t) + int64(offset)*1000000
	return json.Marshal(adjustedTimestamp)
}

// DatabaseConfig 数据库配置
type DatabaseConfig struct {
	Host      string                 `toml:"host"`
	Port      int                    `toml:"port"`
	User      string                 `toml:"user"`
	Schema    string                 `toml:"schema"`
	Password  string                 `toml:"password"`
	RawConfig map[string]interface{} `toml:"-"`
}

// Config 应用配置主结构
type Config struct {
	Game      GameConfig      `toml:"game"`
	App       AppConfig       `toml:"app"`
	Writer    WriterConfig    `toml:"writer"`
	Log       LogConfig       `toml:"log"`
	ACServer  ACServerConfig  `toml:"ac.server"`
	Database  DatabaseConfig  `toml:"database"`
	Overrides OverridesConfig `toml:"overrides"`
}

// GameConfig 游戏配置
type GameConfig struct {
	Path      string                 `toml:"path"`
	RawConfig map[string]interface{} `toml:"-"`
}

// AppConfig 应用配置
type AppConfig struct {
	Realtime struct {
		Update struct {
			Interval struct {
				MS int `toml:"ms"`
			} `toml:"interval"`
		} `toml:"update"`
	} `toml:"realtime"`
	Server struct {
		Host       string `toml:"host"`
		Port       int    `toml:"port"`
		PublicHost string `toml:"public.host"`
		DocRoot    string `toml:"doc.root"`
		CachePath  string `toml:"cache.path"`
		Threads    int    `toml:"threads"`
	} `toml:"server"`
	Live struct {
		SessionFeedShowChat bool `toml:"session.feed.show.chat"`
	} `toml:"live"`
	SSL struct {
		CertFile             string `toml:"cert.file"`
		PrivateKeyFile       string `toml:"private.key.file"`
		PrivateKeyPassphrase string `toml:"private.key.passphrase"`
	} `toml:"ssl"`
	RawConfig map[string]interface{} `toml:"-"`
}

// WriterConfig 写入器配置
type WriterConfig struct {
	HTTP struct {
		Port        int `toml:"port"`
		Leaderboard struct {
			Broadcast struct {
				Interval struct {
					MS int `toml:"ms"`
				} `toml:"interval"`
			} `toml:"broadcast"`
		} `toml:"leaderboard"`
	} `toml:"http"`
	RawConfig map[string]interface{} `toml:"-"`
}

// LogConfig 日志配置
type LogConfig struct {
	Target    string                 `toml:"target"`
	Level     string                 `toml:"level"`
	RawConfig map[string]interface{} `toml:"-"`
}

// ACServerConfig AC服务器配置
type ACServerConfig struct {
	Host             string `toml:"host"`
	UpdateDriverName bool   `toml:"update.driver.name"`
	UDP              struct {
		Port       int  `toml:"port"`
		LocalPort  int  `toml:"local.port"`
		UseACSMUDP bool `toml:"use.acsm.udp"`
		Realtime   struct {
			Update struct {
				Interval struct {
					MS int `toml:"ms"`
				} `toml:"interval"`
			} `toml:"update"`
		} `toml:"realtime"`
	} `toml:"udp"`
	Event struct {
		Name               string `toml:"name"`
		LapTelemetryEnable bool   `toml:"lap.telemetry.enable"`
		Team               struct {
			Enable              bool `toml:"enable"`
			UseNumber           bool `toml:"use.number"`
			LiveryPreviewEnable bool `toml:"livery.preview.enable"`
		} `toml:"team"`
	} `toml:"event"`
	RawConfig map[string]interface{} `toml:"-"`
}

// OverridesConfig 覆盖配置
type OverridesConfig struct {
	CarClass    map[string]string      `toml:"car.class"`
	UserCountry map[string]string      `toml:"user.country"`
	RawConfig   map[string]interface{} `toml:"-"`
}

// LoadConfig 从文件加载配置
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %v", err)
	}

	config, err := ExtractConfigFromTOML(data)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

// 收集ACServer所有配置项
// collectConfigSection 从原始配置中收集指定节的配置
func collectConfigSection(rawConfig map[string]interface{}, section string) map[string]interface{} {
	parts := strings.Split(section, ".")
	current := rawConfig
	for _, part := range parts {
		if data, ok := current[part].(map[string]interface{}); ok {
			current = data
		} else {
			return map[string]interface{}{}
		}
	}

	result := make(map[string]interface{})
	collectNestedConfig(current, "", result)
	return result
}

func collectNestedConfig(data map[string]interface{}, prefix string, result map[string]interface{}) {
	for key, value := range data {
		fullKey := key
		if prefix != "" {
			fullKey = prefix + "." + key
		}

		if nestedMap, ok := value.(map[string]interface{}); ok {
			collectNestedConfig(nestedMap, fullKey, result)
		} else {
			result[fullKey] = value
		}
	}
}

// getStringValue 从配置映射中获取字符串值
func getStringValue(config map[string]interface{}, key string) string {
	if value, ok := config[key].(string); ok {
		return value
	}
	return ""
}

// getIntValue 从配置映射中获取整数值
func getIntValue(config map[string]interface{}, key string) int {
	if value, ok := config[key].(int64); ok {
		return int(value)
	}
	return 0
}

// getBoolValue 从配置映射中获取布尔值
func getBoolValue(config map[string]interface{}, key string) bool {
	if value, ok := config[key].(bool); ok {
		return value
	}
	return false
}

// getMapStringValue 从配置映射中获取字符串映射
func getMapStringValue(config map[string]interface{}, key string) map[string]string {
	result := make(map[string]string)
	if value, ok := config[key].(map[string]interface{}); ok {
		for k, v := range value {
			if strVal, ok := v.(string); ok {
				result[k] = strVal
			}
		}
	}
	return result
}

// ExtractConfigFromTOML 从TOML数据中提取配置，统一使用字符串识别方式
func ExtractConfigFromTOML(data []byte) (Config, error) {
	// 初始化配置实例
	config := Config{}

	// 解析原始TOML数据
	var rawConfig map[string]interface{}
	if err := toml.Unmarshal(data, &rawConfig); err != nil {
		return config, fmt.Errorf("解析TOML配置失败: %v", err)
	}

	// 初始化并收集各模块配置到RawConfig
	config.Database.RawConfig = collectConfigSection(rawConfig, "database")
	config.App.RawConfig = collectConfigSection(rawConfig, "app")
	config.Game.RawConfig = collectConfigSection(rawConfig, "game")
	config.Log.RawConfig = collectConfigSection(rawConfig, "log")
	config.Writer.RawConfig = collectConfigSection(rawConfig, "writer")
	config.Overrides.RawConfig = collectConfigSection(rawConfig, "overrides")

	// 映射数据库配置
	config.Database.Host = getStringValue(config.Database.RawConfig, "host")
	config.Database.Port = getIntValue(config.Database.RawConfig, "port")
	config.Database.User = getStringValue(config.Database.RawConfig, "user")
	config.Database.Password = getStringValue(config.Database.RawConfig, "password")
	config.Database.Schema = getStringValue(config.Database.RawConfig, "schema")

	// 映射应用配置
	config.App.Server.Host = getStringValue(config.App.RawConfig, "server.host")
	config.App.Server.Port = getIntValue(config.App.RawConfig, "server.port")
	config.App.Server.PublicHost = getStringValue(config.App.RawConfig, "server.public.host")
	config.App.Server.DocRoot = getStringValue(config.App.RawConfig, "server.doc.root")
	config.App.Server.CachePath = getStringValue(config.App.RawConfig, "server.cache.path")
	config.App.Server.Threads = getIntValue(config.App.RawConfig, "server.threads")
	config.App.Live.SessionFeedShowChat = getBoolValue(config.App.RawConfig, "live.session.feed.show.chat")
	config.App.SSL.CertFile = getStringValue(config.App.RawConfig, "ssl.cert.file")
	config.App.SSL.PrivateKeyFile = getStringValue(config.App.RawConfig, "ssl.private.key.file")
	config.App.SSL.PrivateKeyPassphrase = getStringValue(config.App.RawConfig, "ssl.private.key.passphrase")
	config.App.Realtime.Update.Interval.MS = getIntValue(config.App.RawConfig, "realtime.update.interval.ms")

	// 映射游戏配置
	config.Game.Path = getStringValue(config.Game.RawConfig, "path")

	// 映射日志配置
	config.Log.Target = getStringValue(config.Log.RawConfig, "target")
	config.Log.Level = getStringValue(config.Log.RawConfig, "level")

	// 映射ACServer配置
	config.ACServer.RawConfig = collectConfigSection(rawConfig, "ac.server")
	// 验证ACServer配置
	if len(config.ACServer.RawConfig) == 0 {
		log.Printf("警告: 未找到ac.server配置节")
	}
	config.ACServer.Host = getStringValue(config.ACServer.RawConfig, "host")
	config.ACServer.UpdateDriverName = getBoolValue(config.ACServer.RawConfig, "update.driver.name")
	config.ACServer.UDP.Port = getIntValue(config.ACServer.RawConfig, "udp.port")
	config.ACServer.UDP.LocalPort = getIntValue(config.ACServer.RawConfig, "udp.local.port")
	config.ACServer.UDP.UseACSMUDP = getBoolValue(config.ACServer.RawConfig, "udp.use.acsm.udp")
	config.ACServer.Event.Team.LiveryPreviewEnable = getBoolValue(config.ACServer.RawConfig, "event.team.livery.preview.enable")
	config.ACServer.UDP.Realtime.Update.Interval.MS = getIntValue(config.ACServer.RawConfig, "udp.realtime.update.interval.ms")

	// 映射Writer配置
	config.Writer.HTTP.Port = getIntValue(config.Writer.RawConfig, "http.port")
	config.Writer.HTTP.Leaderboard.Broadcast.Interval.MS = getIntValue(config.Writer.RawConfig, "http.leaderboard.broadcast.interval.ms")

	// 映射覆盖配置
	config.Overrides.CarClass = getMapStringValue(config.Overrides.RawConfig, "car.class")
	config.Overrides.UserCountry = getMapStringValue(config.Overrides.RawConfig, "user.country")
	return config, nil
}
