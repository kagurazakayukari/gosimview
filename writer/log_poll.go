// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
// See LICENSE.md for license information.

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const sessionLogPollInterval = 3 * time.Second

var lastLogDiscovery time.Time

// discoverSessionLog 跟随正在运行的服务器新日志；已有文件继续沿用字节游标。
// 启动历史恢复仍由 restoreConnectionsFromSessionLog 完成，这里不重放旧文件。
// 调用者持有 stateMutex 和 lapDataMutex，旧文件尾部已读取。
func discoverSessionLog() error {
	if time.Since(lastLogDiscovery) < sessionLogPollInterval {
		return nil
	}
	lastLogDiscovery = time.Now()
	candidate, err := findLatestSessionLogFile(logDir)
	if err != nil {
		if os.IsNotExist(err) && latestFile == nil {
			logger.Printf("等待服务器生成 output*.log: %s", logDir)
			return nil
		}
		return fmt.Errorf("查找当前服务器日志: %w", err)
	}
	if latestFile != nil && candidate.Name() == latestFile.Name() {
		return nil
	}
	latestFile = candidate
	lastFileSize, lastLineIdx, partialLine = 0, 0, ""
	// 同一会话的日志轮转是续读，不再次回放、清空恢复出的圈成绩。
	restoredSessionLogPath = filepath.Join(logDir, candidate.Name())
	restoredSessionLogSize = 0
	if globalSession != nil {
		recoveredLogSessionID = globalSession.ID
	}
	logger.Printf("开始跟读服务器日志: %s", candidate.Name())
	return nil
}

// startSessionLogPoll 轮胎与计时点来自独立日志源，不能依赖车辆位置包驱动读取。
// 和 UDP 共用状态锁；退出时等待本协程结束，再关闭数据库。
func startSessionLogPoll(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(sessionLogPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				stateMutex.Lock()
				if sessionCollecting() {
					if err := collectCarUpdates(); err != nil {
						logger.Printf("跟读服务器日志失败，保留游标待重试: %v", err)
					}
				}
				stateMutex.Unlock()
			}
		}
	}()
	return done
}
