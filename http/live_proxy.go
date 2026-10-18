// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

import (
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"

	"github.com/gorilla/websocket"
)

var liveProxyUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func configuredWriterLivePort() int {
	if cfg == nil {
		return 0
	}
	if cfg.Writer.HTTP.Port > 0 {
		return cfg.Writer.HTTP.Port
	}
	if cfg.ACServer.UDP.LocalPort > 0 {
		return cfg.ACServer.UDP.LocalPort
	}
	return 0
}

// handleLiveProxy 将浏览器 WebSocket 请求桥接到当前 HTTP 节点配对的 Writer。
func handleLiveProxy(w http.ResponseWriter, r *http.Request) {
	if !websocket.IsWebSocketUpgrade(r) {
		http.Error(w, "需要 WebSocket Upgrade", http.StatusUpgradeRequired)
		return
	}
	port := configuredWriterLivePort()
	if port <= 0 || port > 65535 {
		http.Error(w, "Writer Live 端口配置无效", http.StatusServiceUnavailable)
		return
	}
	upstreamURL := url.URL{Scheme: "ws", Host: net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), Path: "/live"}
	writerConn, _, err := websocket.DefaultDialer.Dial(upstreamURL.String(), nil)
	if err != nil {
		logger.Printf("连接本机 Writer Live 失败(port=%d): %v", port, err)
		http.Error(w, "Writer Live 不可用", http.StatusBadGateway)
		return
	}
	browserConn, err := liveProxyUpgrader.Upgrade(w, r, nil)
	if err != nil {
		_ = writerConn.Close()
		return
	}
	bridgeWebSockets(browserConn, writerConn)
}

func bridgeWebSockets(browserConn, writerConn *websocket.Conn) {
	var closeOnce sync.Once
	closeBoth := func() {
		closeOnce.Do(func() {
			_ = browserConn.Close()
			_ = writerConn.Close()
		})
	}
	var pumps sync.WaitGroup
	pumps.Add(2)
	pump := func(dst, src *websocket.Conn) {
		defer pumps.Done()
		defer closeBoth()
		for {
			messageType, payload, err := src.ReadMessage()
			if err != nil {
				return
			}
			if err := dst.WriteMessage(messageType, payload); err != nil {
				return
			}
		}
	}
	go pump(writerConn, browserConn)
	go pump(browserConn, writerConn)
	pumps.Wait()
}
