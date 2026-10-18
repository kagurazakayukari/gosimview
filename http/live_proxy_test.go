package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	configpkg "gosimview/config"

	"github.com/gorilla/websocket"
)

func TestLiveProxyRelaysBinaryFrame(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	writerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		typ, payload, err := conn.ReadMessage()
		if err == nil {
			_ = conn.WriteMessage(typ, payload)
		}
	}))
	defer writerServer.Close()

	writerURL, err := url.Parse(writerServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	writerPort, err := strconv.Atoi(writerURL.Port())
	if err != nil {
		t.Fatal(err)
	}

	proxyServer := httptest.NewServer(http.HandlerFunc(handleLiveProxy))
	defer proxyServer.Close()
	proxyURL, err := url.Parse(proxyServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxyPort, err := strconv.Atoi(proxyURL.Port())
	if err != nil {
		t.Fatal(err)
	}

	previousCfg := cfg
	cfg = &configpkg.Config{}
	cfg.Writer.HTTP.Port = writerPort
	cfg.App.Server.Port = proxyPort
	defer func() { cfg = previousCfg }()

	browserURL := "ws" + proxyURL.String()[4:] + "/live"
	browserConn, _, err := websocket.DefaultDialer.Dial(browserURL, nil)
	if err != nil {
		t.Fatalf("proxy handshake failed: %v", err)
	}
	defer browserConn.Close()
	deadline := time.Now().Add(2 * time.Second)
	_ = browserConn.SetReadDeadline(deadline)
	_ = browserConn.SetWriteDeadline(deadline)

	payload := []byte{0, 1, 2, 127, 255}
	if err := browserConn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
		t.Fatal(err)
	}
	messageType, got, err := browserConn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if messageType != websocket.BinaryMessage || string(got) != string(payload) {
		t.Fatalf("relayed message = type %d, payload %v; want binary %v", messageType, got, payload)
	}
}
