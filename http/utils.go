// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package main

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// TimeWithTimezone 自定义时间类型，用于JSON序列化时添加时区偏移

type TimeWithTimezone int64

func (t TimeWithTimezone) MarshalJSON() ([]byte, error) {
	// 获取本地时区偏移（秒）
	_, offset := time.Now().Zone()
	// 转换为微秒并添加到时间戳
	adjustedTimestamp := int64(t) + int64(offset)*1000000
	return json.Marshal(adjustedTimestamp)
}

// 响应记录器用于捕获状态码
type responseRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (rec *responseRecorder) WriteHeader(code int) {
	rec.statusCode = code
	rec.ResponseWriter.WriteHeader(code)
}

// 统一错误响应工具函数
func sendErrorResponse(w http.ResponseWriter, statusCode int, message string, dataField string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	response := map[string]interface{}{
		"status":  "error",
		"message": message,
	}
	if dataField != "" {
		response[dataField] = []interface{}{}
	}
	jsonData, err := json.Marshal(response)
	if err != nil {
		logger.Printf("编码错误响应失败: %v", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(jsonData); err != nil {
		logger.Printf("写入错误响应失败: %v", err)
	}
}

// 统一成功响应工具函数
func sendSuccessResponse(w http.ResponseWriter, message string, dataField string, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{
		"status":  "success",
		"message": message,
	}
	if dataField != "" {
		if data == nil {
			response[dataField] = []interface{}{}
		} else {
			response[dataField] = data
		}
	}
	// 日志响应数据用于调试
	jsonData, err := json.Marshal(response)
	if err != nil {
		logger.Printf("编码响应失败: %v", err)
		sendErrorResponse(w, http.StatusInternalServerError, "数据序列化失败", "")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(jsonData); err != nil {
		logger.Printf("编码成功响应失败: %v", err)
	}
}

// 方法验证中间件(集成日志功能)
func methodMiddleware(next http.HandlerFunc, allowedMethods ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 初始化响应记录器和计时器
		rec := &responseRecorder{w, http.StatusOK}

		// 方法验证逻辑
		allowed := false
		for _, method := range allowedMethods {
			if r.Method == method {
				allowed = true
				break
			}
		}

		// 处理不允许的请求
		if !allowed {
			sendErrorResponse(rec, http.StatusMethodNotAllowed, "方法不允许", "")
		} else {
			// 处理正常请求
			next.ServeHTTP(rec, r)
		}
	}
}

// serveFileWithGzip 统一处理静态文件，支持自动处理.gz压缩文件
func serveFileWithGzip(w http.ResponseWriter, _ *http.Request, fileName string) {
	// 构建文件路径
	basePath := filepath.Join("html", fileName)
	gzPath := basePath + ".gz"
	var filePath string
	var isGzip bool

	// 优先检查gzip版本
	if _, err := os.Stat(gzPath); err == nil {
		filePath = gzPath
		isGzip = true
	} else if _, err := os.Stat(basePath); err == nil {
		filePath = basePath
		isGzip = false
	} else {
		// 文件不存在
		sendErrorResponse(w, http.StatusNotFound, "File not found", "staticFile")
		return
	}

	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		sendErrorResponse(w, http.StatusNotFound, "File not found", "staticFile")
		return
	}
	defer file.Close()

	// 确定原始文件扩展名以设置正确的Content-Type
	var originalExt string
	if isGzip {
		originalExt = filepath.Ext(strings.TrimSuffix(fileName, ".gz"))
	} else {
		originalExt = filepath.Ext(fileName)
	}

	// 设置适当的Content-Type
	switch originalExt {
	case ".html":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	case ".js":
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	case ".css":
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
	case ".png":
		w.Header().Set("Content-Type", "image/png")
	case ".jpg", ".jpeg":
		w.Header().Set("Content-Type", "image/jpeg")
	case ".svg":
		w.Header().Set("Content-Type", "image/svg+xml")
	}

	// 处理gzip压缩
	if isGzip {
		w.Header().Set("Content-Encoding", "gzip")
	}

	io.Copy(w, file)
}
