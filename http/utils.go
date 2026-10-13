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
	// 0 表示“无时间”，必须原样输出 0，不能加时区偏移（否则会变成假日期）
	if int64(t) == 0 {
		return []byte("0"), nil
	}
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

// safeFileName 清理由数据派生的文件名：去掉目录分隔符与控制字符，避免越出缓存目录。
// 保留大小写与空格等，以便与 Writer 生成的缓存文件名（原始 track/config 名）保持一致。
func safeFileName(name string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '/', '\\', ':', '*', '?', '"', '<', '>', '|':
			return '_'
		}
		if r < 32 {
			return '_'
		}
		return r
	}, name)
}

// serveFileWithGzip 统一处理静态文件，支持自动处理.gz压缩文件（遵循 Accept-Encoding 协商）
func serveFileWithGzip(w http.ResponseWriter, r *http.Request, fileName string) {
	// 构建文件路径
	basePath := filepath.Join(docRoot, fileName)
	gzPath := basePath + ".gz"
	var filePath string
	var isGzip bool

	_, gzErr := os.Stat(gzPath)
	_, plainErr := os.Stat(basePath)
	acceptsGzip := false
	if r != nil {
		acceptsGzip = strings.Contains(strings.ToLower(r.Header.Get("Accept-Encoding")), "gzip")
	}

	switch {
	case gzErr == nil && acceptsGzip:
		filePath, isGzip = gzPath, true
	case plainErr == nil:
		filePath, isGzip = basePath, false
	case gzErr == nil:
		// 客户端未声明支持 gzip，但仓库只提供 .gz 版本：仍按 gzip 返回，避免 404
		filePath, isGzip = gzPath, true
	default:
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

	w.Header().Add("Vary", "Accept-Encoding")

	// 处理gzip压缩
	if isGzip {
		w.Header().Set("Content-Encoding", "gzip")
	}

	io.Copy(w, file)
}
