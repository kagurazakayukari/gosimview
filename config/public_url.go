package config

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// PublicHTTPPort 返回可供浏览器访问的 HTTP(S) URL 端口；省略时按协议默认端口返回。
func PublicHTTPPort(raw string) (int, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return 0, fmt.Errorf("app.server.public.url 必须是绝对 HTTP(S) URL")
	}
	if port := u.Port(); port != "" {
		value, err := strconv.Atoi(port)
		if err != nil || value < 1 || value > 65535 {
			return 0, fmt.Errorf("app.server.public.url 端口无效")
		}
		return value, nil
	}
	if u.Scheme == "https" {
		return 443, nil
	}
	return 80, nil
}

func ValidatePublicHTTPURL(raw string) error {
	if _, err := PublicHTTPPort(raw); err != nil {
		return err
	}
	u, _ := url.Parse(strings.TrimSpace(raw))
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if host == "localhost" || !strings.Contains(host, ".") || strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".lan") {
		return fmt.Errorf("app.server.public.url 必须使用可公开访问的主机名")
	}
	if ip := net.ParseIP(host); ip != nil && (ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsUnspecified()) {
		return fmt.Errorf("app.server.public.url 不能使用本地或私有 IP")
	}
	return nil
}
