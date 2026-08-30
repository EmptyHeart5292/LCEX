// cex-web:PC 网站入口 = 静态托管 + 迷你网关。
//
// 单一 origin(:8080)消除 CORS,并为未来独立 api-gateway 打底:
//   /                       → apps/web/public 静态文件
//   /api/v1/orders*         → order   :8081(交易与资产)
//   /api/v1/depth|tickers|… → market  :8082(行情,含 /stream WS 反代)
//
// MVP 鉴权占位:浏览器以 X-User-Id 头标识用户,登录态由后续网关统一签发。
package main

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func newProxy(target string) *httputil.ReverseProxy {
	u, err := url.Parse(target)
	if err != nil {
		slog.Error("proxy target", "err", err)
		os.Exit(1)
	}
	return httputil.NewSingleHostReverseProxy(u)
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	orderProxy := newProxy(envOr("CEX_ORDER_URL", "http://localhost:8081"))
	marketProxy := newProxy(envOr("CEX_MARKET_URL", "http://localhost:8082"))
	staticDir := envOr("CEX_WEB_STATIC", "apps/web/public")

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	mux.Handle("/api/v1/orders", orderProxy)
	mux.Handle("/api/v1/orders/", orderProxy)
	mux.Handle("/api/v1/account/", orderProxy)
	mux.Handle("/api/v1/depth", marketProxy)
	mux.Handle("/api/v1/tickers", marketProxy)
	mux.Handle("/api/v1/trades", marketProxy)
	mux.Handle("/api/v1/klines", marketProxy)
	mux.Handle("/stream", marketProxy) // WebSocket:ReverseProxy 自动处理 Upgrade
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			http.NotFound(w, r)
			return
		}
		// SPA:未知路径回落到 index.html
		p := filepath.Join(staticDir, filepath.Clean("/"+r.URL.Path))
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			http.ServeFile(w, r, p)
			return
		}
		http.ServeFile(w, r, filepath.Join(staticDir, "index.html"))
	})

	addr := envOr("CEX_HTTP_ADDR", ":8080")
	slog.Info("cex-web started", "addr", addr, "static", staticDir)
	if err := http.ListenAndServe(addr, mux); err != nil {
		slog.Error("http exited", "err", err)
		os.Exit(1)
	}
}
