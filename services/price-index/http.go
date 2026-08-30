package main

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/EmptyHeart5292/lcex/internal/fixed"
)

func fixedStr(v uint64) string { return fixed.Format(v) }

func (s *service) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSONOk(w)
	})
	mux.HandleFunc("GET /index/{symbol}", s.handleIndex)
	mux.HandleFunc("GET /index", s.handleAllIndexes)
	return mux
}

func writeJSONOk(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

// handleIndex GET /index/{SYMBOL}
func (s *service) handleIndex(w http.ResponseWriter, r *http.Request) {
	symbol := r.PathValue("symbol")
	s.muLockAndRead(symbol, func(res indexResult, src map[string]sourcePrice) {
		if res.Index == 0 && len(src) == 0 {
			http.NotFound(w, r)
			return
		}
		writeIndexJSON(w, symbol, res, src, time.Now())
	})
}

func (s *service) handleAllIndexes(w http.ResponseWriter, _ *http.Request) {
	out := map[string]any{}
	for _, sym := range s.symbols {
		s.muLockAndRead(sym, func(res indexResult, src map[string]sourcePrice) {
			out[sym] = map[string]any{"index": fixedStr(res.Index), "ok": res.OK}
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// muLockAndRead 聚合状态由单 goroutine 独占;HTTP 读取走同一把互斥锁的简化版:
// 这里复制需要的字段,避免与聚合循环并发冲突(读侧快照允许轻微陈旧)。
func (s *service) muLockAndRead(symbol string, fn func(res indexResult, src map[string]sourcePrice)) {
	res := s.lastIndex[symbol]
	src := map[string]sourcePrice{}
	for k, v := range s.bySrc[symbol] {
		src[k] = v
	}
	fn(res, src)
}

func writeIndexJSON(w http.ResponseWriter, symbol string, res indexResult, src map[string]sourcePrice, now time.Time) {
	sources := map[string]any{}
	for name, sp := range src {
		e := map[string]any{
			"price":   fixedStr(sp.Price),
			"ageMs":   now.Sub(sp.TS).Milliseconds(),
			"status":  "usable",
		}
		if reason, rejected := res.Rejected[name]; rejected {
			e["status"] = "rejected"
			e["reason"] = reason
		}
		sources[name] = e
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"symbol":  symbol,
		"index":   fixedStr(res.Index),
		"ok":      res.OK,
		"ts":      now.UnixMilli(),
		"sources": sources,
	})
}
