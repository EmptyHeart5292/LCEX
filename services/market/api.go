package main

import (
	"encoding/json"
	"net/http"
	"strconv"
)

func writeJSON(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(data)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 10003, "message": msg})
}

func (s *service) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]bool{"ok": true})
	})
	mux.HandleFunc("GET /api/v1/depth", s.handleDepth)
	mux.HandleFunc("GET /api/v1/tickers", s.handleTickers)
	mux.HandleFunc("GET /api/v1/trades", s.handleTrades)
	mux.HandleFunc("GET /api/v1/klines", s.handleKlines)
	mux.HandleFunc("GET /stream", s.handleWS)
	return mux
}

func (s *service) handleDepth(w http.ResponseWriter, r *http.Request) {
	symbol := normalizeSymbol(r.URL.Query().Get("symbol"))
	if symbol == "" {
		writeErr(w, http.StatusBadRequest, "symbol required")
		return
	}
	limit := atoiDefault(r.URL.Query().Get("limit"), 50)
	if limit > 500 {
		limit = 500
	}
	seq, bids, asks, ok := s.states.Depth(symbol, limit)
	if !ok {
		writeErr(w, http.StatusBadRequest, "unknown market: "+symbol)
		return
	}
	writeJSON(w, map[string]any{"symbol": symbol, "seq": seq, "bids": bids, "asks": asks})
}

func (s *service) handleTickers(w http.ResponseWriter, r *http.Request) {
	if symbol := normalizeSymbol(r.URL.Query().Get("symbol")); symbol != "" {
		t, ok := s.states.Ticker(symbol)
		if !ok {
			writeErr(w, http.StatusBadRequest, "unknown market: "+symbol)
			return
		}
		writeJSON(w, []tickerJSON{t})
		return
	}
	writeJSON(w, s.states.Tickers())
}

func (s *service) handleTrades(w http.ResponseWriter, r *http.Request) {
	symbol := normalizeSymbol(r.URL.Query().Get("symbol"))
	if symbol == "" {
		writeErr(w, http.StatusBadRequest, "symbol required")
		return
	}
	limit := atoiDefault(r.URL.Query().Get("limit"), 50)
	if limit > 500 {
		limit = 500
	}
	rows, ok := s.states.RecentTrades(symbol, limit)
	if !ok {
		writeErr(w, http.StatusBadRequest, "unknown market: "+symbol)
		return
	}
	type tradeJSON struct {
		TradeID uint64 `json:"tradeId"`
		Symbol  string `json:"symbol"`
		Price   string `json:"price"`
		Qty     string `json:"qty"`
		Side    string `json:"side"`
		TS      int64  `json:"ts"`
	}
	out := make([]tradeJSON, 0, len(rows))
	for _, t := range rows {
		out = append(out, tradeJSON{
			TradeID: t.TradeID, Symbol: symbol,
			Price: fixedFormat(t.Price), Qty: fixedFormat(t.Qty), Side: t.Side, TS: t.TS,
		})
	}
	writeJSON(w, out)
}

func (s *service) handleKlines(w http.ResponseWriter, r *http.Request) {
	symbol := normalizeSymbol(r.URL.Query().Get("symbol"))
	interval := r.URL.Query().Get("interval")
	if symbol == "" || interval == "" {
		writeErr(w, http.StatusBadRequest, "symbol and interval required")
		return
	}
	limit := atoiDefault(r.URL.Query().Get("limit"), 100)
	if limit > 1000 {
		limit = 1000
	}
	kls, ok := s.states.Klines(symbol, interval, limit)
	if !ok {
		writeErr(w, http.StatusBadRequest, "unknown market or interval")
		return
	}
	writeJSON(w, kls)
}

func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		return def
	}
	return v
}
