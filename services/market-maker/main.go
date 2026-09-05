// market-maker:以 price-index 为锚,经 order 服务正常链路双边挂单。
//
// ADR-006/007:每交易对一个 goroutine;不旁路撮合/账本。
// MVP:HTTP 拉指数 + REST 下单/撤单;对冲通道与多档盘口后续补。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/EmptyHeart5292/lcex/internal/fixed"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func atoiOr(s string, def int) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

type config struct {
	orderURL    string
	indexURL    string
	userID      int64
	symbols     []string
	halfSpread  int // bps each side
	qty         string
	refresh     time.Duration
	httpAddr    string
}

func loadConfig() config {
	uid, _ := strconv.ParseInt(envOr("CEX_MM_USER_ID", "9001"), 10, 64)
	return config{
		orderURL:   strings.TrimRight(envOr("CEX_ORDER_URL", "http://localhost:8081"), "/"),
		indexURL:   strings.TrimRight(envOr("CEX_INDEX_URL", "http://localhost:8083"), "/"),
		userID:     uid,
		symbols:    splitCSV(envOr("CEX_SYMBOLS", "BTC-USDT")),
		halfSpread: atoiOr(envOr("CEX_MM_HALF_SPREAD_BPS", "10"), 10),
		qty:        envOr("CEX_MM_QTY", "0.05"),
		refresh:    time.Duration(atoiOr(envOr("CEX_MM_REFRESH_MS", "800"), 800)) * time.Millisecond,
		httpAddr:   envOr("CEX_HTTP_ADDR", ":8085"),
	}
}

type service struct {
	cfg    config
	httpc  *http.Client
	paused atomic.Bool
	log    *slog.Logger
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg := loadConfig()
	s := &service{
		cfg:   cfg,
		httpc: &http.Client{Timeout: 5 * time.Second},
		log:   slog.Default(),
	}
	for _, sym := range cfg.symbols {
		go s.runSymbol(ctx, sym)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	mux.HandleFunc("POST /pause", func(w http.ResponseWriter, _ *http.Request) {
		s.paused.Store(true)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /resume", func(w http.ResponseWriter, _ *http.Request) {
		s.paused.Store(false)
		w.WriteHeader(http.StatusNoContent)
	})
	srv := &http.Server{Addr: cfg.httpAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sh, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(sh)
	}()
	s.log.Info("market-maker started", "addr", cfg.httpAddr, "user", cfg.userID, "symbols", cfg.symbols)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.log.Error("http exited", "err", err)
		os.Exit(1)
	}
}

func (s *service) runSymbol(ctx context.Context, symbol string) {
	t := time.NewTicker(s.cfg.refresh)
	defer t.Stop()
	s.tick(ctx, symbol)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.tick(ctx, symbol)
		}
	}
}

func (s *service) tick(ctx context.Context, symbol string) {
	if s.paused.Load() {
		_ = s.cancelOpen(ctx, symbol)
		return
	}
	idx, ok, err := s.fetchIndex(ctx, symbol)
	if err != nil || !ok || idx == 0 {
		return
	}
	bidPx, err1 := scaleBps(idx, -s.cfg.halfSpread)
	askPx, err2 := scaleBps(idx, s.cfg.halfSpread)
	if err1 != nil || err2 != nil || bidPx == 0 || askPx == 0 || bidPx >= askPx {
		s.log.Warn("skip quote, bad prices", "symbol", symbol, "index", idx, "bid", bidPx, "ask", askPx)
		return
	}
	open, err := s.openOrders(ctx, symbol)
	if err != nil {
		s.log.Error("open orders", "err", err)
		return
	}
	wantBid, wantAsk := fixed.Format(bidPx), fixed.Format(askPx)
	haveBid, haveAsk := false, false
	for _, o := range open {
		keep := (o.Side == "bid" && o.Price == wantBid && o.Qty == s.cfg.qty) ||
			(o.Side == "ask" && o.Price == wantAsk && o.Qty == s.cfg.qty)
		if keep && o.Side == "bid" && !haveBid {
			haveBid = true
			continue
		}
		if keep && o.Side == "ask" && !haveAsk {
			haveAsk = true
			continue
		}
		_ = s.cancel(ctx, o.OrderID)
	}
	if !haveBid {
		if err := s.place(ctx, symbol, "bid", wantBid); err != nil {
			s.log.Error("place bid", "err", err, "px", wantBid)
		}
	}
	if !haveAsk {
		if err := s.place(ctx, symbol, "ask", wantAsk); err != nil {
			s.log.Error("place ask", "err", err, "px", wantAsk)
		}
	}
}

func scaleBps(px uint64, bps int) (uint64, error) {
	const base = 10000
	if bps >= 0 {
		return fixed.MulDiv(px, uint64(base+bps), base)
	}
	n := uint64(-bps)
	if n >= base {
		return 0, errors.New("bps too large")
	}
	return fixed.MulDiv(px, base-n, base)
}

func (s *service) fetchIndex(ctx context.Context, symbol string) (uint64, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.indexURL+"/index/"+symbol, nil)
	if err != nil {
		return 0, false, err
	}
	res, err := s.httpc.Do(req)
	if err != nil {
		return 0, false, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return 0, false, fmt.Errorf("index http %d", res.StatusCode)
	}
	var body struct {
		Index string `json:"index"`
		OK    bool   `json:"ok"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return 0, false, err
	}
	px, err := fixed.Parse(body.Index)
	return px, body.OK, err
}

type orderView struct {
	OrderID int64  `json:"orderId"`
	Side    string `json:"side"`
	Price   string `json:"price"`
	Qty     string `json:"qty"`
	Status  string `json:"status"`
}

func (s *service) openOrders(ctx context.Context, symbol string) ([]orderView, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.orderURL+"/api/v1/orders/open?symbol="+symbol, nil)
	if err != nil {
		return nil, err
	}
	s.auth(req)
	res, err := s.httpc.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	var wrap struct {
		Code int         `json:"code"`
		Data []orderView `json:"data"`
	}
	if err := json.NewDecoder(res.Body).Decode(&wrap); err != nil {
		return nil, err
	}
	if wrap.Code != 0 {
		return nil, fmt.Errorf("open orders code %d", wrap.Code)
	}
	return wrap.Data, nil
}

func (s *service) cancelOpen(ctx context.Context, symbol string) error {
	open, err := s.openOrders(ctx, symbol)
	if err != nil {
		return err
	}
	for _, o := range open {
		_ = s.cancel(ctx, o.OrderID)
	}
	return nil
}

func (s *service) cancel(ctx context.Context, id int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, fmt.Sprintf("%s/api/v1/orders/%d", s.cfg.orderURL, id), nil)
	if err != nil {
		return err
	}
	s.auth(req)
	res, err := s.httpc.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	return nil
}

func (s *service) place(ctx context.Context, symbol, side, price string) error {
	body, _ := json.Marshal(map[string]any{
		"symbol":        symbol,
		"clientOrderId": fmt.Sprintf("mm-%s-%s-%d", symbol, side, time.Now().UnixNano()),
		"side":          side,
		"type":          "LIMIT",
		"timeInForce":   "GTC",
		"postOnly":      true,
		"price":         price,
		"qty":           s.cfg.qty,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.orderURL+"/api/v1/orders", bytes.NewReader(body))
	if err != nil {
		return err
	}
	s.auth(req)
	req.Header.Set("Content-Type", "application/json")
	res, err := s.httpc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	var wrap struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	_ = json.Unmarshal(raw, &wrap)
	if wrap.Code != 0 {
		return fmt.Errorf("place %s: code=%d %s", side, wrap.Code, wrap.Message)
	}
	return nil
}

func (s *service) auth(req *http.Request) {
	req.Header.Set("X-User-Id", strconv.FormatInt(s.cfg.userID, 10))
}
