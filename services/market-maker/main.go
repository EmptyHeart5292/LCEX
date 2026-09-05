// market-maker:以 price-index 为锚,经 order 服务正常链路双边挂单。
//
// ADR-006/007:每交易对一个 goroutine;不旁路撮合/账本。
// 多档 + 库存偏移;对冲走 packages/exchange-connector(默认 mock)。
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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/EmptyHeart5292/lcex/internal/fixed"
	"github.com/EmptyHeart5292/lcex/packages/exchange-connector"
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
	orderURL      string
	indexURL      string
	userID        int64
	symbols       []string
	halfSpread    int
	levels        int
	qty           string
	maxSkew       int
	maxOffset     int
	hedgeTrigger  int
	refresh       time.Duration
	httpAddr      string
	baseCcy       string
	quoteCcy      string
}

func loadConfig() config {
	uid, _ := strconv.ParseInt(envOr("CEX_MM_USER_ID", "9001"), 10, 64)
	return config{
		orderURL:     strings.TrimRight(envOr("CEX_ORDER_URL", "http://localhost:8081"), "/"),
		indexURL:     strings.TrimRight(envOr("CEX_INDEX_URL", "http://localhost:8083"), "/"),
		userID:       uid,
		symbols:      splitCSV(envOr("CEX_SYMBOLS", "BTC-USDT")),
		halfSpread:   atoiOr(envOr("CEX_MM_HALF_SPREAD_BPS", "10"), 10),
		levels:       atoiOr(envOr("CEX_MM_LEVELS", "3"), 3),
		qty:          envOr("CEX_MM_QTY", "0.05"),
		maxSkew:      atoiOr(envOr("CEX_MM_MAX_SKEW_BPS", "20"), 20),
		maxOffset:    atoiOr(envOr("CEX_MM_MAX_OFFSET_BPS", "80"), 80),
		hedgeTrigger: atoiOr(envOr("CEX_MM_HEDGE_TRIGGER_BPS", "15"), 15),
		refresh:      time.Duration(atoiOr(envOr("CEX_MM_REFRESH_MS", "800"), 800)) * time.Millisecond,
		httpAddr:     envOr("CEX_HTTP_ADDR", ":8085"),
		baseCcy:      envOr("CEX_MM_BASE", "BTC"),
		quoteCcy:     envOr("CEX_MM_QUOTE", "USDT"),
	}
}

type symbolSnap struct {
	Index     string         `json:"index"`
	SkewBps   int            `json:"skewBps"`
	Quotes    []map[string]string `json:"quotes"`
	Inventory map[string]string   `json:"inventory"`
	LastHedge map[string]any      `json:"lastHedge"`
}

type service struct {
	cfg    config
	httpc  *http.Client
	hedger connector.Hedger
	paused atomic.Bool
	mu     sync.Mutex
	snap   map[string]symbolSnap
	log    *slog.Logger
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg := loadConfig()
	s := &service{
		cfg:    cfg,
		httpc:  &http.Client{Timeout: 5 * time.Second},
		hedger: &connector.Mock{},
		snap:   map[string]symbolSnap{},
		log:    slog.Default(),
	}
	for _, sym := range cfg.symbols {
		go s.runSymbol(ctx, sym)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	mux.HandleFunc("GET /status", s.handleStatus)
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
	s.log.Info("market-maker started", "addr", cfg.httpAddr, "user", cfg.userID, "levels", cfg.levels, "hedge", s.hedger.Name())
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.log.Error("http exited", "err", err)
		os.Exit(1)
	}
}

func (s *service) handleStatus(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]any{"paused": s.paused.Load(), "hedgeHealthy": s.hedger.Healthy(), "symbols": s.snap}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
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
	if s.paused.Load() || !s.hedger.Healthy() {
		_ = s.cancelOpen(ctx, symbol)
		return
	}
	idx, ok, err := s.fetchIndex(ctx, symbol)
	if err != nil || !ok || idx == 0 {
		return
	}
	baseQty, quoteQty := s.balances(ctx)
	skew := inventorySkewBps(baseQty, quoteQty, idx, s.cfg.maxSkew)
	s.maybeHedge(ctx, symbol, skew, baseQty)

	grid := buildGrid(idx, s.cfg.levels, s.cfg.halfSpread, skew, s.cfg.maxOffset, s.cfg.qty)
	open, err := s.openOrders(ctx, symbol)
	if err != nil {
		s.log.Error("open orders", "err", err)
		return
	}
	want := map[string]Quote{}
	for _, q := range grid {
		want[quoteKey(q.Side, fixed.Format(q.Price), q.Qty)] = q
	}
	have := map[string]bool{}
	for _, o := range open {
		k := quoteKey(o.Side, o.Price, o.Qty)
		if _, ok := want[k]; ok && !have[k] {
			have[k] = true
			continue
		}
		_ = s.cancel(ctx, o.OrderID)
	}
	for k, q := range want {
		if have[k] {
			continue
		}
		if err := s.place(ctx, symbol, q.Side, fixed.Format(q.Price), q.Qty); err != nil {
			s.log.Error("place", "err", err, "side", q.Side, "px", q.Price)
		}
	}

	quotes := make([]map[string]string, 0, len(grid))
	for _, q := range grid {
		quotes = append(quotes, map[string]string{"side": q.Side, "price": fixed.Format(q.Price), "qty": q.Qty})
	}
	inv := map[string]string{
		s.cfg.baseCcy:  fixed.Format(baseQty),
		s.cfg.quoteCcy: fixed.Format(quoteQty),
	}
	var last map[string]any
	if m, ok := s.hedger.(*connector.Mock); ok {
		if o, ok := m.Last(); ok {
			last = map[string]any{"side": o.Side, "qty": fixed.Format(o.Qty), "reason": o.Reason}
		}
	}
	s.mu.Lock()
	s.snap[symbol] = symbolSnap{
		Index: fixed.Format(idx), SkewBps: skew, Quotes: quotes, Inventory: inv, LastHedge: last,
	}
	s.mu.Unlock()
}

func (s *service) maybeHedge(ctx context.Context, symbol string, skew int, baseQty uint64) {
	if s.cfg.hedgeTrigger <= 0 || absInt(skew) < s.cfg.hedgeTrigger {
		return
	}
	qty, _ := fixed.Parse(s.cfg.qty)
	if qty == 0 || baseQty < qty {
		return
	}
	side := "ask" // 多 base → 外部卖
	if skew > 0 {
		side = "bid"
	}
	ack, err := s.hedger.Hedge(ctx, connector.HedgeOrder{
		Symbol: symbol, Side: side, Qty: qty, Reason: fmt.Sprintf("skew=%d", skew),
	})
	if err != nil {
		s.log.Error("hedge failed, will not quote this tick", "err", err)
		return
	}
	s.log.Info("hedge", "venue", ack.Venue, "txid", ack.TxID, "side", side)
}

func (s *service) balances(ctx context.Context) (base, quote uint64) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.orderURL+"/api/v1/account/balances", nil)
	if err != nil {
		return 0, 0
	}
	s.auth(req)
	res, err := s.httpc.Do(req)
	if err != nil {
		return 0, 0
	}
	defer res.Body.Close()
	var wrap struct {
		Code int `json:"code"`
		Data []struct {
			Currency  string `json:"currency"`
			Available string `json:"available"`
		} `json:"data"`
	}
	if err := json.NewDecoder(res.Body).Decode(&wrap); err != nil || wrap.Code != 0 {
		return 0, 0
	}
	for _, b := range wrap.Data {
		v, _ := fixed.Parse(b.Available)
		switch b.Currency {
		case s.cfg.baseCcy:
			base = v
		case s.cfg.quoteCcy:
			quote = v
		}
	}
	return base, quote
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
	if wrap.Data == nil {
		return []orderView{}, nil
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

func (s *service) place(ctx context.Context, symbol, side, price, qty string) error {
	body, _ := json.Marshal(map[string]any{
		"symbol":        symbol,
		"clientOrderId": fmt.Sprintf("mm-%s-%s-%d", symbol, side, time.Now().UnixNano()),
		"side":          side,
		"type":          "LIMIT",
		"timeInForce":   "GTC",
		"postOnly":      true,
		"price":         price,
		"qty":           qty,
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
