// risk:同步闸(提现/做市) + kill switch。
//
// 提现:地址黑名单、日限额、提现暂停。做市:mmPaused 时 MM 停报价。
// 名单与限额来自环境变量;日累计查 withdrawals 表。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/EmptyHeart5292/lcex/internal/fixed"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type service struct {
	pool         *pgxpool.Pool
	deny         map[string]struct{}
	daily        map[string]uint64 // currency -> max amount / UTC day
	withdrawOff  atomic.Bool
	mmOff        atomic.Bool
	tradingOff   atomic.Bool
	log          *slog.Logger
}

func parseDeny(s string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out[p] = struct{}{}
		}
	}
	return out
}

func parseDaily(s string) map[string]uint64 {
	out := map[string]uint64{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		k, v, ok := strings.Cut(p, ":")
		if !ok {
			continue
		}
		amt, err := fixed.Parse(strings.TrimSpace(v))
		if err != nil {
			continue
		}
		out[strings.ToUpper(strings.TrimSpace(k))] = amt
	}
	return out
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	dsn := envOr("DATABASE_URL", "postgres://cex:cex_dev@localhost:5432/cex")
	addr := envOr("CEX_HTTP_ADDR", ":8086")
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		slog.Error("db", "err", err)
		os.Exit(1)
	}
	s := &service{
		pool:  pool,
		deny:  parseDeny(envOr("CEX_RISK_DENY_ADDRESSES", "")),
		daily: parseDaily(envOr("CEX_RISK_DAILY_WITHDRAW", "")),
		log:   slog.Default(),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	mux.HandleFunc("GET /v1/status", s.handleStatus)
	mux.HandleFunc("POST /v1/withdraw/check", s.handleWithdrawCheck)
	mux.HandleFunc("POST /v1/pause", s.handlePause)
	mux.HandleFunc("POST /v1/resume", s.handleResume)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sh, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		_ = srv.Shutdown(sh)
	}()
	s.log.Info("risk started", "addr", addr, "deny", len(s.deny), "daily", len(s.daily))
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("http", "err", err)
		os.Exit(1)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (s *service) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{
		"withdrawPaused": s.withdrawOff.Load(),
		"mmPaused":       s.mmOff.Load(),
		"tradingPaused":  s.tradingOff.Load(),
	})
}

func (s *service) handlePause(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Withdraw *bool `json:"withdraw"`
		MM       *bool `json:"mm"`
		Trading  *bool `json:"trading"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.Withdraw != nil && *body.Withdraw {
		s.withdrawOff.Store(true)
	}
	if body.MM != nil && *body.MM {
		s.mmOff.Store(true)
	}
	if body.Trading != nil && *body.Trading {
		s.tradingOff.Store(true)
	}
	s.handleStatus(w, r)
}

func (s *service) handleResume(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Withdraw *bool `json:"withdraw"`
		MM       *bool `json:"mm"`
		Trading  *bool `json:"trading"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.Withdraw != nil && *body.Withdraw {
		s.withdrawOff.Store(false)
	}
	if body.MM != nil && *body.MM {
		s.mmOff.Store(false)
	}
	if body.Trading != nil && *body.Trading {
		s.tradingOff.Store(false)
	}
	s.handleStatus(w, r)
}

type withdrawCheck struct {
	UserID   int64  `json:"userId"`
	Currency string `json:"currency"`
	Network  string `json:"network"`
	Address  string `json:"address"`
	Amount   string `json:"amount"`
}

func (s *service) handleWithdrawCheck(w http.ResponseWriter, r *http.Request) {
	var req withdrawCheck
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, map[string]any{"allow": false, "code": 10003, "message": "invalid json"})
		return
	}
	if s.withdrawOff.Load() {
		writeJSON(w, map[string]any{"allow": false, "code": 60004, "message": "withdrawal suspended"})
		return
	}
	if _, denied := s.deny[req.Address]; denied {
		writeJSON(w, map[string]any{"allow": false, "code": 60005, "message": "address denied"})
		return
	}
	amt, err := fixed.Parse(req.Amount)
	if err != nil {
		writeJSON(w, map[string]any{"allow": false, "code": 10003, "message": "invalid amount"})
		return
	}
	cur := strings.ToUpper(req.Currency)
	if cap, ok := s.daily[cur]; ok {
		used, err := s.usedToday(r.Context(), req.UserID, cur)
		if err != nil {
			s.log.Error("daily sum", "err", err)
			writeJSON(w, map[string]any{"allow": false, "code": 10001, "message": "internal error"})
			return
		}
		if used+amt > cap {
			writeJSON(w, map[string]any{"allow": false, "code": 60005, "message": "daily withdraw limit"})
			return
		}
	}
	writeJSON(w, map[string]any{"allow": true, "code": 0})
}

func (s *service) usedToday(ctx context.Context, userID int64, currency string) (uint64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount),0) FROM withdrawals
		WHERE user_id=$1 AND currency=$2
		  AND status IN ('broadcasting','completed')
		  AND created_at >= date_trunc('day', timezone('utc', now()))`,
		userID, currency).Scan(&n)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, nil
	}
	return uint64(n), nil
}
