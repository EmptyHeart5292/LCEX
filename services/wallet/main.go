// wallet 服务:托管对接层 + 充提入账。
//
// MVP(Phase 2 起步):mock Provider,内部入账接口走账本 deposit 分录
// (debit 热钱包 + credit 用户 available),幂等键 journals(deposit, biz_id)。
// 真托管(Fireblocks/Cobo)与扫链/提现广播后续替换 mock,不改入账路径。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/EmptyHeart5292/lcex/internal/fixed"
	"github.com/EmptyHeart5292/lcex/internal/ledger"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type server struct {
	ledger *ledger.Ledger
	log    *slog.Logger
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn := envOr("DATABASE_URL", "postgres://cex:cex_dev@localhost:5432/cex")
	addr := envOr("CEX_HTTP_ADDR", ":8084")
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		slog.Error("connect database", "err", err)
		os.Exit(1)
	}
	s := &server{ledger: ledger.New(pool), log: slog.Default()}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]bool{"ok": true})
	})
	mux.HandleFunc("POST /internal/deposits", s.handleDeposit)

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sh, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(sh)
	}()
	slog.Info("wallet started", "addr", addr, "provider", "mock")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("http exited", "err", err)
		os.Exit(1)
	}
}

type depositReq struct {
	UserID   int64  `json:"userId"`
	Currency string `json:"currency"`
	Amount   string `json:"amount"`
	BizID    string `json:"bizId"`
}

func (s *server) handleDeposit(w http.ResponseWriter, r *http.Request) {
	var req depositReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 10003, "invalid json body")
		return
	}
	if req.UserID <= 0 || req.Currency == "" || req.BizID == "" {
		writeErr(w, 10003, "userId, currency, bizId required")
		return
	}
	amt, err := fixed.Parse(req.Amount)
	if err != nil || amt == 0 {
		writeErr(w, 10003, "invalid amount")
		return
	}
	err = s.ledger.Post(r.Context(), "deposit", req.BizID, []ledger.Move{
		{Account: ledger.System(0, req.Currency, "available"), Delta: int64(amt)},
		{Account: ledger.User(req.UserID, req.Currency, "available"), Delta: int64(amt)},
	})
	if errors.Is(err, ledger.ErrAlreadyProcessed) {
		writeJSON(w, map[string]any{"code": 0, "data": map[string]any{"bizId": req.BizID, "replayed": true}})
		return
	}
	if err != nil {
		s.log.Error("deposit failed", "err", err, "biz", req.BizID)
		writeErr(w, 10001, "internal error")
		return
	}
	writeJSON(w, map[string]any{"code": 0, "data": map[string]any{"bizId": req.BizID, "replayed": false}})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	_ = json.NewEncoder(w).Encode(map[string]any{"code": code, "message": msg, "data": nil})
}
