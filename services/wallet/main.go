// wallet 服务:托管对接层 + 充提闭环。
//
// Phase 2:mock Provider 派生充值地址;扫链回调按确认数入账;
// 提现扣 available+fee 后进入 broadcasting,链上确认后终态。
// 真托管只替换 Provider,入账/扣账仍走本服务账本分录。
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/EmptyHeart5292/lcex/internal/chains"
	"github.com/EmptyHeart5292/lcex/internal/ledger"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type server struct {
	pool     *pgxpool.Pool
	ledger   *ledger.Ledger
	chains   *chains.Config
	provider Provider
	riskURL  string
	httpc    *http.Client
	log      *slog.Logger
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn := envOr("DATABASE_URL", "postgres://cex:cex_dev@localhost:5432/cex")
	addr := envOr("CEX_HTTP_ADDR", ":8084")
	chainsFile := envOr("CEX_CURRENCIES_FILE", "packages/api-spec/currencies.yaml")

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		slog.Error("connect database", "err", err)
		os.Exit(1)
	}
	ccfg, err := chains.Load(chainsFile)
	if err != nil {
		slog.Error("load currencies", "err", err)
		os.Exit(1)
	}
	s := &server{
		pool: pool, ledger: ledger.New(pool), chains: ccfg,
		provider: mockProvider{}, riskURL: strings.TrimRight(envOr("CEX_RISK_URL", ""), "/"),
		httpc: &http.Client{Timeout: 3 * time.Second}, log: slog.Default(),
	}

	srv := &http.Server{Addr: addr, Handler: s.routes(), ReadHeaderTimeout: 5 * time.Second}
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
