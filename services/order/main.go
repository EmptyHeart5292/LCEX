// order 服务:交易入口的资金守门人。
//
// 职责:下单校验 → 同事务落订单行 + 冻结资金 → 投递撮合指令;
// 消费 cex.events.{symbol} 同步订单状态(资金变动由 clearing 全权负责)。
//
// MVP 约定:
//   - 鉴权:X-User-Id 请求头(网关与登录态在后续版本接入)
//   - 市价买单暂不支持(冻结额无锚价),仅市价卖单
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/EmptyHeart5292/lcex/internal/ledger"
	"github.com/EmptyHeart5292/lcex/internal/markets"
)

type config struct {
	databaseURL string
	brokers     string
	symbols     []string
	marketsFile string
	httpAddr    string
	topicPrefix string
}

func loadConfig() config {
	return config{
		databaseURL: envOr("DATABASE_URL", "postgres://cex:cex_dev@localhost:5432/cex"),
		brokers:     envOr("CEX_KAFKA_BROKERS", "localhost:9092"),
		symbols:     splitCSV(envOr("CEX_SYMBOLS", "BTC-USDT,ETH-USDT,SOL-USDT")),
		marketsFile: envOr("CEX_MARKETS_FILE", "packages/api-spec/markets.yaml"),
		httpAddr:    envOr("CEX_HTTP_ADDR", ":8081"),
		topicPrefix: envOr("CEX_TOPIC_PREFIX", "cex"),
	}
}

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

func lower(s string) string { return strings.ToLower(s) }

func strconvParseInt(s string) (int64, error) { return strconv.ParseInt(s, 10, 64) }

type server struct {
	cfg     config
	pool    *pgxpool.Pool
	ledger  *ledger.Ledger
	markets *markets.Config
	prod    *kgo.Client
	log     *slog.Logger
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	// SIGTERM/SIGINT → 优雅退出:HTTP shutdown + kafka client close(离开消费组)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg := loadConfig()

	pool, err := pgxpool.New(ctx, cfg.databaseURL)
	if err != nil {
		slog.Error("connect database", "err", err)
		os.Exit(1)
	}
	mcfg, err := markets.Load(cfg.marketsFile)
	if err != nil {
		slog.Error("load markets", "err", err)
		os.Exit(1)
	}
	prod, err := kgo.NewClient(kgo.SeedBrokers(cfg.brokers))
	if err != nil {
		slog.Error("kafka producer", "err", err)
		os.Exit(1)
	}
	defer prod.Close()
	// 确认 broker 可达(快速失败)
	if err := prod.Ping(ctx); err != nil {
		slog.Error("kafka ping", "err", err)
		os.Exit(1)
	}

	s := &server{
		cfg: cfg, pool: pool,
		ledger:  ledger.New(pool),
		markets: mcfg, prod: prod,
		log: slog.Default(),
	}

	// 状态同步消费(独立 client,随 ctx 取消而退出)
	go s.runStatusSync(ctx)

	slog.Info("order service started", "addr", cfg.httpAddr, "symbols", cfg.symbols)
	mux := s.routes()
	srv := &http.Server{Addr: cfg.httpAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("http server exited", "err", err)
		os.Exit(1)
	}
}

// inputTopic cex.orders.in.{symbol}
func (s *server) inputTopic(symbol string) string {
	return fmt.Sprintf("%s.orders.in.%s", s.cfg.topicPrefix, strings.ToLower(symbol))
}

// eventsTopic cex.events.{symbol}
func (s *server) eventsTopic(symbol string) string {
	return fmt.Sprintf("%s.events.%s", s.cfg.topicPrefix, strings.ToLower(symbol))
}

func produceSync(ctx context.Context, cli *kgo.Client, topic, key string, payload []byte) error {
	ch := make(chan error, 1)
	cli.Produce(ctx, &kgo.Record{Topic: topic, Key: []byte(key), Value: payload},
		func(_ *kgo.Record, err error) { ch <- err })
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		return fmt.Errorf("produce timeout: %s", topic)
	}
}

func parseUserID(r *http.Request) (int64, error) {
	v := r.Header.Get("X-User-Id")
	if v == "" {
		return 0, fmt.Errorf("missing X-User-Id")
	}
	id, err := strconv.ParseInt(v, 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("invalid X-User-Id: %q", v)
	}
	return id, nil
}
