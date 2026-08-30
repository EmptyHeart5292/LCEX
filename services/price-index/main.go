package main

// price-index 服务:聚合五所公共行情 → 指数价(ADR-006)。
//
// 输出:
//   - Redis:  cex:index:{SYMBOL}(JSON,含各源状态,供 market-maker/risk/行情回退读取)
//   - Kafka:  cex.index.{symbol}(500ms 节流的快照事件)
//   - HTTP:   GET /index/{symbol} 与 /index(调试与验收)
//
// 网络受限环境:适配器走 HTTPS_PROXY;URL 可用 CEX_PX_{SOURCE}_URL 覆盖(mock 测试)。

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func jsonUnmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

func fmtAppend(format string, args ...any) []byte {
	return []byte(fmt.Sprintf(format, args...))
}

type tick struct {
	Source string
	Symbol string // 内部符号 BTC-USDT
	Price  uint64
	TS     time.Time
}

type service struct {
	symbols    []string
	stale      time.Duration
	maxDev     uint64
	prefix     string
	rdb        *redis.Client
	prod       *kgo.Client
	bySrc      map[string]map[string]sourcePrice // symbol -> source -> 最新价
	lastIndex  map[string]indexResult
	log        *slog.Logger
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	brokers := envOr("CEX_KAFKA_BROKERS", "localhost:9092")
	prefix := envOr("CEX_TOPIC_PREFIX", "cex")
	symbols := strings.Split(envOr("CEX_SYMBOLS", "BTC-USDT,ETH-USDT,SOL-USDT"), ",")
	for i := range symbols {
		symbols[i] = strings.TrimSpace(symbols[i])
	}
	redisAddr := envOr("CEX_REDIS_ADDR", "localhost:6379")
	staleMs := atoiOr(envOr("CEX_PX_STALE_MS", "10000"), 10000)
	maxDev := uint64(atoiOr(envOr("CEX_PX_DEVIATION_PCT", "5"), 5)) * 1_000_000 // 5% → 5e6 定点
	enabled := strings.Split(envOr("CEX_PX_SOURCES", "binance,okx,bybit,mexc,bitget"), ",")

	adapterBy := map[string]exchangeAdapter{
		"binance": binanceSource{baseURL: envOr("CEX_PX_BINANCE_URL", "")},
		"okx":     okxSource{baseURL: envOr("CEX_PX_OKX_URL", "")},
		"bybit":   bybitSource{baseURL: envOr("CEX_PX_BYBIT_URL", "")},
		"mexc":    mexcSource{baseURL: envOr("CEX_PX_MEXC_URL", "")},
		"bitget":  bitgetSource{baseURL: envOr("CEX_PX_BITGET_URL", "")},
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	prod, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	if err != nil {
		slog.Error("kafka producer", "err", err)
		os.Exit(1)
	}
	defer prod.Close()

	s := &service{
		symbols:   symbols,
		stale:     time.Duration(staleMs) * time.Millisecond,
		maxDev:    maxDev,
		prefix:    prefix,
		rdb:       rdb,
		prod:      prod,
		bySrc:     map[string]map[string]sourcePrice{},
		lastIndex: map[string]indexResult{},
		log:       slog.Default(),
	}

	// 每源每符号一个 goroutine
	ticks := make(chan tick, 256)
	for _, name := range enabled {
		a, ok := adapterBy[strings.ToLower(strings.TrimSpace(name))]
		if !ok {
			slog.Warn("unknown source, skipped", "source", name)
			continue
		}
		for _, sym := range symbols {
			go runSource(ctx, a, sym, ticks, s.log)
		}
	}

	// 聚合循环
	aggCtx, cancel := context.WithCancel(ctx)
	go s.aggregateLoop(aggCtx, ticks)
	defer cancel()

	mux := s.routes()
	srv := &http.Server{Addr: envOr("CEX_HTTP_ADDR", ":8083"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		_ = srv.Shutdown(shutdownCtx)
	}()
	s.log.Info("price-index started", "symbols", symbols, "sources", enabled, "addr", ":8083")
	if err := srv.ListenAndServe(); err != nil && ctx.Err() == nil {
		slog.Error("http exited", "err", err)
		os.Exit(1)
	}
	s.log.Info("price-index stopped")
}

func atoiOr(s string, def int) int {
	n := 0
	ok := false
	for _, c := range s {
		if c < '0' || c > '9' {
			ok = false
			break
		}
		n = n*10 + int(c-'0')
		ok = true
	}
	if !ok {
		return def
	}
	return n
}
