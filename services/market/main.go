// market 服务:撮合事件 → 盘口/成交/ticker/K线,REST 查询 + WebSocket 推送。
//
// 盘口重建:order_update(open/partially_filled/filled/canceled)携带
// remaining = qty - filled_qty,足以增量维护全部挂单;trade 只产出成交流与
// 聚合数据,不改盘口(挂单变化由对应 order_update 表达)。
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
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

type service struct {
	symbols []string
	states  *stateStore
	hub     *wsHub
	log     *slog.Logger
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	// SIGTERM/SIGINT → ctx 取消 → 消费循环退出 + http shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	brokers := envOr("CEX_KAFKA_BROKERS", "localhost:9092")
	symbols := splitCSV(envOr("CEX_SYMBOLS", "BTC-USDT,ETH-USDT,SOL-USDT"))
	addr := envOr("CEX_HTTP_ADDR", ":8082")
	prefix := envOr("CEX_TOPIC_PREFIX", "cex")

	s := &service{
		symbols: symbols,
		states:  newStateStore(symbols),
		hub:     newHub(),
		log:     slog.Default(),
	}

	topics := make([]string, 0, len(symbols))
	for _, sym := range symbols {
		topics = append(topics, prefix+".events."+strings.ToLower(sym))
	}
	cli, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumerGroup("market"),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.SessionTimeout(6*time.Second),
		kgo.HeartbeatInterval(2*time.Second),
	)
	if err != nil {
		slog.Error("consumer", "err", err)
		os.Exit(1)
	}
	if err := cli.Ping(ctx); err != nil {
		slog.Error("kafka ping", "err", err)
		os.Exit(1)
	}
	go s.consumeLoop(ctx, cli, prefix)
	s.log.Info("market service started", "addr", addr, "topics", topics)

	mux := s.routes()
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && ctx.Err() == nil {
		slog.Error("http server exited", "err", err)
		os.Exit(1)
	}
	s.log.Info("market service stopped")
}
