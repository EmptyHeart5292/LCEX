package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// aggregateLoop 单 goroutine 消费 tick 并周期性重算指数(无锁,状态独占)。
// 输出:Redis(每次重算)+ Kafka(价格变化或距上次发布超过 1s)。
func (s *service) aggregateLoop(ctx context.Context, ticks <-chan tick) {
	recompute := time.NewTicker(500 * time.Millisecond)
	defer recompute.Stop()
	var lastPub map[string]time.Time = map[string]time.Time{}

	for {
		select {
		case <-ctx.Done():
			return
		case t := <-ticks:
			if _, ok := s.bySrc[t.Symbol]; !ok {
				s.bySrc[t.Symbol] = map[string]sourcePrice{}
			}
			s.bySrc[t.Symbol][t.Source] = sourcePrice{Price: t.Price, TS: t.TS}
			s.log.Info("tick received", "symbol", t.Symbol, "source", t.Source, "price", fixedStr(t.Price))
			s.recomputeAndPublish(ctx, t.Symbol, lastPub)
		case <-recompute.C:
			for _, sym := range s.symbols {
				s.recomputeAndPublish(ctx, sym, lastPub)
			}
		}
	}
}

// recomputeAndPublish 重算并按条件发布:Redis 总是写;Kafka 仅在指数变化或距上次 >1s 时发。
func (s *service) recomputeAndPublish(ctx context.Context, symbol string, lastPub map[string]time.Time) {
	now := time.Now()
	res := computeIndex(s.bySrc[symbol], now, s.stale, s.maxDev)
	if !res.OK && s.lastIndex[symbol].Index == 0 {
		return // 从未有过可用指数,不发布空值
	}
	s.lastIndex[symbol] = res

	payload := indexSnapshot(symbol, res, now)
	if err := s.rdb.Set(ctx, "cex:index:"+symbol, payload, 0).Err(); err != nil {
		s.log.Error("redis write failed", "symbol", symbol, "err", err)
	}

	changed := true
	if prev, ok := s.lastIndex[symbol+"#pub"]; ok {
		changed = prev.Index != res.Index
	}
	if changed || time.Since(lastPub[symbol]) > time.Second {
		cli := s.prod
		cli.Produce(ctx, &kgo.Record{
			Topic: s.prefix + ".index." + lower(symbol),
			Key:   []byte(symbol),
			Value: payload,
		}, func(_ *kgo.Record, err error) {
			if err != nil {
				s.log.Error("index produce failed", "symbol", symbol, "err", err)
			}
		})
		lastPub[symbol] = now
		s.lastIndex[symbol+"#pub"] = indexResult{Index: res.Index}
	}
}

func indexSnapshot(symbol string, res indexResult, now time.Time) []byte {
	sources := map[string]map[string]any{}
	// res.Rejected 里是剔除源;bySrc 的明细由调用方补齐 —— 这里输出参与/剔除状态
	for _, u := range res.Usable {
		sources[u] = map[string]any{"status": "usable"}
	}
	for name, reason := range res.Rejected {
		sources[name] = map[string]any{"status": "rejected", "reason": reason}
	}
	b, _ := json.Marshal(map[string]any{
		"symbol":  symbol,
		"index":   fixedStr(res.Index),
		"ok":      res.OK,
		"ts":      now.UnixMilli(),
		"sources": sources,
	})
	return b
}

func lower(s string) string {
	out := []byte(s)
	for i := range out {
		if out[i] >= 'A' && out[i] <= 'Z' {
			out[i] += 'a' - 'A'
		}
	}
	return string(out)
}
