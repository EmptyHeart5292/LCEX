package main

import (
	"context"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/EmptyHeart5292/lcex/internal/events"
	"github.com/EmptyHeart5292/lcex/internal/fixed"
)

// consumeLoop 消费撮合事件 → 更新状态 → 向 WS 频道发布。
func (s *service) consumeLoop(ctx context.Context, cli *kgo.Client, prefix string) {
	defer cli.Close()
	for {
		select {
		case <-ctx.Done():
			s.log.Info("market consumer stopped")
			cli.Close()
			return
		default:
		}
		fetches := cli.PollRecords(ctx, 100)
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			if ctx.Err() != nil {
				return
			}
			s.log.Error("consume error", "topic", t, "err", err)
		})
		fetches.EachRecord(func(rec *kgo.Record) {
			s.handleRecord(rec, prefix)
			cli.CommitRecords(ctx, rec)
		})
	}
}

func (s *service) handleRecord(rec *kgo.Record, prefix string) {
	symbol := strings.ToUpper(strings.TrimPrefix(rec.Topic, prefix+".events."))
	ev, err := events.Decode(rec.Value)
	if err != nil {
		s.log.Error("bad event payload", "err", err)
		return
	}
	levels, trade := s.states.apply(symbol, ev)

	if len(levels) > 0 {
		var bids, asks [][2]string
		for _, l := range levels {
			lv := [2]string{fixed.Format(l.Price), fixed.Format(l.Total)}
			if l.SideIdx == 0 {
				bids = append(bids, lv)
			} else {
				asks = append(asks, lv)
			}
		}
		if bids != nil || asks != nil {
			s.hub.publish("depth@"+symbol, mustJSON(map[string]any{
				"channel": "depth", "symbol": symbol, "type": "update",
				"seq": s.states.Seq(symbol), "bids": bids, "asks": asks,
			}))
		}
	}

	if trade != nil {
		s.hub.publish("trades@"+symbol, mustJSON(map[string]any{
			"channel": "trades", "symbol": symbol,
			"data": map[string]any{
				"tradeId": trade.TradeID, "seq": trade.Seq,
				"price": fixed.Format(trade.Price), "qty": fixed.Format(trade.Qty),
				"side": trade.Side, "ts": trade.TS,
			},
		}))
		if t, ok := s.states.Ticker(symbol); ok {
			s.hub.publish("ticker@"+symbol, mustJSON(map[string]any{"channel": "ticker", "symbol": symbol, "data": t}))
		}
		if c := s.states.lastCandleLocked(symbol); c != nil {
			s.hub.publish("kline@"+symbol+"@1m", mustJSON(klineMsg(symbol, "1m", c)))
		}
	}
}
