package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/EmptyHeart5292/lcex/internal/events"
	"github.com/EmptyHeart5292/lcex/internal/fixed"
)

const (
	windows24hMs int64 = 24 * 60 * 60 * 1000
	candle1mMs   int64 = 60 * 1000
	maxKeptTrades      = 5000
	maxKeptCandles     = 3000 // 48h 的 1m 蜡烛
	maxKeptLevels      = 200  // 每侧保留的价格档数
)

type restingOrder struct {
	Price     uint64
	Remaining uint64
}

type tradeRow struct {
	TradeID uint64
	Seq     uint64
	Price   uint64
	Qty     uint64
	Side    string // taker 方向
	TS      int64
}

type candle struct {
	Start, End                int64
	Open, High, Low, Close    uint64
	Volume                    uint64
}

// changedLevel 一次事件造成的盘口价格档变化(sideIdx 0=bid 1=ask),Total=0 表示档位清空
type changedLevel struct {
	SideIdx int
	Price   uint64
	Total   uint64
}

// symbolState 只能在持有 stateStore.mu 的情况下访问
type symbolState struct {
	symbol  string
	resting map[uint64]*restingOrder
	depth   [2]map[uint64]uint64 // [0]=bids [1]=asks
	lastSeq uint64

	trades  []tradeRow        // 24h 窗口,时间升序
	candles map[int64]*candle // 1m bucket start -> candle
}

type stateStore struct {
	mu    sync.RWMutex
	bySym map[string]*symbolState
}

func newStateStore(symbols []string) *stateStore {
	s := &stateStore{bySym: map[string]*symbolState{}}
	for _, sym := range symbols {
		s.bySym[sym] = &symbolState{
			symbol:  sym,
			resting: map[uint64]*restingOrder{},
			depth:   [2]map[uint64]uint64{map[uint64]uint64{}, map[uint64]uint64{}},
			candles: map[int64]*candle{},
		}
	}
	return s
}

// apply 处理一条事件;返回盘口变化档位与成交(供 WS 发布)。重复 seq 静默跳过。
func (s *stateStore) apply(symbol string, ev *events.Event) (levels []changedLevel, trade *tradeRow) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.bySym[symbol]
	if !ok {
		return
	}
	if ev.Seq <= st.lastSeq {
		return
	}
	st.lastSeq = ev.Seq

	switch ev.Kind {
	case "order_update":
		u, err := ev.OrderUpdate()
		if err != nil {
			return
		}
		levels = st.applyOrderUpdate(u)
	case "trade":
		t, err := ev.Trade()
		if err != nil {
			return
		}
		trade = st.applyTrade(t, ev.Seq)
	}
	return
}

func (st *symbolState) applyOrderUpdate(u *events.OrderUpdate) []changedLevel {
	sideIdx := 0
	if u.Side == "ask" {
		sideIdx = 1
	}
	remaining := u.Qty - u.FilledQty
	old, inBook := st.resting[u.OrderID]

	switch u.Status {
	case "open", "partially_filled":
		if inBook {
			if u.Price == nil || old.Price != *u.Price {
				// 价格不该变;防御:按移除+新增处理
				st.removeLevel(sideIdx, old.Price, old.Remaining, u.OrderID)
				inBook = false
			} else {
				newTotal := uint64(int64(st.depth[sideIdx][old.Price]) + int64(remaining) - int64(old.Remaining))
				st.depth[sideIdx][old.Price] = newTotal
				old.Remaining = remaining
				return []changedLevel{{sideIdx, old.Price, newTotal}}
			}
		}
		if u.Price == nil {
			return nil // 市价单不挂簿
		}
		st.resting[u.OrderID] = &restingOrder{Price: *u.Price, Remaining: remaining}
		st.depth[sideIdx][*u.Price] += remaining
		st.trim(sideIdx)
		return []changedLevel{{sideIdx, *u.Price, st.depth[sideIdx][*u.Price]}}
	default: // filled / canceled / rejected
		if inBook {
			st.removeLevel(sideIdx, old.Price, old.Remaining, u.OrderID)
			return []changedLevel{{sideIdx, old.Price, st.depth[sideIdx][old.Price]}}
		}
		return nil
	}
}

func (st *symbolState) removeLevel(sideIdx int, price uint64, remaining uint64, orderID uint64) {
	if remaining > st.depth[sideIdx][price] {
		remaining = st.depth[sideIdx][price] // 防御:不允许负总量
	}
	st.depth[sideIdx][price] -= remaining
	if st.depth[sideIdx][price] == 0 {
		delete(st.depth[sideIdx], price)
	}
	delete(st.resting, orderID)
}

// trim 价格档过多时按劣价方向淘汰(防长尾爆内存)
func (st *symbolState) trim(sideIdx int) {
	if len(st.depth[sideIdx]) <= maxKeptLevels {
		return
	}
	prices := make([]uint64, 0, len(st.depth[sideIdx]))
	for p := range st.depth[sideIdx] {
		prices = append(prices, p)
	}
	if sideIdx == 0 {
		sort.Slice(prices, func(i, j int) bool { return prices[i] < prices[j] }) // 买侧淘汰最低价
	} else {
		sort.Slice(prices, func(i, j int) bool { return prices[i] > prices[j] }) // 卖侧淘汰最高价
	}
	for _, p := range prices[:len(prices)-maxKeptLevels] {
		delete(st.depth[sideIdx], p)
	}
}

func (st *symbolState) applyTrade(t *events.Trade, seq uint64) *tradeRow {
	now := time.Now().UnixMilli()
	row := tradeRow{TradeID: t.TradeID, Seq: seq, Price: t.Price, Qty: t.Qty, Side: t.Side, TS: now}
	st.trades = append(st.trades, row)
	cutoff := now - windows24hMs
	i := 0
	for i < len(st.trades) && st.trades[i].TS < cutoff {
		i++
	}
	if i > 0 {
		st.trades = append([]tradeRow{}, st.trades[i:]...)
	}
	if len(st.trades) > maxKeptTrades {
		st.trades = st.trades[len(st.trades)-maxKeptTrades:]
	}

	bucket := now - now%candle1mMs
	c, ok := st.candles[bucket]
	if !ok {
		c = &candle{Start: bucket, End: bucket + candle1mMs - 1, Open: t.Price, High: t.Price, Low: t.Price, Close: t.Price}
		st.candles[bucket] = c
	} else {
		if t.Price > c.High {
			c.High = t.Price
		}
		if t.Price < c.Low {
			c.Low = t.Price
		}
		c.Close = t.Price
	}
	c.Volume += t.Qty

	for b := range st.candles {
		if b < now-maxKeptCandles*candle1mMs {
			delete(st.candles, b)
		}
	}
	return &row
}

// ---- 快照(调用方需持锁或经由 stateStore 包装)----

type levelJSON [2]string

func topLevels(m map[uint64]uint64, limit int, desc bool) []levelJSON {
	prices := make([]uint64, 0, len(m))
	for p := range m {
		prices = append(prices, p)
	}
	sort.Slice(prices, func(i, j int) bool {
		if desc {
			return prices[i] > prices[j]
		}
		return prices[i] < prices[j]
	})
	if limit > 0 && len(prices) > limit {
		prices = prices[:limit]
	}
	out := make([]levelJSON, 0, len(prices))
	for _, p := range prices {
		out = append(out, levelJSON{fixed.Format(p), fixed.Format(m[p])})
	}
	return out
}

type tickerJSON struct {
	Symbol         string `json:"symbol"`
	Last           string `json:"last"`
	Bid            string `json:"bid"`
	Ask            string `json:"ask"`
	High24h        string `json:"high24h"`
	Low24h         string `json:"low24h"`
	Volume24h      string `json:"volume24h"`
	QuoteVolume24h string `json:"quoteVolume24h"`
	ChangePct24h   string `json:"changePct24h"`
	TS             int64  `json:"ts"`
}

func (s *stateStore) Depth(symbol string, limit int) (uint64, []levelJSON, []levelJSON, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.bySym[symbol]
	if !ok {
		return 0, nil, nil, false
	}
	return st.lastSeq, topLevels(st.depth[0], limit, true), topLevels(st.depth[1], limit, false), true
}

func (s *stateStore) Ticker(symbol string) (tickerJSON, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.bySym[symbol]
	if !ok {
		return tickerJSON{}, false
	}
	return st.tickerLocked(), true
}

func (s *stateStore) Tickers() []tickerJSON {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]tickerJSON, 0, len(s.bySym))
	for _, sym := range sortedSymbols(s.bySym) {
		out = append(out, s.bySym[sym].tickerLocked())
	}
	return out
}

func (s *stateStore) RecentTrades(symbol string, limit int) ([]tradeRow, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.bySym[symbol]
	if !ok {
		return nil, false
	}
	if limit <= 0 || limit > len(st.trades) {
		limit = len(st.trades)
	}
	out := make([]tradeRow, limit)
	copy(out, st.trades[len(st.trades)-limit:])
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, true
}

var intervalMs = map[string]int64{
	"1m": 60_000, "5m": 300_000, "15m": 900_000, "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000,
}

func (s *stateStore) Klines(symbol, interval string, limit int) ([][]string, bool) {
	ms, ok := intervalMs[interval]
	if !ok {
		return nil, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok2 := s.bySym[symbol]
	if !ok2 {
		return nil, false
	}
	buckets := map[int64]*candle{}
	var keys []int64
	for _, c := range st.candles {
		b := c.Start - c.Start%ms
		agg, exists := buckets[b]
		if !exists {
			buckets[b] = &candle{Start: b, End: b + ms - 1, Open: c.Open, High: c.High, Low: c.Low, Close: c.Close, Volume: c.Volume}
			keys = append(keys, b)
		} else {
			if c.High > agg.High {
				agg.High = c.High
			}
			if c.Low < agg.Low {
				agg.Low = c.Low
			}
			agg.Close = c.Close
			agg.Volume += c.Volume
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	if limit > 0 && len(keys) > limit {
		keys = keys[len(keys)-limit:]
	}
	out := make([][]string, 0, len(keys))
	for _, b := range keys {
		c := buckets[b]
		out = append(out, []string{
			fmt.Sprint(c.Start), fixed.Format(c.Open), fixed.Format(c.High), fixed.Format(c.Low),
			fixed.Format(c.Close), fixed.Format(c.Volume), fmt.Sprint(c.End),
		})
	}
	return out, true
}

// PublishStaleBySeq 供 WS 判断:当前 seq
func (s *stateStore) Seq(symbol string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if st, ok := s.bySym[symbol]; ok {
		return st.lastSeq
	}
	return 0
}

func (st *symbolState) tickerLocked() tickerJSON {
	t := tickerJSON{Symbol: st.symbol, TS: time.Now().UnixMilli()}
	if n := len(st.trades); n > 0 {
		first, last := st.trades[0], st.trades[n-1]
		t.Last = fixed.Format(last.Price)
		high, low := last.Price, last.Price
		var vol, qvol uint64
		for _, tr := range st.trades {
			if tr.Price > high {
				high = tr.Price
			}
			if tr.Price < low {
				low = tr.Price
			}
			vol += tr.Qty
			if qa, err := fixed.MulDiv(tr.Price, tr.Qty, fixed.Scale); err == nil {
				qvol += qa
			}
		}
		t.High24h = fixed.Format(high)
		t.Low24h = fixed.Format(low)
		t.Volume24h = fixed.Format(vol)
		t.QuoteVolume24h = fixed.Format(qvol)
		if first.Price > 0 {
			pct := (last.Price - first.Price) * fixed.Scale / first.Price
			t.ChangePct24h = fixed.Format(pct)
		}
	}
	if bids := topLevels(st.depth[0], 1, true); len(bids) > 0 {
		t.Bid = bids[0][0]
	}
	if asks := topLevels(st.depth[1], 1, false); len(asks) > 0 {
		t.Ask = asks[0][0]
	}
	return t
}

func (s *stateStore) lastCandleLocked(symbol string) *candle {
	st, ok := s.bySym[symbol]
	if !ok {
		return nil
	}
	var latest *candle
	for _, c := range st.candles {
		if latest == nil || c.Start > latest.Start {
			latest = c
		}
	}
	return latest
}

func sortedSymbols(m map[string]*symbolState) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// normalizeSymbol 大小写归一
func normalizeSymbol(s string) string { return strings.ToUpper(s) }
