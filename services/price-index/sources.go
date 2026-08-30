package main

// 外部交易所公共行情适配器(ADR-006)。
//
// 每个源一个 goroutine:连 WS → 发订阅 → 解析中间价/最新价 → 推入通道。
// 任何故障只影响该源自己(退避重连),不影响聚合;支持 HTTPS_PROXY 代理。

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/EmptyHeart5292/lcex/internal/fixed"
)

type exchangeAdapter interface {
	Name() string
	// URL 指定内部 symbol 的 WS 地址(可被环境变量覆盖,供 mock 测试)
	URL(symbol string) string
	// SubscribeMsg 连接后发送的订阅消息,nil 表示不发
	SubscribeMsg(symbol string) ([]byte, error)
	// Parse 一条消息 → (中间价, 是否相关)。symbol 由 runSource 按订阅目标传入,
	// 适配器不得用交易所侧符号回填(避免 BTCUSDT ≠ BTC-USDT 类错位)。
	Parse(symbol string, data []byte) (price uint64, ok bool)
}

// ---- 工具 ----

func midOf(bidStr, askStr string) (uint64, bool) {
	bid, err1 := fixed.Parse(bidStr)
	ask, err2 := fixed.Parse(askStr)
	if err1 != nil || err2 != nil || bid == 0 || ask == 0 {
		return 0, false
	}
	return (bid + ask) / 2, true
}

func lastOf(px string) (uint64, bool) {
	p, err := fixed.Parse(px)
	if err != nil || p == 0 {
		return 0, false
	}
	return p, true
}

// runSource 单源生命周期:断线退避重连,ctx 取消即退出。
func runSource(ctx context.Context, a exchangeAdapter, symbol string, out chan<- tick, log *slog.Logger) {
	name := a.Name() + "/" + symbol
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		err := sourceOnce(ctx, a, symbol, out, log)
		if ctx.Err() != nil {
			return
		}
		log.Warn("source disconnected, reconnecting", "source", name, "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func sourceOnce(ctx context.Context, a exchangeAdapter, symbol string, out chan<- tick, log *slog.Logger) error {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		Proxy:            http.ProxyFromEnvironment, // 网络受限环境走代理
	}
	ws, _, err := dialer.Dial(a.URL(symbol), nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer ws.Close()
	log.Info("source connected", "source", a.Name()+"/"+symbol, "url", a.URL(symbol))

	if msg, err := a.SubscribeMsg(symbol); err != nil {
		return fmt.Errorf("subscribe build: %w", err)
	} else if msg != nil {
		ws.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := ws.WriteMessage(websocket.TextMessage, msg); err != nil {
			return fmt.Errorf("subscribe: %w", err)
		}
	}

	// 读超时兜底:60s 无数据则重连(正常行情远快于此)
	deadline := func() { ws.SetReadDeadline(time.Now().Add(60 * time.Second)) }
	deadline()
	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		deadline()

		// 文本层 ping(OKX/Bitget 等):统一回 pong;协议层 ping 由库自动处理
		if strings.Contains(string(data), `"event":"ping"`) || strings.Contains(string(data), `"op":"ping"`) {
			ws.SetWriteDeadline(time.Now().Add(5 * time.Second))
			_ = ws.WriteMessage(websocket.TextMessage, []byte(`{"event":"pong","op":"pong"}`))
			continue
		}

		price, ok := a.Parse(symbol, data)
		if !ok {
			continue
		}
		select {
		case out <- tick{Source: a.Name(), Symbol: symbol, Price: price, TS: time.Now()}:
		default: // 聚合器繁忙则丢弃旧 tick,宁缺毋堵
		}
	}
}

// ---- 各所适配器 ----

type binanceSource struct{ baseURL string }

func (s binanceSource) Name() string { return "binance" }
func (s binanceSource) URL(symbol string) string {
	if s.baseURL != "" {
		return s.baseURL
	}
	return fmt.Sprintf("wss://stream.binance.com:9443/stream?streams=%s@bookTicker", strings.ToLower(symbol))
}
func (s binanceSource) SubscribeMsg(string) ([]byte, error) { return nil, nil } // URL 组合流无需订阅
func (s binanceSource) Parse(_ string, data []byte) (uint64, bool) {
	// bookTicker 帧同时含 "b"(价格)与 "B"(数量):struct 解码的大小写不敏感回退
	// 会让 "B"/"A" 覆盖价格字段,必须用 map 精确取键。
	var m struct {
		Data map[string]string `json:"data"`
	}
	if err := jsonUnmarshal(data, &m); err != nil {
		return 0, false
	}
	return midOf(m.Data["b"], m.Data["a"])
}

type okxSource struct{ baseURL string }

func (s okxSource) Name() string { return "okx" }
func (s okxSource) URL(symbol string) string {
	if s.baseURL != "" {
		return s.baseURL
	}
	return "wss://ws.okx.com:8443/ws/v5/public"
}
func (s okxSource) SubscribeMsg(symbol string) ([]byte, error) {
	return fmtAppend(`{"op":"subscribe","args":[{"channel":"books5","instId":"%s"}]}`, symbol), nil
}
func (s okxSource) Parse(_ string, data []byte) (uint64, bool) {
	var m struct {
		Data []struct {
			Bids [][2]string `json:"bids"`
			Asks [][2]string `json:"asks"`
		} `json:"data"`
	}
	if err := jsonUnmarshal(data, &m); err != nil || len(m.Data) == 0 {
		return 0, false
	}
	d := m.Data[0]
	if len(d.Bids) == 0 || len(d.Asks) == 0 {
		return 0, false
	}
	return midOf(d.Bids[0][0], d.Asks[0][0])
}

type bybitSource struct{ baseURL string }

func (s bybitSource) Name() string { return "bybit" }
func (s bybitSource) URL(symbol string) string {
	if s.baseURL != "" {
		return s.baseURL
	}
	return "wss://stream.bybit.com/v5/public/spot"
}
func (s bybitSource) SubscribeMsg(symbol string) ([]byte, error) {
	return fmtAppend(`{"op":"subscribe","args":["tickers.%s"]}`, strings.ToUpper(symbol)), nil
}
func (s bybitSource) Parse(_ string, data []byte) (uint64, bool) {
	var m struct {
		Topic string `json:"topic"`
		Data  struct {
			LastPrice string `json:"lastPrice"`
		} `json:"data"`
	}
	if err := jsonUnmarshal(data, &m); err != nil || !strings.HasPrefix(m.Topic, "tickers.") {
		return 0, false
	}
	return lastOf(m.Data.LastPrice)
}

type mexcSource struct{ baseURL string }

func (s mexcSource) Name() string { return "mexc" }
func (s mexcSource) URL(symbol string) string {
	if s.baseURL != "" {
		return s.baseURL
	}
	return "wss://wbs.mexc.com/ws"
}
func (s mexcSource) SubscribeMsg(symbol string) ([]byte, error) {
	return fmtAppend(`{"method":"SUBSCRIPTION","params":["spot@public.bookticker.v3.api@%s"]}`, strings.ToUpper(symbol)), nil
}
func (s mexcSource) Parse(_ string, data []byte) (uint64, bool) {
	var m struct {
		Channel string `json:"channel"`
		Data    struct {
			Bid string `json:"bidPrice"`
			Ask string `json:"askPrice"`
		} `json:"data"`
	}
	if err := jsonUnmarshal(data, &m); err != nil || !strings.Contains(m.Channel, "bookticker") {
		return 0, false
	}
	return midOf(m.Data.Bid, m.Data.Ask)
}

type bitgetSource struct{ baseURL string }

func (s bitgetSource) Name() string { return "bitget" }
func (s bitgetSource) URL(symbol string) string {
	if s.baseURL != "" {
		return s.baseURL
	}
	return "wss://ws.bitget.com/v2/ws/public"
}
func (s bitgetSource) SubscribeMsg(symbol string) ([]byte, error) {
	return fmtAppend(`{"op":"subscribe","args":[{"instType":"SPOT","channel":"books1","instId":"%s"}]}`, strings.ToUpper(symbol)), nil
}
func (s bitgetSource) Parse(_ string, data []byte) (uint64, bool) {
	var m struct {
		Data []struct {
			Bids [][2]string `json:"bids"`
			Asks [][2]string `json:"asks"`
		} `json:"data"`
	}
	if err := jsonUnmarshal(data, &m); err != nil || len(m.Data) == 0 {
		return 0, false
	}
	d := m.Data[0]
	if len(d.Bids) == 0 || len(d.Asks) == 0 {
		return 0, false
	}
	return midOf(d.Bids[0][0], d.Asks[0][0])
}
