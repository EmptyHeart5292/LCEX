package main

// WebSocket 推送:协议见 packages/api-spec/ws/protocol.md(公共频道部分)。
// 私有频道(order/balance)待网关接入后随鉴权一起提供。

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/EmptyHeart5292/lcex/internal/fixed"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(*http.Request) bool { return true }, // dev;生产由网关收紧
}

type wsConn struct {
	ws   *websocket.Conn
	send chan []byte
}

type wsHub struct {
	mu   sync.RWMutex
	subs map[string]map[*wsConn]struct{}
}

func newHub() *wsHub {
	return &wsHub{subs: map[string]map[*wsConn]struct{}{}}
}

func (h *wsHub) subscribe(c *wsConn, topic string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	m, ok := h.subs[topic]
	if !ok {
		m = map[*wsConn]struct{}{}
		h.subs[topic] = m
	}
	m[c] = struct{}{}
}

func (h *wsHub) unsubscribe(c *wsConn, topic string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if m, ok := h.subs[topic]; ok {
		delete(m, c)
		if len(m) == 0 {
			delete(h.subs, topic)
		}
	}
}

func (h *wsHub) remove(c *wsConn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for topic, m := range h.subs {
		delete(m, c)
		if len(m) == 0 {
			delete(h.subs, topic)
		}
	}
}

func (h *wsHub) publish(topic string, payload []byte) {
	h.mu.RLock()
	conns := make([]*wsConn, 0, len(h.subs[topic]))
	for c := range h.subs[topic] {
		conns = append(conns, c)
	}
	h.mu.RUnlock()
	for _, c := range conns {
		select {
		case c.send <- payload:
		default: // 发送缓冲满:断开慢连接
			h.remove(c)
			close(c.send)
		}
	}
}

func (s *service) handleWS(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := &wsConn{ws: ws, send: make(chan []byte, 256)}
	go s.wsWriteLoop(c)
	s.wsReadLoop(c)
}

func (s *service) wsWriteLoop(c *wsConn) {
	pingSecs := atoiDefault(envOr("CEX_WS_PING_SECONDS", "20"), 20)
	ping := time.NewTicker(time.Duration(pingSecs) * time.Second)
	defer ping.Stop()
	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				_ = c.ws.WriteMessage(websocket.CloseMessage, nil)
				return
			}
			c.ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.ws.WriteMessage(websocket.TextMessage, msg); err != nil {
				s.hub.remove(c)
				return
			}
		case <-ping.C:
			c.ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.ws.WriteMessage(websocket.TextMessage, []byte(`{"channel":"ping"}`)); err != nil {
				s.hub.remove(c)
				return
			}
		}
	}
}

func (s *service) wsReadLoop(c *wsConn) {
	defer func() {
		s.hub.remove(c)
		_ = c.ws.Close()
	}()
	c.ws.SetReadLimit(16 * 1024)
	for {
		_, data, err := c.ws.ReadMessage()
		if err != nil {
			return
		}
		var msg struct {
			Op   string   `json:"op"`
			Args []string `json:"args"`
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		switch msg.Op {
		case "ping":
			s.hub.publishConn(c, []byte(`{"event":"pong"}`))
		case "subscribe":
			accepted := make([]string, 0, len(msg.Args))
			for _, ch := range msg.Args {
				if topic, ok := validChannel(ch, s.symbols); ok {
					s.hub.subscribe(c, topic)
					accepted = append(accepted, ch)
					s.sendInitial(topic, c)
				} else {
					writeJSONErr := mustJSON(map[string]any{"event": "error", "code": 70004, "message": "invalid channel: " + ch})
					s.hub.publishConn(c, writeJSONErr)
				}
			}
			if len(accepted) > 0 {
				s.hub.publishConn(c, mustJSON(map[string]any{"event": "subscribe", "args": accepted, "code": 0}))
			}
		case "unsubscribe":
			for _, ch := range msg.Args {
				if topic, ok := validChannel(ch, s.symbols); ok {
					s.hub.unsubscribe(c, topic)
				}
			}
			s.hub.publishConn(c, mustJSON(map[string]any{"event": "unsubscribe", "args": msg.Args, "code": 0}))
		}
	}
}

func (h *wsHub) publishConn(c *wsConn, payload []byte) {
	select {
	case c.send <- payload:
	default:
	}
}

// validChannel 校验频道名并返回内部 topic key
func validChannel(ch string, symbols []string) (string, bool) {
	parts := strings.Split(ch, "@")
	if len(parts) < 2 {
		return "", false
	}
	sym := normalizeSymbol(parts[1])
	found := false
	for _, s := range symbols {
		if s == sym {
			found = true
		}
	}
	if !found {
		return "", false
	}
	switch parts[0] {
	case "ticker":
		return "ticker@" + sym, true
	case "trades":
		return "trades@" + sym, true
	case "depth":
		return "depth@" + sym, true // 更新不按 limit 过滤档位;limit 仅影响订阅快照
	case "kline":
		if len(parts) != 3 {
			return "", false
		}
		if _, ok := intervalMs[parts[2]]; !ok {
			return "", false
		}
		return "kline@" + sym + "@" + parts[2], true
	default:
		return "", false
	}
}

// sendInitial 订阅瞬间的快照(depth 快照 / ticker 当前值 / kline 当前蜡烛)
func (s *service) sendInitial(topic string, c *wsConn) {
	parts := strings.Split(topic, "@")
	sym := parts[1]
	switch parts[0] {
	case "depth":
		limit := 50
		if len(parts) == 3 {
			limit = atoiDefault(parts[2], 50)
		}
		seq, bids, asks, _ := s.states.Depth(sym, limit)
		s.hub.publishConn(c, mustJSON(map[string]any{
			"channel": "depth", "symbol": sym, "type": "snapshot", "seq": seq, "bids": bids, "asks": asks,
		}))
	case "ticker":
		if t, ok := s.states.Ticker(sym); ok {
			s.hub.publishConn(c, mustJSON(map[string]any{"channel": "ticker", "symbol": sym, "data": t}))
		}
	case "kline":
		if st := s.states.lastCandleLocked(sym); st != nil {
			s.hub.publishConn(c, mustJSON(klineMsg(sym, parts[2], st)))
		}
	}
}

func klineMsg(symbol, interval string, c *candle) map[string]any {
	return map[string]any{
		"channel": "kline", "symbol": symbol, "interval": interval,
		"data": map[string]any{
			"start": c.Start, "end": c.End,
			"open": fixedFormat(c.Open), "high": fixed.Format(c.High),
			"low": fixed.Format(c.Low), "close": fixed.Format(c.Close),
			"volume": fixed.Format(c.Volume),
		},
	}
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func fixedFormat(v uint64) string { return fixed.Format(v) }
