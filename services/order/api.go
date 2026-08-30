package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/EmptyHeart5292/lcex/internal/events"
	"github.com/EmptyHeart5292/lcex/internal/fixed"
	"github.com/EmptyHeart5292/lcex/internal/ledger"
	"github.com/EmptyHeart5292/lcex/internal/markets"
)

// ---- 错误响应(errors.md 错误码)----

type apiErr struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func writeErr(w http.ResponseWriter, httpStatus, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	_ = json.NewEncoder(w).Encode(apiErr{Code: code, Message: msg})
}

func writeData(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": data})
}

func statusFor(code int) int {
	switch code {
	case 10003, 20102, 50001, 50002, 50003, 50004, 50005, 50006, 50007, 50008, 50009, 50011, 51001, 51002, 50012:
		return http.StatusBadRequest
	case 50010:
		return http.StatusNotFound
	case 20001, 20002, 20003, 20004, 20005, 60005:
		return http.StatusUnauthorized
	case 20101:
		return http.StatusTooManyRequests
	default:
		return http.StatusInternalServerError
	}
}

func fail(w http.ResponseWriter, code int, msg string) {
	writeErr(w, statusFor(code), code, msg)
}

// ---- 路由 ----

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeData(w, map[string]bool{"ok": true}) })
	mux.HandleFunc("POST /api/v1/orders", s.handlePlace)
	mux.HandleFunc("DELETE /api/v1/orders/{orderId}", s.handleCancel)
	mux.HandleFunc("GET /api/v1/orders/open", s.handleOpenOrders)
	mux.HandleFunc("GET /api/v1/orders/{orderId}", s.handleGetOrder)
	mux.HandleFunc("GET /api/v1/account/balances", s.handleBalances)
	return mux
}

// ---- 下单 ----

type placeReq struct {
	Symbol        string `json:"symbol"`
	ClientOrderID string `json:"clientOrderId"`
	Side          string `json:"side"` // bid | ask
	Type          string `json:"type"` // LIMIT | MARKET
	TimeInForce   string `json:"timeInForce"`
	StpMode       string `json:"stpMode"`
	PostOnly      bool   `json:"postOnly"`
	Price         string `json:"price"`
	Qty           string `json:"qty"`
}

func (s *server) handlePlace(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUserID(r)
	if err != nil {
		fail(w, 20005, err.Error())
		return
	}
	var req placeReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, 10003, "invalid json body")
		return
	}

	m, err := s.markets.Get(req.Symbol)
	if err != nil {
		fail(w, 50001, "unknown market: "+req.Symbol)
		return
	}
	if !m.Trading() {
		fail(w, 50002, "market halted: "+req.Symbol)
		return
	}
	if req.ClientOrderID == "" || len(req.ClientOrderID) > 64 {
		fail(w, 10003, "clientOrderId required (<=64)")
		return
	}
	side := req.Side
	if side != "bid" && side != "ask" {
		fail(w, 10003, `side must be "bid" or "ask"`)
		return
	}
	orderType := "limit"
	if req.Type != "" {
		orderType = lower(req.Type)
	}
	if orderType != "limit" && orderType != "market" {
		fail(w, 10003, `type must be LIMIT or MARKET`)
		return
	}
	tif := "gtc"
	if req.TimeInForce != "" {
		tif = lower(req.TimeInForce)
	}
	if orderType == "limit" && tif != "gtc" && tif != "ioc" && tif != "fok" {
		fail(w, 10003, "timeInForce must be GTC/IOC/FOK")
		return
	}
	stp := "none"
	if req.StpMode != "" {
		stp = lower(req.StpMode)
	}
	if stp != "none" && stp != "cancel_taker" {
		fail(w, 10003, "stpMode must be none/cancel_taker")
		return
	}

	qty, err := fixed.Parse(req.Qty)
	if err != nil || qty == 0 {
		fail(w, 10003, "invalid qty")
		return
	}
	var price *uint64
	if orderType == "limit" {
		p, err := fixed.Parse(req.Price)
		if err != nil || p == 0 {
			fail(w, 50007, "invalid price")
			return
		}
		price = &p
	} else if req.Price != "" {
		fail(w, 10003, "market order takes no price")
		return
	}

	// 市价买单:MVP 暂不支持(冻结额无锚价)
	if orderType == "market" && side == "bid" {
		fail(w, 50012, "market buy not supported yet, use limit")
		return
	}
	if orderType == "market" && m.MaxMktQty > 0 && qty > m.MaxMktQty {
		fail(w, 51002, "market order qty exceeds limit")
		return
	}
	if qty < m.MinQty {
		fail(w, 50005, "qty below minimum")
		return
	}

	// 冻结额:买单冻结 quote(名义额 + taker 费上浮),卖单冻结 base
	reserved := qty
	if side == "bid" {
		qa, err := fixed.MulDiv(*price, qty, fixed.Scale)
		if err != nil {
			fail(w, 10003, "amount overflow")
			return
		}
		if qa < m.MinNotion {
			fail(w, 50006, "notional below minimum")
			return
		}
		feeCeil, err := fixed.MulDivCeil(qa, m.TakerRate, fixed.Scale)
		if err != nil {
			fail(w, 10003, "fee overflow")
			return
		}
		reserved = qa + feeCeil
	}

	orderID, status, replayed, apiCode, apiMsg := s.place(userID, m.Symbol, req.ClientOrderID, side, orderType, tif, stp, req.PostOnly, price, qty, int64(reserved))
	if apiCode != 0 {
		fail(w, apiCode, apiMsg)
		return
	}

	// 幂等命中:已有订单原样返回,不得重复投递撮合
	if !replayed {
		cmd := events.PlaceCommand{
			OrderID: uint64(orderID), UserID: uint64(userID), Side: side,
			OrderType: orderType, Tif: tif, Stp: stp, PostOnly: req.PostOnly,
			Price: price, Qty: qty,
		}
		payload, err := events.PlaceEnvelope(cmd)
		if err != nil {
			fail(w, 10001, "internal error")
			return
		}
		if err := produceSync(r.Context(), s.prod, s.inputTopic(m.Symbol), m.Symbol, payload); err != nil {
			s.log.Error("produce place failed, compensating", "order", orderID, "err", err)
			s.rejectAndUnfreeze(orderID, userID, m, side, int64(reserved))
			fail(w, 10002, "order dispatch failed, refunded")
			return
		}
	}
	writeData(w, map[string]any{"orderId": orderID, "clientOrderId": req.ClientOrderID, "status": status})
}

// place 同事务:落订单行 + 冻结资金;replayed=true 表示 clientOrderId 已存在(幂等命中,勿再投递)
func (s *server) place(userID int64, symbol, clientOrderID, side, orderType, tif, stp string, postOnly bool, price *uint64, qty uint64, reserved int64) (orderID int64, status string, replayed bool, apiCode int, apiMsg string) {
	currency := s.currencyOf(symbol, side) // 冻结币种:bid→quote, ask→base
	err := s.ledger.WithTx(context.Background(), func(tx pgx.Tx) error {
		err := tx.QueryRow(context.Background(), `
			INSERT INTO orders (user_id, symbol, client_order_id, side, order_type, tif, stp, post_only, price, qty, reserved, status)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'pending')
			RETURNING order_id`,
			userID, symbol, clientOrderID, side, orderType, tif, stp, postOnly, price, int64(qty), reserved).Scan(&orderID)
		if err != nil {
			return err
		}
		bizID := fmt.Sprintf("order-%d", orderID)
		return s.ledger.PostTx(context.Background(), tx, "order_freeze", bizID, []ledger.Move{
			{Account: ledger.User(userID, currency, "available"), Delta: -reserved},
			{Account: ledger.User(userID, currency, "frozen"), Delta: +reserved},
		})
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			// clientOrderId 幂等:返回已存在的订单,且不得重复投递
			row := s.pool.QueryRow(context.Background(),
				`SELECT order_id, status FROM orders WHERE user_id=$1 AND client_order_id=$2`, userID, clientOrderID)
			var st string
			if err := row.Scan(&orderID, &st); err == nil {
				return orderID, st, true, 0, ""
			}
			return 0, "", false, 20102, "duplicate client order id"
		}
		if errors.Is(err, ledger.ErrInsufficientBalance) {
			return 0, "", false, 51001, "insufficient balance"
		}
		s.log.Error("place failed", "err", err)
		return 0, "", false, 10001, "internal error"
	}
	return orderID, "pending", false, 0, ""
}

func (s *server) currencyOf(symbol, side string) string {
	m, _ := s.markets.Get(symbol)
	if side == "bid" {
		return m.Quote
	}
	return m.Base
}

// rejectAndUnfreeze 投递失败补偿:标记 rejected + 全额解冻(与 clearing 终态解冻同一幂等键)
func (s *server) rejectAndUnfreeze(orderID int64, userID int64, m *markets.Market, side string, reserved int64) {
	currency := m.Quote
	if side == "ask" {
		currency = m.Base
	}
	_, _ = s.pool.Exec(context.Background(),
		`UPDATE orders SET status='rejected', updated_at=now() WHERE order_id=$1 AND status IN ('pending','open','partially_filled')`, orderID)
	if reserved > 0 {
		bizID := fmt.Sprintf("final-%d", orderID)
		if err := s.ledger.Post(context.Background(), "order_unfreeze", bizID, []ledger.Move{
			{Account: ledger.User(userID, currency, "frozen"), Delta: -reserved},
			{Account: ledger.User(userID, currency, "available"), Delta: +reserved},
		}); err != nil && !errors.Is(err, ledger.ErrAlreadyProcessed) {
			s.log.Error("compensating unfreeze failed", "order", orderID, "err", err)
		}
	}
}

// ---- 撤单 ----

func (s *server) handleCancel(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUserID(r)
	if err != nil {
		fail(w, 20005, err.Error())
		return
	}
	orderID, err := strconvParseInt(r.PathValue("orderId"))
	if err != nil {
		fail(w, 10003, "invalid orderId")
		return
	}
	var symbol, side, status string
	var reserved int64
	err = s.pool.QueryRow(r.Context(), `
		SELECT symbol, side, status, reserved FROM orders
		WHERE order_id=$1 AND user_id=$2`, orderID, userID).Scan(&symbol, &side, &status, &reserved)
	if errors.Is(err, pgx.ErrNoRows) {
		fail(w, 50010, "order not found")
		return
	}
	if err != nil {
		fail(w, 10001, "internal error")
		return
	}
	if status != "open" && status != "partially_filled" && status != "pending" {
		fail(w, 50011, "order not cancelable, status="+status)
		return
	}
	payload, err := events.CancelEnvelope(events.CancelCommand{OrderID: uint64(orderID), UserID: uint64(userID)})
	if err != nil {
		fail(w, 10001, "internal error")
		return
	}
	if err := produceSync(r.Context(), s.prod, s.inputTopic(symbol), symbol, payload); err != nil {
		fail(w, 10002, "cancel dispatch failed")
		return
	}
	writeData(w, map[string]any{"orderId": orderID, "status": status})
}

// ---- 查询 ----

type orderView struct {
	OrderID       int64  `json:"orderId"`
	ClientOrderID string `json:"clientOrderId"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`
	Type          string `json:"type"`
	Price         string `json:"price"`
	Qty           string `json:"qty"`
	FilledQty     string `json:"filledQty"`
	Status        string `json:"status"`
	CreatedAt     string `json:"createdAt"`
}

func scanOrder(row pgx.Row) (*orderView, error) {
	var o orderView
	var price *int64
	var qty, filled int64
	var createdAt time.Time
	if err := row.Scan(&o.OrderID, &o.ClientOrderID, &o.Symbol, &o.Side, &o.Type, &price, &qty, &filled, &o.Status, &createdAt); err != nil {
		return nil, err
	}
	o.Qty = fixed.Format(uint64(qty))
	o.FilledQty = fixed.Format(uint64(filled))
	if price != nil {
		o.Price = fixed.Format(uint64(*price))
	}
	o.CreatedAt = createdAt.UTC().Format("2006-01-02T15:04:05.000Z")
	return &o, nil
}

const orderCols = `order_id, client_order_id, symbol, side, order_type, price, qty, filled_qty, status, created_at`

func (s *server) handleOpenOrders(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUserID(r)
	if err != nil {
		fail(w, 20005, err.Error())
		return
	}
	symbol := r.URL.Query().Get("symbol")
	args := []any{userID}
	q := `SELECT ` + orderCols + ` FROM orders WHERE user_id=$1 AND status IN ('pending','open','partially_filled')`
	if symbol != "" {
		q += ` AND symbol=$2`
		args = append(args, symbol)
	}
	q += ` ORDER BY order_id DESC LIMIT 100`
	rows, err := s.pool.Query(r.Context(), q, args...)
	if err != nil {
		fail(w, 10001, "internal error")
		return
	}
	defer rows.Close()
	var out []*orderView
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			fail(w, 10001, "internal error")
			return
		}
		out = append(out, o)
	}
	writeData(w, out)
}

func (s *server) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUserID(r)
	if err != nil {
		fail(w, 20005, err.Error())
		return
	}
	orderID, err := strconvParseInt(r.PathValue("orderId"))
	if err != nil {
		fail(w, 10003, "invalid orderId")
		return
	}
	o, err := scanOrder(s.pool.QueryRow(r.Context(),
		`SELECT `+orderCols+` FROM orders WHERE order_id=$1 AND user_id=$2`, orderID, userID))
	if errors.Is(err, pgx.ErrNoRows) {
		fail(w, 50010, "order not found")
		return
	}
	if err != nil {
		fail(w, 10001, "internal error")
		return
	}
	writeData(w, o)
}

func (s *server) handleBalances(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUserID(r)
	if err != nil {
		fail(w, 20005, err.Error())
		return
	}
	balances, err := s.ledger.UserBalances(r.Context(), userID)
	if err != nil {
		fail(w, 10001, "internal error")
		return
	}
	writeData(w, balances)
}

// ---- 状态同步消费 ----

func (s *server) runStatusSync(ctx context.Context) {
	topics := make([]string, 0, len(s.cfg.symbols))
	for _, sym := range s.cfg.symbols {
		topics = append(topics, s.eventsTopic(sym))
	}
	cli, err := kgo.NewClient(
		kgo.SeedBrokers(s.cfg.brokers),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumerGroup("order-status-sync"),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.SessionTimeout(6*time.Second),
		kgo.HeartbeatInterval(2*time.Second),
	)
	if err != nil {
		s.log.Error("status sync consumer", "err", err)
		return
	}
	defer cli.Close()
	s.log.Info("status sync consumer started", "topics", topics)

	for {
		select {
		case <-ctx.Done():
			s.log.Info("status sync stopped")
			cli.Close()
			return
		default:
		}
		fetches := cli.PollRecords(ctx, 100)
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			s.log.Error("consume error", "topic", t, "err", err)
		})
		fetches.EachRecord(func(rec *kgo.Record) {
			s.applyOrderUpdate(ctx, rec.Value)
			cli.CommitRecords(ctx, rec)
		})
	}
}

// applyOrderUpdate 同步订单状态(资金不动 —— 全部由 clearing 负责)
func (s *server) applyOrderUpdate(ctx context.Context, payload []byte) {
	ev, err := events.Decode(payload)
	if err != nil {
		s.log.Error("bad event payload", "err", err)
		return
	}
	if ev.Kind != "order_update" {
		return
	}
	u, err := ev.OrderUpdate()
	if err != nil {
		s.log.Error("bad order_update", "err", err)
		return
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE orders SET filled_qty=$1, status=$2, updated_at=now()
		WHERE order_id=$3 AND status IN ('pending','open','partially_filled')`,
		int64(u.FilledQty), u.Status, int64(u.OrderID))
	if err != nil {
		s.log.Error("sync order status failed", "order", u.OrderID, "err", err)
		return
	}
	if tag.RowsAffected() > 0 {
		s.log.Info("order status synced", "order", u.OrderID, "status", u.Status, "seq", ev.Seq)
	}
}
