// clearing 服务:资金变动的清算执行者。
//
// 按分区顺序消费 cex.events.{symbol}:
//   - trade        → 复式记账结算(买卖双方划转 + 手续费),同事务扣减 orders.reserved
//   - order_update → 终态(filled/canceled/rejected)时解冻剩余冻结额
//
// 同一分区事件严格有序,保证"先结算后解冻";journal 幂等键兜底重复消费。
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/EmptyHeart5292/lcex/internal/events"
	"github.com/EmptyHeart5292/lcex/internal/fixed"
	"github.com/EmptyHeart5292/lcex/internal/ledger"
	"github.com/EmptyHeart5292/lcex/internal/markets"
)

// ErrPoison 业务不一致(订单缺失/方向矛盾):跳过并提交,不阻塞分区;
// 与之相对,账本/DB 错误必须退出重试(at-least-once)。
var ErrPoison = errors.New("clearing: poison message")

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type service struct {
	pool    *pgxpool.Pool
	ledger  *ledger.Ledger
	markets *markets.Config
	prefix  string
	log     *slog.Logger
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	// SIGTERM/SIGINT → ctx 取消 → 消费循环退出 → cli.Close() 离开消费组
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn := envOr("DATABASE_URL", "postgres://cex:cex_dev@localhost:5432/cex")
	brokers := envOr("CEX_KAFKA_BROKERS", "localhost:9092")
	prefix := envOr("CEX_TOPIC_PREFIX", "cex")
	symbols := strings.Split(envOr("CEX_SYMBOLS", "BTC-USDT,ETH-USDT,SOL-USDT"), ",")
	marketsFile := envOr("CEX_MARKETS_FILE", "packages/api-spec/markets.yaml")

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		slog.Error("connect database", "err", err)
		os.Exit(1)
	}
	mcfg, err := markets.Load(marketsFile)
	if err != nil {
		slog.Error("load markets", "err", err)
		os.Exit(1)
	}

	s := &service{pool: pool, ledger: ledger.New(pool), markets: mcfg, prefix: prefix, log: slog.Default()}

	var topics []string
	for _, sym := range symbols {
		topics = append(topics, fmt.Sprintf("%s.events.%s", prefix, strings.ToLower(strings.TrimSpace(sym))))
	}
	cli, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumerGroup("clearing"),
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
	defer cli.Close()
	s.log.Info("clearing started", "topics", topics)

	for {
		select {
		case <-ctx.Done():
			s.log.Info("clearing stopped")
			cli.Close()
			return
		default:
		}
		fetches := cli.PollRecords(ctx, 100)
		if fetches.IsClientClosed() {
			s.log.Info("clearing stopped")
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			s.log.Error("consume error", "topic", t, "err", err)
		})
		fetches.EachRecord(func(rec *kgo.Record) {
			s.handleRecord(ctx, rec)
			cli.CommitRecords(ctx, rec)
		})
	}
}

func (s *service) handleRecord(ctx context.Context, rec *kgo.Record) {
	ev, err := events.Decode(rec.Value)
	if err != nil {
		s.log.Error("bad payload, skipped", "topic", rec.Topic, "err", err)
		return
	}
	switch ev.Kind {
	case "trade":
		t, err := ev.Trade()
		if err != nil {
			s.log.Error("bad trade event", "err", err)
			return
		}
		if err := s.settleTrade(ctx, strings.TrimPrefix(rec.Topic, s.prefix+".events."), t); err != nil {
			if errors.Is(err, ErrPoison) {
				s.log.Error("poison trade event skipped", "trade", t.TradeID, "err", err)
				return
			}
			// 不提交 offset 会让分区卡死;记账有 journal 幂等,此条等待重试 —— 直接 fatal 交给重启
			s.log.Error("settle failed, exiting for retry", "trade", t.TradeID, "err", err)
			os.Exit(1)
		}
	case "order_update":
		u, err := ev.OrderUpdate()
		if err != nil {
			s.log.Error("bad order_update event", "err", err)
			return
		}
		if u.Status == "filled" || u.Status == "canceled" || u.Status == "rejected" {
			if err := s.unfreezeReserved(ctx, u.OrderID); err != nil {
				s.log.Error("final unfreeze failed, exiting for retry", "order", u.OrderID, "err", err)
				os.Exit(1)
			}
		}
	}
}

type orderRow struct {
	UserID   int64
	Side     string
	Reserved int64
}

func (s *service) loadOrder(ctx context.Context, tx pgx.Tx, orderID uint64) (*orderRow, error) {
	row := tx.QueryRow(ctx, `SELECT user_id, side, reserved FROM orders WHERE order_id = $1`, int64(orderID))
	var o orderRow
	if err := row.Scan(&o.UserID, &o.Side, &o.Reserved); err != nil {
		return nil, fmt.Errorf("%w: load order %d: %v", ErrPoison, orderID, err)
	}
	return &o, nil
}

// settleTrade 结算一笔成交;幂等键 trade-{id}
func (s *service) settleTrade(ctx context.Context, symbol string, t *events.Trade) error {
	m, err := s.markets.Get(symbol)
	if err != nil {
		return fmt.Errorf("%w: settle: %v", ErrPoison, err)
	}
	qa, err := fixed.MulDiv(t.Price, t.Qty, fixed.Scale)
	if err != nil {
		return fmt.Errorf("settle quote amount: %w", err)
	}
	takerFee, err := fixed.MulDiv(qa, m.TakerRate, fixed.Scale)
	if err != nil {
		return err
	}
	makerFee, err := fixed.MulDiv(qa, m.MakerRate, fixed.Scale)
	if err != nil {
		return err
	}

	var moves []ledger.Move
	var takerConsume, makerConsume int64 // reserved 扣减额
	if t.Side == "bid" {
		// taker 买:付 (qa + takerFee) quote,收 qty base;maker 卖:付 qty base,收 (qa - makerFee) quote
		moves = []ledger.Move{
			{Account: ledger.User(int64(t.TakerUserID), m.Quote, "frozen"), Delta: -int64(qa + takerFee)},
			{Account: ledger.User(int64(t.TakerUserID), m.Base, "available"), Delta: +int64(t.Qty)},
			{Account: ledger.User(int64(t.MakerUserID), m.Base, "frozen"), Delta: -int64(t.Qty)},
			{Account: ledger.User(int64(t.MakerUserID), m.Quote, "available"), Delta: +int64(qa - makerFee)},
			{Account: ledger.Fee(0, m.Quote, "available"), Delta: +int64(takerFee + makerFee)},
		}
		takerConsume, makerConsume = int64(qa+takerFee), int64(t.Qty)
	} else {
		// taker 卖:付 qty base,收 (qa - takerFee) quote;maker 买:付 (qa + makerFee) quote,收 qty base
		moves = []ledger.Move{
			{Account: ledger.User(int64(t.TakerUserID), m.Base, "frozen"), Delta: -int64(t.Qty)},
			{Account: ledger.User(int64(t.TakerUserID), m.Quote, "available"), Delta: +int64(qa - takerFee)},
			{Account: ledger.User(int64(t.MakerUserID), m.Quote, "frozen"), Delta: -int64(qa + makerFee)},
			{Account: ledger.User(int64(t.MakerUserID), m.Base, "available"), Delta: +int64(t.Qty)},
			{Account: ledger.Fee(0, m.Quote, "available"), Delta: +int64(takerFee + makerFee)},
		}
		takerConsume, makerConsume = int64(t.Qty), int64(qa+makerFee)
	}

	bizID := fmt.Sprintf("trade-%d", t.TradeID)
	err = s.ledger.WithTx(ctx, func(tx pgx.Tx) error {
		maker, err := s.loadOrder(ctx, tx, t.MakerOrderID)
		if err != nil {
			return err
		}
		taker, err := s.loadOrder(ctx, tx, t.TakerOrderID)
		if err != nil {
			return err
		}
		// 一致性校验:taker 行方向应与成交事件一致,maker 行与之相反
		if taker.Side != t.Side {
			return fmt.Errorf("%w: taker side mismatch: event=%s order=%s", ErrPoison, t.Side, taker.Side)
		}
		if maker.Side == taker.Side {
			return fmt.Errorf("%w: maker/taker sides identical: %s", ErrPoison, maker.Side)
		}
		if err := s.ledger.PostTx(ctx, tx, "trade", bizID, moves); err != nil {
			return err
		}
		// reserved 扣减与入账同事务;guard 防止负值(数据 bug 会被拦下)
		if _, err := tx.Exec(ctx,
			`UPDATE orders SET reserved = reserved - $1 WHERE order_id = $2 AND reserved >= $1`,
			takerConsume, int64(t.TakerOrderID)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE orders SET reserved = reserved - $1 WHERE order_id = $2 AND reserved >= $1`,
			makerConsume, int64(t.MakerOrderID)); err != nil {
			return err
		}
		return nil
	})
	if errors.Is(err, ledger.ErrAlreadyProcessed) {
		s.log.Info("trade already settled (idempotent skip)", "trade", t.TradeID)
		return nil
	}
	if err != nil {
		return err
	}
	s.log.Info("trade settled", "trade", t.TradeID, "qty", fixed.Format(t.Qty), "price", fixed.Format(t.Price))
	return nil
}

// unfreezeReserved 终态解冻:把订单剩余冻结额(filled 的费率尾差 / canceled 的剩余量)退回可用
func (s *service) unfreezeReserved(ctx context.Context, orderID uint64) error {
	var userID int64
	var side, symbol string
	var reserved int64
	err := s.pool.QueryRow(ctx,
		`SELECT user_id, side, symbol, reserved FROM orders WHERE order_id = $1`,
		int64(orderID)).Scan(&userID, &side, &symbol, &reserved)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("unfreeze: order %d not found", orderID)
	}
	if err != nil {
		return err
	}
	if reserved == 0 {
		return nil
	}
	m, err := s.markets.Get(symbol)
	if err != nil {
		return err
	}
	currency := m.Quote
	if side == "ask" {
		currency = m.Base
	}
	bizID := fmt.Sprintf("final-%d", orderID)
	err = s.ledger.Post(ctx, "order_unfreeze", bizID, []ledger.Move{
		{Account: ledger.User(userID, currency, "frozen"), Delta: -reserved},
		{Account: ledger.User(userID, currency, "available"), Delta: +reserved},
	})
	if errors.Is(err, ledger.ErrAlreadyProcessed) {
		return nil
	}
	if err != nil {
		return err
	}
	s.log.Info("final unfreeze", "order", orderID, "amount", fixed.Format(uint64(reserved)), "currency", currency)
	return nil
}
