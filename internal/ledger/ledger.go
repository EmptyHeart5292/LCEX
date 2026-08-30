// Package ledger 是资金变动的唯一入口:所有余额变动必须经由
// journal(业务凭证)+ ledger_entries(分录)落库,借贷必平、事务内完成、
// 以 journals(biz_type, biz_id) 唯一约束保证幂等。
//
// 方向语义(见 db/README.md):
//   - 资产类(system):debit 为正
//   - 负债/收入/权益类(user/market_maker/fee/equity):credit 为正
// 调用方只需给出每个账户的带符号余额变化(Delta),方向自动推导;
// 一条 journal 的全部 Delta 之和必须为 0(借贷平衡的代数表达)。
package ledger

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/EmptyHeart5292/lcex/internal/fixed"
)

var (
	// ErrAlreadyProcessed 同一 (biz_type, biz_id) 已入账,消费端应跳过
	ErrAlreadyProcessed = errors.New("ledger: journal already processed")
	// ErrUnbalanced journal 存在币种的 Delta 之和非 0(借贷不平,编程错误)
	ErrUnbalanced = errors.New("ledger: unbalanced moves")
	// ErrInsufficientBalance 余额不足
	ErrInsufficientBalance = errors.New("ledger: insufficient balance")
)

// AccountRef 定位一个余额节点
type AccountRef struct {
	OwnerID   int64
	OwnerType string // user | market_maker | system | fee | equity
	Currency  string
	Type      string // available | frozen
}

// Move 一个账户的带符号余额变化
type Move struct {
	Account AccountRef
	Delta   int64
}

func User(owner int64, currency, typ string) AccountRef {
	return AccountRef{OwnerID: owner, OwnerType: "user", Currency: currency, Type: typ}
}
func System(owner int64, currency, typ string) AccountRef {
	return AccountRef{OwnerID: owner, OwnerType: "system", Currency: currency, Type: typ}
}
func Fee(owner int64, currency, typ string) AccountRef {
	return AccountRef{OwnerID: owner, OwnerType: "fee", Currency: currency, Type: typ}
}

type Ledger struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Ledger { return &Ledger{pool: pool} }

// WithTx 在一个数据库事务里执行 fn(PostTx 必须传入该 tx,保证入账与业务写同事务)
func (l *Ledger) WithTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func directionOf(ownerType string, delta int64) (direction string, amount int64) {
	positiveAsDebit := ownerType == "system" // 资产类借方为正
	if (positiveAsDebit && delta >= 0) || (!positiveAsDebit && delta < 0) {
		return "debit", abs(delta)
	}
	return "credit", abs(delta)
}

func abs(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

// PostTx 入账一条 journal(幂等)。moves 按币种分组后,每组 Delta 之和必须为 0
// (一笔成交天然跨 base/quote 两币种,逐币种平衡见 db/README.md 不变式)。
func (l *Ledger) PostTx(ctx context.Context, tx pgx.Tx, bizType, bizID string, moves []Move) error {
	if len(moves) < 2 {
		return errors.New("ledger: journal needs at least 2 entries")
	}
	sums := map[string]int64{}
	for _, m := range moves {
		sums[m.Account.Currency] += m.Delta
	}
	for cur, sum := range sums {
		if sum != 0 {
			return fmt.Errorf("%w: %s sum(delta) = %d", ErrUnbalanced, cur, sum)
		}
	}

	tag, err := tx.Exec(ctx,
		`INSERT INTO journals (biz_type, biz_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		bizType, bizID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrAlreadyProcessed
	}
	var journalID int64
	if err := tx.QueryRow(ctx,
		`SELECT id FROM journals WHERE biz_type = $1 AND biz_id = $2`, bizType, bizID).Scan(&journalID); err != nil {
		return err
	}

	for _, m := range moves {
		ref := m.Account
		var accountID int64
		if err := tx.QueryRow(ctx, `
			INSERT INTO accounts (owner_id, owner_type, currency, type)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (owner_id, owner_type, currency, type)
			DO UPDATE SET updated_at = now()
			RETURNING id`,
			ref.OwnerID, ref.OwnerType, ref.Currency, ref.Type).Scan(&accountID); err != nil {
			return err
		}
		var balance int64
		if err := tx.QueryRow(ctx, `
			UPDATE accounts SET balance = balance + $1, updated_at = now()
			WHERE id = $2 RETURNING balance`,
			m.Delta, accountID).Scan(&balance); err != nil {
			// DB 层 CHECK(balance>=0) 先于应用层拦截,翻译为友好错误
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23514" &&
				strings.Contains(pgErr.ConstraintName, "balance_check") {
				return fmt.Errorf("%w: account %v/%v/%v", ErrInsufficientBalance, ref.OwnerID, ref.Currency, ref.Type)
			}
			return err
		}
		if balance < 0 {
			return fmt.Errorf("%w: account %v/%v/%v", ErrInsufficientBalance, ref.OwnerID, ref.Currency, ref.Type)
		}
		direction, amount := directionOf(ref.OwnerType, m.Delta)
		if _, err := tx.Exec(ctx, `
			INSERT INTO ledger_entries (journal_id, account_id, direction, amount, currency, balance_after)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			journalID, accountID, direction, amount, ref.Currency, balance); err != nil {
			return err
		}
	}
	return nil
}

// Post 无业务表伴生写入时的便捷入口
func (l *Ledger) Post(ctx context.Context, bizType, bizID string, moves []Move) error {
	return l.WithTx(ctx, func(tx pgx.Tx) error { return l.PostTx(ctx, tx, bizType, bizID, moves) })
}

type Balance struct {
	Currency  string `json:"currency"`
	Available string `json:"available"`
	Frozen    string `json:"frozen"`
}

// UserBalances 汇总某用户的 available/frozen(十进制字符串,定点 ×1e8)
func (l *Ledger) UserBalances(ctx context.Context, ownerID int64) ([]*Balance, error) {
	rows, err := l.pool.Query(ctx, `
		SELECT currency, type, balance FROM accounts
		WHERE owner_id = $1 AND owner_type = 'user'
		ORDER BY currency, type`, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*Balance
	merged := map[string]*Balance{}
	for rows.Next() {
		var currency, typ string
		var balance int64
		if err := rows.Scan(&currency, &typ, &balance); err != nil {
			return nil, err
		}
		b, ok := merged[currency]
		if !ok {
			b = &Balance{Currency: currency}
			merged[currency] = b
			out = append(out, b)
		}
		switch typ {
		case "available":
			b.Available = fixed.Format(uint64(balance))
		case "frozen":
			b.Frozen = fixed.Format(uint64(balance))
		}
	}
	return out, rows.Err()
}
