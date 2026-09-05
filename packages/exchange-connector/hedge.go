// Package connector 外部交易所接入(ADR-006/007)。
// 当前落地对冲通道接口;公共行情仍由 price-index 内适配器承担,后续再迁入。
package connector

import "context"

// HedgeOrder 在外部所对冲一笔(数量定点 ×1e8)。
type HedgeOrder struct {
	Symbol string // 内部符号 BTC-USDT
	Side   string // bid=外部买入 base, ask=外部卖出 base
	Qty    uint64
	Reason string
}

type HedgeAck struct {
	Venue string
	TxID  string
}

// Hedger 对冲通道。Healthy=false 时做市必须停报价(ADR-006)。
type Hedger interface {
	Name() string
	Healthy() bool
	Hedge(ctx context.Context, req HedgeOrder) (HedgeAck, error)
}
