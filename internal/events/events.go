// Package events 是 matching/crates/protocol JSON 格式的 Go 镜像,
// 字段名与 serde 序列化严格一致(扁平信封,判别字段在外层)。
package events

import "encoding/json"

type Event struct {
	Seq  uint64          `json:"seq"`
	Kind string          `json:"kind"` // trade | order_update
	Data json.RawMessage `json:"data"`
}

type Trade struct {
	TradeID       uint64 `json:"trade_id"`
	MakerOrderID  uint64 `json:"maker_order_id"`
	TakerOrderID  uint64 `json:"taker_order_id"`
	MakerUserID   uint64 `json:"maker_user_id"`
	TakerUserID   uint64 `json:"taker_user_id"`
	Side          string `json:"side"` // taker 方向: bid | ask
	Price         uint64 `json:"price"`
	Qty           uint64 `json:"qty"`
}

type OrderUpdate struct {
	OrderID   uint64  `json:"order_id"`
	UserID    uint64  `json:"user_id"`
	Side      string  `json:"side"`
	Price     *uint64 `json:"price"`
	Status    string  `json:"status"` // open | partially_filled | filled | canceled | rejected
	FilledQty uint64  `json:"filled_qty"`
	Qty       uint64  `json:"qty"`
}

type PlaceCommand struct {
	OrderID  uint64  `json:"order_id"`
	UserID   uint64  `json:"user_id"`
	Side     string  `json:"side"`
	OrderType string `json:"order_type"` // limit | market
	Tif      string  `json:"tif"`        // gtc | ioc | fok
	Stp      string  `json:"stp"`        // none | cancel_taker
	PostOnly bool    `json:"post_only"`
	Price    *uint64 `json:"price"`
	Qty      uint64  `json:"qty"`
}

type CancelCommand struct {
	OrderID uint64 `json:"order_id"`
	UserID  uint64 `json:"user_id"`
}

// Decode 解析事件;kind 分发由调用方按 Kind 字符串处理
func Decode(payload []byte) (*Event, error) {
	var ev Event
	if err := json.Unmarshal(payload, &ev); err != nil {
		return nil, err
	}
	return &ev, nil
}

// ---- 指令信封(与 Rust Command serde 格式一一对应,勿在别处手工拼装)----

type placeEnvelope struct {
	Type string `json:"type"`
	PlaceCommand
}

type cancelEnvelope struct {
	Type string `json:"type"`
	CancelCommand
}

func PlaceEnvelope(p PlaceCommand) ([]byte, error) { return json.Marshal(placeEnvelope{"place", p}) }

func CancelEnvelope(c CancelCommand) ([]byte, error) { return json.Marshal(cancelEnvelope{"cancel", c}) }

func (e *Event) Trade() (*Trade, error) {
	var t Trade
	if err := json.Unmarshal(e.Data, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

func (e *Event) OrderUpdate() (*OrderUpdate, error) {
	var u OrderUpdate
	if err := json.Unmarshal(e.Data, &u); err != nil {
		return nil, err
	}
	return &u, nil
}
