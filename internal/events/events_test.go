package events

import (
	"encoding/json"
	"testing"
)

// 信封格式与 matching/crates/protocol 的 serde 输出逐字段一致(契约锁死)
func TestEnvelopeShape(t *testing.T) {
	pc, err := PlaceEnvelope(PlaceCommand{
		OrderID: 42, UserID: 7, Side: "bid", OrderType: "limit",
		Tif: "gtc", Stp: "none", PostOnly: false,
		Price: u64ptr(5000000000000), Qty: 100000000,
	})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(pc, &m); err != nil {
		t.Fatal(err)
	}
	if m["type"] != "place" || m["order_id"] != float64(42) || m["side"] != "bid" ||
		m["order_type"] != "limit" || m["tif"] != "gtc" || m["stp"] != "none" ||
		m["post_only"] != false || m["qty"] != float64(100000000) {
		t.Fatalf("place envelope shape mismatch: %s", pc)
	}

	cc, err := CancelEnvelope(CancelCommand{OrderID: 5, UserID: 102})
	if err != nil {
		t.Fatal(err)
	}
	var cm map[string]any
	if err := json.Unmarshal(cc, &cm); err != nil {
		t.Fatal(err)
	}
	if cm["type"] != "cancel" || cm["order_id"] != float64(5) || cm["user_id"] != float64(102) {
		t.Fatalf("cancel envelope shape mismatch: %s", cc)
	}
}

func u64ptr(v uint64) *uint64 { return &v }
