package connector

import "testing"

func TestMockHedgeRecords(t *testing.T) {
	m := &Mock{}
	ack, err := m.Hedge(nil, HedgeOrder{Symbol: "BTC-USDT", Side: "ask", Qty: 1})
	if err != nil || ack.Venue != "mock" || ack.TxID == "" {
		t.Fatalf("ack=%v err=%v", ack, err)
	}
	last, ok := m.Last()
	if !ok || last.Side != "ask" {
		t.Fatalf("last=%v ok=%v", last, ok)
	}
	m.SetDown(true)
	if m.Healthy() {
		t.Fatal("expected down")
	}
	if _, err := m.Hedge(nil, HedgeOrder{}); err == nil {
		t.Fatal("expected error when down")
	}
}
