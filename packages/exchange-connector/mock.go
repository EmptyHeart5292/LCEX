package connector

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// Mock 记录对冲意图,不打真实交易所。Fail 时 Healthy=false。
type Mock struct {
	mu     sync.Mutex
	seq    atomic.Uint64
	Orders []HedgeOrder
	down   atomic.Bool
}

func (m *Mock) Name() string { return "mock" }

func (m *Mock) Healthy() bool { return !m.down.Load() }

func (m *Mock) SetDown(v bool) { m.down.Store(v) }

func (m *Mock) Hedge(_ context.Context, req HedgeOrder) (HedgeAck, error) {
	if !m.Healthy() {
		return HedgeAck{}, fmt.Errorf("hedge venue down")
	}
	m.mu.Lock()
	m.Orders = append(m.Orders, req)
	n := m.seq.Add(1)
	m.mu.Unlock()
	return HedgeAck{Venue: "mock", TxID: fmt.Sprintf("mock-hedge-%d", n)}, nil
}

func (m *Mock) Last() (HedgeOrder, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.Orders) == 0 {
		return HedgeOrder{}, false
	}
	return m.Orders[len(m.Orders)-1], true
}
