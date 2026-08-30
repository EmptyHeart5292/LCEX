package main

import (
	"testing"
	"time"
)

const (
	p50000 = 50_000 * fixedScale
	p50001 = 50_001 * fixedScale
	p60000 = 60_000 * fixedScale
)

// 5% 偏离阈值(定点分数)
const dev5pct = 5_000_000

const fixedScale = uint64(100_000_000)

func TestComputeIndexBasic(t *testing.T) {
	now := time.Now()
	pxs := map[string]sourcePrice{
		"binance": {Price: p50000, TS: now},
		"okx":     {Price: p50001, TS: now},
		"bybit":   {Price: p60000, TS: now},
	}
	res := computeIndex(pxs, now, 10*time.Second, dev5pct)
	if !res.OK {
		t.Fatal("应有可用源")
	}
	if got := res.Index; got != (p50000+p50001)/2 {
		t.Fatalf("index = %d, want %d", got, (p50000+p50001)/2)
	}
	if len(res.Usable) != 2 || res.Rejected["bybit"] != "deviation" {
		t.Fatalf("bybit 应因偏离被剔除: %+v", res)
	}
}

func TestComputeIndexStaleRejected(t *testing.T) {
	now := time.Now()
	pxs := map[string]sourcePrice{
		"binance": {Price: p50000, TS: now},
		"mexc":    {Price: p50000, TS: now.Add(-11 * time.Second)},
	}
	res := computeIndex(pxs, now, 10*time.Second, dev5pct)
	if res.Rejected["mexc"] != "stale" {
		t.Fatalf("mexc 应因过期被剔除: %+v", res)
	}
	if res.Index != p50000 || len(res.Usable) != 1 {
		t.Fatalf("index 应只由 binance 构成: %+v", res)
	}
}

func TestComputeIndexAllStale(t *testing.T) {
	now := time.Now()
	pxs := map[string]sourcePrice{"binance": {Price: p50000, TS: now.Add(-time.Minute)}}
	res := computeIndex(pxs, now, 10*time.Second, dev5pct)
	if res.OK {
		t.Fatal("全部过期时 OK 应为 false")
	}
}

func TestComputeIndexAllAgree(t *testing.T) {
	now := time.Now()
	pxs := map[string]sourcePrice{}
	for _, n := range []string{"binance", "okx", "bybit", "mexc", "bitget"} {
		pxs[n] = sourcePrice{Price: p50000, TS: now}
	}
	res := computeIndex(pxs, now, 10*time.Second, dev5pct)
	if !res.OK || res.Index != p50000 || len(res.Usable) != 5 {
		t.Fatalf("五源一致: %+v", res)
	}
}
