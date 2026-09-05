package main

import (
	"testing"

	"github.com/EmptyHeart5292/lcex/internal/fixed"
)

func TestBuildGridThreeLevels(t *testing.T) {
	idx := uint64(50_000) * fixed.Scale
	g := buildGrid(idx, 3, 10, 0, 100, "0.05")
	if len(g) != 6 {
		t.Fatalf("want 6 quotes, got %d", len(g))
	}
	var bids, asks []uint64
	for _, q := range g {
		if q.Side == "bid" {
			bids = append(bids, q.Price)
		} else {
			asks = append(asks, q.Price)
		}
	}
	if !(bids[0] > bids[1] && bids[1] > bids[2]) {
		t.Fatalf("bids should descend: %v", bids)
	}
	if !(asks[0] < asks[1] && asks[1] < asks[2]) {
		t.Fatalf("asks should ascend: %v", asks)
	}
	if bids[0] >= idx || asks[0] <= idx {
		t.Fatalf("grid should straddle index idx=%d bid0=%d ask0=%d", idx, bids[0], asks[0])
	}
}

func TestInventorySkewShiftsDownWhenLongBase(t *testing.T) {
	idx := uint64(50_000) * fixed.Scale
	base := uint64(50) * fixed.Scale
	quote := uint64(10_000) * fixed.Scale
	sk := inventorySkewBps(base, quote, idx, 30)
	if sk >= 0 {
		t.Fatalf("long base should skew down, got %d", sk)
	}
	g0 := buildGrid(idx, 1, 10, 0, 100, "0.05")
	g1 := buildGrid(idx, 1, 10, sk, 100, "0.05")
	var bid0, bid1 uint64
	for _, q := range g0 {
		if q.Side == "bid" {
			bid0 = q.Price
		}
	}
	for _, q := range g1 {
		if q.Side == "bid" {
			bid1 = q.Price
		}
	}
	if bid1 >= bid0 {
		t.Fatalf("skewed bid should be lower: %d vs %d", bid1, bid0)
	}
}

func TestMaxOffsetDropsOuterLevels(t *testing.T) {
	idx := uint64(50_000) * fixed.Scale
	g := buildGrid(idx, 5, 10, 0, 25, "0.05") // level 3 = 30bps > 25
	if len(g) != 4 {                          // levels 1-2 only
		t.Fatalf("want 4 quotes, got %d", len(g))
	}
}
