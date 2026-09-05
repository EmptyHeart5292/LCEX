package main

import (
	"errors"

	"github.com/EmptyHeart5292/lcex/internal/fixed"
)

type Quote struct {
	Side  string
	Price uint64
	Qty   string
}

func scaleBps(px uint64, bps int) (uint64, error) {
	const base = 10000
	if bps >= 0 {
		return fixed.MulDiv(px, uint64(base+bps), base)
	}
	n := uint64(-bps)
	if n >= base {
		return 0, errors.New("bps too large")
	}
	return fixed.MulDiv(px, base-n, base)
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

// inventorySkewBps:多 base(相对报价资产)→ 负偏移(整格下移,更积极卖)。
func inventorySkewBps(baseQty, quoteQty, index uint64, maxSkew int) int {
	if maxSkew <= 0 || index == 0 {
		return 0
	}
	baseVal, err := fixed.MulDiv(baseQty, index, fixed.Scale)
	if err != nil {
		return 0
	}
	total := baseVal + quoteQty
	if total == 0 {
		return 0
	}
	num := int64(baseVal) - int64(quoteQty)
	s := int(-num * int64(maxSkew) / int64(total))
	if s > maxSkew {
		s = maxSkew
	}
	if s < -maxSkew {
		s = -maxSkew
	}
	return s
}

// buildGrid 买卖各 levels 档;第 i 档偏移 halfSpread*i + skew。超过 maxOffset 的档丢弃。
func buildGrid(index uint64, levels, halfSpread, skew, maxOffset int, qty string) []Quote {
	if levels < 1 {
		levels = 1
	}
	var out []Quote
	for i := 1; i <= levels; i++ {
		bidBps := skew - halfSpread*i
		askBps := skew + halfSpread*i
		if maxOffset > 0 && (absInt(bidBps) > maxOffset || absInt(askBps) > maxOffset) {
			continue
		}
		bid, err1 := scaleBps(index, bidBps)
		ask, err2 := scaleBps(index, askBps)
		if err1 != nil || err2 != nil || bid == 0 || ask == 0 || bid >= ask {
			continue
		}
		out = append(out, Quote{Side: "bid", Price: bid, Qty: qty})
		out = append(out, Quote{Side: "ask", Price: ask, Qty: qty})
	}
	return out
}

func quoteKey(side, price, qty string) string {
	return side + "|" + price + "|" + qty
}
