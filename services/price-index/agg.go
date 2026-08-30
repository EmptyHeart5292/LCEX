package main

// 指数价聚合规则(ADR-006):
//   1. 过期源剔除(最后心跳距今超过 stale);
//   2. 偏离中位数超过 maxDev(定点分数,如 5% = 5e6)的异常源剔除;
//   3. 剩余可用源等权平均;全部不可用时指数标记为过期(返回 ok=false)。

import (
	"sort"
	"time"

	"github.com/EmptyHeart5292/lcex/internal/fixed"
)

type sourcePrice struct {
	Price uint64 // 定点 ×1e8
	TS    time.Time
}

type indexResult struct {
	Index    uint64            // 定点;ok=false 时为最后已知值
	OK       bool              // 是否有可用源
	Usable   []string          // 参与计算的源
	Rejected map[string]string // 源 → 剔除原因("stale" / "deviation")
}

// computeIndex 纯函数,便于单测。
func computeIndex(pxs map[string]sourcePrice, now time.Time, stale time.Duration, maxDev uint64) indexResult {
	res := indexResult{Rejected: map[string]string{}}
	fresh := map[string]uint64{}
	for name, sp := range pxs {
		if now.Sub(sp.TS) > stale || sp.Price == 0 {
			res.Rejected[name] = "stale"
			continue
		}
		fresh[name] = sp.Price
	}
	if len(fresh) == 0 {
		return res
	}

	med := medianOf(fresh)
	for name, p := range fresh {
		dev, err := fixed.MulDiv(abs64(p, med), fixed.Scale, med)
		if err != nil || dev > maxDev {
			res.Rejected[name] = "deviation"
			continue
		}
		res.Usable = append(res.Usable, name)
	}
	if len(res.Usable) == 0 {
		return res
	}
	sort.Strings(res.Usable)

	var sum uint64
	for _, name := range res.Usable {
		sum += fresh[name]
	}
	res.Index = sum / uint64(len(res.Usable))
	res.OK = true
	return res
}

// medianOf 下中位(偶数个取中间偏小者),确定性要求
func medianOf(pxs map[string]uint64) uint64 {
	vals := make([]uint64, 0, len(pxs))
	for _, p := range pxs {
		vals = append(vals, p)
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	return vals[(len(vals)-1)/2]
}

func abs64(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return b - a
}
