// Package fixed 提供定点整数运算:全部金额以 uint64(×1e8)表示,禁止浮点。
package fixed

import (
	"errors"
	"fmt"
	"math/bits"
	"strconv"
	"strings"
)

// Scale 定点缩放:与 matching/crates/protocol 的 SCALE 一致
const Scale = 100_000_000

var (
	ErrInvalid   = errors.New("fixed: invalid decimal string")
	ErrOverflow  = errors.New("fixed: overflow")
	ErrDivByZero = errors.New("fixed: division by zero")
)

// Parse 解析最多 8 位小数的非负十进制字符串为定点整数。
func Parse(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" || strings.ContainsAny(s, "-+eE") {
		return 0, ErrInvalid
	}
	intPart, fracPart := s, ""
	if i := strings.IndexByte(s, '.'); i >= 0 {
		intPart, fracPart = s[:i], s[i+1:]
	}
	if intPart == "" && fracPart == "" {
		return 0, ErrInvalid
	}
	if len(fracPart) > 8 {
		return 0, fmt.Errorf("%w: more than 8 decimals: %q", ErrInvalid, s)
	}
	if intPart == "" {
		intPart = "0"
	}
	for _, c := range intPart + fracPart {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("%w: %q", ErrInvalid, s)
		}
	}
	for len(fracPart) < 8 {
		fracPart += "0"
	}
	v, err := strconv.ParseUint(intPart+fracPart, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %q", ErrOverflow, s)
	}
	return v, nil
}

// Format 输出十进制字符串,去掉多余的尾随零("50000.00000000" → "50000")。
func Format(v uint64) string {
	ip := v / Scale
	fp := v % Scale
	if fp == 0 {
		return strconv.FormatUint(ip, 10)
	}
	fs := fmt.Sprintf("%08d", fp)
	fs = strings.TrimRight(fs, "0")
	return strconv.FormatUint(ip, 10) + "." + fs
}

// MulDiv 计算 a*b/div,128 位中间值,向下取整。
func MulDiv(a, b, div uint64) (uint64, error) {
	if div == 0 {
		return 0, ErrDivByZero
	}
	hi, lo := bits.Mul64(a, b)
	if hi == 0 {
		return lo / div, nil
	}
	if hi >= div {
		return 0, ErrOverflow
	}
	q, _ := bits.Div64(hi, lo, div)
	return q, nil
}

// MulDivCeil 同 MulDiv,但向上取整(冻结估算用,宁可多冻不可少冻)。
func MulDivCeil(a, b, div uint64) (uint64, error) {
	if div == 0 {
		return 0, ErrDivByZero
	}
	hi, lo := bits.Mul64(a, b)
	if hi >= div {
		return 0, ErrOverflow
	}
	q, r := bits.Div64(hi, lo, div)
	if r > 0 {
		q++
	}
	return q, nil
}
