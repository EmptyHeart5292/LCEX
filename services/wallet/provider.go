package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// Provider 托管商抽象(ADR-002)。mock 实现可替换为 Fireblocks/Cobo。
type Provider interface {
	DeriveAddress(userID int64, currency, network string) string
}

type mockProvider struct{}

func (mockProvider) DeriveAddress(userID int64, currency, network string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%d", network, strings.ToUpper(currency), userID)))
	switch {
	case network == "bitcoin":
		return "bcrt1q" + hex.EncodeToString(sum[:20])
	case strings.HasPrefix(network, "ethereum"):
		return "0x" + hex.EncodeToString(sum[:20])
	default:
		return hex.EncodeToString(sum[:32])
	}
}
