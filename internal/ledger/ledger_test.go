package ledger

import "testing"

func TestCheckBalancedFreeze(t *testing.T) {
	err := checkBalanced([]Move{
		{Account: User(1, "USDT", "available"), Delta: -100},
		{Account: User(1, "USDT", "frozen"), Delta: +100},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCheckBalancedDeposit(t *testing.T) {
	// 资产↑ + 负债↑,signed 之和不为 0,但借贷平衡
	err := checkBalanced([]Move{
		{Account: System(0, "USDT", "available"), Delta: 1_000},
		{Account: User(101, "USDT", "available"), Delta: 1_000},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCheckBalancedUnbalanced(t *testing.T) {
	err := checkBalanced([]Move{
		{Account: User(1, "USDT", "available"), Delta: 100},
	})
	if err == nil {
		t.Fatal("expected unbalanced")
	}
}
