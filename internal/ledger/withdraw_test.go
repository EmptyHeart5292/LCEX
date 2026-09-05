package ledger

import "testing"

func TestCheckBalancedWithdraw(t *testing.T) {
	err := checkBalanced([]Move{
		{Account: User(1, "BTC", "available"), Delta: -1200},
		{Account: System(0, "BTC", "available"), Delta: -1000},
		{Account: Fee(0, "BTC", "available"), Delta: 200},
	})
	if err != nil {
		t.Fatal(err)
	}
}
