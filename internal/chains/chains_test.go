package chains

import "testing"

func TestLoadCurrenciesYAML(t *testing.T) {
	cfg, err := Load("../../packages/api-spec/currencies.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ch, err := cfg.Chain("BTC", "bitcoin")
	if err != nil {
		t.Fatal(err)
	}
	if ch.Confirmations != 2 || ch.MinWithdraw == 0 || !ch.DepositOn {
		t.Fatalf("unexpected chain: %+v", ch)
	}
	if _, err := cfg.Chain("BTC", "tron"); err == nil {
		t.Fatal("expected unknown network")
	}
}
