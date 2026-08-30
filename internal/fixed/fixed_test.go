package fixed

import "testing"

func TestParseAndFormat(t *testing.T) {
	cases := []struct {
		in   string
		want uint64
		str  string
	}{
		{"50000", 50_000 * Scale, "50000"},
		{"50000.5", 50_000*Scale + Scale/2, "50000.5"},
		{"0.00000001", 1, "0.00000001"},
		{"0.1", Scale / 10, "0.1"},
		{" 100 ", 100 * Scale, "100"},
	}
	for _, c := range cases {
		v, err := Parse(c.in)
		if err != nil || v != c.want {
			t.Fatalf("Parse(%q) = %v, %v; want %d", c.in, v, err, c.want)
		}
		if got := Format(v); got != c.str {
			t.Fatalf("Format(%d) = %q; want %q", v, got, c.str)
		}
	}
	for _, bad := range []string{"", "-1", "1.2.3", "1e5", "0.123456789", "abc", "1.00000001a"} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("Parse(%q) 应该失败", bad)
		}
	}
}

func TestMulDiv(t *testing.T) {
	// 50000 USDT(5e12)× 1 BTC(1e8)÷ 1e8 = 50000 USDT(5e12)——128 位中间值 5e20 > uint64
	v, err := MulDiv(50_000*Scale, Scale, Scale)
	if err != nil || v != 50_000*Scale {
		t.Fatalf("MulDiv = %v, %v", v, err)
	}
	// 手续费:5e11 × 0.001(1e5)/ 1e8 = 5e8(5 USDT)
	v, err = MulDiv(5e11, 1e5, Scale)
	if err != nil || v != 5e8 {
		t.Fatalf("fee MulDiv = %v, %v", v, err)
	}
	if _, err := MulDiv(1, 1, 0); err == nil {
		t.Fatal("除零应报错")
	}
}

func TestMulDivCeil(t *testing.T) {
	// 10.00000001 × 1 / 10 = 1.000000001 → ceil 2 (at scale: 1.00000001=1000000001, /10 → 100000000 r1 → ceil 100000001? )
	v, err := MulDivCeil(1_000_000_001, 1, 10)
	if err != nil || v != 100_000_001 {
		t.Fatalf("MulDivCeil = %v, %v", v, err)
	}
	// 整除不进位
	v, err = MulDivCeil(100, 3, 3)
	if err != nil || v != 100 {
		t.Fatalf("MulDivCeil exact = %v, %v", v, err)
	}
}
