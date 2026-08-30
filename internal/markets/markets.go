// Package markets 加载交易对配置(packages/api-spec/markets.yaml,单一事实源)。
package markets

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/EmptyHeart5292/lcex/internal/fixed"
)

type Market struct {
	Symbol    string `yaml:"symbol"`
	Base      string `yaml:"base"`
	Quote     string `yaml:"quote"`
	Status    string `yaml:"status"`
	MinQty    uint64 `yaml:"-"` // 定点
	MinNotion uint64 `yaml:"-"` // 定点
	MaxMktQty uint64 `yaml:"-"` // 定点,0 = 不限
	MakerRate uint64 `yaml:"-"` // 定点 ×1e8
	TakerRate uint64 `yaml:"-"` // 定点 ×1e8

	raw rawMarket
}

type rawMarket struct {
	Symbol          string `yaml:"symbol"`
	Base            string `yaml:"base"`
	Quote           string `yaml:"quote"`
	Status          string `yaml:"status"`
	MinQty          string `yaml:"minQty"`
	MinNotional     string `yaml:"minNotional"`
	MaxMarketQty    string `yaml:"maxMarketQty"`
	DisplayQS       int    `yaml:"displayQuoteScale"`
	OverrideMaker   string `yaml:"makerFeeRate"`
	OverrideTaker   string `yaml:"takerFeeRate"`
}

type rawDefaults struct {
	OrderTypes  []string `yaml:"orderTypes"`
	TimeInForce []string `yaml:"timeInForce"`
	StpModes    []string `yaml:"stpModes"`
	MakerRate   string   `yaml:"makerFeeRate"`
	TakerRate   string   `yaml:"takerFeeRate"`
}

type rawFile struct {
	Defaults rawDefaults `yaml:"defaults"`
	Markets  []rawMarket `yaml:"markets"`
}

type Config struct {
	bySymbol map[string]*Market
}

var ErrUnknownMarket = errors.New("markets: unknown market")

// Load 读取并解析交易对配置;费率默认取 defaults,交易对可覆盖。
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("markets: read %s: %w", path, err)
	}
	var f rawFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("markets: parse: %w", err)
	}
	cfg := &Config{bySymbol: map[string]*Market{}}
	makerRate, err := fixed.Parse(f.Defaults.MakerRate)
	if err != nil {
		return nil, fmt.Errorf("markets: defaults.makerFeeRate: %w", err)
	}
	takerRate, err := fixed.Parse(f.Defaults.TakerRate)
	if err != nil {
		return nil, fmt.Errorf("markets: defaults.takerFeeRate: %w", err)
	}
	for _, rm := range f.Markets {
		m := &Market{
			Symbol: rm.Symbol, Base: rm.Base, Quote: rm.Quote, Status: rm.Status, raw: rm,
			MakerRate: makerRate, TakerRate: takerRate,
		}
		if rm.MinQty != "" {
			if m.MinQty, err = fixed.Parse(rm.MinQty); err != nil {
				return nil, fmt.Errorf("markets: %s minQty: %w", rm.Symbol, err)
			}
		}
		if rm.MinNotional != "" {
			if m.MinNotion, err = fixed.Parse(rm.MinNotional); err != nil {
				return nil, fmt.Errorf("markets: %s minNotional: %w", rm.Symbol, err)
			}
		}
		if rm.MaxMarketQty != "" {
			if m.MaxMktQty, err = fixed.Parse(rm.MaxMarketQty); err != nil {
				return nil, fmt.Errorf("markets: %s maxMarketQty: %w", rm.Symbol, err)
			}
		}
		if rm.OverrideMaker != "" {
			if m.MakerRate, err = fixed.Parse(rm.OverrideMaker); err != nil {
				return nil, err
			}
		}
		if rm.OverrideTaker != "" {
			if m.TakerRate, err = fixed.Parse(rm.OverrideTaker); err != nil {
				return nil, err
			}
		}
		cfg.bySymbol[strings.ToUpper(m.Symbol)] = m
	}
	return cfg, nil
}

// Get 返回交易对配置;大小写无关(kafka topic 为小写,配置为大写)
func (c *Config) Get(symbol string) (*Market, error) {
	m, ok := c.bySymbol[strings.ToUpper(symbol)]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownMarket, symbol)
	}
	return m, nil
}

// Trading 交易对是否处于可交易状态
func (m *Market) Trading() bool { return m.Status == "trading" }
