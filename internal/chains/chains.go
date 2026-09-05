// Package chains 加载币种与充提网络配置(packages/api-spec/currencies.yaml)。
package chains

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/EmptyHeart5292/lcex/internal/fixed"
)

type Chain struct {
	Network      string
	MinDeposit   uint64
	MinWithdraw  uint64
	WithdrawFee  uint64
	Confirmations int
	DepositOn    bool
	WithdrawOn   bool
}

type Currency struct {
	Code   string
	Chains map[string]*Chain // network -> chain
}

type Config struct {
	byCode map[string]*Currency
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("chains: read %s: %w", path, err)
	}
	var f struct {
		Currencies []struct {
			Code   string `yaml:"code"`
			Chains []struct {
				Network         string `yaml:"network"`
				MinDeposit      string `yaml:"minDeposit"`
				MinWithdrawal   string `yaml:"minWithdrawal"`
				WithdrawFee     string `yaml:"withdrawFee"`
				Confirmations   int    `yaml:"confirmations"`
				DepositEnabled  bool   `yaml:"depositEnabled"`
				WithdrawEnabled bool   `yaml:"withdrawEnabled"`
			} `yaml:"chains"`
		} `yaml:"currencies"`
	}
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("chains: parse: %w", err)
	}
	cfg := &Config{byCode: map[string]*Currency{}}
	for _, rc := range f.Currencies {
		c := &Currency{Code: rc.Code, Chains: map[string]*Chain{}}
		for _, rn := range rc.Chains {
			ch := &Chain{
				Network:       rn.Network,
				Confirmations: rn.Confirmations,
				DepositOn:     rn.DepositEnabled,
				WithdrawOn:    rn.WithdrawEnabled,
			}
			if ch.MinDeposit, err = fixed.Parse(rn.MinDeposit); err != nil {
				return nil, fmt.Errorf("chains: %s/%s minDeposit: %w", rc.Code, rn.Network, err)
			}
			if ch.MinWithdraw, err = fixed.Parse(rn.MinWithdrawal); err != nil {
				return nil, fmt.Errorf("chains: %s/%s minWithdrawal: %w", rc.Code, rn.Network, err)
			}
			if ch.WithdrawFee, err = fixed.Parse(rn.WithdrawFee); err != nil {
				return nil, fmt.Errorf("chains: %s/%s withdrawFee: %w", rc.Code, rn.Network, err)
			}
			c.Chains[rn.Network] = ch
		}
		cfg.byCode[strings.ToUpper(rc.Code)] = c
	}
	return cfg, nil
}

func (c *Config) Chain(currency, network string) (*Chain, error) {
	cur, ok := c.byCode[strings.ToUpper(currency)]
	if !ok {
		return nil, fmt.Errorf("chains: unknown currency %s", currency)
	}
	ch, ok := cur.Chains[network]
	if !ok {
		return nil, fmt.Errorf("chains: unknown network %s for %s", network, currency)
	}
	return ch, nil
}
