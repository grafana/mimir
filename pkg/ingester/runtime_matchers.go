// SPDX-License-Identifier: AGPL-3.0-only

package ingester

// RuntimeMatchers holds the definition of custom tracking rules
type RuntimeMatchersConfig struct {
	DefaultMatchers        ActiveSeriesMatchers            `yaml:"default_matchers"`
	TenantSpecificMatchers map[string]ActiveSeriesMatchers `yaml:"tenant_matchers"`
}

type RuntimeMatchersConfigProvider struct {
	Getter func() *RuntimeMatchersConfig
}

func (p *RuntimeMatchersConfigProvider) Get() *RuntimeMatchersConfig {
	if p == nil || p.Getter == nil {
		return nil
	}
	return p.Getter()
}
