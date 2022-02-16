// SPDX-License-Identifier: AGPL-3.0-only

package ingester

// RuntimeMatchers holds the definition of custom tracking rules
type RuntimeMatchersConfig struct {
	DefaultMatchers        ActiveSeriesMatchers            `yaml:"default_matchers"`
	TenantSpecificMatchers map[string]ActiveSeriesMatchers `yaml:"tenant_matchers"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface. If give
func (l *RuntimeMatchersConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain RuntimeMatchersConfig // type indirection to make sure we don't go into recursive loop
	return unmarshal((*plain)(l))
}
