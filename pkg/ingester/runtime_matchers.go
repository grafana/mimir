// SPDX-License-Identifier: AGPL-3.0-only

package ingester

// RuntimeMatchers holds the definition of custom tracking rules
type RuntimeMatchersConfig struct {
	GenericMatchers        *ActiveSeriesCustomTrackersConfig            `yaml:"generic_matchers"`
	TenantSpecificMatchers map[string]*ActiveSeriesCustomTrackersConfig `yaml:"tenant_machers"`
}

// Sets default runtime matchers for unmarshalling.
var defaultRuntimeMatchers *RuntimeMatchersConfig = nil

// UnmarshalYAML implements the yaml.Unmarshaler interface. If give
func (l *RuntimeMatchersConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	if defaultInstanceLimits != nil {
		*l = *defaultRuntimeMatchers
	}
	type plain RuntimeMatchersConfig // type indirection to make sure we don't go into recursive loop
	return unmarshal((*plain)(l))
}
