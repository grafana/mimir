// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/flagext/secret.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package flagext

type Secret struct {
	Value string
}

// String implements flag.Value
func (v Secret) String() string {
	return v.Value
}

// Set implements flag.Value
func (v *Secret) Set(s string) error {
	v.Value = s
	return nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (v *Secret) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}

	return v.Set(s)
}

// MarshalYAML implements yaml.Marshaler.
func (v Secret) MarshalYAML() (interface{}, error) {
	if len(v.Value) == 0 {
		return "", nil
	}
	return "********", nil
}
