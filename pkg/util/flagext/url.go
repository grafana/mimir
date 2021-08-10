// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/flagext/url.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package flagext

import "net/url"

// URLValue is a url.URL that can be used as a flag.
type URLValue struct {
	*url.URL
}

// String implements flag.Value
func (v URLValue) String() string {
	if v.URL == nil {
		return ""
	}
	return v.URL.String()
}

// Set implements flag.Value
func (v *URLValue) Set(s string) error {
	u, err := url.Parse(s)
	if err != nil {
		return err
	}
	v.URL = u
	return nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (v *URLValue) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}

	// An empty string means no URL has been configured.
	if s == "" {
		v.URL = nil
		return nil
	}

	return v.Set(s)
}

// MarshalYAML implements yaml.Marshaler.
func (v URLValue) MarshalYAML() (interface{}, error) {
	if v.URL == nil {
		return "", nil
	}

	// Mask out passwords when marshalling URLs back to YAML.
	u := *v.URL
	if u.User != nil {
		if _, set := u.User.Password(); set {
			u.User = url.UserPassword(u.User.Username(), "********")
		}
	}

	return u.String(), nil
}
