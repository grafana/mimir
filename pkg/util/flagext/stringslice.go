// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/flagext/stringslice.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package flagext

import "fmt"

// StringSlice is a slice of strings that implements flag.Value
type StringSlice []string

// String implements flag.Value
func (v StringSlice) String() string {
	return fmt.Sprintf("%s", []string(v))
}

// Set implements flag.Value
func (v *StringSlice) Set(s string) error {
	*v = append(*v, s)
	return nil
}
