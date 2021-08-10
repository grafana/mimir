// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/flagext/register.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package flagext

import "flag"

// Registerer is a thing that can RegisterFlags
type Registerer interface {
	RegisterFlags(*flag.FlagSet)
}

// RegisterFlags registers flags with the provided Registerers
func RegisterFlags(rs ...Registerer) {
	for _, r := range rs {
		r.RegisterFlags(flag.CommandLine)
	}
}

// DefaultValues initiates a set of configs (Registerers) with their defaults.
func DefaultValues(rs ...Registerer) {
	fs := flag.NewFlagSet("", flag.PanicOnError)
	for _, r := range rs {
		r.RegisterFlags(fs)
	}
	_ = fs.Parse([]string{})
}
