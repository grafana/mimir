// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/flagext/ignored.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package flagext

import (
	"flag"
)

type ignoredFlag struct {
	name string
}

func (ignoredFlag) String() string {
	return "ignored"
}

func (d ignoredFlag) Set(string) error {
	return nil
}

// IgnoredFlag ignores set value, without any warning
func IgnoredFlag(f *flag.FlagSet, name, message string) {
	f.Var(ignoredFlag{name}, name, message)
}
