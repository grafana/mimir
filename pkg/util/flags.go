// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"flag"
	"strings"
)

// RegisteredFlags contains the flags registered by some config.
type RegisteredFlags struct {
	// Prefix is the prefix used by the flag
	Prefix string
	// Flags are the flag definitions of each one of the flag names. Flag names don't contain the prefix here.
	Flags map[string]*flag.Flag
}

// TrackRegisteredFlags returns the flags that were registered by the register function.
// It only tracks the flags that have the given prefix.
func TrackRegisteredFlags(prefix string, f *flag.FlagSet, register func(prefix string, f *flag.FlagSet)) RegisteredFlags {
	old := map[string]bool{}
	f.VisitAll(func(f *flag.Flag) { old[f.Name] = true })

	register(prefix, f)

	rf := RegisteredFlags{
		Prefix: prefix,
		Flags:  map[string]*flag.Flag{},
	}

	f.VisitAll(func(f *flag.Flag) {
		if !strings.HasPrefix(f.Name, prefix) {
			return
		}
		if !old[f.Name] {
			rf.Flags[f.Name[len(prefix):]] = f
		}
	})

	return rf
}
