// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
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

func WarnDeprecatedConfig(flagName string, logger log.Logger) {
	flagext.DeprecatedFlagsUsed.Inc()
	level.Warn(logger).Log("msg", fmt.Sprintf("the configuration parameter -%s is deprecated and will be soon removed from Mimir", flagName))
}

// ParseFlagsAndArguments calls Parse() on the input flag.FlagSet and returns the parsed arguments.
func ParseFlagsAndArguments(f *flag.FlagSet) ([]string, error) {
	err := f.Parse(os.Args[1:])
	return f.Args(), err
}

// ParseFlags calls Parse() on the input flag.FlagSet and enforces no arguments have been parsed.
// This utility should be called whenever we only expect CLI flags but no arguments.
func ParseFlags(f *flag.FlagSet) error {
	if err := f.Parse(os.Args[1:]); err != nil {
		return err
	}

	if f.NArg() > 0 {
		return fmt.Errorf("the command does not support any argument, but some were provided: %s", strings.Join(f.Args(), " "))
	}

	return nil
}
