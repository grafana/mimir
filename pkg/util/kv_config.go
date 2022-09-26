package util

import (
	"flag"

	"github.com/grafana/dskit/kv"
)

// KVConfig adds RegisteredFlags to kv.Config.
type KVConfig struct {
	kv.Config `yaml:",inline"`

	// Used to keep track of the flag names registered in this config, to be able to overwrite them later properly.
	RegisteredFlags RegisteredFlags `yaml:"-"`
}

func (cfg *KVConfig) RegisterFlagsWithPrefix(flagsPrefix, defaultPrefix string, f *flag.FlagSet) {
	cfg.RegisteredFlags = TrackRegisteredFlags(flagsPrefix, f, func(prefix string, f *flag.FlagSet) {
		cfg.Config.RegisterFlagsWithPrefix(prefix, defaultPrefix, f)
	})
}
