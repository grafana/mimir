// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"flag"

	"github.com/grafana/dskit/grpcclient"
)

type GRPCClientConfig struct {
	grpcclient.Config `yaml:",inline"`
	RegisteredFlags   RegisteredFlags `yaml:"-"`
}

func (cfg *GRPCClientConfig) Validate() error {
	return cfg.Config.Validate()
}

func (cfg *GRPCClientConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

func (cfg *GRPCClientConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.RegisteredFlags = TrackRegisteredFlags(prefix, f, func(prefix string, f *flag.FlagSet) {
		cfg.Config.RegisterFlagsWithPrefix(prefix, f)
	})
}
