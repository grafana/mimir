// SPDX-License-Identifier: AGPL-3.0-only

package grpc

import (
	"flag"
)

type Config struct {
	Address string `yaml:"server_address,omitempty"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Address, "grpc-store.server-address", "", "Hostname or IP of the gRPC store instance.")
}
