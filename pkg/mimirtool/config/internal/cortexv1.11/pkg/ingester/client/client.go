// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"flag"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/util/grpcclient"
)

type Config struct {
	GRPCClientConfig grpcclient.Config `yaml:"grpc_client_config"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("ingester.client", f)
}
