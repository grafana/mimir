// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"flag"
	"time"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/util/tls"
)

type ClientConfig struct {
	RemoteTimeout time.Duration    `yaml:"remote_timeout"`
	TLSEnabled    bool             `yaml:"tls_enabled"`
	TLS           tls.ClientConfig `yaml:",inline"`
}

func (cfg *ClientConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.TLSEnabled, prefix+".tls-enabled", cfg.TLSEnabled, "Enable TLS in the GRPC client. This flag needs to be enabled when any other TLS flag is set. If set to false, insecure connection to gRPC server will be used.")
	f.DurationVar(&cfg.RemoteTimeout, prefix+".remote-timeout", 2*time.Second, "Timeout for downstream alertmanagers.")
	cfg.TLS.RegisterFlagsWithPrefix(prefix, f)
}
