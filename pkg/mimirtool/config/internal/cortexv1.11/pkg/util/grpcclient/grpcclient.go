// SPDX-License-Identifier: AGPL-3.0-only

package grpcclient

import (
	"flag"

	"github.com/grafana/dskit/backoff"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/util/tls"
)

type Config struct {
	MaxRecvMsgSize  int     `yaml:"max_recv_msg_size"`
	MaxSendMsgSize  int     `yaml:"max_send_msg_size"`
	GRPCCompression string  `yaml:"grpc_compression"`
	RateLimit       float64 `yaml:"rate_limit"`
	RateLimitBurst  int     `yaml:"rate_limit_burst"`

	BackoffOnRatelimits bool           `yaml:"backoff_on_ratelimits"`
	BackoffConfig       backoff.Config `yaml:"backoff_config"`

	TLSEnabled bool             `yaml:"tls_enabled"`
	TLS        tls.ClientConfig `yaml:",inline"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.IntVar(&cfg.MaxRecvMsgSize, prefix+".grpc-max-recv-msg-size", 100<<20, "gRPC client max receive message size (bytes).")
	f.IntVar(&cfg.MaxSendMsgSize, prefix+".grpc-max-send-msg-size", 16<<20, "gRPC client max send message size (bytes).")
	f.StringVar(&cfg.GRPCCompression, prefix+".grpc-compression", "", "Use compression when sending messages. Supported values are: 'gzip', 'snappy' and '' (disable compression)")
	f.Float64Var(&cfg.RateLimit, prefix+".grpc-client-rate-limit", 0., "Rate limit for gRPC client; 0 means disabled.")
	f.IntVar(&cfg.RateLimitBurst, prefix+".grpc-client-rate-limit-burst", 0, "Rate limit burst for gRPC client.")
	f.BoolVar(&cfg.BackoffOnRatelimits, prefix+".backoff-on-ratelimits", false, "Enable backoff and retry when we hit ratelimits.")
	f.BoolVar(&cfg.TLSEnabled, prefix+".tls-enabled", cfg.TLSEnabled, "Enable TLS in the GRPC client. This flag needs to be enabled when any other TLS flag is set. If set to false, insecure connection to gRPC server will be used.")

	cfg.BackoffConfig.RegisterFlagsWithPrefix(prefix, f)

	cfg.TLS.RegisterFlagsWithPrefix(prefix, f)
}
