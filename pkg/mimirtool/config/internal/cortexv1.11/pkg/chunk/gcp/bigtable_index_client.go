// SPDX-License-Identifier: AGPL-3.0-only

package gcp

import (
	"flag"
	"time"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/util/grpcclient"
)

type Config struct {
	Project  string `yaml:"project"`
	Instance string `yaml:"instance"`

	GRPCClientConfig grpcclient.Config `yaml:"grpc_client_config"`

	TableCacheEnabled    bool          `yaml:"table_cache_enabled"`
	TableCacheExpiration time.Duration `yaml:"table_cache_expiration"`
}

const (
	columnFamily = "f"
	columnPrefix = columnFamily + ":"
	column       = "c"
	separator    = "\000"
	maxRowReads  = 100
)

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Project, "bigtable.project", "", "Bigtable project ID.")
	f.StringVar(&cfg.Instance, "bigtable.instance", "", "Bigtable instance ID. Please refer to https://cloud.google.com/docs/authentication/production for more information about how to configure authentication.")
	f.BoolVar(&cfg.TableCacheEnabled, "bigtable.table-cache.enabled", true, "If enabled, once a tables info is fetched, it is cached.")
	f.DurationVar(&cfg.TableCacheExpiration, "bigtable.table-cache.expiration", 30*time.Minute, "Duration to cache tables before checking again.")

	// This overrides our default from TLS disabled to TLS enabled
	cfg.GRPCClientConfig.TLSEnabled = true
	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("bigtable", f)
}
