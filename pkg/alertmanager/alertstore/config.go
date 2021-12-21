// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/alertstore/config.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertstore

import (
	"flag"

	"github.com/grafana/mimir/pkg/alertmanager/alertstore/local"
	"github.com/grafana/mimir/pkg/storage/bucket"
)

// Config configures a the alertmanager storage backend.
type Config struct {
	bucket.Config `yaml:",inline"`
	Local         local.StoreConfig `yaml:"local"`
}

// RegisterFlags registers the backend storage config.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	prefix := "alertmanager-storage."

	cfg.ExtraBackends = []string{local.Name}
	cfg.Local.RegisterFlagsWithPrefix(prefix, f)
	cfg.RegisterFlagsWithPrefix(prefix, f)
}

// IsFullStateSupported returns if the given configuration supports access to FullState objects.
func (cfg *Config) IsFullStateSupported() bool {
	for _, backend := range bucket.SupportedBackends {
		if cfg.Backend == backend {
			return true
		}
	}
	return false
}
