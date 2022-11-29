// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/alertstore/config.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertstore

import (
	"flag"

	"github.com/go-kit/log"

	"github.com/grafana/mimir/pkg/alertmanager/alertstore/local"
	"github.com/grafana/mimir/pkg/storage/bucket"
)

// Config configures a the alertmanager storage backend.
type Config struct {
	bucket.Config `yaml:",inline"`
	Local         local.StoreConfig `yaml:"local"`
}

// RegisterFlags registers the backend storage config.
func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	prefix := "alertmanager-storage."

	cfg.StorageBackendConfig.ExtraBackends = []string{local.Name}
	cfg.Local.RegisterFlagsWithPrefix(prefix, f)
	cfg.RegisterFlagsWithPrefixAndDefaultDirectory(prefix, "alertmanager", f, logger)
}
