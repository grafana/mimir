// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/index_cache.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdb

import (
	"flag"
	"fmt"
	"strings"

	"github.com/alecthomas/units"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/cache"
	"github.com/grafana/mimir/pkg/util"
)

const (
	// IndexCacheBackendInMemory is the value for the in-memory index cache backend.
	IndexCacheBackendInMemory = "inmemory"

	// IndexCacheBackendMemcached is the value for the memcached index cache backend.
	IndexCacheBackendMemcached = cache.BackendMemcached

	// IndexCacheBackendDefault is the value for the default index cache backend.
	IndexCacheBackendDefault = IndexCacheBackendInMemory
)

var (
	supportedIndexCacheBackends = []string{IndexCacheBackendInMemory, IndexCacheBackendMemcached}

	ErrUnsupportedIndexCacheBackend = errors.New("unsupported index cache backend")
)

type IndexCacheConfig struct {
	cache.BackendConfig `yaml:",inline"`
	InMemory            InMemoryIndexCacheConfig `yaml:"inmemory"`
}

func (cfg *IndexCacheConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix(f, "blocks-storage.bucket-store.index-cache.")
}

func (cfg *IndexCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Backend, prefix+"backend", IndexCacheBackendDefault, fmt.Sprintf("The index cache backend type. Supported values: %s.", strings.Join(supportedIndexCacheBackends, ", ")))

	cfg.InMemory.RegisterFlagsWithPrefix(f, prefix+"inmemory.")
	cfg.Memcached.RegisterFlagsWithPrefix(f, prefix+"memcached.")
}

// Validate the config.
func (cfg *IndexCacheConfig) Validate() error {
	if !util.StringsContain(supportedIndexCacheBackends, cfg.Backend) {
		return ErrUnsupportedIndexCacheBackend
	}

	if cfg.Backend == IndexCacheBackendMemcached {
		if err := cfg.Memcached.Validate(); err != nil {
			return err
		}
	}

	return nil
}

type InMemoryIndexCacheConfig struct {
	MaxSizeBytes uint64 `yaml:"max_size_bytes"`
}

func (cfg *InMemoryIndexCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.Uint64Var(&cfg.MaxSizeBytes, prefix+"max-size-bytes", uint64(1*units.Gibibyte), "Maximum size in bytes of in-memory index cache used to speed up blocks index lookups (shared between all tenants).")
}
