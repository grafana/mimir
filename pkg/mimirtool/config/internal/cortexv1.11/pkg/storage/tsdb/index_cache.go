// SPDX-License-Identifier: AGPL-3.0-only

package tsdb

import (
	"flag"
	"fmt"
	"strings"

	"github.com/alecthomas/units"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/model"
)

type IndexCacheConfig struct {
	Backend   string                   `yaml:"backend"`
	InMemory  InMemoryIndexCacheConfig `yaml:"inmemory"`
	Memcached MemcachedClientConfig    `yaml:"memcached"`
}

type InMemoryIndexCacheConfig struct {
	MaxSizeBytes uint64 `yaml:"max_size_bytes"`
}

const (
	// IndexCacheBackendInMemory is the value for the in-memory index cache backend.
	IndexCacheBackendInMemory = "inmemory"

	// IndexCacheBackendMemcached is the value for the memcached index cache backend.
	IndexCacheBackendMemcached = "memcached"

	// IndexCacheBackendDefault is the value for the default index cache backend.
	IndexCacheBackendDefault = IndexCacheBackendInMemory

	defaultMaxItemSize = model.Bytes(128 * units.MiB)
)

var (
	supportedIndexCacheBackends = []string{IndexCacheBackendInMemory, IndexCacheBackendMemcached}

	errUnsupportedIndexCacheBackend = errors.New("unsupported index cache backend")
	errNoIndexCacheAddresses        = errors.New("no index cache backend addresses")
)

func (cfg *IndexCacheConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix(f, "blocks-storage.bucket-store.index-cache.")
}
func (cfg *IndexCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Backend, prefix+"backend", IndexCacheBackendDefault, fmt.Sprintf("The index cache backend type. Supported values: %s.", strings.Join(supportedIndexCacheBackends, ", ")))

	cfg.InMemory.RegisterFlagsWithPrefix(f, prefix+"inmemory.")
	cfg.Memcached.RegisterFlagsWithPrefix(f, prefix+"memcached.")
}
func (cfg *InMemoryIndexCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.Uint64Var(&cfg.MaxSizeBytes, prefix+"max-size-bytes", uint64(1*units.Gibibyte), "Maximum size in bytes of in-memory index cache used to speed up blocks index lookups (shared between all tenants).")
}
