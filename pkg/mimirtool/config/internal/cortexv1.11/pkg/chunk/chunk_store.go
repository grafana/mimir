// SPDX-License-Identifier: AGPL-3.0-only

package chunk

import (
	"flag"

	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk/cache"
)

type StoreConfig struct {
	ChunkCacheConfig       cache.Config `yaml:"chunk_cache_config"`
	WriteDedupeCacheConfig cache.Config `yaml:"write_dedupe_cache_config"`

	CacheLookupsOlderThan model.Duration `yaml:"cache_lookups_older_than"`

	// Not visible in yaml because the setting shouldn't be common between ingesters and queriers.
	// This exists in case we don't want to cache all the chunks but still want to take advantage of
	// ingester chunk write deduplication. But for the queriers we need the full value. So when this option
	// is set, use different caches for ingesters and queriers.
	chunkCacheStubs bool // don't write the full chunk to cache, just a stub entry

	// When DisableIndexDeduplication is true and chunk is already there in cache, only index would be written to the store and not chunk.
}

func (cfg *StoreConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.ChunkCacheConfig.RegisterFlagsWithPrefix("store.chunks-cache.", "Cache config for chunks. ", f)
	f.BoolVar(&cfg.chunkCacheStubs, "store.chunks-cache.cache-stubs", false, "If true, don't write the full chunk to cache, just a stub entry.")
	cfg.WriteDedupeCacheConfig.RegisterFlagsWithPrefix("store.index-cache-write.", "Cache config for index entry writing. ", f)

	f.Var(&cfg.CacheLookupsOlderThan, "store.cache-lookups-older-than", "Cache index entries older than this period. 0 to disable.")
}
