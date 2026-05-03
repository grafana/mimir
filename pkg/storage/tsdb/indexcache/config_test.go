// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/index_cache_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package indexcache

import (
	"testing"
	"time"

	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/assert"
)

func TestIndexCacheConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		cfg      IndexCacheConfig
		expected error
	}{
		"default config should pass": {
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)
				return cfg
			}(),
		},
		"unsupported backend should fail": {
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)

				cfg.Backend = "xxx"

				return cfg
			}(),
			expected: errUnsupportedIndexCacheBackend,
		},
		"no memcached addresses should fail": {
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)

				cfg.Backend = BackendMemcached

				return cfg
			}(),
			expected: cache.ErrNoMemcachedAddresses,
		},
		"one memcached address should pass": {
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)

				cfg.Backend = BackendMemcached
				cfg.Memcached.Addresses = []string{"dns+localhost:11211"}

				return cfg
			}(),
		},
		"inmemory should pass": {
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)

				cfg.Backend = BackendInMemory

				return cfg
			}(),
		},
		"negative memcached-inmemory-max-items should fail": {
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)
				cfg.Backend = BackendMemcached
				cfg.Memcached.Addresses = []string{"dns+localhost:11211"}
				cfg.MemcachedInMemoryMaxItems = -1
				return cfg
			}(),
			expected: errInvalidMemcachedInMemoryMaxItems,
		},
		"zero memcached-inmemory-ttl with L1 enabled should fail": {
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)
				cfg.Backend = BackendMemcached
				cfg.Memcached.Addresses = []string{"dns+localhost:11211"}
				cfg.MemcachedInMemoryMaxItems = 100
				cfg.MemcachedInMemoryTTL = 0
				return cfg
			}(),
			expected: errInvalidMemcachedInMemoryTTL,
		},
		"zero memcached-inmemory-ttl with per-type L1 enabled should fail": {
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)
				cfg.Backend = BackendMemcached
				cfg.Memcached.Addresses = []string{"dns+localhost:11211"}
				cfg.MemcachedInMemoryMaxItemsSeriesForRef = 1000
				cfg.MemcachedInMemoryTTL = 0
				return cfg
			}(),
			expected: errInvalidMemcachedInMemoryTTL,
		},
		"zero memcached-inmemory-ttl with L1 disabled should pass": {
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)
				cfg.Backend = BackendMemcached
				cfg.Memcached.Addresses = []string{"dns+localhost:11211"}
				cfg.MemcachedInMemoryMaxItems = 0
				cfg.MemcachedInMemoryTTL = 0
				return cfg
			}(),
		},
		"negative memcached-setasync-dedup-max-items should fail": {
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)
				cfg.Backend = BackendMemcached
				cfg.Memcached.Addresses = []string{"dns+localhost:11211"}
				cfg.MemcachedSetAsyncDedupMaxItems = -1
				return cfg
			}(),
			expected: errInvalidMemcachedSetAsyncDedupMaxItems,
		},
		"zero memcached-setasync-dedup-window with dedup enabled should fail": {
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)
				cfg.Backend = BackendMemcached
				cfg.Memcached.Addresses = []string{"dns+localhost:11211"}
				cfg.MemcachedSetAsyncDedupMaxItems = 100
				cfg.MemcachedSetAsyncDedupWindow = 0
				return cfg
			}(),
			expected: errInvalidMemcachedSetAsyncDedupWindow,
		},
		"L1 and dedup both enabled with valid config should pass": {
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)
				cfg.Backend = BackendMemcached
				cfg.Memcached.Addresses = []string{"dns+localhost:11211"}
				cfg.MemcachedInMemoryMaxItems = 100
				cfg.MemcachedSetAsyncDedupMaxItems = 1024
				return cfg
			}(),
		},
		"dedup window equal to a per-type TTL should fail": {
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)
				cfg.Backend = BackendMemcached
				cfg.Memcached.Addresses = []string{"dns+localhost:11211"}
				cfg.MemcachedSetAsyncDedupMaxItems = 1024
				cfg.MemcachedSetAsyncDedupWindow = 5 * time.Second
				cfg.TTLLabelValues = 5 * time.Second
				return cfg
			}(),
			expected: errInvalidMemcachedSetAsyncDedupWindowExceedsTTL,
		},
		"dedup window greater than a per-type TTL should fail": {
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)
				cfg.Backend = BackendMemcached
				cfg.Memcached.Addresses = []string{"dns+localhost:11211"}
				cfg.MemcachedSetAsyncDedupMaxItems = 1024
				cfg.MemcachedSetAsyncDedupWindow = 10 * time.Second
				cfg.TTLExpandedPostings = 1 * time.Second
				return cfg
			}(),
			expected: errInvalidMemcachedSetAsyncDedupWindowExceedsTTL,
		},
		"dedup disabled with window > a TTL should pass (validator only fires when dedup is on)": {
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)
				cfg.Backend = BackendMemcached
				cfg.Memcached.Addresses = []string{"dns+localhost:11211"}
				cfg.MemcachedSetAsyncDedupMaxItems = 0
				cfg.MemcachedSetAsyncDedupWindow = 10 * time.Second
				cfg.TTLLabelValues = 1 * time.Second
				return cfg
			}(),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.cfg.Validate())
		})
	}
}

func TestIndexCacheConfig_EffectivePerTypeMaxItems(t *testing.T) {
	t.Run("L1 disabled when no flag is set", func(t *testing.T) {
		cfg := IndexCacheConfig{}
		flagext.DefaultValues(&cfg)
		assert.False(t, cfg.effectivePerTypeMaxItems().AnyEnabled())
	})

	t.Run("global flag distributes evenly across all types", func(t *testing.T) {
		cfg := IndexCacheConfig{}
		flagext.DefaultValues(&cfg)
		cfg.MemcachedInMemoryMaxItems = 600
		sizes := cfg.effectivePerTypeMaxItems()
		assert.Equal(t, 100, sizes.Postings)
		assert.Equal(t, 100, sizes.SeriesForRef)
		assert.Equal(t, 100, sizes.ExpandedPostings)
		assert.Equal(t, 100, sizes.SeriesForPostings)
		assert.Equal(t, 100, sizes.LabelNames)
		assert.Equal(t, 100, sizes.LabelValues)
	})

	t.Run("global flag rounds up to ensure every type has at least one slot", func(t *testing.T) {
		// 1 / 6 == 0 with truncating division would disable all types; the rounded-
		// up division gives every type a single slot instead.
		cfg := IndexCacheConfig{}
		flagext.DefaultValues(&cfg)
		cfg.MemcachedInMemoryMaxItems = 1
		sizes := cfg.effectivePerTypeMaxItems()
		assert.True(t, sizes.AnyEnabled())
		assert.Equal(t, 1, sizes.Postings)
	})

	t.Run("per-type flag overrides global", func(t *testing.T) {
		cfg := IndexCacheConfig{}
		flagext.DefaultValues(&cfg)
		cfg.MemcachedInMemoryMaxItems = 600
		cfg.MemcachedInMemoryMaxItemsSeriesForRef = 5000
		sizes := cfg.effectivePerTypeMaxItems()
		// Per-type wins entirely — the global is ignored.
		assert.Equal(t, 5000, sizes.SeriesForRef)
		// Other types fall back to their per-type values (zero), NOT the global
		// distribution. This is intentional: setting any per-type flag is the
		// signal that the operator wants explicit per-type control.
		assert.Equal(t, 0, sizes.Postings)
		assert.Equal(t, 0, sizes.LabelNames)
	})
}

func TestIndexCacheConfig_MemcachedInMemoryDefaults(t *testing.T) {
	cfg := IndexCacheConfig{}
	flagext.DefaultValues(&cfg)

	assert.Equal(t, 0, cfg.MemcachedInMemoryMaxItems, "L1 cache should default to disabled")
	assert.Equal(t, 24*time.Hour, cfg.MemcachedInMemoryTTL, "L1 cache TTL should default to 24h")
}

func TestIndexCacheConfig_MemcachedSetAsyncDedupDefaults(t *testing.T) {
	cfg := IndexCacheConfig{}
	flagext.DefaultValues(&cfg)

	assert.Equal(t, 0, cfg.MemcachedSetAsyncDedupMaxItems, "SetAsync dedup should default to disabled")
	assert.Equal(t, 5*time.Second, cfg.MemcachedSetAsyncDedupWindow, "SetAsync dedup window should default to 5s")
}
