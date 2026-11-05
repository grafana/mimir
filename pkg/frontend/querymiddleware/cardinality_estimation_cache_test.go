// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestCardinalityEstimationCacheConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		mutate      func(cfg *CardinalityEstimationCacheConfig)
		expectedErr error
	}{
		"should pass with default config": {
			mutate: func(*CardinalityEstimationCacheConfig) {},
		},
		"should pass with memcached backend": {
			mutate: func(cfg *CardinalityEstimationCacheConfig) {
				cfg.Backend = cache.BackendMemcached
				cfg.Memcached.Addresses = []string{"localhost"}
			},
		},
		"should fail with invalid memcached config": {
			mutate: func(cfg *CardinalityEstimationCacheConfig) {
				cfg.Backend = cache.BackendMemcached
				cfg.Memcached.Addresses = []string{}
			},
			expectedErr: cache.ErrNoMemcachedAddresses,
		},
		"should fail with unsupported backend": {
			mutate: func(cfg *CardinalityEstimationCacheConfig) {
				cfg.Backend = "redis"
			},
			expectedErr: errUnsupportedBackend,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := CardinalityEstimationCacheConfig{}
			flagext.DefaultValues(&cfg)
			testData.mutate(&cfg)

			err := cfg.Validate()
			if testData.expectedErr != nil {
				require.ErrorIs(t, err, testData.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNewCardinalityEstimationCache_EmptyBackendReturnsResultsCache(t *testing.T) {
	cfg := CardinalityEstimationCacheConfig{}
	flagext.DefaultValues(&cfg)
	results := cache.NewMockCache()

	c, err := NewCardinalityEstimationCache(cfg, results, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.Same(t, results, c)
}

func TestNewCardinalityEstimationCache_MemcachedAlongsideResultsCache(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()

	resultsCfg := ResultsCacheConfig{}
	flagext.DefaultValues(&resultsCfg)
	resultsCfg.Backend = cache.BackendMemcached
	resultsCfg.Memcached.Addresses = []string{"localhost:11211"}
	results, err := NewResultsCache(resultsCfg, log.NewNopLogger(), reg)
	require.NoError(t, err)
	t.Cleanup(results.Stop)

	cfg := CardinalityEstimationCacheConfig{}
	flagext.DefaultValues(&cfg)
	cfg.Backend = cache.BackendMemcached
	cfg.Memcached.Addresses = []string{"localhost:11211"}
	c, err := NewCardinalityEstimationCache(cfg, results, log.NewNopLogger(), reg)
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	require.IsType(t, &cache.SpanlessTracingCache{}, c)
	require.Equal(t, "frontend-cardinality-cache", c.Name())
}
