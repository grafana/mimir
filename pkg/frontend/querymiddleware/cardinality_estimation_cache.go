// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"flag"
	"fmt"
	"slices"
	"strings"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// CardinalityEstimationCacheConfig is the config for the optional dedicated cache of cardinality estimates.
type CardinalityEstimationCacheConfig struct {
	cache.BackendConfig `yaml:",inline"`
}

// RegisterFlags registers flags.
func (cfg *CardinalityEstimationCacheConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Backend, "query-frontend.cardinality-estimation-cache.backend", "", fmt.Sprintf("Backend for the query-frontend cardinality estimation cache, if not empty. If empty, cardinality estimates are stored in the results cache. Supported values: %s.", strings.Join(supportedResultsCacheBackends, ", ")))
	cfg.Memcached.RegisterFlagsWithPrefix("query-frontend.cardinality-estimation-cache.memcached.", f)
}

func (cfg *CardinalityEstimationCacheConfig) Validate() error {
	if cfg.Backend != "" && !slices.Contains(supportedResultsCacheBackends, cfg.Backend) {
		return errUnsupportedResultsCacheBackend(cfg.Backend)
	}

	if cfg.Backend == cache.BackendMemcached {
		if err := cfg.Memcached.Validate(); err != nil {
			return errors.Wrap(err, "query-frontend cardinality estimation cache")
		}
	}
	return nil
}

// NewCardinalityEstimationCache returns the cache to use for cardinality estimates. If no backend is
// configured, it returns results.
func NewCardinalityEstimationCache(cfg CardinalityEstimationCacheConfig, results cache.Cache, logger log.Logger, reg prometheus.Registerer) (cache.Cache, error) {
	if cfg.Backend == "" {
		return results, nil
	}

	reg = prometheus.WrapRegistererWith(prometheus.Labels{"component": "query-frontend"}, reg)

	client, err := cache.CreateClient("frontend-cardinality-cache", cfg.BackendConfig, logger, prometheus.WrapRegistererWithPrefix("thanos_", reg))
	if err != nil {
		return nil, err
	} else if client == nil {
		return nil, errUnsupportedResultsCacheBackend(cfg.Backend)
	}

	return cache.NewSpanlessTracingCache(client, logger, tenant.NewMultiResolver()), nil
}
