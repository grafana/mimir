// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/cache/cache.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/cacheutil"
)

// Cache is a generic interface.
type Cache interface {
	// Store data into the cache.
	//
	// Note that individual byte buffers may be retained by the cache!
	Store(ctx context.Context, data map[string][]byte, ttl time.Duration)

	// Fetch multiple keys from cache. Returns map of input keys to data.
	// If key isn't in the map, data for given key was not found.
	Fetch(ctx context.Context, keys []string) map[string][]byte

	Name() string
}

const (
	BackendMemcached = "memcached"
)

type BackendConfig struct {
	Backend   string          `yaml:"backend"`
	Memcached MemcachedConfig `yaml:"memcached"`
}

// Validate the config.
func (cfg *BackendConfig) Validate() error {
	if cfg.Backend != "" && cfg.Backend != BackendMemcached {
		return fmt.Errorf("unsupported cache backend: %s", cfg.Backend)
	}

	if cfg.Backend == BackendMemcached {
		if err := cfg.Memcached.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func CreateClient(cacheName string, cfg BackendConfig, logger log.Logger, reg prometheus.Registerer) (Cache, error) {
	switch cfg.Backend {
	case "":
		// No caching.
		return nil, nil

	case BackendMemcached:
		client, err := cacheutil.NewMemcachedClientWithConfig(logger, cacheName, cfg.Memcached.ToMemcachedClientConfig(), reg)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create memcached client")
		}
		return NewMemcachedCache(cacheName, logger, client, reg), nil

	default:
		return nil, errors.Errorf("unsupported cache type for cache %s: %s", cacheName, cfg.Backend)
	}
}
