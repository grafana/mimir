// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/cache/cache.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package cache

import (
	"fmt"

	"github.com/thanos-io/thanos/pkg/cache"
)

// Cache is a generic interface. Re-mapping Thanos one for convenience (same packages name make it annoying to use).
type Cache = cache.Cache

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
