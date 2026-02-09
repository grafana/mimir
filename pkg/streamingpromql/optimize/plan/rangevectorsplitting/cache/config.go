// SPDX-License-Identifier: AGPL-3.0-only

package cache

import (
	"flag"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/grafana/dskit/cache"
	"github.com/pkg/errors"
)

const (
	// resultsCacheVersion should be increased every time the cache format changes.
	resultsCacheVersion = 1

	defaultTTL = 24 * time.Hour
)

var (
	supportedResultsCacheBackends = []string{cache.BackendMemcached}

	errUnsupportedBackend = errors.New("unsupported cache backend")
)

// Config is the config for the results cache.
type Config struct {
	cache.BackendConfig `yaml:",inline"`
	Compression         cache.CompressionConfig `yaml:",inline"`
}

// RegisterFlags registers flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix(f, "querier.mimir-query-engine.intermediate-results-cache.")
}

// RegisterFlagsWithPrefix registers flags with the given prefix.
func (cfg *Config) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Backend, prefix+"backend", "", fmt.Sprintf("Backend for intermediate results cache, if not empty. Supported values: %s.", strings.Join(supportedResultsCacheBackends, ", ")))
	cfg.Memcached.RegisterFlagsWithPrefix(prefix+"memcached.", f)
	cfg.Compression.RegisterFlagsWithPrefix(f, prefix)
}

func (cfg *Config) Validate() error {
	if cfg.Backend != "" && !slices.Contains(supportedResultsCacheBackends, cfg.Backend) {
		return errUnsupportedResultsCacheBackend(cfg.Backend)
	}

	switch cfg.Backend {
	case cache.BackendMemcached:
		if err := cfg.Memcached.Validate(); err != nil {
			return errors.Wrap(err, "querier intermediate results cache")
		}
	}

	if err := cfg.Compression.Validate(); err != nil {
		return errors.Wrap(err, "querier intermediate results cache")
	}
	return nil
}

func errUnsupportedResultsCacheBackend(backend string) error {
	return fmt.Errorf("%w: %q, supported values: %v", errUnsupportedBackend, backend, supportedResultsCacheBackends)
}
