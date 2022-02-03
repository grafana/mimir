// SPDX-License-Identifier: AGPL-3.0-only

package cache

import (
	"flag"
	"time"
)

type Config struct {
	EnableFifoCache bool `yaml:"enable_fifocache"`

	DefaultValidity time.Duration `yaml:"default_validity"`

	Background     BackgroundConfig      `yaml:"background"`
	Memcache       MemcachedConfig       `yaml:"memcached"`
	MemcacheClient MemcachedClientConfig `yaml:"memcached_client"`
	Redis          RedisConfig           `yaml:"redis"`
	Fifocache      FifoCacheConfig       `yaml:"fifocache"`

	// This is to name the cache metrics properly.
	Prefix string `yaml:"prefix" doc:"hidden"`

	// For tests to inject specific implementations.
}

func (cfg *Config) RegisterFlagsWithPrefix(prefix string, description string, f *flag.FlagSet) {
	cfg.Background.RegisterFlagsWithPrefix(prefix, description, f)
	cfg.Memcache.RegisterFlagsWithPrefix(prefix, description, f)
	cfg.MemcacheClient.RegisterFlagsWithPrefix(prefix, description, f)
	cfg.Redis.RegisterFlagsWithPrefix(prefix, description, f)
	cfg.Fifocache.RegisterFlagsWithPrefix(prefix, description, f)

	f.BoolVar(&cfg.EnableFifoCache, prefix+"cache.enable-fifocache", false, description+"Enable in-memory cache.")
	f.DurationVar(&cfg.DefaultValidity, prefix+"default-validity", 0, description+"The default validity of entries for caches unless overridden.")

	cfg.Prefix = prefix
}
