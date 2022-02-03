// SPDX-License-Identifier: AGPL-3.0-only

package cache

import (
	"flag"
	"time"
)

type MemcachedConfig struct {
	Expiration time.Duration `yaml:"expiration"`

	BatchSize   int `yaml:"batch_size"`
	Parallelism int `yaml:"parallelism"`
}

func (cfg *MemcachedConfig) RegisterFlagsWithPrefix(prefix, description string, f *flag.FlagSet) {
	f.DurationVar(&cfg.Expiration, prefix+"memcached.expiration", 0, description+"How long keys stay in the memcache.")
	f.IntVar(&cfg.BatchSize, prefix+"memcached.batchsize", 1024, description+"How many keys to fetch in each batch.")
	f.IntVar(&cfg.Parallelism, prefix+"memcached.parallelism", 100, description+"Maximum active requests to memcache.")
}
