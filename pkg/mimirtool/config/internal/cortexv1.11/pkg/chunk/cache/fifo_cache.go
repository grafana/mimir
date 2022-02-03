// SPDX-License-Identifier: AGPL-3.0-only

package cache

import (
	"flag"
	"time"
)

type FifoCacheConfig struct {
	MaxSizeBytes string        `yaml:"max_size_bytes"`
	MaxSizeItems int           `yaml:"max_size_items"`
	Validity     time.Duration `yaml:"validity"`

	DeprecatedSize int `yaml:"size"`
}

func (cfg *FifoCacheConfig) RegisterFlagsWithPrefix(prefix, description string, f *flag.FlagSet) {
	f.StringVar(&cfg.MaxSizeBytes, prefix+"fifocache.max-size-bytes", "", description+"Maximum memory size of the cache in bytes. A unit suffix (KB, MB, GB) may be applied.")
	f.IntVar(&cfg.MaxSizeItems, prefix+"fifocache.max-size-items", 0, description+"Maximum number of entries in the cache.")
	f.DurationVar(&cfg.Validity, prefix+"fifocache.duration", 0, description+"The expiry duration for the cache.")

	f.IntVar(&cfg.DeprecatedSize, prefix+"fifocache.size", 0, "Deprecated (use max-size-items or max-size-bytes instead): "+description+"The number of entries to cache. ")
}
