// SPDX-License-Identifier: AGPL-3.0-only

package hlltracker

import (
	"flag"
)

// Config holds configuration for the HLL-based partition series tracker.
type Config struct {
	Enabled bool `yaml:"enabled" category:"experimental"`

	// MaxSeriesPerPartition is the global limit on the number of series per partition
	// across all tenants in the configured time window. This is analogous to ingester
	// instance limits. 0 = disabled.
	// Default: 3000000 (3 million)
	MaxSeriesPerPartition int `yaml:"max_series_per_partition" category:"experimental"`

	// TimeWindowMinutes is the number of minutes to track series cardinality.
	// Default: 20
	TimeWindowMinutes int `yaml:"time_window_minutes" category:"experimental"`

	// UpdateIntervalSeconds is how often to push local HLL state to KV store.
	// This is not used in Phase 1 (local-only mode).
	// Default: 1
	UpdateIntervalSeconds int `yaml:"update_interval_seconds" category:"experimental"`

	// HLLPrecision controls the HLL precision parameter (log2(m) where m is
	// the number of registers). Higher precision = lower error but more memory.
	// Valid range: 4-18. Default: 11 (2048 registers, ~2KB per HLL, ~5-6% error)
	HLLPrecision int `yaml:"hll_precision" category:"experimental"`
}

// RegisterFlags registers flags for the Config.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled,
		"distributor.partition-series-tracker.enabled",
		false,
		"Enable distributed partition series tracking using HyperLogLog. This feature is experimental.")

	f.IntVar(&cfg.MaxSeriesPerPartition,
		"distributor.partition-series-tracker.max-series-per-partition",
		3000000,
		"Global maximum number of series per partition across all tenants in the configured time window. This is analogous to ingester instance limits. 0 = disabled.")

	f.IntVar(&cfg.TimeWindowMinutes,
		"distributor.partition-series-tracker.time-window-minutes",
		20,
		"Time window in minutes for tracking partition series cardinality.")

	f.IntVar(&cfg.UpdateIntervalSeconds,
		"distributor.partition-series-tracker.update-interval-seconds",
		1,
		"How often to push local HLL state to memberlist KV, in seconds. Currently unused in Phase 1.")

	f.IntVar(&cfg.HLLPrecision,
		"distributor.partition-series-tracker.hll-precision",
		11,
		"HyperLogLog precision (log2 of number of registers). Valid range: 4-18. Default 11 (2048 registers, ~2KB, ~5-6% error). Higher = more accurate but more memory.")
}
