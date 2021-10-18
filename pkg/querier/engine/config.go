// SPDX-License-Identifier: AGPL-3.0-only

package engine

import (
	"flag"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
)

// Config holds the PromQL engine config exposed by Mimir.
type Config struct {
	MaxConcurrent     int           `yaml:"max_concurrent"`
	Timeout           time.Duration `yaml:"timeout"`
	MaxSamples        int           `yaml:"max_samples"`
	AtModifierEnabled bool          `yaml:"at_modifier_enabled"`

	// The default evaluation interval for the promql engine.
	// Needs to be configured for subqueries to work as it is the default
	// step if not specified.
	DefaultEvaluationInterval time.Duration `yaml:"default_evaluation_interval"`

	// Directory for ActiveQueryTracker. If empty, ActiveQueryTracker will be disabled and MaxConcurrent will not be applied (!).
	// ActiveQueryTracker logs queries that were active during the last crash, but logs them on the next startup.
	// However, we need to use active query tracker, otherwise we cannot limit Max Concurrent queries in the PromQL
	// engine.
	ActiveQueryTrackerDir string `yaml:"active_query_tracker_dir"`

	// LookbackDelta determines the time since the last sample after which a time
	// series is considered stale.
	LookbackDelta time.Duration `yaml:"lookback_delta"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	sharedWithQueryFrontend := func(help string) string {
		if !strings.HasSuffix(help, " ") {
			help = help + " "
		}

		return help + "This config option should be set on query-frontend too when query sharding is enabled."
	}

	f.IntVar(&cfg.MaxConcurrent, "querier.max-concurrent", 20, sharedWithQueryFrontend("The maximum number of concurrent queries."))
	f.DurationVar(&cfg.Timeout, "querier.timeout", 2*time.Minute, sharedWithQueryFrontend("The timeout for a query."))
	f.IntVar(&cfg.MaxSamples, "querier.max-samples", 50e6, sharedWithQueryFrontend("Maximum number of samples a single query can load into memory."))
	f.BoolVar(&cfg.AtModifierEnabled, "querier.at-modifier-enabled", false, sharedWithQueryFrontend("Enable the @ modifier in PromQL."))
	f.DurationVar(&cfg.DefaultEvaluationInterval, "querier.default-evaluation-interval", time.Minute, sharedWithQueryFrontend("The default evaluation interval or step size for subqueries."))
	f.DurationVar(&cfg.LookbackDelta, "querier.lookback-delta", 5*time.Minute, sharedWithQueryFrontend("Time since the last sample after which a time series is considered stale and ignored by expression evaluations."))
	f.StringVar(&cfg.ActiveQueryTrackerDir, "querier.active-query-tracker-dir", "./active-query-tracker", sharedWithQueryFrontend("Active query tracker monitors active queries, and writes them to the file in given directory. If any queries are discovered in this file during startup, it will log them to the log file. Setting to empty value disables active query tracker, which also disables -querier.max-concurrent option."))
}

// NewPromQLEngineOptions returns the PromQL engine options based on the provided config.
func NewPromQLEngineOptions(cfg Config, logger log.Logger, reg prometheus.Registerer) promql.EngineOpts {
	return promql.EngineOpts{
		Logger:             logger,
		Reg:                reg,
		ActiveQueryTracker: createActiveQueryTracker(cfg, logger),
		MaxSamples:         cfg.MaxSamples,
		Timeout:            cfg.Timeout,
		LookbackDelta:      cfg.LookbackDelta,
		EnableAtModifier:   cfg.AtModifierEnabled,
		NoStepSubqueryIntervalFn: func(int64) int64 {
			return cfg.DefaultEvaluationInterval.Milliseconds()
		},
	}
}

func createActiveQueryTracker(cfg Config, logger log.Logger) *promql.ActiveQueryTracker {
	dir := cfg.ActiveQueryTrackerDir

	if dir != "" {
		return promql.NewActiveQueryTracker(dir, cfg.MaxConcurrent, logger)
	}

	return nil
}
