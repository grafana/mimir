// SPDX-License-Identifier: AGPL-3.0-only

package engine

import (
	"flag"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/util/activitytracker" //lint:ignore faillint activitytracker is fine
)

// Config holds the PromQL engine config exposed by Mimir.
type Config struct {
	MaxConcurrent int           `yaml:"max_concurrent"`
	Timeout       time.Duration `yaml:"timeout"`
	MaxSamples    int           `yaml:"max_samples"`

	// The default evaluation interval for the promql engine.
	// Needs to be configured for subqueries to work as it is the default
	// step if not specified.
	DefaultEvaluationInterval time.Duration `yaml:"default_evaluation_interval"`

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
	f.DurationVar(&cfg.DefaultEvaluationInterval, "querier.default-evaluation-interval", time.Minute, sharedWithQueryFrontend("The default evaluation interval or step size for subqueries."))
	f.DurationVar(&cfg.LookbackDelta, "querier.lookback-delta", 5*time.Minute, sharedWithQueryFrontend("Time since the last sample after which a time series is considered stale and ignored by expression evaluations."))
}

// NewPromQLEngineOptions returns the PromQL engine options based on the provided config.
func NewPromQLEngineOptions(cfg Config, activityTracker *activitytracker.ActivityTracker, logger log.Logger, reg prometheus.Registerer) promql.EngineOpts {
	return promql.EngineOpts{
		Logger:               logger,
		Reg:                  reg,
		ActiveQueryTracker:   newQueryTracker(cfg.MaxConcurrent, activityTracker),
		MaxSamples:           cfg.MaxSamples,
		Timeout:              cfg.Timeout,
		LookbackDelta:        cfg.LookbackDelta,
		EnableAtModifier:     true,
		EnableNegativeOffset: false, // If this can be enabled, please change the error mapping in errorTranslateQueryEngine.
		NoStepSubqueryIntervalFn: func(int64) int64 {
			return cfg.DefaultEvaluationInterval.Milliseconds()
		},
	}
}
