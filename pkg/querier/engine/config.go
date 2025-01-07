// SPDX-License-Identifier: AGPL-3.0-only

package engine

import (
	"flag"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql"      //lint:ignore faillint streamingpromql is fine
	"github.com/grafana/mimir/pkg/util/activitytracker" //lint:ignore faillint activitytracker is fine
	util_log "github.com/grafana/mimir/pkg/util/log"    //lint:ignore faillint log is fine
)

// Config holds the PromQL engine config exposed by Mimir.
type Config struct {
	MaxConcurrent int           `yaml:"max_concurrent"`
	Timeout       time.Duration `yaml:"timeout"`
	MaxSamples    int           `yaml:"max_samples"`

	// The default evaluation interval for the promql engine.
	// Needs to be configured for subqueries to work as it is the default
	// step if not specified.
	DefaultEvaluationInterval time.Duration `yaml:"default_evaluation_interval" category:"advanced"`

	// LookbackDelta determines the time since the last sample after which a time
	// series is considered stale.
	LookbackDelta time.Duration `yaml:"lookback_delta" category:"advanced"`

	PromQLExperimentalFunctionsEnabled bool `yaml:"promql_experimental_functions_enabled" category:"experimental"`

	MimirQueryEngine streamingpromql.FeatureToggles `yaml:"mimir_query_engine" category:"experimental"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	sharedWithQueryFrontend := func(help string) string {
		if !strings.HasSuffix(help, " ") {
			help = help + " "
		}

		return help + "This config option should be set on query-frontend too when query sharding is enabled."
	}

	f.IntVar(&cfg.MaxConcurrent, "querier.max-concurrent", 20, "The number of workers running in each querier process. This setting limits the maximum number of concurrent queries in each querier. The minimum value is four; lower values are ignored and set to the minimum")
	f.DurationVar(&cfg.Timeout, "querier.timeout", 2*time.Minute, sharedWithQueryFrontend("The timeout for a query.")+" This also applies to queries evaluated by the ruler (internally or remotely).")
	f.IntVar(&cfg.MaxSamples, "querier.max-samples", 50e6, sharedWithQueryFrontend("Maximum number of samples a single query can load into memory."))
	f.DurationVar(&cfg.DefaultEvaluationInterval, "querier.default-evaluation-interval", time.Minute, sharedWithQueryFrontend("The default evaluation interval or step size for subqueries."))
	f.DurationVar(&cfg.LookbackDelta, "querier.lookback-delta", 5*time.Minute, sharedWithQueryFrontend("Time since the last sample after which a time series is considered stale and ignored by expression evaluations."))
	f.BoolVar(&cfg.PromQLExperimentalFunctionsEnabled, "querier.promql-experimental-functions-enabled", false, sharedWithQueryFrontend("Enable experimental PromQL functions."))

	cfg.MimirQueryEngine.RegisterFlags(f)
}

// NewPromQLEngineOptions returns the PromQL engine options based on the provided config and a boolean
// to indicate whether the experimental PromQL functions should be enabled.
func NewPromQLEngineOptions(cfg Config, activityTracker *activitytracker.ActivityTracker, logger log.Logger, reg prometheus.Registerer) (promql.EngineOpts, streamingpromql.EngineOpts, bool) {
	commonOpts := promql.EngineOpts{
		Logger:               util_log.SlogFromGoKit(logger),
		Reg:                  reg,
		ActiveQueryTracker:   newQueryTracker(activityTracker),
		MaxSamples:           cfg.MaxSamples,
		Timeout:              cfg.Timeout,
		LookbackDelta:        cfg.LookbackDelta,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
		NoStepSubqueryIntervalFn: func(int64) int64 {
			return cfg.DefaultEvaluationInterval.Milliseconds()
		},
	}

	mqeOpts := streamingpromql.EngineOpts{
		CommonOpts:     commonOpts,
		FeatureToggles: cfg.MimirQueryEngine,
	}

	return commonOpts, mqeOpts, cfg.PromQLExperimentalFunctionsEnabled
}
