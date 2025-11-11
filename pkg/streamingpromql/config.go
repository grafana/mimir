// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"flag"
	"fmt"
	"math"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/cache"
)

type EngineOpts struct {
	CommonOpts promql.EngineOpts `yaml:"-"`

	ActiveQueryTracker QueryTracker `yaml:"-"`
	Logger             log.Logger   `yaml:"-"`

	// When operating in pedantic mode, we panic if memory consumption is > 0 after Query.Close()
	// (indicating something was not returned to a pool).
	// Should only be used in tests.
	Pedantic bool `yaml:"-"`

	// Prometheus' engine evaluates all selectors (ie. calls Querier.Select()) before evaluating any part of the query.
	// We rely on this behavior in query-frontends when evaluating shardable queries so that all selectors are evaluated in parallel.
	// When sharding is just another optimization pass, we'll be able to trigger this eager loading from the sharding operator,
	// but for now, we use this option to change the behavior of selectors.
	EagerLoadSelectors bool `yaml:"-"`

	EnablePruneToggles                                                            bool `yaml:"enable_prune_toggles" category:"experimental"`
	EnableCommonSubexpressionElimination                                          bool `yaml:"enable_common_subexpression_elimination" category:"experimental"`
	EnableCommonSubexpressionEliminationForRangeVectorExpressionsInInstantQueries bool `yaml:"enable_common_subexpression_elimination_for_range_vector_expressions_in_instant_queries" category:"experimental"`
	EnableSkippingHistogramDecoding                                               bool `yaml:"enable_skipping_histogram_decoding" category:"experimental"`
	EnableNarrowBinarySelectors                                                   bool `yaml:"enable_narrow_binary_selectors" category:"experimental"`
	EnableEliminateDeduplicateAndMerge                                            bool `yaml:"enable_eliminate_deduplicate_and_merge" category:"experimental"`

	// InstantQuerySplitting configures splitting range vector queries in instant queries into smaller blocks.
	// This enables caching intermediate results and can help with memory management.
	InstantQuerySplitting QuerySplittingConfig `yaml:"instant_query_splitting" category:"experimental"`

	// Intermediate result cache instance (populated from InstantQuerySplitting config)
	IntermediateResultCache cache.IntermediateResultsCache `yaml:"-"`
}

// QuerySplittingConfig configures query splitting for range vector queries in instant queries.
type QuerySplittingConfig struct {
	// Enabled enables splitting range vector queries into smaller blocks.
	// When enabled, queries like rate(metric[6h]) are split into multiple blocks based on SplitInterval.
	Enabled bool `yaml:"enabled" category:"experimental"`

	// SplitInterval is the time interval used for splitting range vector computations into cacheable blocks.
	// For example, with a 2-hour interval, rate(metric[6h]) will be split into 3 blocks of 2 hours each.
	// Must be greater than 0. Defaults to 2 hours if not specified.
	SplitInterval time.Duration `yaml:"split_interval" category:"experimental"`

	// IntermediateResultsCache configures caching of intermediate results from split queries.
	// If not configured, query splitting will still work but results won't be cached.
	IntermediateResultsCache cache.ResultsCacheConfig `yaml:"intermediate_results_cache" category:"experimental"`
}

func (o *EngineOpts) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&o.EnablePruneToggles, "querier.mimir-query-engine.enable-prune-toggles", true, "Enable pruning query expressions that are toggled off with constants.")
	f.BoolVar(&o.EnableCommonSubexpressionElimination, "querier.mimir-query-engine.enable-common-subexpression-elimination", true, "Enable common subexpression elimination when evaluating queries.")
	f.BoolVar(&o.EnableCommonSubexpressionEliminationForRangeVectorExpressionsInInstantQueries, "querier.mimir-query-engine.enable-common-subexpression-elimination-for-range-vector-expressions-in-instant-queries", true, "Enable common subexpression elimination for range vector expressions when evaluating instant queries. This has no effect if common subexpression elimination is disabled.")
	f.BoolVar(&o.EnableSkippingHistogramDecoding, "querier.mimir-query-engine.enable-skipping-histogram-decoding", true, "Enable skipping decoding native histograms when evaluating queries that do not require full histograms.")
	f.BoolVar(&o.EnableNarrowBinarySelectors, "querier.mimir-query-engine.enable-narrow-binary-selectors", false, "Enable generating selectors for one side of a binary expression based on results from the other side.")
	f.BoolVar(&o.EnableEliminateDeduplicateAndMerge, "querier.mimir-query-engine.enable-eliminate-deduplicate-and-merge", false, "Enable eliminating redundant DeduplicateAndMerge nodes from the query plan when it can be proven that each input series produces a unique output series.")
	o.InstantQuerySplitting.RegisterFlags(f)
}

// RegisterFlags registers flags for query splitting configuration.
func (c *QuerySplittingConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&c.Enabled, "querier.mimir-query-engine.instant-query-splitting.enabled", false, "Enable splitting range vector queries in instant queries into smaller blocks for caching and memory management.")
	f.DurationVar(&c.SplitInterval, "querier.mimir-query-engine.instant-query-splitting.split-interval", 2*time.Hour, "Time interval used for splitting range vector computations into cacheable blocks. For example, with a 2-hour interval, rate(metric[6h]) will be split into 3 blocks of 2 hours each. Must be greater than 0.")
	c.IntermediateResultsCache.RegisterFlagsWithPrefix(f, "querier.mimir-query-engine.instant-query-splitting.")
}

// Validate validates the query splitting configuration.
func (c *QuerySplittingConfig) Validate() error {
	if c.Enabled && c.IntermediateResultsCache.Backend == "" {
		return fmt.Errorf("instant query splitting is enabled but intermediate results cache backend is not configured")
	}
	if err := c.IntermediateResultsCache.Validate(); err != nil {
		return errors.Wrap(err, "invalid intermediate results cache config")
	}
	return nil
}

func NewTestEngineOpts() EngineOpts {
	return EngineOpts{
		CommonOpts: promql.EngineOpts{
			Logger:                   nil,
			Reg:                      nil,
			MaxSamples:               math.MaxInt,
			Timeout:                  100 * time.Second,
			EnableAtModifier:         true,
			EnableNegativeOffset:     true,
			NoStepSubqueryIntervalFn: func(int64) int64 { return time.Minute.Milliseconds() },
		},

		Pedantic: true,
		Logger:   log.NewNopLogger(),

		EnablePruneToggles:                   true,
		EnableCommonSubexpressionElimination: true,
		EnableCommonSubexpressionEliminationForRangeVectorExpressionsInInstantQueries: true,
		EnableSkippingHistogramDecoding:                                               true,
		EnableNarrowBinarySelectors:                                                   true,
		EnableEliminateDeduplicateAndMerge:                                            true,
	}
}
