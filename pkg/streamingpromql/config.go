// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"flag"
	"math"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
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
	EnableReduceMatchers                                                          bool `yaml:"enable_reduce_matchers" category:"experimental"`
	EnableProjectionPushdown                                                      bool `yaml:"enable_projection_pushdown" category:"experimental"`
}

func (o *EngineOpts) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&o.EnablePruneToggles, "querier.mimir-query-engine.enable-prune-toggles", true, "Enable pruning query expressions that are toggled off with constants.")
	f.BoolVar(&o.EnableCommonSubexpressionElimination, "querier.mimir-query-engine.enable-common-subexpression-elimination", true, "Enable common subexpression elimination when evaluating queries.")
	f.BoolVar(&o.EnableCommonSubexpressionEliminationForRangeVectorExpressionsInInstantQueries, "querier.mimir-query-engine.enable-common-subexpression-elimination-for-range-vector-expressions-in-instant-queries", true, "Enable common subexpression elimination for range vector expressions when evaluating instant queries. This has no effect if common subexpression elimination is disabled.")
	f.BoolVar(&o.EnableSkippingHistogramDecoding, "querier.mimir-query-engine.enable-skipping-histogram-decoding", true, "Enable skipping decoding native histograms when evaluating queries that do not require full histograms.")
	f.BoolVar(&o.EnableNarrowBinarySelectors, "querier.mimir-query-engine.enable-narrow-binary-selectors", false, "Enable generating selectors for one side of a binary expression based on results from the other side.")
	f.BoolVar(&o.EnableEliminateDeduplicateAndMerge, "querier.mimir-query-engine.enable-eliminate-deduplicate-and-merge", false, "Enable eliminating redundant DeduplicateAndMerge nodes from the query plan when it can be proven that each input series produces a unique output series.")
	f.BoolVar(&o.EnableReduceMatchers, "querier.mimir-query-engine.enable-reduce-matchers", true, "Enable eliminating duplicate or redundant matchers that are part of selector expressions.")
	f.BoolVar(&o.EnableProjectionPushdown, "querier.mimir-query-engine.enable-projection-pushdown", false, "Enable projection pushdown to only fetch labels required for the query from storage.")
}

func NewTestEngineOpts() EngineOpts {
	return EngineOpts{
		CommonOpts: promql.EngineOpts{
			Logger:                   nil,
			Reg:                      prometheus.NewPedanticRegistry(),
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
		EnableReduceMatchers:                                                          true,
		EnableProjectionPushdown:                                                      true,
	}
}
