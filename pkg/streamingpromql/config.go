// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"flag"
	"fmt"
	"math"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/rangevectorsplitting/cache"
)

type Limits interface {
	OutOfOrderTimeWindow(userID string) time.Duration
}

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

	Limits Limits `yaml:"-"`

	EnablePruneToggles                   bool `yaml:"enable_prune_toggles" category:"experimental"`
	EnableCommonSubexpressionElimination bool `yaml:"enable_common_subexpression_elimination" category:"experimental"`
	EnableNarrowBinarySelectors          bool `yaml:"enable_narrow_binary_selectors" category:"experimental"`
	EnableEliminateDeduplicateAndMerge   bool `yaml:"enable_eliminate_deduplicate_and_merge" category:"experimental"`
	EnableReduceMatchers                 bool `yaml:"enable_reduce_matchers" category:"experimental"`
	EnableProjectionPushdown             bool `yaml:"enable_projection_pushdown" category:"experimental"`
	EnableMultiAggregation               bool `yaml:"enable_multi_aggregation" category:"experimental"`

	RangeVectorSplitting RangeVectorSplittingConfig `yaml:"range_vector_splitting" category:"experimental"`
}

// RangeVectorSplittingConfig configures the splitting of functions over range vectors queries.
type RangeVectorSplittingConfig struct {
	Enabled bool `yaml:"enabled" category:"experimental"`

	// SplitInterval is the time interval used for splitting.
	// Must be greater than 0. Defaults to 2 hours if not specified.
	SplitInterval time.Duration `yaml:"split_interval" category:"experimental"`

	// IntermediateResultsCache configures caching of intermediate results from split queries.
	// TODO: consider making the cache an optional part of query splitting. We might want to just do query splitting
	//  without caching (e.g. possibly if splitting is extended to range queries in the future, or if we add
	//  parallelisation and just want to use query splitting for that and not cache).
	IntermediateResultsCache cache.Config `yaml:"intermediate_results_cache" category:"experimental"`
}

func (o *EngineOpts) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&o.EnablePruneToggles, "querier.mimir-query-engine.enable-prune-toggles", true, "Enable pruning query expressions that are toggled off with constants.")
	f.BoolVar(&o.EnableCommonSubexpressionElimination, "querier.mimir-query-engine.enable-common-subexpression-elimination", true, "Enable common subexpression elimination when evaluating queries.")
	f.BoolVar(&o.EnableNarrowBinarySelectors, "querier.mimir-query-engine.enable-narrow-binary-selectors", false, "Enable generating selectors for one side of a binary expression based on results from the other side.")
	f.BoolVar(&o.EnableEliminateDeduplicateAndMerge, "querier.mimir-query-engine.enable-eliminate-deduplicate-and-merge", true, "Enable eliminating redundant DeduplicateAndMerge nodes from the query plan when it can be proven that each input series produces a unique output series.")
	f.BoolVar(&o.EnableReduceMatchers, "querier.mimir-query-engine.enable-reduce-matchers", true, "Enable eliminating duplicate or redundant matchers that are part of selector expressions.")
	f.BoolVar(&o.EnableProjectionPushdown, "querier.mimir-query-engine.enable-projection-pushdown", false, "Enable projection pushdown to only fetch labels required for the query from storage.")
	f.BoolVar(&o.EnableMultiAggregation, "querier.mimir-query-engine.enable-multi-aggregation", true, "Enable computing multiple aggregations over the same data without buffering. Requires common subexpression elimination to be enabled.")

	o.RangeVectorSplitting.RegisterFlags(f)
}

func (c *RangeVectorSplittingConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&c.Enabled, "querier.mimir-query-engine.range-vector-splitting.enabled", false, "Enable splitting function over range vectors queries into smaller blocks for caching.")
	f.DurationVar(&c.SplitInterval, "querier.mimir-query-engine.range-vector-splitting.split-interval", 2*time.Hour, "Time interval used for splitting function over range vectors queries into cacheable blocks.")
	c.IntermediateResultsCache.RegisterFlagsWithPrefix(f, "querier.mimir-query-engine.range-vector-splitting.")
}

func (c *RangeVectorSplittingConfig) Validate() error {
	if c.Enabled && c.IntermediateResultsCache.Backend == "" {
		return fmt.Errorf("range vector splitting is enabled but intermediate results cache backend is not configured")
	}
	if err := c.IntermediateResultsCache.Validate(); err != nil {
		return errors.Wrap(err, "invalid intermediate results cache config")
	}
	return nil
}

type noopLimits struct{}

func (noopLimits) OutOfOrderTimeWindow(string) time.Duration { return 0 }

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
		Limits:   noopLimits{},

		EnablePruneToggles:                   true,
		EnableCommonSubexpressionElimination: true,
		EnableNarrowBinarySelectors:          true,
		EnableEliminateDeduplicateAndMerge:   true,
		EnableReduceMatchers:                 true,
		EnableProjectionPushdown:             true,
		EnableMultiAggregation:               true,
	}
}
