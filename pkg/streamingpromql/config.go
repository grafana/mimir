// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"flag"
	"fmt"
	"math"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/caching"
	rangevectorsplittingcache "github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/rangevectorsplitting/cache"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/splitandcache"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

type EngineOpts struct {
	CommonOpts promql.EngineOpts `yaml:"-"`

	ActiveQueryTracker              QueryTracker                              `yaml:"-"`
	MemoryConsumptionTrackerFactory *limiter.InflightMemoryConsumptionTracker `yaml:"-"`
	Logger                          log.Logger                                `yaml:"-"`

	// When operating in pedantic mode, we panic if memory consumption is > 0 after Query.Close()
	// (indicating something was not returned to a pool).
	// Should only be used in tests.
	Pedantic bool `yaml:"-"`

	// Prometheus' engine evaluates all selectors (ie. calls Querier.Select()) before evaluating any part of the query.
	// We rely on this behavior in query-frontends when evaluating shardable queries so that all selectors are evaluated in parallel.
	// When sharding is just another optimization pass, we'll be able to trigger this eager loading from the sharding operator,
	// but for now, we use this option to change the behavior of selectors.
	EagerLoadSelectors bool `yaml:"-"`

	Limits QueryLimitsProvider `yaml:"-"`

	// TimeNow returns the current time. It is used when materializing range vector splitting operators to compute
	// out-of-order thresholds. Defaults to time.Now if nil. Useful for tests that need a fixed "now".
	TimeNow func() time.Time `yaml:"-"`

	EnablePruneToggles                                        bool `yaml:"enable_prune_toggles" category:"experimental"`
	EnableCommonSubexpressionElimination                      bool `yaml:"enable_common_subexpression_elimination" category:"experimental"`
	EnableSubsetSelectorElimination                           bool `yaml:"enable_subset_selector_elimination" category:"experimental"`
	EnableRangeQueryRangeVectorCommonSubexpressionElimination bool `yaml:"enable_range_query_range_vector_common_subexpression_elimination" category:"experimental"`
	EnableNarrowBinarySelectors                               bool `yaml:"enable_narrow_binary_selectors" category:"experimental"`
	EnableEliminateDeduplicateAndMerge                        bool `yaml:"enable_eliminate_deduplicate_and_merge" category:"experimental"`
	EnableReduceMatchers                                      bool `yaml:"enable_reduce_matchers" category:"experimental"`
	EnableMultiAggregation                                    bool `yaml:"enable_multi_aggregation" category:"experimental"`
	EnableRemoveStaticallyEmptyExpressions                    bool `yaml:"enable_remove_statically_empty_expressions" category:"experimental"`

	RangeVectorSplitting RangeVectorSplittingConfig `yaml:"range_vector_splitting" category:"experimental"`

	// These values are populated from the query-frontend config, so are not exposed as config flags or in the config file.
	// FIXME: Once we no longer support running splitting and caching in the frontend middleware, we can move the options here.
	RangeQuerySplittingAndCaching RangeQuerySplittingAndCachingConfig `yaml:"-"`

	// CachePrefixGenerator should return a prefix for all cache keys for a given context.
	// It should contain the tenant ID and any other relevant information that should be used to partition cache entries.
	CachePrefixGenerator caching.PrefixGenerator `yaml:"-"`
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
	IntermediateResultsCache rangevectorsplittingcache.Config `yaml:"intermediate_results_cache" category:"experimental"`
}

type RangeQuerySplittingAndCachingConfig struct {
	SplitEnabled  bool
	SplitInterval time.Duration
	CacheEnabled  bool

	// MinCacheExtent is the minimum length of a cached extent for it to be used.
	// Extents smaller than this are ignored and re-evaluated, to avoid freshly evaluating many small extents.
	// If the desired time range is smaller than MinCacheExtent, then MinCacheExtent is ignored and all cache extents are used.
	// A value of zero disables small extent avoidance.
	MinCacheExtent time.Duration

	// FIXME: Once we no longer support running splitting and caching in the frontend middleware, move the cache client options here.
	CacheClient cache.Cache

	// See the comment where this field is set in createQueryFrontendPromQLEngineOptions for an explanation of why
	// we don't just register these metrics in the engine like all other metrics.
	CacheMetrics *splitandcache.ResultsCacheMetrics
}

func (o *EngineOpts) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&o.EnablePruneToggles, "querier.mimir-query-engine.enable-prune-toggles", true, "Enable pruning query expressions that are toggled off with constants.")
	f.BoolVar(&o.EnableCommonSubexpressionElimination, "querier.mimir-query-engine.enable-common-subexpression-elimination", true, "Enable common subexpression elimination when evaluating queries.")
	f.BoolVar(&o.EnableSubsetSelectorElimination, "querier.mimir-query-engine.enable-subset-selector-elimination", false, "Enable subset selector elimination when evaluating queries.")
	f.BoolVar(&o.EnableRangeQueryRangeVectorCommonSubexpressionElimination, "querier.mimir-query-engine.enable-range-query-range-vector-common-subexpression-elimination", false, "Enable deduplication of range vector selectors in range queries as part of common subexpression elimination. Requires common subexpression elimination to be enabled.")
	f.BoolVar(&o.EnableNarrowBinarySelectors, "querier.mimir-query-engine.enable-narrow-binary-selectors", false, "Enable generating selectors for one side of a binary expression based on results from the other side.")
	f.BoolVar(&o.EnableEliminateDeduplicateAndMerge, "querier.mimir-query-engine.enable-eliminate-deduplicate-and-merge", true, "Enable eliminating redundant DeduplicateAndMerge nodes from the query plan when it can be proven that each input series produces a unique output series.")
	f.BoolVar(&o.EnableReduceMatchers, "querier.mimir-query-engine.enable-reduce-matchers", true, "Enable eliminating duplicate or redundant matchers that are part of selector expressions.")
	f.BoolVar(&o.EnableMultiAggregation, "querier.mimir-query-engine.enable-multi-aggregation", true, "Enable computing multiple aggregations over the same data without buffering. Requires common subexpression elimination to be enabled.")
	f.BoolVar(&o.EnableRemoveStaticallyEmptyExpressions, "querier.mimir-query-engine.enable-remove-statically-empty-expressions", true, "Enable removing expressions that are guaranteed to produce no results.")

	o.RangeVectorSplitting.RegisterFlags(f)
}

func (c *RangeVectorSplittingConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&c.Enabled, "querier.mimir-query-engine.range-vector-splitting.enabled", false, "Enable splitting function over range vectors queries into smaller blocks for caching.")
	f.DurationVar(&c.SplitInterval, "querier.mimir-query-engine.range-vector-splitting.split-interval", 2*time.Hour, "Time interval used for splitting function over range vectors queries into cacheable blocks.")
	c.IntermediateResultsCache.RegisterFlagsWithPrefix(f, "querier.mimir-query-engine.range-vector-splitting.")
}

func (o *EngineOpts) Validate() error {
	return o.RangeVectorSplitting.Validate()
}

func (c *RangeVectorSplittingConfig) Validate() error {
	if c.Enabled {
		if c.SplitInterval <= 0 {
			return fmt.Errorf("range vector splitting is enabled but split interval is not greater than 0")
		}
		if c.IntermediateResultsCache.Backend == "" {
			return fmt.Errorf("range vector splitting is enabled but intermediate results cache backend is not configured")
		}
		if err := c.IntermediateResultsCache.Validate(); err != nil {
			return errors.Wrap(err, "invalid intermediate results cache config")
		}
	}
	return nil
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
			Parser:                   promqlext.NewPromQLParser(),
		},

		Pedantic: true,
		Logger:   log.NewNopLogger(),
		Limits:   NewStaticQueryLimitsProvider(),

		EnablePruneToggles:                                        true,
		EnableCommonSubexpressionElimination:                      true,
		EnableSubsetSelectorElimination:                           true,
		EnableNarrowBinarySelectors:                               true,
		EnableEliminateDeduplicateAndMerge:                        true,
		EnableReduceMatchers:                                      true,
		EnableMultiAggregation:                                    true,
		EnableRemoveStaticallyEmptyExpressions:                    true,
		EnableRangeQueryRangeVectorCommonSubexpressionElimination: true,

		CachePrefixGenerator: caching.TenantPrefixGenerator,
	}
}
