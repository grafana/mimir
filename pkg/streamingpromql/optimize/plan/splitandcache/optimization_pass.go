// SPDX-License-Identifier: AGPL-3.0-only

package splitandcache

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/timestamp"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type OptimizationPass struct {
	splitEnabled  bool
	splitInterval time.Duration
	cacheEnabled  bool

	limits              LimitsProvider
	cacheSkippedCounter *prometheus.CounterVec
	timeNow             func() time.Time
}

func NewOptimizationPass(splitEnabled bool, splitInterval time.Duration, cacheEnabled bool, limits LimitsProvider, reg prometheus.Registerer) *OptimizationPass {
	return &OptimizationPass{
		splitEnabled:        splitEnabled,
		splitInterval:       splitInterval,
		cacheEnabled:        cacheEnabled,
		limits:              limits,
		cacheSkippedCounter: NewQueryResultCacheSkippedCounter(reg),
		timeNow:             time.Now,
	}
}

// OverrideTimeNow overrides the time.Now function used by this optimization pass.
// This should only be used for testing purposes.
func (o *OptimizationPass) OverrideTimeNow(f func() time.Time) {
	o.timeNow = f
}

func (o *OptimizationPass) Name() string {
	return "Range query splitting and caching"
}

func (o *OptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, _ planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	// We do not need to check the maximum supported query plan version here: the nodes injected by this optimization pass
	// only run in query-frontends (and therefore would only run inside this process).

	if plan.Parameters.TimeRange.IsInstant {
		// Nothing to do.
		return plan, nil
	}

	now := o.timeNow()
	maxFreshness, err := o.limits.GetMaxCacheFreshness(ctx)
	if err != nil {
		return nil, err
	}
	freshnessThreshold := now.Add(-maxFreshness)

	root := plan.Root
	root, err = o.applyCaching(ctx, root, plan.Parameters.TimeRange, freshnessThreshold)
	if err != nil {
		return nil, err
	}

	root = o.applySplitting(root)

	plan.Root = root
	return plan, nil
}

func (o *OptimizationPass) applyCaching(ctx context.Context, node planning.Node, timeRange types.QueryTimeRange, freshnessThreshold time.Time) (planning.Node, error) {
	if !o.cacheEnabled || requestoptions.OptionsFromContext(ctx).CacheDisabled {
		return node, nil
	}

	if timeRange.StartT > timestamp.FromTime(freshnessThreshold) {
		// The query starts after the freshness threshold, so the entire query is not cacheable.
		// If the query straddles the freshness threshold, the cache operator will only cache the
		// portion of the query that is before the freshness threshold.
		o.cacheSkippedCounter.WithLabelValues(NotCachableReasonTooNew).Inc()
		return node, nil
	}

	if !isStepAligned(timeRange) {
		if allowed, err := o.limits.AllowCachingUnalignedQueries(ctx); err != nil {
			return nil, err
		} else if !allowed {
			o.cacheSkippedCounter.WithLabelValues(NotCachableReasonUnalignedTimeRange).Inc()
			return node, nil
		}
	}

	if !o.modifiersAllowCaching(node, timeRange, freshnessThreshold) {
		o.cacheSkippedCounter.WithLabelValues(NotCachableReasonModifiersNotCachable).Inc()
		return node, nil
	}

	return &Cache{
		CacheDetails: &CacheDetails{},
		Inner:        node,
	}, nil
}

func (o *OptimizationPass) modifiersAllowCaching(node planning.Node, timeRange types.QueryTimeRange, freshnessThreshold time.Time) bool {
	switch node := node.(type) {
	case *core.VectorSelector:
		return node.Offset >= 0 && timestampAllowsCaching(node.Timestamp, timeRange, freshnessThreshold)

	case *core.MatrixSelector:
		return node.Offset >= 0 && timestampAllowsCaching(node.Timestamp, timeRange, freshnessThreshold)

	case *core.Subquery:
		return node.Offset >= 0 && timestampAllowsCaching(node.Timestamp, timeRange, freshnessThreshold) && o.modifiersAllowCaching(node.Inner, timeRange, freshnessThreshold)

	default:
		for n := range planning.ChildrenIter(node) {
			if !o.modifiersAllowCaching(n, timeRange, freshnessThreshold) {
				return false
			}
		}

		return true
	}
}

func timestampAllowsCaching(ts *time.Time, timeRange types.QueryTimeRange, freshnessThreshold time.Time) bool {
	if ts == nil {
		return true
	}

	return timestamp.FromTime(*ts) <= timeRange.EndT && freshnessThreshold.After(*ts)
}

func isStepAligned(timeRange types.QueryTimeRange) bool {
	return timeRange.StartT%timeRange.IntervalMilliseconds == 0
}

func (o *OptimizationPass) applySplitting(node planning.Node) planning.Node {
	if !o.splitEnabled {
		return node
	}

	// If splitting is enabled, we always add a TimeRangeSplit node, regardless of the time range.
	// This keeps the split time range calculation logic in one place, and simplifies the logic here.
	// The materializer for TimeRangeSplit will omit the splitting operator if there is only one range.

	return &TimeRangeSplit{
		TimeRangeSplitDetails: &TimeRangeSplitDetails{
			SplitInterval: o.splitInterval,
		},
		Inner: node,
	}

}
