// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streamingpromql

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

func init() {
	promqlext.ExtendPromQL()
}

const defaultLookbackDelta = 5 * time.Minute // This should be the same value as github.com/prometheus/prometheus/promql.defaultLookbackDelta.

func NewEngine(opts EngineOpts, limitsProvider QueryLimitsProvider, metrics *stats.QueryMetrics, planner *QueryPlanner, logger log.Logger) (*Engine, error) {
	lookbackDelta := opts.CommonOpts.LookbackDelta
	if lookbackDelta == 0 {
		lookbackDelta = defaultLookbackDelta
	}

	if !opts.CommonOpts.EnableAtModifier {
		return nil, errors.New("disabling @ modifier not supported by Mimir query engine")
	}

	if !opts.CommonOpts.EnableNegativeOffset {
		return nil, errors.New("disabling negative offsets not supported by Mimir query engine")
	}

	if opts.CommonOpts.EnablePerStepStats {
		return nil, errors.New("enabling per-step stats not supported by Mimir query engine")
	}

	if opts.CommonOpts.EnableDelayedNameRemoval {
		return nil, errors.New("enabling delayed name removal not supported by Mimir query engine")
	}

	if opts.UseQueryPlanning && planner == nil {
		return nil, errors.New("query planning enabled but no planner provided")
	}

	return &Engine{
		lookbackDelta:            lookbackDelta,
		timeout:                  opts.CommonOpts.Timeout,
		limitsProvider:           limitsProvider,
		activeQueryTracker:       opts.CommonOpts.ActiveQueryTracker,
		noStepSubqueryIntervalFn: opts.CommonOpts.NoStepSubqueryIntervalFn,

		logger: logger,
		estimatedPeakMemoryConsumption: promauto.With(opts.CommonOpts.Reg).NewHistogram(prometheus.HistogramOpts{
			Name:                        "cortex_mimir_query_engine_estimated_query_peak_memory_consumption",
			Help:                        "Estimated peak memory consumption of each query (in bytes)",
			NativeHistogramBucketFactor: 1.1,
		}),
		queriesRejectedDueToPeakMemoryConsumption: metrics.QueriesRejectedTotal.WithLabelValues(stats.RejectReasonMaxEstimatedQueryMemoryConsumption),

		pedantic:         opts.Pedantic,
		useQueryPlanning: opts.UseQueryPlanning,
		planner:          planner,
	}, nil
}

type Engine struct {
	lookbackDelta      time.Duration
	timeout            time.Duration
	limitsProvider     QueryLimitsProvider
	activeQueryTracker promql.QueryTracker

	noStepSubqueryIntervalFn func(rangeMillis int64) int64

	logger                                    log.Logger
	estimatedPeakMemoryConsumption            prometheus.Histogram
	queriesRejectedDueToPeakMemoryConsumption prometheus.Counter

	// When operating in pedantic mode:
	// - Query.Exec() will call Close() on the root operator a second time to ensure it behaves correctly if Close() is called multiple times.
	// - Query.Close() will panic if memory consumption is > 0, which indicates something was not returned to a pool.
	//
	// Pedantic mode should only be enabled in tests. It is not intended to be used in production.
	pedantic bool

	useQueryPlanning bool
	planner          *QueryPlanner
}

func (e *Engine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	if e.useQueryPlanning {
		return e.newQueryFromPlanner(ctx, q, opts, qs, types.NewInstantQueryTimeRange(ts))
	}

	return e.newQueryFromExpression(ctx, q, opts, qs, ts, ts, 0)
}

func (e *Engine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("%v is not a valid interval for a range query, must be greater than 0", interval)
	}

	if end.Before(start) {
		return nil, fmt.Errorf("range query time range is invalid: end time %v is before start time %v", end.Format(time.RFC3339), start.Format(time.RFC3339))
	}

	if e.useQueryPlanning {
		return e.newQueryFromPlanner(ctx, q, opts, qs, types.NewRangeQueryTimeRange(start, end, interval))
	}

	return e.newQueryFromExpression(ctx, q, opts, qs, start, end, interval)
}

func (e *Engine) newQueryFromPlanner(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, timeRange types.QueryTimeRange) (promql.Query, error) {
	plan, err := e.planner.NewQueryPlan(ctx, qs, timeRange, NoopPlanningObserver{})
	if err != nil {
		return nil, err
	}

	query, err := e.Materialize(ctx, plan, q, opts)
	if err != nil {
		return nil, err
	}

	return query, nil
}

type QueryLimitsProvider interface {
	// GetMaxEstimatedMemoryConsumptionPerQuery returns the maximum estimated memory allowed to be consumed by a query in bytes, or 0 to disable the limit.
	GetMaxEstimatedMemoryConsumptionPerQuery(ctx context.Context) (uint64, error)
}

// NewStaticQueryLimitsProvider returns a QueryLimitsProvider that always returns the provided limits.
//
// This should generally only be used in tests.
func NewStaticQueryLimitsProvider(maxEstimatedMemoryConsumptionPerQuery uint64) QueryLimitsProvider {
	return staticQueryLimitsProvider{
		maxEstimatedMemoryConsumptionPerQuery: maxEstimatedMemoryConsumptionPerQuery,
	}
}

type staticQueryLimitsProvider struct {
	maxEstimatedMemoryConsumptionPerQuery uint64
}

func (p staticQueryLimitsProvider) GetMaxEstimatedMemoryConsumptionPerQuery(_ context.Context) (uint64, error) {
	return p.maxEstimatedMemoryConsumptionPerQuery, nil
}
