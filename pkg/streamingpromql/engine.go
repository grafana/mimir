// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streamingpromql

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"go.opentelemetry.io/otel"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/engineopts"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

func init() {
	promqlext.ExtendPromQL()
}

var tracer = otel.Tracer("pkg/streamingpromql")

const defaultLookbackDelta = 5 * time.Minute // This should be the same value as github.com/prometheus/prometheus/promql.defaultLookbackDelta.

func NewEngine(opts engineopts.EngineOpts, limitsProvider QueryLimitsProvider, metrics *stats.QueryMetrics, planner *QueryPlanner, logger log.Logger) (*Engine, error) {
	if !opts.CommonOpts.EnableAtModifier {
		return nil, errors.New("disabling @ modifier not supported by Mimir query engine")
	}

	if !opts.CommonOpts.EnableNegativeOffset {
		return nil, errors.New("disabling negative offsets not supported by Mimir query engine")
	}

	if opts.CommonOpts.EnableDelayedNameRemoval {
		return nil, errors.New("enabling delayed name removal not supported by Mimir query engine")
	}

	if planner == nil {
		return nil, errors.New("no query planner provided")
	}

	activeQueryTracker := opts.CommonOpts.ActiveQueryTracker
	if activeQueryTracker == nil {
		activeQueryTracker = &NoopQueryTracker{}
	}

	return &Engine{
		lookbackDelta:            DetermineLookbackDelta(opts.CommonOpts),
		timeout:                  opts.CommonOpts.Timeout,
		limitsProvider:           limitsProvider,
		activeQueryTracker:       activeQueryTracker,
		noStepSubqueryIntervalFn: opts.CommonOpts.NoStepSubqueryIntervalFn,
		enablePerStepStats:       opts.CommonOpts.EnablePerStepStats,

		logger: logger,
		estimatedPeakMemoryConsumption: promauto.With(opts.CommonOpts.Reg).NewHistogram(prometheus.HistogramOpts{
			Name:                        "cortex_mimir_query_engine_estimated_query_peak_memory_consumption",
			Help:                        "Estimated peak memory consumption of each query (in bytes)",
			NativeHistogramBucketFactor: 1.1,
		}),
		queriesRejectedDueToPeakMemoryConsumption: metrics.QueriesRejectedTotal.WithLabelValues(stats.RejectReasonMaxEstimatedQueryMemoryConsumption),

		pedantic:           opts.Pedantic,
		eagerLoadSelectors: opts.EagerLoadSelectors,
		planner:            planner,
	}, nil
}

func DetermineLookbackDelta(opts promql.EngineOpts) time.Duration {
	lookbackDelta := opts.LookbackDelta
	if lookbackDelta == 0 {
		lookbackDelta = defaultLookbackDelta
	}

	return lookbackDelta
}

type Engine struct {
	lookbackDelta      time.Duration
	timeout            time.Duration
	limitsProvider     QueryLimitsProvider
	activeQueryTracker promql.QueryTracker
	enablePerStepStats bool

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

	eagerLoadSelectors bool

	planner *QueryPlanner
}

func (e *Engine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	return e.newQueryFromPlanner(ctx, q, opts, qs, types.NewInstantQueryTimeRange(ts))
}

func (e *Engine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("%v is not a valid interval for a range query, must be greater than 0", interval)
	}

	if end.Before(start) {
		return nil, fmt.Errorf("range query time range is invalid: end time %v is before start time %v", end.Format(time.RFC3339), start.Format(time.RFC3339))
	}

	return e.newQueryFromPlanner(ctx, q, opts, qs, types.NewRangeQueryTimeRange(start, end, interval))
}

func (e *Engine) newQueryFromPlanner(ctx context.Context, queryable storage.Queryable, opts promql.QueryOpts, qs string, timeRange types.QueryTimeRange) (promql.Query, error) {
	plan, err := e.planner.NewQueryPlan(ctx, qs, timeRange, NoopPlanningObserver{})
	if err != nil {
		return nil, err
	}

	if opts == nil {
		opts = promql.NewPrometheusQueryOpts(false, 0)
	}

	queryID, err := e.activeQueryTracker.Insert(ctx, plan.OriginalExpression+" # (materialization)")
	if err != nil {
		return nil, err
	}

	defer e.activeQueryTracker.Delete(queryID)

	lookbackDelta := opts.LookbackDelta()
	if lookbackDelta == 0 {
		lookbackDelta = e.lookbackDelta
	}

	maxEstimatedMemoryConsumptionPerQuery, err := e.limitsProvider.GetMaxEstimatedMemoryConsumptionPerQuery(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not get memory consumption limit for query: %w", err)
	}

	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, maxEstimatedMemoryConsumptionPerQuery, e.queriesRejectedDueToPeakMemoryConsumption, plan.OriginalExpression)

	operatorParams := &planning.OperatorParameters{
		Queryable:                queryable,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		Annotations:              annotations.New(),
		LookbackDelta:            lookbackDelta,
		EagerLoadSelectors:       e.eagerLoadSelectors,
	}

	materializer := NewMaterializer(operatorParams)
	root, err := materializer.ConvertNodeToOperator(plan.Root, plan.TimeRange)
	if err != nil {
		return nil, err
	}

	evaluator, err := NewEvaluator(root, operatorParams, plan.TimeRange, e, opts, plan.OriginalExpression)
	if err != nil {
		return nil, err
	}

	statement := &parser.EvalStmt{
		Expr:          nil, // Nothing seems to use this, and we don't have a good expression to use here anyway, so don't bother setting this.
		Start:         timestamp.Time(plan.TimeRange.StartT),
		End:           timestamp.Time(plan.TimeRange.EndT),
		Interval:      time.Duration(plan.TimeRange.IntervalMilliseconds) * time.Millisecond,
		LookbackDelta: lookbackDelta,
	}

	if plan.TimeRange.IsInstant {
		statement.Interval = 0 // MQE uses an interval of 1ms in instant queries, but the Prometheus API contract expects this to be 0 in this case.
	}

	return &Query{
		evaluator:                evaluator,
		engine:                   e,
		statement:                statement,
		memoryConsumptionTracker: memoryConsumptionTracker,
		originalExpression:       plan.OriginalExpression,
		topLevelQueryTimeRange:   plan.TimeRange,
	}, nil
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

type NoopQueryTracker struct{}

func (n *NoopQueryTracker) GetMaxConcurrent() int {
	return math.MaxInt
}

func (n *NoopQueryTracker) Insert(_ context.Context, _ string) (int, error) {
	// Nothing to do.
	return 0, nil
}

func (n *NoopQueryTracker) Delete(_ int) {
	// Nothing to do.
}

func (n *NoopQueryTracker) Close() error {
	// Nothing to do.
	return nil
}
