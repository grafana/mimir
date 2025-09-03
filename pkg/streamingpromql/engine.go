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
	"github.com/grafana/dskit/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"go.opentelemetry.io/otel"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/commonsubexpressionelimination"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

func init() {
	promqlext.ExtendPromQL()
}

var tracer = otel.Tracer("pkg/streamingpromql")

const defaultLookbackDelta = 5 * time.Minute // This should be the same value as github.com/prometheus/prometheus/promql.defaultLookbackDelta.

func NewEngine(opts EngineOpts, limitsProvider QueryLimitsProvider, metrics *stats.QueryMetrics, planner *QueryPlanner) (*Engine, error) {
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

	activeQueryTracker := opts.ActiveQueryTracker
	if activeQueryTracker == nil {
		if opts.CommonOpts.ActiveQueryTracker != nil {
			return nil, errors.New("no MQE-style active query tracker provided, but one conforming to Prometheus' interface was provided, this is likely a bug")
		}

		activeQueryTracker = &NoopQueryTracker{}
	}

	nodeMaterializers := map[planning.NodeType]planning.NodeMaterializer{
		planning.NODE_TYPE_VECTOR_SELECTOR:       planning.NodeMaterializerFunc[*core.VectorSelector](core.MaterializeVectorSelector),
		planning.NODE_TYPE_MATRIX_SELECTOR:       planning.NodeMaterializerFunc[*core.MatrixSelector](core.MaterializeMatrixSelector),
		planning.NODE_TYPE_AGGREGATE_EXPRESSION:  planning.NodeMaterializerFunc[*core.AggregateExpression](core.MaterializeAggregateExpression),
		planning.NODE_TYPE_BINARY_EXPRESSION:     planning.NodeMaterializerFunc[*core.BinaryExpression](core.MaterializeBinaryExpression),
		planning.NODE_TYPE_FUNCTION_CALL:         planning.NodeMaterializerFunc[*core.FunctionCall](core.MaterializeFunctionCall),
		planning.NODE_TYPE_NUMBER_LITERAL:        planning.NodeMaterializerFunc[*core.NumberLiteral](core.MaterializeNumberLiteral),
		planning.NODE_TYPE_STRING_LITERAL:        planning.NodeMaterializerFunc[*core.StringLiteral](core.MaterializeStringLiteral),
		planning.NODE_TYPE_UNARY_EXPRESSION:      planning.NodeMaterializerFunc[*core.UnaryExpression](core.MaterializeUnaryExpression),
		planning.NODE_TYPE_SUBQUERY:              planning.NodeMaterializerFunc[*core.Subquery](core.MaterializeSubquery),
		planning.NODE_TYPE_DEDUPLICATE_AND_MERGE: planning.NodeMaterializerFunc[*core.DeduplicateAndMerge](core.MaterializeDeduplicateAndMerge),

		planning.NODE_TYPE_DUPLICATE: planning.NodeMaterializerFunc[*commonsubexpressionelimination.Duplicate](commonsubexpressionelimination.MaterializeDuplicate),
	}

	return &Engine{
		lookbackDelta:            DetermineLookbackDelta(opts.CommonOpts),
		timeout:                  opts.CommonOpts.Timeout,
		limitsProvider:           limitsProvider,
		activeQueryTracker:       activeQueryTracker,
		noStepSubqueryIntervalFn: opts.CommonOpts.NoStepSubqueryIntervalFn,
		enablePerStepStats:       opts.CommonOpts.EnablePerStepStats,

		logger: opts.Logger,
		estimatedPeakMemoryConsumption: promauto.With(opts.CommonOpts.Reg).NewHistogram(prometheus.HistogramOpts{
			Name:                        "cortex_mimir_query_engine_estimated_query_peak_memory_consumption",
			Help:                        "Estimated peak memory consumption of each query (in bytes)",
			NativeHistogramBucketFactor: 1.1,
		}),
		queriesRejectedDueToPeakMemoryConsumption: metrics.QueriesRejectedTotal.WithLabelValues(stats.RejectReasonMaxEstimatedQueryMemoryConsumption),

		pedantic:           opts.Pedantic,
		eagerLoadSelectors: opts.EagerLoadSelectors,
		planner:            planner,
		nodeMaterializers:  nodeMaterializers,
	}, nil
}

func DetermineLookbackDelta(opts promql.EngineOpts) time.Duration {
	lookbackDelta := opts.LookbackDelta
	if lookbackDelta == 0 {
		lookbackDelta = defaultLookbackDelta
	}

	return lookbackDelta
}

// QueryTracker is like promql.QueryTracker, but includes more information about the query.
type QueryTracker interface {
	InsertWithDetails(ctx context.Context, query string, stage string, timeRange types.QueryTimeRange) (int, error)

	Delete(insertIndex int)
}

type Engine struct {
	lookbackDelta      time.Duration
	timeout            time.Duration
	limitsProvider     QueryLimitsProvider
	activeQueryTracker QueryTracker
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

	planner           *QueryPlanner
	nodeMaterializers map[planning.NodeType]planning.NodeMaterializer
}

func (e *Engine) RegisterNodeMaterializer(nodeType planning.NodeType, materializer planning.NodeMaterializer) error {
	if _, exists := e.nodeMaterializers[nodeType]; exists {
		return fmt.Errorf("materializer for node type %s already registered", nodeType)
	}

	e.nodeMaterializers[nodeType] = materializer
	return nil
}

func (e *Engine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	return e.newQueryFromPlanner(ctx, q, opts, qs, types.NewInstantQueryTimeRange(ts))
}

func (e *Engine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	if interval <= 0 {
		return nil, apierror.Newf(apierror.TypeBadData, "%v is not a valid interval for a range query, must be greater than 0", interval)
	}

	if end.Before(start) {
		return nil, apierror.Newf(apierror.TypeBadData, "range query time range is invalid: end time %v is before start time %v", end.Format(time.RFC3339), start.Format(time.RFC3339))
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

	lookbackDelta := opts.LookbackDelta()
	if lookbackDelta == 0 {
		lookbackDelta = e.lookbackDelta
	}

	evaluator, err := e.materializeAndCreateEvaluator(ctx, queryable, opts, plan, plan.Root, plan.TimeRange, lookbackDelta)
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
		memoryConsumptionTracker: evaluator.MemoryConsumptionTracker,
		originalExpression:       plan.OriginalExpression,
		topLevelQueryTimeRange:   plan.TimeRange,
	}, nil
}

func (e *Engine) NewEvaluator(ctx context.Context, queryable storage.Queryable, opts promql.QueryOpts, plan *planning.QueryPlan, node planning.Node, nodeTimeRange types.QueryTimeRange) (*Evaluator, error) {
	if opts == nil {
		opts = promql.NewPrometheusQueryOpts(false, 0)
	}

	lookbackDelta := opts.LookbackDelta()
	if lookbackDelta == 0 {
		lookbackDelta = e.lookbackDelta
	}

	return e.materializeAndCreateEvaluator(ctx, queryable, opts, plan, node, nodeTimeRange, lookbackDelta)
}

func (e *Engine) materializeAndCreateEvaluator(ctx context.Context, queryable storage.Queryable, opts promql.QueryOpts, plan *planning.QueryPlan, node planning.Node, nodeTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (*Evaluator, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Engine.materializeAndCreateEvaluator")
	defer span.Finish()

	queryID, err := e.activeQueryTracker.InsertWithDetails(ctx, plan.OriginalExpression, "materialization", plan.TimeRange)
	if err != nil {
		return nil, err
	}

	defer e.activeQueryTracker.Delete(queryID)

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

	materializer := planning.NewMaterializer(operatorParams, e.nodeMaterializers)
	op, err := materializer.ConvertNodeToOperator(node, nodeTimeRange)
	if err != nil {
		return nil, err
	}

	return NewEvaluator(op, operatorParams, nodeTimeRange, e, opts, plan.OriginalExpression)
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

func (n *NoopQueryTracker) InsertWithDetails(_ context.Context, _ string, _ string, _ types.QueryTimeRange) (int, error) {
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
