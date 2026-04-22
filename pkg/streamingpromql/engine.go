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
	"github.com/go-kit/log/level"
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
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/multiaggregation"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/rangevectorsplitting"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/rangevectorsplitting/cache"
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
var errPerStepStatsNotSupported = errors.New("per-step stats are not supported by Mimir query engine")

const DefaultLookbackDelta = 5 * time.Minute // This should be the same value as github.com/prometheus/prometheus/promql.defaultLookbackDelta.

func NewEngine(opts EngineOpts, metrics *stats.QueryMetrics, planner *QueryPlanner) (*Engine, error) {
	var cacheFactory *cache.CacheFactory
	if opts.RangeVectorSplitting.Enabled {
		var err error
		cacheFactory, err = cache.NewCacheFactory(opts.RangeVectorSplitting.IntermediateResultsCache, opts.Limits, opts.Logger, opts.CommonOpts.Reg)
		if err != nil {
			return nil, fmt.Errorf("failed to init range vector splitting cache, err: %w", err)
		}
		level.Info(opts.Logger).Log("msg", "intermediate results cache enabled", "backend", opts.RangeVectorSplitting.IntermediateResultsCache.Backend)
	}
	return NewEngineWithCache(opts, metrics, planner, cacheFactory)
}

func NewEngineWithCache(opts EngineOpts, metrics *stats.QueryMetrics, planner *QueryPlanner, intermediateCache *cache.CacheFactory) (*Engine, error) {
	if !opts.CommonOpts.EnableAtModifier {
		return nil, errors.New("disabling @ modifier not supported by Mimir query engine")
	}

	if !opts.CommonOpts.EnableNegativeOffset {
		return nil, errors.New("disabling negative offsets not supported by Mimir query engine")
	}

	if opts.CommonOpts.EnablePerStepStats {
		return nil, errPerStepStatsNotSupported
	}

	if planner == nil {
		return nil, errors.New("no query planner provided")
	}

	if opts.Limits == nil {
		return nil, errors.New("no limits provider provided")
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
		planning.NODE_TYPE_MATRIX_SELECTOR:       planning.RangeAwareNodeMaterializerFunc[*core.MatrixSelector](core.MaterializeMatrixSelector),
		planning.NODE_TYPE_AGGREGATE_EXPRESSION:  planning.NodeMaterializerFunc[*core.AggregateExpression](core.MaterializeAggregateExpression),
		planning.NODE_TYPE_BINARY_EXPRESSION:     planning.NodeMaterializerFunc[*core.BinaryExpression](core.MaterializeBinaryExpression),
		planning.NODE_TYPE_FUNCTION_CALL:         planning.NodeMaterializerFunc[*core.FunctionCall](core.MaterializeFunctionCall),
		planning.NODE_TYPE_NUMBER_LITERAL:        planning.NodeMaterializerFunc[*core.NumberLiteral](core.MaterializeNumberLiteral),
		planning.NODE_TYPE_STRING_LITERAL:        planning.NodeMaterializerFunc[*core.StringLiteral](core.MaterializeStringLiteral),
		planning.NODE_TYPE_UNARY_EXPRESSION:      planning.NodeMaterializerFunc[*core.UnaryExpression](core.MaterializeUnaryExpression),
		planning.NODE_TYPE_SUBQUERY:              planning.NodeMaterializerFunc[*core.Subquery](core.MaterializeSubquery),
		planning.NODE_TYPE_DEDUPLICATE_AND_MERGE: planning.NodeMaterializerFunc[*core.DeduplicateAndMerge](core.MaterializeDeduplicateAndMerge),
		planning.NODE_TYPE_DROP_NAME:             planning.NodeMaterializerFunc[*core.DropName](core.MaterializeDropName),
		planning.NODE_TYPE_NO_OP:                 planning.NodeMaterializerFunc[*core.NoOp](core.MaterializeNoOp),

		planning.NODE_TYPE_DUPLICATE:                  planning.RangeAwareNodeMaterializerFunc[*commonsubexpressionelimination.Duplicate](commonsubexpressionelimination.MaterializeDuplicate),
		planning.NODE_TYPE_DUPLICATE_FILTER:           planning.RangeAwareNodeMaterializerFunc[*commonsubexpressionelimination.DuplicateFilter](commonsubexpressionelimination.MaterializeDuplicateFilter),
		planning.NODE_TYPE_STEP_INVARIANT_EXPRESSION:  planning.NodeMaterializerFunc[*core.StepInvariantExpression](core.MaterializeStepInvariantExpression),
		planning.NODE_TYPE_MULTI_AGGREGATION_GROUP:    planning.NodeMaterializerFunc[*multiaggregation.MultiAggregationGroup](multiaggregation.MaterializeMultiAggregationGroup),
		planning.NODE_TYPE_MULTI_AGGREGATION_INSTANCE: planning.NodeMaterializerFunc[*multiaggregation.MultiAggregationInstance](multiaggregation.MaterializeMultiAggregationInstance),

		planning.NODE_TYPE_SPLIT_FUNCTION_OVER_RANGE_VECTOR: rangevectorsplitting.NewMaterializer(opts.RangeVectorSplitting.Enabled, intermediateCache, opts.Logger),
	}

	memoryConsumptionTrackerFactory := opts.MemoryConsumptionTrackerFactory
	if memoryConsumptionTrackerFactory == nil {
		memoryConsumptionTrackerFactory = limiter.NewInflightMemoryConsumptionTracker(opts.CommonOpts.Reg, metrics.QueriesRejectedTotal.WithLabelValues(stats.RejectReasonMaxEstimatedQueryMemoryConsumption))
	}

	return &Engine{
		lookbackDelta:            DetermineLookbackDelta(opts.CommonOpts),
		timeout:                  opts.CommonOpts.Timeout,
		limitsProvider:           opts.Limits,
		activeQueryTracker:       activeQueryTracker,
		noStepSubqueryIntervalFn: opts.CommonOpts.NoStepSubqueryIntervalFn,

		logger: opts.Logger,
		estimatedPeakMemoryConsumption: promauto.With(opts.CommonOpts.Reg).NewHistogram(prometheus.HistogramOpts{
			Name:                        "cortex_mimir_query_engine_estimated_query_peak_memory_consumption",
			Help:                        "Estimated peak memory consumption of each query (in bytes)",
			NativeHistogramBucketFactor: 1.1,
		}),

		pedantic:                        opts.Pedantic,
		eagerLoadSelectors:              opts.EagerLoadSelectors,
		planner:                         planner,
		nodeMaterializers:               nodeMaterializers,
		memoryConsumptionTrackerFactory: memoryConsumptionTrackerFactory,
	}, nil
}

func DetermineLookbackDelta(opts promql.EngineOpts) time.Duration {
	lookbackDelta := opts.LookbackDelta
	if lookbackDelta == 0 {
		lookbackDelta = DefaultLookbackDelta
	}

	return lookbackDelta
}

// QueryTracker is like promql.QueryTracker, but includes more information about the query.
type QueryTracker interface {
	InsertWithDetails(ctx context.Context, query string, stage string, includeTimeRange bool, timeRange types.QueryTimeRange) (int, error)

	Delete(insertIndex int)
}

type Engine struct {
	lookbackDelta      time.Duration
	timeout            time.Duration
	limitsProvider     QueryLimitsProvider
	activeQueryTracker QueryTracker

	noStepSubqueryIntervalFn func(rangeMillis int64) int64

	logger                         log.Logger
	estimatedPeakMemoryConsumption prometheus.Histogram

	// When operating in pedantic mode:
	// - Query.Exec() will call Close() on the root operator a second time to ensure it behaves correctly if Close() is called multiple times.
	// - Query.Close() will panic if memory consumption is > 0, which indicates something was not returned to a pool.
	//
	// Pedantic mode should only be enabled in tests. It is not intended to be used in production.
	pedantic bool

	eagerLoadSelectors bool

	planner                         *QueryPlanner
	nodeMaterializers               map[planning.NodeType]planning.NodeMaterializer
	memoryConsumptionTrackerFactory *limiter.InflightMemoryConsumptionTracker
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
	enableDelayedNameRemoval, err := e.limitsProvider.GetEnableDelayedNameRemoval(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not get 'enable delayed name removal' setting for tenant: %w", err)
	}

	if opts == nil {
		opts = promql.NewPrometheusQueryOpts(false, 0)
	}

	if opts.EnablePerStepStats() {
		return nil, errPerStepStatsNotSupported
	}

	lookbackDelta := opts.LookbackDelta()
	if lookbackDelta == 0 {
		lookbackDelta = e.lookbackDelta
	}

	plan, err := e.planner.NewQueryPlan(ctx, qs, timeRange, lookbackDelta, enableDelayedNameRemoval, NoopPlanningObserver{})
	if err != nil {
		return nil, err
	}

	nodeRequests := []NodeEvaluationRequest{
		{
			Node:      plan.Root,
			TimeRange: timeRange,
		},
	}

	evaluator, err := e.materializeAndCreateEvaluator(ctx, queryable, plan.Parameters, nodeRequests)
	if err != nil {
		return nil, err
	}

	statement := &parser.EvalStmt{
		Expr:          nil, // Nothing seems to use this, and we don't have a good expression to use here anyway, so don't bother setting this.
		Start:         timestamp.Time(plan.Parameters.TimeRange.StartT),
		End:           timestamp.Time(plan.Parameters.TimeRange.EndT),
		Interval:      time.Duration(plan.Parameters.TimeRange.IntervalMilliseconds) * time.Millisecond,
		LookbackDelta: lookbackDelta,
	}

	if plan.Parameters.TimeRange.IsInstant {
		statement.Interval = 0 // MQE uses an interval of 1ms in instant queries, but the Prometheus API contract expects this to be 0 in this case.
	}

	topLevelValueType, err := plan.Root.ResultType()
	if err != nil {
		evaluator.Close()
		return nil, err
	}

	return &Query{
		evaluator:                evaluator,
		engine:                   e,
		statement:                statement,
		memoryConsumptionTracker: evaluator.MemoryConsumptionTracker,
		originalExpression:       plan.Parameters.OriginalExpression,
		topLevelQueryTimeRange:   plan.Parameters.TimeRange,
		topLevelValueType:        topLevelValueType,
	}, nil
}

type NodeEvaluationRequest struct {
	Node      planning.Node
	TimeRange types.QueryTimeRange

	operator types.Operator
}

func (e *Engine) NewEvaluator(ctx context.Context, queryable storage.Queryable, params *planning.QueryParameters, nodeRequests []NodeEvaluationRequest) (*Evaluator, error) {
	if params.LookbackDelta == 0 {
		// If we've received a query plan from an older query-frontend that does not explicitly set the lookback delta, use the configured default value.
		params.LookbackDelta = e.lookbackDelta
	}

	return e.materializeAndCreateEvaluator(ctx, queryable, params, nodeRequests)
}

func (e *Engine) materializeAndCreateEvaluator(ctx context.Context, queryable storage.Queryable, params *planning.QueryParameters, nodeRequests []NodeEvaluationRequest) (*Evaluator, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Engine.materializeAndCreateEvaluator")
	defer span.Finish()

	queryID, err := e.activeQueryTracker.InsertWithDetails(ctx, params.OriginalExpression, "materialization", true, params.TimeRange)
	if err != nil {
		return nil, err
	}

	defer e.activeQueryTracker.Delete(queryID)

	operatorParams := &planning.OperatorParameters{
		Queryable:          queryable,
		Annotations:        annotations.New(),
		QueryStats:         types.NewQueryStats(),
		EagerLoadSelectors: e.eagerLoadSelectors,
		QueryParameters:    params,
		Logger:             e.logger,
	}

	// When we are executing from within a time split query the memory consumption tracker will be allocated for the entire query
	// All the time split queries must fit within a single memory consumption tracker.
	// Note that we use a wrapped memory consumption tracker so we can still log the memory stats for this sub query, but still enforce
	// the overall memory consumption tracking for the entire query.
	if parentTracker, parentErr := limiter.MemoryConsumptionTrackerFromContext(ctx); parentErr == nil {
		// Note that it is safe for this to parent tracker to be passed to memoryConsumptionTrackerFactory.Deregister()
		operatorParams.MemoryConsumptionTracker = parentTracker
	} else {
		maxEstimatedMemoryConsumptionPerQuery, err := e.limitsProvider.GetMaxEstimatedMemoryConsumptionPerQuery(ctx)
		if err != nil {
			return nil, fmt.Errorf("could not get memory consumption limit for query: %w", err)
		}
		operatorParams.MemoryConsumptionTracker = e.memoryConsumptionTrackerFactory.NewMemoryConsumptionTracker(ctx, maxEstimatedMemoryConsumptionPerQuery, params.OriginalExpression)
	}

	materializer := planning.NewMaterializer(operatorParams, e.nodeMaterializers)
	for idx, req := range nodeRequests {
		op, err := materializer.ConvertNodeToOperator(req.Node, req.TimeRange)
		if err != nil {
			e.memoryConsumptionTrackerFactory.Deregister(operatorParams.MemoryConsumptionTracker)
			return nil, err
		}

		nodeRequests[idx].operator = op
	}

	evaluator, err := NewEvaluator(nodeRequests, operatorParams, e, params.OriginalExpression)
	if err != nil {
		e.memoryConsumptionTrackerFactory.Deregister(operatorParams.MemoryConsumptionTracker)
		return nil, err
	}
	return evaluator, nil
}

type QueryLimitsProvider interface {
	// GetMaxEstimatedMemoryConsumptionPerQuery returns the maximum estimated memory allowed to be consumed by a query in bytes, or 0 to disable the limit.
	GetMaxEstimatedMemoryConsumptionPerQuery(ctx context.Context) (uint64, error)
	// GetEnableDelayedNameRemoval indicates if the experimental feature for delayed name removal should be enabled.
	GetEnableDelayedNameRemoval(ctx context.Context) (bool, error)
	// GetMaxOutOfOrderTimeWindow returns the out-of-order time window for the tenant(s) in the context.
	GetMaxOutOfOrderTimeWindow(ctx context.Context) (time.Duration, error)
	// GetMinResultsCacheTTL returns the TTL for cached results for the tenant(s) in the context.
	GetMinResultsCacheTTL(ctx context.Context) (time.Duration, error)
}

// NewStaticQueryLimitsProvider returns a QueryLimitsProvider that always returns the provided limits.
// This should generally only be used in tests.
func NewStaticQueryLimitsProvider() StaticQueryLimitsProvider {
	return StaticQueryLimitsProvider{
		MinResultsCacheTTL: 7 * 24 * time.Hour,
	}
}

type StaticQueryLimitsProvider struct {
	MaxEstimatedMemoryConsumptionPerQuery uint64
	EnableDelayedNameRemoval              bool
	MaxOutOfOrderTimeWindow               time.Duration
	MinResultsCacheTTL                    time.Duration
}

func (p StaticQueryLimitsProvider) GetMaxEstimatedMemoryConsumptionPerQuery(_ context.Context) (uint64, error) {
	return p.MaxEstimatedMemoryConsumptionPerQuery, nil
}

func (p StaticQueryLimitsProvider) GetEnableDelayedNameRemoval(_ context.Context) (bool, error) {
	return p.EnableDelayedNameRemoval, nil
}

func (p StaticQueryLimitsProvider) GetMaxOutOfOrderTimeWindow(_ context.Context) (time.Duration, error) {
	return p.MaxOutOfOrderTimeWindow, nil
}

func (p StaticQueryLimitsProvider) GetMinResultsCacheTTL(_ context.Context) (time.Duration, error) {
	return p.MinResultsCacheTTL, nil
}

type NoopQueryTracker struct{}

func (n *NoopQueryTracker) GetMaxConcurrent() int {
	return math.MaxInt
}

func (n *NoopQueryTracker) InsertWithDetails(ctx context.Context, query string, stage string, includeTimeRange bool, timeRange types.QueryTimeRange) (int, error) {
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
