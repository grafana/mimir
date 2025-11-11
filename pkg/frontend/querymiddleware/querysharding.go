// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/querysharding.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/regexp"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/lazyquery"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

const shardingTimeout = 10 * time.Second

type querySharding struct {
	sharder *QuerySharder
	engine  promql.QueryEngine
	next    MetricsQueryHandler
}

type queryShardingMetrics struct {
	shardingAttempts       prometheus.Counter
	shardingSuccesses      prometheus.Counter
	shardedQueries         prometheus.Counter
	shardedQueriesPerQuery prometheus.Histogram
}

func newQueryShardingMetrics(reg prometheus.Registerer) queryShardingMetrics {
	return queryShardingMetrics{
		shardingAttempts: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_query_sharding_rewrites_attempted_total",
			Help: "Total number of queries the query-frontend attempted to shard.",
		}),
		shardingSuccesses: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_query_sharding_rewrites_succeeded_total",
			Help: "Total number of queries the query-frontend successfully rewritten in a shardable way.",
		}),
		shardedQueries: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_sharded_queries_total",
			Help: "Total number of sharded queries.",
		}),
		shardedQueriesPerQuery: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_frontend_sharded_queries_per_query",
			Help:    "Number of sharded queries a single query has been rewritten to.",
			Buckets: prometheus.ExponentialBuckets(2, 2, 10),
		}),
	}
}

// newQueryShardingMiddleware creates a middleware that will split queries by shard.
// It first looks at the query to determine if it is shardable or not.
// Then rewrite the query into a sharded query and use the PromQL engine to execute the query.
// Sub shard queries are embedded into a single vector selector and a modified `Queryable` (see shardedQueryable) is passed
// to the PromQL engine.
// Finally we can translate the embedded vector selector back into subqueries in the Queryable and send them in parallel to downstream.
func newQueryShardingMiddleware(
	logger log.Logger,
	engine promql.QueryEngine,
	limit Limits,
	maxSeriesPerShard uint64,
	reg prometheus.Registerer,
) MetricsQueryMiddleware {
	sharder := NewQuerySharder(astmapper.EmbeddedQueriesSquasher, limit, maxSeriesPerShard, reg, logger)

	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &querySharding{
			sharder: sharder,
			next:    next,
			engine:  engine,
		}
	})
}

func (s *querySharding) Do(ctx context.Context, r MetricsQueryRequest) (Response, error) {
	if r.GetOptions().ShardingDisabled {
		return s.next.Do(ctx, r)
	}

	queryExpr, err := astmapper.CloneExpr(r.GetParsedQuery())
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	}

	requestedShardCount := int(r.GetOptions().TotalShards) // Honor the number of shards specified in the request (if any).
	var seriesCount *EstimatedSeriesCount
	totalQueries := int32(1)
	if hints := r.GetHints(); hints != nil {
		seriesCount = hints.GetCardinalityEstimate()

		// If the total number of queries (e.g. after time splitting) is unknown, then assume there was no splitting,
		// and we just have 1 query. The hints may not be populated for instant queries, and defaulting to 1 makes
		// this logic work for instant queries too.
		if hints.TotalQueries > 0 {
			totalQueries = hints.TotalQueries
		}
	}

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	shardedQuery, err := s.sharder.Shard(ctx, tenantIDs, queryExpr, requestedShardCount, seriesCount, totalQueries)
	if err != nil {
		return nil, err
	}

	if shardedQuery == nil {
		return s.next.Do(ctx, r)
	}

	r, err = r.WithExpr(shardedQuery)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	annotationAccumulator := NewAnnotationAccumulator()
	shardedQueryable := NewShardedQueryable(r, annotationAccumulator, s.next, nil)

	return ExecuteQueryOnQueryable(ctx, r, s.engine, shardedQueryable, annotationAccumulator)
}

func ExecuteQueryOnQueryable(ctx context.Context, r MetricsQueryRequest, engine promql.QueryEngine, queryable storage.Queryable, annotationAccumulator *AnnotationAccumulator) (Response, error) {
	qry, err := newQuery(ctx, r, engine, lazyquery.NewLazyQueryable(queryable))
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	res := qry.Exec(ctx)
	extracted, err := promqlResultToSamples(res)
	if err != nil {
		return nil, mapEngineError(err)
	}
	// Note that the positions based on the original query may be wrong as the rewritten
	// query which is actually used is different, but the user does not see the rewritten
	// query, so we pass in an empty string as the query so the positions will be hidden.
	warn, info := res.Warnings.AsStrings("", 0, 0)

	if annotationAccumulator != nil {
		// Add any annotations returned by the sharded queries, and remove any duplicates.
		// We remove any position information for the same reason as above: the position information
		// relates to the rewritten expression sent to queriers, not the original expression provided by the user.
		accumulatedWarnings, accumulatedInfos := annotationAccumulator.getAll()
		warn = append(warn, removeAllAnnotationPositionInformation(accumulatedWarnings)...)
		info = append(info, removeAllAnnotationPositionInformation(accumulatedInfos)...)
		warn = removeDuplicates(warn)
		info = removeDuplicates(info)
	}

	var headers []*PrometheusHeader
	shardedQueryable, ok := queryable.(*shardedQueryable)
	if ok {
		headers = shardedQueryable.getResponseHeaders()
	}

	return &PrometheusResponseWithFinalizer{
		PrometheusResponse: &PrometheusResponse{
			Status: statusSuccess,
			Data: &PrometheusData{
				ResultType: string(res.Value.Type()),
				Result:     extracted,
			},
			Headers:  headers,
			Warnings: warn,
			Infos:    info,
		},
		finalizer: qry.Close,
	}, nil
}

func newQuery(ctx context.Context, r MetricsQueryRequest, engine promql.QueryEngine, queryable storage.Queryable) (promql.Query, error) {
	switch r := r.(type) {
	case *PrometheusRangeQueryRequest:
		return engine.NewRangeQuery(
			ctx,
			queryable,
			nil,
			r.GetQuery(),
			util.TimeFromMillis(r.GetStart()),
			util.TimeFromMillis(r.GetEnd()),
			time.Duration(r.GetStep())*time.Millisecond,
		)
	case *PrometheusInstantQueryRequest:
		return engine.NewInstantQuery(
			ctx,
			queryable,
			nil,
			r.GetQuery(),
			util.TimeFromMillis(r.GetTime()),
		)
	default:
		return nil, fmt.Errorf("unsupported query type %T", r)
	}
}

func mapEngineError(err error) error {
	// If already comes mapped to an apierror, just return that (we received an error from upstream).
	if apierror.IsAPIError(err) {
		return err
	}

	// Extract the root cause of the error wrapped by fmt.Errorf with "%w" (used by PromQL engine).
	cause := errors.Unwrap(err)
	if cause == nil {
		cause = err
	}

	errorType := apierror.TypeForError(cause, apierror.TypeExec)
	return apierror.New(errorType, cause.Error())
}

type QuerySharder struct {
	squasher          astmapper.Squasher
	limit             ShardingLimits
	maxSeriesPerShard uint64
	logger            log.Logger
	metrics           queryShardingMetrics
}

type ShardingLimits interface {
	QueryShardingTotalShards(userID string) int
	QueryShardingMaxRegexpSizeBytes(userID string) int
	QueryShardingMaxShardedQueries(userID string) int
	CompactorSplitAndMergeShards(userID string) int
}

func NewQuerySharder(
	squasher astmapper.Squasher,
	limit ShardingLimits,
	maxSeriesPerShard uint64,
	reg prometheus.Registerer,
	logger log.Logger,
) *QuerySharder {
	return &QuerySharder{
		squasher:          squasher,
		limit:             limit,
		maxSeriesPerShard: maxSeriesPerShard,
		metrics:           newQueryShardingMetrics(reg),
		logger:            logger,
	}
}

// Shard attempts to rewrite expr in a sharded way.
//
// expr may be modified in place, including if an error is returned.
//
// If the query can't be sharded, Shard returns nil and no error, and expr is unchanged.
func (s *QuerySharder) Shard(ctx context.Context, tenantIDs []string, expr parser.Expr, requestedShardCount int, seriesCount *EstimatedSeriesCount, totalQueries int32) (parser.Expr, error) {
	log := spanlogger.FromContext(ctx, s.logger)
	log.DebugLog("msg", "attempting query sharding", "query", expr)
	s.metrics.shardingAttempts.Inc()

	totalShards, err := s.getShardsForQuery(ctx, tenantIDs, expr, requestedShardCount, seriesCount, totalQueries, log)
	if err != nil {
		log.DebugLog("msg", "calculating the number of shards for the query failed", "err", err)
		return nil, err
	}

	if totalShards <= 1 {
		log.DebugLog("msg", "query sharding is disabled for this query or tenant")
		return nil, nil
	}

	log.DebugLog(
		"msg", "computed shard count for query, rewriting in shardable form",
		"total shards", totalShards,
	)

	shardedExpr, shardingStats, err := s.shardQuery(ctx, expr, totalShards)
	if err != nil {
		return nil, err
	}

	if shardingStats.GetShardedQueries() == 0 {
		// The query can't be sharded, fallback to execute it via queriers.
		log.DebugLog("msg", "query is not supported for being rewritten into a shardable query")
		return nil, nil
	}

	log.DebugLog("msg", "query has been rewritten into a shardable query", "rewritten", shardedExpr, "sharded_queries", shardingStats.GetShardedQueries())

	// Update metrics.
	s.metrics.shardingSuccesses.Inc()
	s.metrics.shardedQueries.Add(float64(shardingStats.GetShardedQueries()))
	s.metrics.shardedQueriesPerQuery.Observe(float64(shardingStats.GetShardedQueries()))

	// Update query stats.
	queryStats := stats.FromContext(ctx)
	queryStats.AddShardedQueries(uint32(shardingStats.GetShardedQueries()))
	return shardedExpr, nil
}

// shardQuery attempts to rewrite the input query in a shardable way. Returns the rewritten query
// to be executed by PromQL engine with shardedQueryable or an empty string if the input query
// can't be sharded.
func (s *QuerySharder) shardQuery(ctx context.Context, expr parser.Expr, totalShards int) (parser.Expr, *astmapper.MapperStats, error) {
	stats := astmapper.NewMapperStats()
	ctx, cancel := context.WithTimeoutCause(ctx, shardingTimeout, fmt.Errorf("%w: %s", context.DeadlineExceeded, "timeout while rewriting the input query into a shardable query"))
	defer cancel()

	summer := astmapper.NewQueryShardSummer(totalShards, s.squasher, s.logger, stats)
	shardedQuery, err := summer.Map(ctx, expr)
	if err != nil {
		return nil, nil, err
	}

	return shardedQuery, stats, nil
}

// getShardsForQuery calculates and return the number of shards that should be used to run the query.
func (s *QuerySharder) getShardsForQuery(ctx context.Context, tenantIDs []string, queryExpr parser.Expr, requestedShardCount int, seriesCount *EstimatedSeriesCount, totalQueries int32, spanLog *spanlogger.SpanLogger) (int, error) {
	// Check the default number of shards configured for the given tenant.
	totalShards := validation.SmallestPositiveIntPerTenant(tenantIDs, s.limit.QueryShardingTotalShards)
	if totalShards <= 1 {
		return 1, nil
	}

	// Ensure there's no regexp matcher longer than the configured limit.
	maxRegexpSizeBytes := validation.SmallestPositiveNonZeroIntPerTenant(tenantIDs, s.limit.QueryShardingMaxRegexpSizeBytes)
	if maxRegexpSizeBytes > 0 {
		if longest := longestRegexpMatcherBytes(queryExpr); longest > maxRegexpSizeBytes {
			spanLog.DebugLog(
				"msg", "query sharding has been disabled because the query contains a regexp matcher longer than the limit",
				"longest regexp bytes", longest,
				"limit bytes", maxRegexpSizeBytes,
			)

			return 1, nil
		}
	}

	if requestedShardCount > 0 {
		spanLog.DebugLog(
			"msg", "number of shards has been adjusted to match the requested shard count",
			"requested shard count", requestedShardCount,
			"previous total shards", totalShards,
		)
		totalShards = requestedShardCount
	}

	if seriesCount != nil && s.maxSeriesPerShard > 0 {
		prevTotalShards := totalShards
		// If an estimate for query cardinality is available, use it to limit the number
		// of shards based on linear interpolation.
		totalShards = min(totalShards, int(seriesCount.EstimatedSeriesCount/s.maxSeriesPerShard)+1)

		if prevTotalShards != totalShards {
			spanLog.DebugLog(
				"msg", "number of shards has been adjusted to match the estimated series count",
				"updated total shards", totalShards,
				"previous total shards", prevTotalShards,
				"estimated series count", seriesCount.EstimatedSeriesCount,
			)
		}
	}

	// Calculate how many legs are shardable.
	analyzer := astmapper.NewShardingAnalyzer(s.logger)
	result, err := analyzer.Analyze(queryExpr)
	if err != nil {
		return 0, err
	}

	spanLog.DebugLog(
		"msg", "computed number of shardable legs for the query",
		"shardable legs", result.ShardedSelectors,
		"can shard all selectors", result.WillShardAllSelectors,
	)

	if !result.WillShardAllSelectors || result.ShardedSelectors == 0 {
		// If we can't shard all selectors, then we won't shard the query at all.
		return 1, nil
	}

	numShardableLegs := result.ShardedSelectors

	// If total queries is provided through hints, then we adjust the number of shards for the query
	// based on the configured max sharded queries limit.
	if maxShardedQueries := validation.SmallestPositiveIntPerTenant(tenantIDs, s.limit.QueryShardingMaxShardedQueries); maxShardedQueries > 0 {
		prevTotalShards := totalShards
		totalShards = max(1, min(totalShards, (maxShardedQueries/int(totalQueries))/numShardableLegs))

		if prevTotalShards != totalShards {
			spanLog.DebugLog(
				"msg", "number of shards has been adjusted to honor the max sharded queries limit",
				"updated total shards", totalShards,
				"previous total shards", prevTotalShards,
				"max sharded queries", maxShardedQueries,
				"shardable legs", numShardableLegs,
				"total queries", totalQueries)
		}
	}

	// Adjust totalShards such that one of the following is true:
	//
	// 1) totalShards % compactorShards == 0
	// 2) compactorShards % totalShards == 0
	//
	// This allows optimization with sharded blocks in querier to be activated.
	//
	// (Optimization is only activated when given *block* was sharded with correct compactor shards,
	// but we can only adjust totalShards "globally", ie. for all queried blocks.)
	compactorShardCount := validation.SmallestPositiveNonZeroIntPerTenant(tenantIDs, s.limit.CompactorSplitAndMergeShards)
	if compactorShardCount > 1 {
		prevTotalShards := totalShards

		if totalShards > compactorShardCount {
			totalShards = totalShards - (totalShards % compactorShardCount)
		} else if totalShards < compactorShardCount {
			// Adjust totalShards down to the nearest divisor of "compactor shards".
			for totalShards > 0 && compactorShardCount%totalShards != 0 {
				totalShards--
			}

			// If there was no divisor, just use original total shards.
			if totalShards <= 1 {
				totalShards = prevTotalShards
			}
		}

		if prevTotalShards != totalShards {
			spanLog.DebugLog("msg", "number of shards has been adjusted to be compatible with compactor shards",
				"previous total shards", prevTotalShards,
				"updated total shards", totalShards,
				"compactor shards", compactorShardCount)
		}
	}

	return totalShards, nil
}

// promqlResultToSamples transforms a promql query result into a samplestream
func promqlResultToSamples(res *promql.Result) ([]SampleStream, error) {
	if res.Err != nil {
		return nil, res.Err
	}
	switch v := res.Value.(type) {
	case promql.String:
		return []SampleStream{
			{
				Labels:  []mimirpb.LabelAdapter{{Name: "value", Value: v.V}},
				Samples: []mimirpb.Sample{{TimestampMs: v.T}},
			},
		}, nil
	case promql.Scalar:
		return []SampleStream{
			{Samples: []mimirpb.Sample{{TimestampMs: v.T, Value: v.V}}},
		}, nil

	case promql.Vector:
		res := make([]SampleStream, 0, len(v))
		for _, sample := range v {
			ss := SampleStream{
				Labels: mimirpb.FromLabelsToLabelAdapters(sample.Metric),
			}
			if sample.H != nil {
				ss.Histograms = mimirpb.FromHPointsToHistograms([]promql.HPoint{{T: sample.T, H: sample.H}})
			} else {
				ss.Samples = mimirpb.FromFPointsToSamples([]promql.FPoint{{T: sample.T, F: sample.F}})
			}
			res = append(res, ss)
		}
		return res, nil

	case promql.Matrix:
		res := make([]SampleStream, 0, len(v))
		for _, series := range v {
			ss := SampleStream{
				Labels: mimirpb.FromLabelsToLabelAdapters(series.Metric),
			}
			samples := mimirpb.FromFPointsToSamples(series.Floats)
			if len(samples) > 0 {
				ss.Samples = samples
			}
			histograms := mimirpb.FromHPointsToHistograms(series.Histograms)
			if len(histograms) > 0 {
				ss.Histograms = histograms
			}
			res = append(res, ss)
		}
		return res, nil

	}

	return nil, errors.Errorf("unexpected value type: [%s]", res.Value.Type())
}

// longestRegexpMatcherBytes returns the length (in bytes) of the longest regexp
// matcher found in the query, or 0 if the query has no regexp matcher.
func longestRegexpMatcherBytes(expr parser.Expr) int {
	var longest int

	for _, selectors := range parser.ExtractSelectors(expr) {
		for _, matcher := range selectors {
			if matcher.Type != labels.MatchRegexp && matcher.Type != labels.MatchNotRegexp {
				continue
			}

			longest = max(longest, len(matcher.Value))
		}
	}

	return longest
}

// AnnotationAccumulator collects annotations returned by sharded queries.
type AnnotationAccumulator struct {
	warnings *sync.Map
	infos    *sync.Map
}

func NewAnnotationAccumulator() *AnnotationAccumulator {
	return &AnnotationAccumulator{
		warnings: &sync.Map{},
		infos:    &sync.Map{},
	}
}

// addWarning collects the warning annotation w.
//
// addWarning is safe to call from multiple goroutines.
func (a *AnnotationAccumulator) addWarning(w string) {
	// We use LoadOrStore here to add the annotation if it doesn't already exist or otherwise do nothing.
	a.warnings.LoadOrStore(w, struct{}{})
}

// addWarnings collects all of the warning annotations in warnings.
//
// addWarnings is safe to call from multiple goroutines.
func (a *AnnotationAccumulator) addWarnings(warnings []string) {
	for _, w := range warnings {
		a.addWarning(w)
	}
}

// addInfo collects the info annotation i.
//
// addInfo is safe to call from multiple goroutines.
func (a *AnnotationAccumulator) addInfo(i string) {
	// We use LoadOrStore here to add the annotation if it doesn't already exist or otherwise do nothing.
	a.infos.LoadOrStore(i, struct{}{})
}

// addInfos collects all of the info annotations in infos.
//
// addInfo is safe to call from multiple goroutines.
func (a *AnnotationAccumulator) addInfos(infos []string) {
	for _, i := range infos {
		a.addInfo(i)
	}
}

// getAll returns all annotations collected by this accumulator.
//
// getAll may return inconsistent or unexpected results if it is called concurrently with addInfo or addWarning.
func (a *AnnotationAccumulator) getAll() (warnings, infos []string) {
	return getAllKeys(a.warnings), getAllKeys(a.infos)
}

func getAllKeys(m *sync.Map) []string {
	var keys []string

	m.Range(func(k, _ interface{}) bool {
		keys = append(keys, k.(string))
		return true
	})

	return keys
}

// removeDuplicates removes duplicate entries from s.
//
// s may be modified and should not be used after removeDuplicates returns.
func removeDuplicates(s []string) []string {
	slices.Sort(s)
	return slices.Compact(s)
}

var annotationPositionPattern = regexp.MustCompile(`\s+\(\d+:\d+\)$`)

func removeAnnotationPositionInformation(annotation string) string {
	return annotationPositionPattern.ReplaceAllLiteralString(annotation, "")
}

// removeAllAnnotationPositionInformation removes position information from each annotation in annotations,
// modifying annotations in-place and returning it for convenience.
func removeAllAnnotationPositionInformation(annotations []string) []string {
	for i, annotation := range annotations {
		annotations[i] = removeAnnotationPositionInformation(annotation)
	}

	return annotations
}
