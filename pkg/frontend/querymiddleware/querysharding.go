// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/querysharding.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
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
	util_math "github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

const shardingTimeout = 10 * time.Second

type querySharding struct {
	limit Limits

	engine            *promql.Engine
	defaultStepFunc   func(rangeMillis int64) int64
	next              MetricsQueryHandler
	logger            log.Logger
	maxSeriesPerShard uint64

	queryShardingMetrics
}

type queryShardingMetrics struct {
	shardingAttempts          prometheus.Counter
	shardingSuccesses         prometheus.Counter
	shardedQueries            prometheus.Counter
	shardedQueriesPerQuery    prometheus.Histogram
	spunOffSubqueries         prometheus.Counter
	spunOffSubqueriesPerQuery prometheus.Histogram
}

// newQueryShardingMiddleware creates a middleware that will split queries by shard.
// It first looks at the query to determine if it is shardable or not.
// Then rewrite the query into a sharded query and use the PromQL engine to execute the query.
// Sub shard queries are embedded into a single vector selector and a modified `Queryable` (see shardedQueryable) is passed
// to the PromQL engine.
// Finally we can translate the embedded vector selector back into subqueries in the Queryable and send them in parallel to downstream.
func newQueryShardingMiddleware(
	logger log.Logger,
	engine *promql.Engine,
	defaultStepFunc func(rangeMillis int64) int64,
	limit Limits,
	maxSeriesPerShard uint64,
	registerer prometheus.Registerer,
) MetricsQueryMiddleware {
	metrics := queryShardingMetrics{
		shardingAttempts: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_query_sharding_rewrites_attempted_total",
			Help: "Total number of queries the query-frontend attempted to shard.",
		}),
		shardingSuccesses: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_query_sharding_rewrites_succeeded_total",
			Help: "Total number of queries the query-frontend successfully rewritten in a shardable way.",
		}),
		shardedQueries: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_sharded_queries_total",
			Help: "Total number of sharded queries.",
		}),
		shardedQueriesPerQuery: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_frontend_sharded_queries_per_query",
			Help:    "Number of sharded queries a single query has been rewritten to.",
			Buckets: prometheus.ExponentialBuckets(2, 2, 10),
		}),
		spunOffSubqueries: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_query_sharding_spun_off_subqueries_total",
			Help: "Total number of subqueries spun off as range queries.",
		}),
		spunOffSubqueriesPerQuery: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_frontend_query_sharding_spun_off_subqueries_per_query",
			Help:    "Number of subqueries spun off as range queries per query.",
			Buckets: prometheus.ExponentialBuckets(2, 2, 10),
		}),
	}

	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &querySharding{
			next:                 next,
			queryShardingMetrics: metrics,
			engine:               engine,
			defaultStepFunc:      defaultStepFunc,
			logger:               logger,
			limit:                limit,
			maxSeriesPerShard:    maxSeriesPerShard,
		}
	})
}

func (s *querySharding) Do(ctx context.Context, r MetricsQueryRequest) (Response, error) {
	log := spanlogger.FromContext(ctx, s.logger)

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	// Parse the query.
	queryExpr, err := parser.ParseExpr(r.GetQuery())
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	}

	totalShards := s.getShardsForQuery(ctx, tenantIDs, r, queryExpr, log)
	if totalShards <= 1 {
		level.Debug(log).Log("msg", "query sharding is disabled for this query or tenant")
		return s.next.Do(ctx, r)
	}

	var fullRangeHandler MetricsQueryHandler
	if v, ok := ctx.Value(fullRangeHandlerContextKey).(MetricsQueryHandler); ok && IsInstantQuery(r.GetPath()) {
		fullRangeHandler = v
	}
	level.Debug(log).Log(fullRangeHandlerContextKey+" set", fullRangeHandler != nil)

	s.shardingAttempts.Inc()
	shardedQuery, shardingStats, err := s.shardQuery(ctx, r.GetQuery(), totalShards, fullRangeHandler != nil)

	// If an error occurred while trying to rewrite the query or the query has not been sharded,
	// then we should fallback to execute it via queriers.
	if (err != nil || shardingStats.GetShardedQueries() == 0) && !strings.Contains(shardedQuery, astmapper.AggregatedSubqueryMetricName) {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			level.Error(log).Log("msg", "timeout while rewriting the input query into a shardable query, please fill in a bug report with this query, falling back to try executing without sharding", "query", r.GetQuery(), "err", err)
		} else if err != nil {
			level.Warn(log).Log("msg", "failed to rewrite the input query into a shardable query, falling back to try executing without sharding", "query", r.GetQuery(), "err", err)
		} else {
			level.Debug(log).Log("msg", "query is not supported for being rewritten into a shardable query", "query", r.GetQuery(), "rewritten", shardedQuery)
		}

		return s.next.Do(ctx, r)
	}

	// TODO: revert to debug
	level.Info(log).Log("msg", "query has been rewritten into a shardable query", "original", r.GetQuery(), "rewritten", shardedQuery, "sharded_queries", shardingStats.GetShardedQueries())

	// Update metrics.
	s.shardingSuccesses.Inc()
	s.shardedQueries.Add(float64(shardingStats.GetShardedQueries()))
	s.shardedQueriesPerQuery.Observe(float64(shardingStats.GetShardedQueries()))
	s.spunOffSubqueries.Add(float64(shardingStats.GetSpunOffSubqueries()))
	s.spunOffSubqueriesPerQuery.Observe(float64(shardingStats.GetSpunOffSubqueries()))

	// Update query stats.
	queryStats := stats.FromContext(ctx)
	queryStats.AddShardedQueries(uint32(shardingStats.GetShardedQueries()))
	queryStats.AddSpunOffSubqueries(uint32(shardingStats.GetSpunOffSubqueries()))

	r, err = r.WithQuery(shardedQuery)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	annotationAccumulator := NewAnnotationAccumulator()

	shardedQueryable := NewShardedQueryable(r, annotationAccumulator, s.next, fullRangeHandler, nil, s.defaultStepFunc)

	return ExecuteQueryOnQueryable(ctx, r, s.engine, shardedQueryable, annotationAccumulator)
}

func ExecuteQueryOnQueryable(ctx context.Context, r MetricsQueryRequest, engine *promql.Engine, queryable storage.Queryable, annotationAccumulator *AnnotationAccumulator) (Response, error) {
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

	return &PrometheusResponse{
		Status: statusSuccess,
		Data: &PrometheusData{
			ResultType: string(res.Value.Type()),
			Result:     extracted,
		},
		Headers:  headers,
		Warnings: warn,
		Infos:    info,
	}, nil
}

func newQuery(ctx context.Context, r MetricsQueryRequest, engine *promql.Engine, queryable storage.Queryable) (promql.Query, error) {
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
	case *remoteReadQueryRequest:
		return engine.NewRangeQuery(
			ctx,
			queryable,
			// Lookback period is not applied to remote read queries in the same way
			// as regular queries. However we cannot set a zero lookback period
			// because the engine will just use the default 5 minutes instead. So we
			// set a lookback period of 1ns and add that amount to the start time so
			// the engine will calculate an effective 0 lookback period.
			promql.NewPrometheusQueryOpts(false, 1*time.Nanosecond),
			r.GetQuery(),
			util.TimeFromMillis(r.GetStart()).Add(1*time.Nanosecond),
			util.TimeFromMillis(r.GetEnd()),
			time.Duration(r.GetStep())*time.Millisecond,
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

	// By default, all errors returned by engine.Eval() are execution errors,
	// This is the same as Prometheus API does: http://github.com/prometheus/prometheus/blob/076109fa1910ad2198bf2c447a174fee31114982/web/api/v1/api.go#L550-L550
	errorType := apierror.TypeExec
	switch cause.(type) {
	case promql.ErrQueryCanceled:
		errorType = apierror.TypeCanceled
	case promql.ErrQueryTimeout:
		errorType = apierror.TypeTimeout
	case promql.ErrStorage:
		errorType = apierror.TypeInternal
	case promql.ErrTooManySamples:
		errorType = apierror.TypeExec
	}

	return apierror.New(errorType, cause.Error())
}

// shardQuery attempts to rewrite the input query in a shardable way. Returns the rewritten query
// to be executed by PromQL engine with shardedQueryable or an empty string if the input query
// can't be sharded.
func (s *querySharding) shardQuery(ctx context.Context, query string, totalShards int, upstreamSubqueries bool) (string, *astmapper.MapperStats, error) {
	stats := astmapper.NewMapperStats()
	ctx, cancel := context.WithTimeout(ctx, shardingTimeout)
	defer cancel()

	summer, err := astmapper.NewQueryShardSummer(ctx, totalShards, astmapper.VectorSquasher, s.logger, stats, upstreamSubqueries)
	if err != nil {
		return "", nil, err
	}

	mapper := astmapper.NewSharding(summer)

	// The mapper can modify the input expression in-place, so we must re-parse the original query
	// each time before passing it to the mapper.
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return "", nil, apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	}

	shardedQuery, err := mapper.Map(expr)
	if err != nil {
		return "", nil, err
	}

	return shardedQuery.String(), stats, nil
}

// getShardsForQuery calculates and return the number of shards that should be used to run the query.
func (s *querySharding) getShardsForQuery(ctx context.Context, tenantIDs []string, r MetricsQueryRequest, queryExpr parser.Expr, spanLog *spanlogger.SpanLogger) int {
	// Check if sharding is disabled for the given request.
	if r.GetOptions().ShardingDisabled {
		return 1
	}

	// Check the default number of shards configured for the given tenant.
	totalShards := validation.SmallestPositiveIntPerTenant(tenantIDs, s.limit.QueryShardingTotalShards)
	if totalShards <= 1 {
		return 1
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

			return 1
		}
	}

	// Honor the number of shards specified in the request (if any).
	if r.GetOptions().TotalShards > 0 {
		totalShards = int(r.GetOptions().TotalShards)
	}

	maxShardedQueries := validation.SmallestPositiveIntPerTenant(tenantIDs, s.limit.QueryShardingMaxShardedQueries)
	hints := r.GetHints()

	var seriesCount *EstimatedSeriesCount
	if hints != nil {
		seriesCount = hints.GetCardinalityEstimate()
	}

	if seriesCount != nil && s.maxSeriesPerShard > 0 {
		prevTotalShards := totalShards
		// If an estimate for query cardinality is available, use it to limit the number
		// of shards based on linear interpolation.
		totalShards = util_math.Min(totalShards, int(seriesCount.EstimatedSeriesCount/s.maxSeriesPerShard)+1)

		if prevTotalShards != totalShards {
			spanLog.DebugLog(
				"msg", "number of shards has been adjusted to match the estimated series count",
				"updated total shards", totalShards,
				"previous total shards", prevTotalShards,
				"estimated series count", seriesCount.EstimatedSeriesCount,
			)
		}
	}

	// If total queries is provided through hints, then we adjust the number of shards for the query
	// based on the configured max sharded queries limit.
	if hints != nil && hints.TotalQueries > 0 && maxShardedQueries > 0 {
		// Calculate how many legs are shardable. To do it we use a trick: rewrite the query passing 1
		// total shards and then we check how many sharded queries are generated. In case of any error,
		// we just consider as if there's only 1 shardable leg (the error will be detected anyway later on).
		//
		// "Leg" is the terminology we use in query sharding to mention a part of the query that can be sharded.
		// For example, look at this query:
		// sum(metric) / count(metric)
		//
		// This query has 2 shardable "legs":
		// - sum(metric)
		// - count(metric)
		//
		// Calling s.shardQuery() with 1 total shards we can see how many shardable legs the query has.
		_, shardingStats, err := s.shardQuery(ctx, r.GetQuery(), 1, false)
		numShardableLegs := 1
		if err == nil && shardingStats.GetShardedQueries() > 0 {
			numShardableLegs = shardingStats.GetShardedQueries()
		}

		prevTotalShards := totalShards
		totalShards = util_math.Max(1, util_math.Min(totalShards, (maxShardedQueries/int(hints.TotalQueries))/numShardableLegs))

		if prevTotalShards != totalShards {
			spanLog.DebugLog(
				"msg", "number of shards has been adjusted to honor the max sharded queries limit",
				"updated total shards", totalShards,
				"previous total shards", prevTotalShards,
				"max sharded queries", maxShardedQueries,
				"shardable legs", numShardableLegs,
				"total queries", hints.TotalQueries)
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

	return totalShards
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

			longest = util_math.Max(longest, len(matcher.Value))
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
