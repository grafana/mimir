// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/querysharding.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/status"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
	next              Handler
	logger            log.Logger
	maxSeriesPerShard uint64

	queryShardingMetrics
}

type queryShardingMetrics struct {
	shardingAttempts       prometheus.Counter
	shardingSuccesses      prometheus.Counter
	shardedQueries         prometheus.Counter
	shardedQueriesPerQuery prometheus.Histogram
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
	limit Limits,
	maxSeriesPerShard uint64,
	registerer prometheus.Registerer,
) Middleware {
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
	}
	return MiddlewareFunc(func(next Handler) Handler {
		return &querySharding{
			next:                 next,
			queryShardingMetrics: metrics,
			engine:               engine,
			logger:               logger,
			limit:                limit,
			maxSeriesPerShard:    maxSeriesPerShard,
		}
	})
}

func (s *querySharding) Do(ctx context.Context, r Request) (Response, error) {
	log, ctx := spanlogger.NewWithLogger(ctx, s.logger, "querySharding.Do")
	defer log.Span.Finish()

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	totalShards := s.getShardsForQuery(ctx, tenantIDs, r, log)
	if totalShards <= 1 {
		level.Debug(log).Log("msg", "query sharding is disabled for this query or tenant")
		return s.next.Do(ctx, r)
	}

	s.shardingAttempts.Inc()
	shardedQuery, shardingStats, err := s.shardQuery(ctx, r.GetQuery(), totalShards)

	// If an error occurred while trying to rewrite the query or the query has not been sharded,
	// then we should fallback to execute it via queriers.
	if err != nil || shardingStats.GetShardedQueries() == 0 {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			level.Error(log).Log("msg", "timeout while rewriting the input query into a shardable query, please fill in a bug report with this query, falling back to try executing without sharding", "query", r.GetQuery(), "err", err)
		} else if err != nil {
			level.Warn(log).Log("msg", "failed to rewrite the input query into a shardable query, falling back to try executing without sharding", "query", r.GetQuery(), "err", err)
		} else {
			level.Debug(log).Log("msg", "query is not supported for being rewritten into a shardable query", "query", r.GetQuery())
		}

		return s.next.Do(ctx, r)
	}

	level.Debug(log).Log("msg", "query has been rewritten into a shardable query", "original", r.GetQuery(), "rewritten", shardedQuery, "sharded_queries", shardingStats.GetShardedQueries())

	// Update metrics.
	s.shardingSuccesses.Inc()
	s.shardedQueries.Add(float64(shardingStats.GetShardedQueries()))
	s.shardedQueriesPerQuery.Observe(float64(shardingStats.GetShardedQueries()))

	// Update query stats.
	queryStats := stats.FromContext(ctx)
	queryStats.AddShardedQueries(uint32(shardingStats.GetShardedQueries()))

	r = r.WithQuery(shardedQuery)
	shardedQueryable := newShardedQueryable(r, s.next)

	qry, err := newQuery(r, s.engine, lazyquery.NewLazyQueryable(shardedQueryable))
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	res := qry.Exec(ctx)
	extracted, err := promqlResultToSamples(res)
	if err != nil {
		return nil, mapEngineError(err)
	}
	return &PrometheusResponse{
		Status: statusSuccess,
		Data: &PrometheusData{
			ResultType: string(res.Value.Type()),
			Result:     extracted,
		},
		Headers: shardedQueryable.getResponseHeaders(),
	}, nil
}

func newQuery(r Request, engine *promql.Engine, queryable storage.Queryable) (promql.Query, error) {
	switch r := r.(type) {
	case *PrometheusRangeQueryRequest:
		return engine.NewRangeQuery(
			queryable,
			nil,
			r.GetQuery(),
			util.TimeFromMillis(r.GetStart()),
			util.TimeFromMillis(r.GetEnd()),
			time.Duration(r.GetStep())*time.Millisecond,
		)
	case *PrometheusInstantQueryRequest:
		return engine.NewInstantQuery(
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
	if err == nil {
		return nil
	}

	// If already comes mapped to an apierror, just return that (we received an error from upstream).
	if apierror.IsAPIError(err) {
		return err
	}

	// Extract the root cause of the error wrapped by fmt.Errorf with "%w" (used by PromQL engine).
	cause := errors.Unwrap(err)
	if cause == nil {
		cause = err
	}

	// If upstream request failed as 5xx, it would be wrapped as httpgrpc error, which is a status error.
	// If that is the case, it's an internal error.
	// We need to check this on the cause, because status.FromError() makes an interface implementation assert instead of using errors.As().
	if _, ok := status.FromError(cause); ok {
		return apierror.New(apierror.TypeInternal, cause.Error())
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
func (s *querySharding) shardQuery(ctx context.Context, query string, totalShards int) (string, *astmapper.MapperStats, error) {
	stats := astmapper.NewMapperStats()
	ctx, cancel := context.WithTimeout(ctx, shardingTimeout)
	defer cancel()

	mapper, err := astmapper.NewSharding(ctx, totalShards, s.logger, stats)
	if err != nil {
		return "", nil, err
	}

	expr, err := parser.ParseExpr(query)
	if err != nil {
		return "", nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	shardedQuery, err := mapper.Map(expr)
	if err != nil {
		return "", nil, err
	}

	return shardedQuery.String(), stats, nil
}

// getShardsForQuery calculates and return the number of shards that should be used to run the query.
func (s *querySharding) getShardsForQuery(ctx context.Context, tenantIDs []string, r Request, spanLog log.Logger) int {
	// Check if sharding is disabled for the given request.
	if r.GetOptions().ShardingDisabled {
		return 1
	}

	// TODO: Remove when https://github.com/grafana/mimir/issues/3992 is solved. Also remove EnabledByAnyTenant (unless used elsewhere)
	if validation.EnabledByAnyTenant(tenantIDs, s.limit.NativeHistogramsIngestionEnabled) {
		return 1
	}

	// Check the default number of shards configured for the given tenant.
	totalShards := validation.SmallestPositiveIntPerTenant(tenantIDs, s.limit.QueryShardingTotalShards)
	if totalShards <= 1 {
		return 1
	}

	// Honor the number of shards specified in the request (if any).
	if r.GetOptions().TotalShards > 0 {
		totalShards = int(r.GetOptions().TotalShards)
	}

	maxShardedQueries := validation.SmallestPositiveIntPerTenant(tenantIDs, s.limit.QueryShardingMaxShardedQueries)
	hints := r.GetHints()

	if v, ok := hints.GetCardinalityEstimate().(*Hints_EstimatedSeriesCount); ok && s.maxSeriesPerShard > 0 {
		prevTotalShards := totalShards
		// If an estimate for query cardinality is available, use it to limit the number
		// of shards based on linear interpolation.
		totalShards = util_math.Min(totalShards, int(v.EstimatedSeriesCount/s.maxSeriesPerShard)+1)

		if prevTotalShards != totalShards {
			level.Debug(spanLog).Log(
				"msg", "number of shards has been adjusted to match the estimated series count",
				"updated total shards", totalShards,
				"previous total shards", prevTotalShards,
				"estimated series count", v.EstimatedSeriesCount,
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
		_, shardingStats, err := s.shardQuery(ctx, r.GetQuery(), 1)
		numShardableLegs := 1
		if err == nil && shardingStats.GetShardedQueries() > 0 {
			numShardableLegs = shardingStats.GetShardedQueries()
		}

		prevTotalShards := totalShards
		totalShards = util_math.Max(1, util_math.Min(totalShards, (maxShardedQueries/int(hints.TotalQueries))/numShardableLegs))

		if prevTotalShards != totalShards {
			level.Debug(spanLog).Log(
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
			level.Debug(spanLog).Log("msg", "number of shards has been adjusted to be compatible with compactor shards",
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
			if sample.Point.H != nil {
				ss.Histograms = mimirpb.FromPointsToHistograms([]promql.Point{sample.Point})
			} else {
				ss.Samples = mimirpb.FromPointsToSamples([]promql.Point{sample.Point})
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
			samples := mimirpb.FromPointsToSamples(series.Points)
			if len(samples) > 0 {
				ss.Samples = samples
			}
			histograms := mimirpb.FromPointsToHistograms(series.Points)
			if len(histograms) > 0 {
				ss.Histograms = histograms
			}
			res = append(res, ss)
		}
		return res, nil

	}

	return nil, errors.Errorf("unexpected value type: [%s]", res.Value.Type())
}
