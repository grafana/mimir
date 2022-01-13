// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/querysharding.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queryrange

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/astmapper"
	"github.com/grafana/mimir/pkg/querier/lazyquery"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/tenant"
	"github.com/grafana/mimir/pkg/util"
	util_math "github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

type querySharding struct {
	limit Limits

	engine *promql.Engine
	next   Handler
	logger log.Logger

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
	registerer prometheus.Registerer,
) Middleware {
	metrics := queryShardingMetrics{
		shardingAttempts: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "frontend_query_sharding_rewrites_attempted_total",
			Help:      "Total number of queries the query-frontend attempted to shard.",
		}),
		shardingSuccesses: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "frontend_query_sharding_rewrites_succeeded_total",
			Help:      "Total number of queries the query-frontend successfully rewritten in a shardable way.",
		}),
		shardedQueries: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "frontend_sharded_queries_total",
			Help:      "Total number of sharded queries.",
		}),
		shardedQueriesPerQuery: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "frontend_sharded_queries_per_query",
			Help:      "Number of sharded queries a single query has been rewritten to.",
			Buckets:   prometheus.ExponentialBuckets(2, 2, 10),
		}),
	}
	return MiddlewareFunc(func(next Handler) Handler {
		return &querySharding{
			next:                 next,
			queryShardingMetrics: metrics,
			engine:               engine,
			logger:               logger,
			limit:                limit,
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

	totalShards := s.getShardsForQuery(tenantIDs, r, log)
	if totalShards <= 1 {
		level.Debug(log).Log("msg", "query sharding is disabled for this query or tenant")
		return s.next.Do(ctx, r)
	}

	s.shardingAttempts.Inc()
	shardedQuery, shardingStats, err := s.shardQuery(r.GetQuery(), totalShards)

	// If an error occurred while trying to rewrite the query or the query has not been sharded,
	// then we should fallback to execute it via queriers.
	if err != nil || shardingStats.GetShardedQueries() == 0 {
		if err != nil {
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
			r.GetQuery(),
			util.TimeFromMillis(r.GetStart()),
			util.TimeFromMillis(r.GetEnd()),
			time.Duration(r.GetStep())*time.Millisecond,
		)
	case *PrometheusInstantQueryRequest:
		return engine.NewInstantQuery(
			queryable,
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

	// By default, all errors returned by engine.Eval() are execution errors,
	// This is the same as Prometheus API does: http://github.com/prometheus/prometheus/blob/076109fa1910ad2198bf2c447a174fee31114982/web/api/v1/api.go#L550-L550
	errorType := apierror.TypeExec
	// However, some of our errors may come from the shardedQueryable,
	// those are internal unless explicitly signalled to be something else.
	if storageErr := (promql.ErrStorage{}); errors.As(err, &storageErr) {
		errorType = apierror.TypeInternal
		// Unwrap the underlying error, so we can get its cause and see if it was a timeout, etc.
		err = storageErr.Err
	}

	// This is the common part:
	// - The error could be wrapped by the PromQL engine and be a timeout, canceled, etc.
	// - Or the error could come from the storage and also be a timeout, canceled, etc.
	// We get the error's cause in order to correctly parse the error in parent callers
	// (eg. gRPC response status code extraction).
	cause := errors.Cause(err)
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
func (s *querySharding) shardQuery(query string, totalShards int) (string, *astmapper.MapperStats, error) {
	mapper, err := astmapper.NewSharding(totalShards, s.logger)
	if err != nil {
		return "", nil, err
	}

	expr, err := parser.ParseExpr(query)
	if err != nil {
		return "", nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	stats := astmapper.NewMapperStats()
	shardedQuery, err := mapper.Map(expr, stats)
	if err != nil {
		return "", nil, err
	}

	return shardedQuery.String(), stats, nil
}

// getShardsForQuery calculates and return the number of shards that should be used to run the query.
func (s *querySharding) getShardsForQuery(tenantIDs []string, r Request, spanLog log.Logger) int {
	// Check if sharding is disabled for the given request.
	if r.GetOptions().ShardingDisabled {
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
		_, shardingStats, err := s.shardQuery(r.GetQuery(), 1)
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
			res = append(res, SampleStream{
				Labels:  mimirpb.FromLabelsToLabelAdapters(sample.Metric),
				Samples: []mimirpb.Sample{{TimestampMs: sample.Point.T, Value: sample.Point.V}}})
		}
		return res, nil

	case promql.Matrix:
		res := make([]SampleStream, 0, len(v))
		for _, series := range v {
			res = append(res, SampleStream{
				Labels:  mimirpb.FromLabelsToLabelAdapters(series.Metric),
				Samples: mimirpb.FromPointsToSamples(series.Points),
			})
		}
		return res, nil

	}

	return nil, errors.Errorf("unexpected value type: [%s]", res.Value.Type())
}
