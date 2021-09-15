// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/querysharding.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queryrange

import (
	"context"
	"net/http"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/grafana/mimir/pkg/querier/astmapper"
	"github.com/grafana/mimir/pkg/querier/lazyquery"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/tenant"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

type querySharding struct {
	limit Limits

	engine *promql.Engine
	next   Handler
	logger log.Logger

	// Metrics.
	shardingAttempts       prometheus.Counter
	shardingSuccesses      prometheus.Counter
	shardedQueries         prometheus.Counter
	shardedQueriesPerQuery prometheus.Histogram
}

// NewQueryShardingMiddleware creates a middleware that will split queries by shard.
// It first looks at the query to determine if it is shardable or not.
// Then rewrite the query into a sharded query and use the PromQL engine to execute the query.
// Sub shard queries are embedded into a single vector selector and a modified `Queryable` (see ShardedQueryable) is passed
// to the PromQL engine.
// Finally we can translate the embedded vector selector back into subqueries in the Queryable and send them in parallel to downstream.
func NewQueryShardingMiddleware(
	logger log.Logger,
	engine *promql.Engine,
	limit Limits,
	registerer prometheus.Registerer,
) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		return &querySharding{
			next: next,
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
			engine: engine,
			logger: logger,
			limit:  limit,
		}
	})
}

func (s *querySharding) Do(ctx context.Context, r Request) (Response, error) {
	log, ctx := spanlogger.NewWithLogger(ctx, s.logger, "querySharding.Do")
	defer log.Span.Finish()

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
	}

	totalShards := validation.SmallestPositiveIntPerTenant(tenantIDs, s.limit.QueryShardingTotalShards)

	if r.GetOptions().ShardingDisabled || totalShards <= 0 {
		return s.next.Do(ctx, r)
	}
	if r.GetOptions().TotalShards > 0 {
		totalShards = int(r.GetOptions().TotalShards)
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
	shardedQueryable := NewShardedQueryable(r, s.next)

	qry, err := s.engine.NewRangeQuery(
		lazyquery.NewLazyQueryable(shardedQueryable),
		r.GetQuery(),
		util.TimeFromMillis(r.GetStart()),
		util.TimeFromMillis(r.GetEnd()),
		time.Duration(r.GetStep())*time.Millisecond,
	)
	if err != nil {
		return nil, err
	}

	res := qry.Exec(ctx)
	extracted, err := FromResult(res)
	if err != nil {
		return nil, err
	}
	return &PrometheusResponse{
		Status: StatusSuccess,
		Data: PrometheusData{
			ResultType: string(res.Value.Type()),
			Result:     extracted,
		},
		Headers: shardedQueryable.getResponseHeaders(),
	}, nil
}

// shardQuery attempts to rewrite the input query in a shardable way. Returns the rewritten query
// to be executed by PromQL engine with ShardedQueryable or an empty string if the input query
// can't be sharded.
func (s *querySharding) shardQuery(query string, totalShards int) (string, *astmapper.MapperStats, error) {
	mapper, err := astmapper.NewSharding(totalShards)
	if err != nil {
		return "", nil, err
	}

	expr, err := parser.ParseExpr(query)
	if err != nil {
		return "", nil, err
	}

	stats := astmapper.NewMapperStats()
	shardedQuery, err := mapper.Map(expr, stats)
	if err != nil {
		return "", nil, err
	}

	return shardedQuery.String(), stats, nil
}
