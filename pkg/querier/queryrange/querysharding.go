// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/querysharding.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queryrange

import (
	"context"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/querier/astmapper"
	"github.com/grafana/mimir/pkg/querier/lazyquery"
	"github.com/grafana/mimir/pkg/util"
)

type querySharding struct {
	totalShards int

	engine *promql.Engine
	next   Handler
	logger log.Logger

	mappedASTCounter      prometheus.Counter
	shardedQueriesCounter prometheus.Counter
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
	totalShards int,
	registerer prometheus.Registerer,
) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		return &querySharding{
			next: next,
			mappedASTCounter: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
				Namespace: "cortex",
				Name:      "frontend_mapped_asts_total",
				Help:      "Total number of queries that have undergone AST mapping",
			}),
			shardedQueriesCounter: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
				Namespace: "cortex",
				Name:      "frontend_sharded_queries_total",
				Help:      "Total number of sharded queries",
			}),
			engine:      engine,
			totalShards: totalShards,
			logger:      logger,
		}
	})
}

func (s *querySharding) Do(ctx context.Context, r Request) (Response, error) {
	mapper, err := astmapper.NewSharding(s.totalShards, s.shardedQueriesCounter)
	if err != nil {
		return nil, err
	}

	expr, err := parser.ParseExpr(r.GetQuery())
	if err != nil {
		return nil, err
	}
	mappedQuery, err := mapper.Map(expr)
	if err != nil {
		return nil, err
	}

	strMappedQuery := mappedQuery.String()
	level.Debug(s.logger).Log("msg", "mapped query", "original", r.GetQuery(), "mapped", strMappedQuery)
	s.mappedASTCounter.Inc()
	r = r.WithQuery(strMappedQuery)

	shardedQueryable := &ShardedQueryable{Req: r, Handler: s.next}

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
