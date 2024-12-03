// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/querysharding.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql/parser"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type subquerySpinoff struct {
	subquerySpinoffMetrics
	limit  Limits
	next   MetricsQueryHandler
	logger log.Logger
}

type subquerySpinoffMetrics struct {
	spunOffSubqueries         prometheus.Counter
	spunOffSubqueriesPerQuery prometheus.Histogram
}

func newSubquerySpinoff(
	logger log.Logger,
	limit Limits,
	registerer prometheus.Registerer,
) MetricsQueryMiddleware {
	metrics := subquerySpinoffMetrics{
		spunOffSubqueries: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_spun_off_subqueries_total",
			Help: "Total number of subqueries spun off as range queries.",
		}),
		spunOffSubqueriesPerQuery: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_frontend_query_sharding_spun_off_subqueries_per_query",
			Help:    "Number of subqueries spun off as range queries per query.",
			Buckets: prometheus.ExponentialBuckets(2, 2, 10),
		}),
	}

	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &subquerySpinoff{
			next:                   next,
			subquerySpinoffMetrics: metrics,
			logger:                 logger,
			limit:                  limit,
		}
	})
}

func (s *subquerySpinoff) Do(ctx context.Context, r MetricsQueryRequest) (Response, error) {
	log := spanlogger.FromContext(ctx, s.logger)

	// Parse the query.
	queryExpr, err := parser.ParseExpr(r.GetQuery())
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	}

	mapStats := astmapper.NewSubqueryMapperStats()
	mapper := astmapper.NewSubqueryMapper(mapStats)

	// Try to rewrite the query
	if queryExpr, err = mapper.Map(queryExpr); err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	if mapStats.GetSpunOffSubqueries() == 0 {
		return s.next.Do(ctx, r)
	}

	newQuery := queryExpr.String()

	// TODO: revert to debug
	level.Info(log).Log("msg", "subqueries will be spun off", "original", r.GetQuery(), "rewritten", newQuery)

	// Update metrics.
	s.spunOffSubqueries.Add(float64(mapStats.GetSpunOffSubqueries()))
	s.spunOffSubqueriesPerQuery.Observe(float64(mapStats.GetSpunOffSubqueries()))

	// Update query stats.
	queryStats := stats.FromContext(ctx)
	queryStats.AddSpunOffSubqueries(uint32(mapStats.GetSpunOffSubqueries()))

	r, err = r.WithQuery(newQuery)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	return s.next.Do(ctx, r)
}
