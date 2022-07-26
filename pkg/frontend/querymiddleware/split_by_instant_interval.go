// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/storage/lazyquery"
)

// TODO: add metrics

// splitByInstantIntervalMiddleware is a Middleware that can (optionally) split the instant query by splitInterval
type splitByInstantIntervalMiddleware struct {
	next   Handler
	limits Limits
	logger log.Logger

	engine *promql.Engine

	splitEnabled  bool
	splitInterval time.Duration
}

// newSplitByInstantIntervalMiddleware makes a new splitByInstantIntervalMiddleware.
func newSplitByInstantIntervalMiddleware(
	splitEnabled bool,
	splitInterval time.Duration,
	limits Limits,
	logger log.Logger,
	engine *promql.Engine) Middleware {

	return MiddlewareFunc(func(next Handler) Handler {
		return &splitByInstantIntervalMiddleware{
			splitEnabled:  splitEnabled,
			next:          next,
			limits:        limits,
			splitInterval: splitInterval,
			logger:        logger,
			engine:        engine,
		}
	})
}

func (s *splitByInstantIntervalMiddleware) Do(ctx context.Context, req Request) (Response, error) {
	_, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	if !s.splitEnabled || s.splitInterval <= 0 {
		return s.next.Do(ctx, req)
	}

	mapper, err := astmapper.NewInstantSplitter(s.splitInterval, s.logger)
	if err != nil {
		return s.next.Do(ctx, req)
	}

	expr, err := parser.ParseExpr(req.GetQuery())
	if err != nil {
		return s.next.Do(ctx, req)
	}

	stats := astmapper.NewMapperStats()
	instantSplitQuery, err := mapper.Map(expr, stats)
	if err != nil {
		return s.next.Do(ctx, req)
	}

	noop := instantSplitQuery.String() == expr.String()
	if noop {
		// the query cannot be split, so continue
		return s.next.Do(ctx, req)
	}

	// Send hint with number of embedded queries to the sharding middleware
	hints := &Hints{TotalQueries: int32(stats.GetShardedQueries())}

	req = req.WithQuery(instantSplitQuery.String()).WithHints(hints)
	shardedQueryable := newShardedQueryable(req, s.next)

	qry, err := newQuery(req, s.engine, lazyquery.NewLazyQueryable(shardedQueryable))
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
