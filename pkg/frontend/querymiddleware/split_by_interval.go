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

// splitByIntervalMiddleware is a Middleware that can (optionally) split the query by interval
type splitByIntervalMiddleware struct {
	next   Handler
	limits Limits
	logger log.Logger

	engine *promql.Engine

	// Split by interval
	splitEnabled  bool
	splitInterval time.Duration
}

// newSplitByIntervalMiddleware makes a new splitAndCacheMiddleware.
func newSplitByIntervalMiddleware(
	splitEnabled bool,
	splitInterval time.Duration,
	limits Limits,
	logger log.Logger,
	engine *promql.Engine) Middleware {

	return MiddlewareFunc(func(next Handler) Handler {
		return &splitByIntervalMiddleware{
			splitEnabled:  splitEnabled,
			next:          next,
			limits:        limits,
			splitInterval: splitInterval,
			logger:        logger,
			engine:        engine,
		}
	})
}

func (s *splitByIntervalMiddleware) Do(ctx context.Context, req Request) (Response, error) {
	_, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	// TODO: configure limit based on tenant ID
	mapper, err := astmapper.NewRangeMapper(s.splitInterval, s.logger)
	if err != nil {
		return s.next.Do(ctx, req)
	}

	expr, err := parser.ParseExpr(req.GetQuery())
	if err != nil {
		return s.next.Do(ctx, req)
	}

	stats := astmapper.NewMapperStats()
	rangedQuery, err := mapper.Map(expr, stats)
	if err != nil {
		return s.next.Do(ctx, req)
	}

	req = req.WithQuery(rangedQuery.String())
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
