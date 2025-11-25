package testutils

import (
	"context"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
)

func RewriteForQuerySharding(ctx context.Context, expr parser.Expr) (parser.Expr, error) {
	const maxShards = 2
	stats := astmapper.NewMapperStats()
	squasher := astmapper.EmbeddedQueriesSquasher
	summer := astmapper.NewQueryShardSummer(maxShards, squasher, log.NewNopLogger(), stats)

	shardedQuery, err := summer.Map(ctx, expr)
	if err != nil {
		return nil, err
	}

	return shardedQuery, nil
}

func RewriteForSubquerySpinoff(ctx context.Context, expr parser.Expr) (parser.Expr, error) {
	stats := astmapper.NewSubquerySpinOffMapperStats()
	defaultStepFunc := func(rangeMillis int64) int64 { return 1000 }
	mapper := astmapper.NewSubquerySpinOffMapper(defaultStepFunc, log.NewNopLogger(), stats)

	rewrittenQuery, err := mapper.Map(ctx, expr)
	if err != nil {
		return nil, err
	}

	if stats.SpunOffSubqueries() == 0 {
		return expr, nil
	}

	return rewrittenQuery, nil
}
