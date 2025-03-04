// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func (e *Engine) NewQueryPlan(ctx context.Context, qs string, timeRange types.QueryTimeRange) (*planning.QueryPlan, error) {
	expr, err := parser.ParseExpr(qs)
	if err != nil {
		return nil, err
	}

	expr = promql.PreprocessExpr(expr, timestamp.Time(timeRange.StartT), timestamp.Time(timeRange.EndT))

	root, err := e.nodeFromExpr(expr)
	if err != nil {
		return nil, err
	}

	// TODO: apply optimisations

	plan := &planning.QueryPlan{
		TimeRange: timeRange,
		Root:      root,
	}

	return plan, nil
}

func (e *Engine) nodeFromExpr(expr parser.Expr) (planning.Node, error) {
	switch e := expr.(type) {
	case *parser.VectorSelector:
		return &planning.VectorSelector{
			Matchers:           e.LabelMatchers,
			Timestamp:          e.Timestamp,
			Offset:             e.OriginalOffset,
			ExpressionPosition: e.PositionRange(),
		}, nil

	default:
		return nil, fmt.Errorf("unknown expression type: %T", e)
	}
}

func (e *Engine) EncodeQueryPlan(plan *planning.QueryPlan) ([]byte, error) {
	return e.jsonConfig.Marshal(plan)
}

func (e *Engine) DecodeQueryPlan(data []byte) (*planning.QueryPlan, error) {
	plan := &planning.QueryPlan{}

	if err := e.jsonConfig.Unmarshal(data, plan); err != nil {
		return nil, err
	}

	return plan, nil
}

func (e *Engine) Materialize(ctx context.Context, plan *planning.QueryPlan, q storage.Queryable, opts promql.QueryOpts) (promql.Query, error) {
	// TODO
	panic("TODO")
}
