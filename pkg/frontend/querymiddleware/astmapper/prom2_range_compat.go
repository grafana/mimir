// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
)

// NewProm2RangeCompat creates a new ASTMapper which modifies the range of subqueries
// with identical ranges and steps (which used to returns results in Prometheus 2 since
// range selectors were left closed right closed) to be compatible with Prometheus 3
// range selectors which are left open right closed.
func NewProm2RangeCompat(ctx context.Context) ASTMapper {
	compat := &prom2RangeCompat{ctx: ctx}
	return NewASTExprMapper(compat)
}

type prom2RangeCompat struct {
	ctx context.Context
}

func (c prom2RangeCompat) MapExpr(expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
	if err := c.ctx.Err(); err != nil {
		return nil, false, err
	}

	e, ok := expr.(*parser.SubqueryExpr)
	if !ok {
		return expr, false, nil
	}

	// Due to range selectors being left open right closed in Prometheus 3, subqueries with identical
	// range and step will only select a single datapoint which breaks functions that need multiple
	// points (rate, increase). Adjust the range here slightly to ensure that multiple data points
	// are returned to match the Prometheus 2 behavior.
	if e.Range == e.Step {
		e.Range = e.Range + time.Millisecond
	}

	return e, false, nil
}
