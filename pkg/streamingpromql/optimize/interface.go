// SPDX-License-Identifier: AGPL-3.0-only

package optimize

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
)

type ASTOptimizer interface {
	Name() string
	Apply(ctx context.Context, expr parser.Expr) (parser.Expr, error)
}

type QueryPlanOptimizer interface {
	Name() string
	Apply(ctx context.Context, plan *planning.QueryPlan) (*planning.QueryPlan, error)
}
