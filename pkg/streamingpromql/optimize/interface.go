// SPDX-License-Identifier: AGPL-3.0-only

package optimize

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/requestoptions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
)

type ASTOptimizationPass interface {
	Name() string
	Apply(ctx context.Context, expr parser.Expr, opts requestoptions.Options) (parser.Expr, error)
}

type QueryPlanOptimizationPass interface {
	Name() string
	Apply(ctx context.Context, plan *planning.QueryPlan, maximumSupportedQueryPlanVersion planning.QueryPlanVersion, opts requestoptions.Options) (*planning.QueryPlan, error)
}
