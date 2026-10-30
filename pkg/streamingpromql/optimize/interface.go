// SPDX-License-Identifier: AGPL-3.0-only

package optimize

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
)

type ASTOptimizationPass interface {
	// Name returns a human-readable name for this optimization pass suitable for a UI.
	Name() string

	// Slug returns a kabob-case (`example-pass`) name for this optimization pass suitable for use in configuration or HTTP requests.
	Slug() string

	Apply(ctx context.Context, expr parser.Expr, params *planning.QueryParameters) (parser.Expr, error)
}

type QueryPlanOptimizationPass interface {
	// Name returns a human-readable name for this optimization pass suitable for a UI.
	Name() string

	// Slug returns a kabob-case (`example-pass`) name for this optimization pass suitable for use in configuration or HTTP requests.
	Slug() string

	Apply(ctx context.Context, plan *planning.QueryPlan, maximumSupportedQueryPlanVersion planning.QueryPlanVersion) (*planning.QueryPlan, error)
}
