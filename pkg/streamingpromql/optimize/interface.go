// SPDX-License-Identifier: AGPL-3.0-only

package optimize

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser"
)

type ASTOptimizer interface {
	Name() string
	Apply(ctx context.Context, expr parser.Expr) (parser.Expr, error)
}
