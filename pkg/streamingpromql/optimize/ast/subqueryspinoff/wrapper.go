// SPDX-License-Identifier: AGPL-3.0-only

package subqueryspinoff

import (
	"fmt"
	"time"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
)

// evaluationRootWrapper is the astmapper.SubquerySpinOffWrapper used to spin off subqueries inside the
// Mimir query engine. Unlike the query-frontend middleware's wrapper, which rewrites downstream queries
// and spun-off subqueries into fake metric selectors resolved by an injected Queryable, this wrapper
// marks each one with the internal __vector_evaluation_root__ or __scalar_evaluation_root__ function so that the sharding, splitting,
// caching and remote execution optimization passes can treat each as a separate query.
type evaluationRootWrapper struct{}

// NewEvaluationRootWrapper returns a SubquerySpinOffWrapper that marks downstream queries and spun-off
// subqueries with the __vector_evaluation_root__ or __scalar_evaluation_root__ function.
func NewEvaluationRootWrapper() astmapper.SubquerySpinOffWrapper {
	return evaluationRootWrapper{}
}

func (evaluationRootWrapper) WrapDownstreamQuery(expr parser.Expr) (parser.Expr, error) {
	switch expr.Type() {
	case parser.ValueTypeVector:
		return wrapInVectorEvaluationRoot(expr), nil
	case parser.ValueTypeScalar:
		return wrapInScalarEvaluationRoot(expr), nil
	default:
		return nil, fmt.Errorf("evaluationRootWrapper: unsupported expression type %s", expr.Type())
	}
}

func (evaluationRootWrapper) WrapSubquery(subquery *parser.SubqueryExpr, step time.Duration) (parser.Expr, error) {
	// Keep the subquery so that the engine evaluates the marked inner expression as a range query over
	// the subquery's range at its step, exactly as a subquery is evaluated today. The __vector_evaluation_root__
	// marker around the inner expression is what causes that range query to be sharded, split, cached and
	// executed remotely as a separate query.
	//
	// The subquery is guaranteed not to have an @ modifier (see subqueryCanBeSpunOff), so there's no
	// timestamp to preserve.
	subquery.Expr = wrapInVectorEvaluationRoot(subquery.Expr)
	subquery.Step = step
	return subquery, nil
}

func wrapInVectorEvaluationRoot(expr parser.Expr) parser.Expr {
	return &parser.Call{
		Func: core.VectorEvaluationRootFunction,
		Args: []parser.Expr{expr},
	}
}

func wrapInScalarEvaluationRoot(expr parser.Expr) parser.Expr {
	return &parser.Call{
		Func: core.ScalarEvaluationRootFunction,
		Args: []parser.Expr{expr},
	}
}
