// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// newDurationsMiddleware returns a MetricsQueryMiddleware that evaluates and
// rewrites duration expressions in the query.
// As long as durations can be calculated solely from consts and the step
// this works. When that is not the case anymore, we'll have to implement
// checks before sharding/splitting/etc middlewares individually to check what
// we _can_ support.
func newDurationsMiddleware(
	logger log.Logger,
) MetricsQueryMiddleware {
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &durationsMiddleware{
			next:   next,
			logger: logger,
		}
	})
}

type durationsMiddleware struct {
	next   MetricsQueryHandler
	logger log.Logger
}

func (d *durationsMiddleware) Do(ctx context.Context, req MetricsQueryRequest) (Response, error) {
	var err error
	req, err = d.rewriteIfNeeded(ctx, req)
	if err != nil {
		return nil, err
	}
	return d.next.Do(ctx, req)
}

func (d *durationsMiddleware) rewriteIfNeeded(ctx context.Context, req MetricsQueryRequest) (MetricsQueryRequest, error) {
	// Log the instant query and its timestamp in every error log, so that we have more information for debugging failures.
	logger := log.With(d.logger, "query", req.GetQuery(), "query_timestamp", req.GetStart())

	spanLog, _ := spanlogger.New(ctx, logger, tracer, "durationsMiddleware.Do")
	defer spanLog.Finish()

	origQuery := req.GetQuery()
	expr, err := astmapper.CloneExpr(req.GetParsedQuery())
	if err != nil {
		// This middleware focuses on duration expressions, so if the query is
		// not valid, we just fall through to the next handler.
		return req, nil
	}

	expr, err = promql.PreprocessExpr(expr, time.UnixMilli(req.GetStart()), time.UnixMilli(req.GetEnd()), time.Duration(req.GetStep())*time.Second)
	if err != nil {
		level.Warn(spanLog).Log("msg", "failed to evaluate duration expressions in query", "err", err)
		return nil, apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	}

	if err := inspect(expr, durationVisitor); err != nil {
		level.Warn(spanLog).Log("msg", "the query contains unsupported duration expressions", "err", err)
		return nil, apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	}

	newQuery := expr.String()
	if origQuery == newQuery {
		return req, nil
	}

	level.Debug(spanLog).Log("msg", "query duration expressions have been rewritten", "rewritten", newQuery)

	req, err = req.WithExpr(expr)
	if err != nil {
		level.Warn(spanLog).Log("msg", "failed to update the query expression after durations expressions were evaluated", "err", err)
		return nil, apierror.New(apierror.TypeInternal, DecorateWithParamName(err, "query").Error())
	}

	return req, nil
}

// durationVisitor verifies that duration expressions only contain durations that Mimir supports.
// And also clear the original expressions to not confuse the frontend.
func durationVisitor(node parser.Node) error {
	switch n := node.(type) {
	case *parser.VectorSelector:
		if n.OriginalOffsetExpr != nil {
			err := checkDuration(n.OriginalOffsetExpr)
			if err != nil {
				return err
			}
			n.OriginalOffsetExpr = nil
		}
	case *parser.MatrixSelector:
		if n.RangeExpr != nil {
			err := checkDuration(n.RangeExpr)
			if err != nil {
				return err
			}
			n.RangeExpr = nil
		}
	case *parser.SubqueryExpr:
		if n.OriginalOffsetExpr != nil {
			err := checkDuration(n.OriginalOffsetExpr)
			if err != nil {
				return err
			}
			n.OriginalOffsetExpr = nil
		}
		if n.StepExpr != nil {
			err := checkDuration(n.StepExpr)
			if err != nil {
				return err
			}
			n.StepExpr = nil
		}
		if n.RangeExpr != nil {
			err := checkDuration(n.RangeExpr)
			if err != nil {
				return err
			}
			n.RangeExpr = nil
		}
	}
	return nil
}

// checkDuration checks if we know how to handle the duration expression.
// Has the same structure as in promql/durations.go, but does not calculate
// the duration.
func checkDuration(expr parser.Expr) error {
	switch n := expr.(type) {
	case *parser.NumberLiteral:
		return nil
	case *parser.DurationExpr:
		var err error

		if n.LHS != nil {
			err = checkDuration(n.LHS)
			if err != nil {
				return err
			}
		}

		if n.RHS != nil {
			err = checkDuration(n.RHS)
			if err != nil {
				return err
			}
		}

		switch n.Op {
		case parser.STEP:
			return nil
		case parser.MIN:
			return nil
		case parser.MAX:
			return nil
		case parser.ADD:
			return nil
		case parser.SUB:
			return nil
		case parser.MUL:
			return nil
		case parser.DIV:
			return nil
		case parser.MOD:
			return nil
		case parser.POW:
			return nil
		default:
			return fmt.Errorf("unexpected duration expression operator %q in query-frontend", n.Op)
		}
	default:
		return fmt.Errorf("unexpected duration expression type %T in query-frontend", n)
	}
}

// inspect recursively traverses a PromQL AST and calls fn for each node encountered.
// If fn returns an error, traversal stops and that error is returned by inspect.
func inspect(node parser.Node, fn func(node parser.Node) error) error {
	if err := fn(node); err != nil {
		return err
	}
	for e := range parser.ChildrenIter(node) {
		if err := inspect(e, fn); err != nil {
			return err
		}
	}
	return nil
}
