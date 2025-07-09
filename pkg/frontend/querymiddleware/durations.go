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
	// Log the instant query and its timestamp in every error log, so that we have more information for debugging failures.
	logger := log.With(d.logger, "query", req.GetQuery(), "query_timestamp", req.GetStart())

	spanLog, ctx := spanlogger.New(ctx, logger, tracer, "durationsMiddleware.Do")
	defer spanLog.Finish()

	expr, err := parser.ParseExpr(req.GetQuery())
	if err != nil {
		level.Warn(spanLog).Log("msg", "failed to parse query", "err", err)
		// This middleware focuses on duration expressions, so if the query is
		// not valid, we just fall through to the next handler.
		return d.next.Do(ctx, req)
	}

	evalVisitor := promql.NewDurationVisitor(time.Duration(req.GetStep()) * time.Second)
	if err := parser.Walk(evalVisitor, expr, nil); err != nil {
		level.Warn(spanLog).Log("msg", "failed to evaluate duration expressions in query", "err", err)
		return nil, apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	}

	checkVisitor := &durationVisitor{}
	if err := parser.Walk(checkVisitor, expr, nil); err != nil {
		level.Warn(spanLog).Log("msg", "the query contains unsupported duration expressions", "err", err)
		return nil, apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	}

	req, err = req.WithExpr(expr)
	if err != nil {
		level.Warn(spanLog).Log("msg", "failed to update the query expression after durations expressions were evaluated", "err", err)
		return nil, apierror.New(apierror.TypeInternal, DecorateWithParamName(err, "query").Error())
	}

	return d.next.Do(ctx, req)
}

type durationVisitor struct{}

// Verify that duration expressions only contain durations that Mimir supports.
// And also clear the original expressions to not confuse the frontend.
func (v *durationVisitor) Visit(node parser.Node, _ []parser.Node) (parser.Visitor, error) {
	switch n := node.(type) {
	case *parser.VectorSelector:
		if n.OriginalOffsetExpr != nil {
			err := v.checkDuration(n.OriginalOffsetExpr)
			if err != nil {
				return nil, err
			}
			n.OriginalOffsetExpr = nil
		}
	case *parser.MatrixSelector:
		if n.RangeExpr != nil {
			err := v.checkDuration(n.RangeExpr)
			if err != nil {
				return nil, err
			}
			n.RangeExpr = nil
		}
	case *parser.SubqueryExpr:
		if n.OriginalOffsetExpr != nil {
			err := v.checkDuration(n.OriginalOffsetExpr)
			if err != nil {
				return nil, err
			}
			n.OriginalOffsetExpr = nil
		}
		if n.StepExpr != nil {
			err := v.checkDuration(n.StepExpr)
			if err != nil {
				return nil, err
			}
			n.StepExpr = nil
		}
		if n.RangeExpr != nil {
			err := v.checkDuration(n.RangeExpr)
			if err != nil {
				return nil, err
			}
			n.RangeExpr = nil
		}
	}
	return v, nil
}

// checkDuration checks if we know how to handle the duration expression.
// Has the same structure as in promql/durations.go, but does not calculate
// the duration.
func (v *durationVisitor) checkDuration(expr parser.Expr) error {
	switch n := expr.(type) {
	case *parser.NumberLiteral:
		return nil
	case *parser.DurationExpr:
		var err error

		if n.LHS != nil {
			err = v.checkDuration(n.LHS)
			if err != nil {
				return err
			}
		}

		if n.RHS != nil {
			err = v.checkDuration(n.RHS)
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
