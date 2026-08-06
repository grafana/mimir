// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize/matchers"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

func NewReduceMatchers(reg prometheus.Registerer, logger log.Logger) *ReduceMatchers {
	return &ReduceMatchers{
		attempts: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_reduce_matchers_attempted_total",
			Help: "Total number of queries that the optimization pass has attempted to reduce matchers for.",
		}),
		success: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_reduce_matchers_modified_total",
			Help: "Total number of queries where the optimization pass has been able to reduce matchers for.",
		}),
		logger: logger,
	}
}

// ReduceMatchers deduplicates matchers from vector or matrix selectors, removes matchers that
// select for all non-empty values if a more selective matcher for the same label name already
// exists, and removes matchers that select a superset of other matchers. Input order of matchers
// is NOT preserved in the rewritten expression.
type ReduceMatchers struct {
	attempts prometheus.Counter
	success  prometheus.Counter
	logger   log.Logger
}

func (c *ReduceMatchers) Name() string {
	return "Reduce matchers"
}

func (c *ReduceMatchers) Apply(ctx context.Context, root parser.Expr, _ *planning.QueryParameters) (parser.Expr, error) {
	spanlog := spanlogger.FromContext(ctx, c.logger)
	c.attempts.Inc()

	matchersReduced := false
	c.apply(root, func(node parser.Node, keepWildcardsForInfoDataSelector bool) {
		switch expr := node.(type) {
		case *parser.VectorSelector:
			retained, dropped := matchers.Reduce(expr.LabelMatchers, keepWildcardsForInfoDataSelector)

			if len(dropped) > 0 {
				expr.LabelMatchers = retained
				matchersReduced = true
				spanlog.DebugLog(
					"msg", "dropped matchers for vector selector",
					"retained", util.MatchersStringer(retained),
					"dropped", util.MatchersStringer(dropped),
				)
			}
		case *parser.MatrixSelector:
			retained, dropped := matchers.Reduce(expr.VectorSelector.(*parser.VectorSelector).LabelMatchers, keepWildcardsForInfoDataSelector)

			if len(dropped) > 0 {
				expr.VectorSelector.(*parser.VectorSelector).LabelMatchers = retained
				matchersReduced = true
				spanlog.DebugLog(
					"msg", "dropped matchers for matrix selector",
					"retained", util.MatchersStringer(retained),
					"dropped", util.MatchersStringer(dropped),
				)
			}
		}
	}, false)

	if matchersReduced {
		c.success.Inc()
	}

	return root, nil
}

func (c *ReduceMatchers) apply(node parser.Node, fn func(parser.Node, bool), keepWildcardsForInfoDataSelector bool) {
	if node == nil {
		return
	}

	if call, ok := node.(*parser.Call); ok && call.Func.Name == "info" {
		// Only reduce matchers for the first argument of info(), not the second.
		c.apply(call.Args[0], fn, false)
		// The InsertOmittedTargetInfoSelector AST pass ensures there are always 2 arguments.
		// Check len(Args) == 2 for safety in case the pass doesn't run (e.g., in tests).
		if len(call.Args) == 2 {
			c.apply(call.Args[1], fn, true)
		}
		return
	}

	fn(node, keepWildcardsForInfoDataSelector)

	for child := range parser.ChildrenIter(node) {
		c.apply(child, fn, false)
	}
}
