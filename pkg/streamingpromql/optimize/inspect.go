// SPDX-License-Identifier: AGPL-3.0-only

package optimize

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
)

type InspectResult struct {
	// HasSelectors indicates if any node in the tree has matrix or vector selectors.
	HasSelectors bool
	// IsRewrittenByMiddleware indicates if the query has been rewritten by query sharding or by
	// subquery spin-off middlewares.
	IsRewrittenByMiddleware bool
	// CreatedLabels is a set of labels that are the result of a function that creates labels.
	CreatedLabels map[string]struct{}
}

// Inspect traverses a tree of Nodes and returns a result that indicates if the query
// can or should be modified by optimization passes. It is up to each optimization pass
// to decide if this is needed or if the values in InspectResult matter to the pass.
func Inspect(node planning.Node) InspectResult {
	var res InspectResult
	crawlPlanFromNode(node, &res)
	return res
}

func crawlPlanFromNode(node planning.Node, res *InspectResult) {
	switch e := node.(type) {
	case *core.FunctionCall:
		if lbl := createdLabel(e); lbl != "" {
			if res.CreatedLabels == nil {
				res.CreatedLabels = make(map[string]struct{})
			}

			res.CreatedLabels[lbl] = struct{}{}
		}
	case *core.MatrixSelector:
		res.HasSelectors = true
		res.IsRewrittenByMiddleware = res.IsRewrittenByMiddleware || isSpunOff(e.Matchers)
	case *core.VectorSelector:
		res.HasSelectors = true
		res.IsRewrittenByMiddleware = res.IsRewrittenByMiddleware || isSharded(e)
	}

	for c := range planning.ChildrenIter(node) {
		crawlPlanFromNode(c, res)
	}
}

func createdLabel(v *core.FunctionCall) string {
	if (v.Function == functions.FUNCTION_LABEL_REPLACE || v.Function == functions.FUNCTION_LABEL_JOIN) && len(v.Args) > 1 {
		if lbl, ok := v.Args[1].(*core.StringLiteral); ok {
			return lbl.Value
		}
	}

	return ""
}

func isSharded(v *core.VectorSelector) bool {
	for _, m := range v.Matchers {
		if m.Name == model.MetricNameLabel && m.Type == labels.MatchEqual && m.Value == astmapper.EmbeddedQueriesMetricName {
			return true
		}
	}

	return false
}

func isSpunOff(matchers []*core.LabelMatcher) bool {
	for _, m := range matchers {
		if m.Name == model.MetricNameLabel && m.Type == labels.MatchEqual && m.Value == astmapper.SubqueryMetricName {
			return true
		}
	}

	return false
}
