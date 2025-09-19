// SPDX-License-Identifier: AGPL-3.0-only

package optimize

import (
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
)

type InspectResult struct {
	// HasSelectors indicates if any node in the tree has matrix or vector selectors.
	HasSelectors bool
	// IsRewritten indicates if the query has been rewritten by query sharding or by
	// subquery spin-off middlewares.
	IsRewritten bool
}

// Inspect traverses a tree of Nodes and returns a result that indicates if the query
// can or should be modified by optimization passes. It is up to each optimization pass
// to decide if this is needed or if the values in InspectResult matter to the pass.
func Inspect(node planning.Node) InspectResult {
	switch e := node.(type) {
	case *core.MatrixSelector:
		return InspectResult{
			HasSelectors: true,
			IsRewritten:  isSpunOff(e.Matchers),
		}
	case *core.VectorSelector:
		return InspectResult{
			HasSelectors: true,
			IsRewritten:  isSharded(e),
		}
	default:
		anyChildContainsSelectors := false

		for _, c := range e.Children() {
			res := Inspect(c)
			if res.IsRewritten {
				return InspectResult{
					HasSelectors: true,
					IsRewritten:  true,
				}
			}

			anyChildContainsSelectors = anyChildContainsSelectors || res.HasSelectors
		}

		return InspectResult{
			HasSelectors: anyChildContainsSelectors,
			IsRewritten:  false,
		}
	}
}

func isSharded(v *core.VectorSelector) bool {
	for _, m := range v.Matchers {
		if m.Name == labels.MetricName && m.Type == labels.MatchEqual && m.Value == astmapper.EmbeddedQueriesMetricName {
			return true
		}
	}

	return false
}

func isSpunOff(matchers []*core.LabelMatcher) bool {
	for _, m := range matchers {
		if m.Name == labels.MetricName && m.Type == labels.MatchEqual && m.Value == astmapper.SubqueryMetricName {
			return true
		}
	}

	return false
}
