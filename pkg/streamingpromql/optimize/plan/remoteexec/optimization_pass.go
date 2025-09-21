// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
)

// OptimizationPass identifies subplans of the provided query plan that can be executed remotely.
type OptimizationPass struct {
}

func NewOptimizationPass() *OptimizationPass {
	return &OptimizationPass{}
}

func (o *OptimizationPass) Name() string {
	return "Remote execution"
}

func (o *OptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan) (*planning.QueryPlan, error) {
	containsSelectors, containsShardedOrSpunOffExpression := o.inspect(plan.Root)

	if !containsSelectors || containsShardedOrSpunOffExpression {
		return plan, nil
	}

	var err error
	plan.Root, err = o.wrapInRemoteExecutionNode(plan.Root)
	if err != nil {
		return nil, err
	}

	return plan, nil
}

func (o *OptimizationPass) wrapInRemoteExecutionNode(child planning.Node) (planning.Node, error) {
	n := &RemoteExecution{}
	if err := n.SetChildren([]planning.Node{child}); err != nil {
		return nil, err
	}

	return n, nil
}

// inspect returns two booleans:
// - the first indicates if node or any of its children is a selector
// - the second indicates if node or any of its children is a vector selector containing a sharded or spun-off expression
func (o *OptimizationPass) inspect(node planning.Node) (bool, bool) {
	switch n := node.(type) {
	case *core.MatrixSelector:
		return true, isSpunOff(n.Matchers)
	case *core.VectorSelector:
		return true, isSharded(n)
	default:
		anyChildContainsSelectors := false

		for _, c := range n.Children() {
			containsSelectors, containsShardedOrSpunOffExpression := o.inspect(c)
			if containsShardedOrSpunOffExpression {
				return true, true
			}

			anyChildContainsSelectors = anyChildContainsSelectors || containsSelectors
		}

		return anyChildContainsSelectors, false
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
