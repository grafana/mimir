// SPDX-License-Identifier: AGPL-3.0-only

package plan

import (
	"context"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions"
)

// NodeIdentifierOptimizationPass assigns a unique ID to each node in a query plan
// in a deterministic order (depth first). The same IDs will be generated for the same
// nodes given the same query plan.
type NodeIdentifierOptimizationPass struct{}

func NewNodeIdentifierOptimizationPass() *NodeIdentifierOptimizationPass {
	return &NodeIdentifierOptimizationPass{}
}

func (n *NodeIdentifierOptimizationPass) Name() string {
	return "node identifier"
}

func (n *NodeIdentifierOptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, _ planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	// Only add an ID to each node if we have been asked to generate extra cost information.
	options := requestoptions.OptionsFromContext(ctx)
	if len(options.Explain) == 0 {
		return plan, nil
	}

	id := int64(1)
	err := optimize.Walk(plan.Root, optimize.VisitorFunc(func(node planning.Node, path []planning.Node) (bool, error) {
		if node.GetPlanningId() == 0 {
			node.SetPlanningId(id)
			id++
		}

		return true, nil
	}))

	return plan, err
}
