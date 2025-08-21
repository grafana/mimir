package plan

import (
	"context"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
)

type NarrowBinarySelectorsOptimizationPass struct{}

func NewNarrowBinarySelectorsOptimizationPass() *NarrowBinarySelectorsOptimizationPass {
	return &NarrowBinarySelectorsOptimizationPass{}
}

func (b *NarrowBinarySelectorsOptimizationPass) Name() string {
	return "Narrow selectors used across binary operations"
}

func (b *NarrowBinarySelectorsOptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan) (*planning.QueryPlan, error) {
	groups, err := b.groups(ctx, plan.Root)
	if err != nil {
		return nil, err
	}

	plan.Hints.Group = append(plan.Hints.Group, groups...)
	return plan, nil
}

func (b *NarrowBinarySelectorsOptimizationPass) groups(ctx context.Context, node planning.Node) ([]string, error) {
	// Check for context cancellation before mapping expressions.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var groups []string

	// TODO: This needs to actually crawl the whole tree
	switch e := node.(type) {
	case *core.AggregateExpression:
		if !e.Without {
			groups = append(groups, e.Grouping...)
		}
	case *core.BinaryExpression:
		if e.VectorMatching.On {
			groups = append(groups, e.VectorMatching.MatchingLabels...)
		}

		lhsGroups, err := b.groups(ctx, e.LHS)
		if err != nil {
			return nil, err
		}

		rhsGroups, err := b.groups(ctx, e.RHS)
		if err != nil {
			return nil, err
		}

		groups = append(groups, lhsGroups...)
		groups = append(groups, rhsGroups...)
	}

	return groups, nil
}
