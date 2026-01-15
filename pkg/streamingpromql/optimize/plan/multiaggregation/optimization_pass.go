// SPDX-License-Identifier: AGPL-3.0-only

package multiaggregation

import (
	"context"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
)

type OptimizationPass struct{}

func NewOptimizationPass() *OptimizationPass {
	return &OptimizationPass{}
}

func (o *OptimizationPass) Name() string {
	return "Multi-aggregation"
}

func (o *OptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, maximumSupportedQueryPlanVersion planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	//TODO implement me
	panic("implement me")
}
