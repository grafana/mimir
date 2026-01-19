// SPDX-License-Identifier: AGPL-3.0-only

package multiaggregation

import (
	"context"
	"fmt"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
)

type OptimizationPass struct{}

func NewOptimizationPass() *OptimizationPass {
	return &OptimizationPass{}
}

func (o *OptimizationPass) Name() string {
	return "Multi-aggregation"
}

func (o *OptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, maximumSupportedQueryPlanVersion planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	return plan, nil
}

func IsSupportedAggregationOperation(o core.AggregationOperation) (bool, error) {
	switch o {
	case core.AGGREGATION_SUM:
		return true, nil
	case core.AGGREGATION_COUNT:
		return true, nil
	case core.AGGREGATION_MIN:
		return true, nil
	case core.AGGREGATION_MAX:
		return true, nil
	case core.AGGREGATION_AVG:
		return true, nil
	case core.AGGREGATION_GROUP:
		return true, nil
	case core.AGGREGATION_STDVAR:
		return true, nil
	case core.AGGREGATION_STDDEV:
		return true, nil

	case core.AGGREGATION_QUANTILE:
		return false, nil
	case core.AGGREGATION_COUNT_VALUES:
		return false, nil
	case core.AGGREGATION_TOPK:
		return false, nil
	case core.AGGREGATION_BOTTOMK:
		return false, nil
	case core.AGGREGATION_LIMITK:
		return false, nil
	case core.AGGREGATION_LIMIT_RATIO:
		return false, nil

	default:
		return false, fmt.Errorf("multiaggregation.IsSupportedAggregationOperation: unknown aggregation operation: %s", o.String())
	}
}
