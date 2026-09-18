// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
)

// noopLookupPlanner passes all matchers to the index unchanged.
// It will be replaced by a cost-based planner once per-block statistics are available.
type noopLookupPlanner struct{}

func (noopLookupPlanner) PlanIndexLookup(_ context.Context, plan index.LookupPlan, _ *storage.SelectHints) (index.LookupPlan, error) {
	return plan, nil
}

func (noopLookupPlanner) Name() string { return "noop" }

// lookupPlannerName returns a stable identifier for the planner, used in cache keys.
func lookupPlannerName(p index.LookupPlanner) string {
	type named interface{ Name() string }
	if n, ok := p.(named); ok {
		return n.Name()
	}
	return "unknown"
}
