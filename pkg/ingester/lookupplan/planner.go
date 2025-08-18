// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"

	"github.com/prometheus/prometheus/tsdb/index"
)

type NoopPlanner struct{}

func (i NoopPlanner) PlanIndexLookup(_ context.Context, plan index.LookupPlan, _, _ int64) (index.LookupPlan, error) {
	return plan, nil
}
