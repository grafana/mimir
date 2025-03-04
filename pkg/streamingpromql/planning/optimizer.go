// SPDX-License-Identifier: AGPL-3.0-only

package planning

import "context"

type Optimizer interface {
	// Name returns the name of this optimizer.
	Name() string

	// Apply applies this optimizer to the provided plan.
	//
	// Apply may modify the provided plan in-place.
	Apply(ctx context.Context, plan QueryPlan) (QueryPlan, error)
}
