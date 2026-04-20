// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"time"

	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/util/validation"
)

// prePushHaDedupeMiddleware performs the common work shared by HA dedupe strategies
// (tenant resolution, AcceptHASamples gating, HA label lookup, active-group
// bookkeeping) and then delegates the actual dedupe decision to a strategy-specific
// implementation.
func (d *Distributor) prePushHaDedupeMiddleware(next PushFunc) PushFunc {
	return WithCleanup(next, func(next PushFunc, ctx context.Context, pushReq *Request) error {
		req, err := pushReq.WriteRequest()
		if err != nil {
			return err
		}

		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return err
		}

		if len(req.Timeseries) == 0 || !d.limits.AcceptHASamples(userID) {
			return next(ctx, pushReq)
		}

		haReplicaLabel := d.limits.HAReplicaLabel(userID)
		haClusterLabel := d.limits.HAClusterLabel(userID)

		now := time.Now()
		group := d.activeGroups.UpdateActiveGroupTimestamp(userID, validation.GroupLabel(d.limits, userID, req.Timeseries), now)

		return d.perRequest.dedupe(ctx, pushReq, req, userID, haReplicaLabel, haClusterLabel, group, now, next)
	})
}
