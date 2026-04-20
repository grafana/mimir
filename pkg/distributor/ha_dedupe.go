// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"time"

	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

// haDedupeStrategy is implemented by HA deduplication strategies. The middleware
// wrapper picks one per request based on the tenant's configuration and delegates
// the dedupe decision (and the call to next) to it.
type haDedupeStrategy interface {
	dedupe(ctx context.Context, pushReq *Request, req *mimirpb.WriteRequest, userID, haReplicaLabel, haClusterLabel, group string, now time.Time, next PushFunc) error
}

// prePushHaDedupeMiddleware performs the common work shared by HA dedupe strategies
// (tenant resolution, AcceptHASamples gating, HA label lookup, active-group
// bookkeeping) and then delegates the actual dedupe decision to a strategy-specific
// implementation. The strategies compute their own sample timestamps from `now`,
// because per-request uses a single request-wide value while per-sample needs a
// per-replica minimum.
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

		return d.haDedupeStrategy(userID).dedupe(ctx, pushReq, req, userID, haReplicaLabel, haClusterLabel, group, now, next)
	})
}

// haDedupeStrategy returns the HA dedupe strategy configured for the given tenant.
func (d *Distributor) haDedupeStrategy(userID string) haDedupeStrategy {
	if d.limits.HATrackerPerSampleDedupe(userID) {
		return d.perSample
	}
	return d.perRequest
}
