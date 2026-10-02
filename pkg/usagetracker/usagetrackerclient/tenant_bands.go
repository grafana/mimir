// SPDX-License-Identifier: AGPL-3.0-only

package usagetrackerclient

import (
	"context"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/usagetracker/trackerop"
	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerpb"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// GetTenantBands reads the retained locality bands of one tracker partition.
// An empty userID returns every tenant on that partition. Counts in the
// response cover only that partition.
func (c *UsageTrackerClient) GetTenantBands(ctx context.Context, partitionID int32, userID string) (*usagetrackerpb.GetTenantBandsResponse, error) {
	set, err := c.partitionRing.GetReplicationSetForPartitionAndOperation(partitionID, trackerop.TrackSeriesOp)
	if err != nil {
		return nil, err
	}
	req := &usagetrackerpb.GetTenantBandsRequest{Partition: partitionID, UserID: userID}
	cfg := ring.DoUntilQuorumConfig{
		Logger:           spanlogger.FromContext(ctx, log.With(c.logger, "component", "usage-tracker-client", "op", "get-tenant-bands", "partition", partitionID)),
		MinimizeRequests: true,
		HedgingDelay:     c.cfg.RequestsHedgingDelay,
		ZoneSorter:       c.sortZones,
		IsTerminalError:  func(error) bool { return false },
	}
	results, err := ring.DoUntilQuorum(ctx, set, cfg, func(ctx context.Context, instance *ring.InstanceDesc) (*usagetrackerpb.GetTenantBandsResponse, error) {
		poolClient, err := c.clientsPool.GetClientForInstance(*instance)
		if err != nil {
			return nil, errors.Errorf("usage-tracker instance %s (%s)", instance.Id, instance.Addr)
		}
		client := poolClient.(usagetrackerpb.UsageTrackerClient)
		return client.GetTenantBands(ctx, req)
	}, func(*usagetrackerpb.GetTenantBandsResponse) {})
	if err != nil {
		return nil, err
	}
	for _, result := range results {
		if result != nil {
			return result, nil
		}
	}
	return nil, errors.New("usage-tracker returned no band snapshot")
}

// localityHashBytesSent records the extra payload of a locality-hash send.
// Kept for callers that enqueue asynchronously and do not observe the RPC result.
