// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/distributor/hlltracker"
	"github.com/grafana/mimir/pkg/util/globalerror"
)

// preKafkaPartitionLimitMiddleware enforces per-partition series limits when using ingest storage.
// This middleware:
// 1. Calculates which partitions the series will be sent to
// 2. Updates HyperLogLog estimates for each partition
// 3. Checks if any partition would exceed its series limit
// 4. Rejects the request if limits would be exceeded, or commits the updates if within limits
//
// This is Phase 1: local-only tracking without memberlist synchronization.
func (d *Distributor) preKafkaPartitionLimitMiddleware(next PushFunc) PushFunc {
	return func(ctx context.Context, pushReq *Request) error {
		next, maybeCleanup := NextOrCleanup(next, pushReq)
		defer maybeCleanup()

		req, err := pushReq.WriteRequest()
		if err != nil {
			return err
		}

		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return err
		}

		// Skip if HLL tracker is not enabled
		if d.hllTracker == nil {
			return next(ctx, pushReq)
		}

		// Get the global partition limit from tracker config
		limit := d.hllTracker.MaxSeriesPerPartition()
		if limit <= 0 {
			// Limit disabled globally
			return next(ctx, pushReq)
		}

		// Only track series, not metadata (as per proposal)
		if len(req.Timeseries) == 0 {
			return next(ctx, pushReq)
		}

		// Get series tokens (hashes)
		seriesKeys := getTokensForSeries(userID, req.Timeseries)

		// Get the partition ring with shuffle sharding
		subring, err := d.partitionsRing.ShuffleShard(
			userID,
			d.limits.IngestionPartitionsTenantShardSize(userID),
		)
		if err != nil {
			return err
		}

		partitionRing := ring.NewActivePartitionBatchRing(subring.PartitionRing())

		// Group series by partition
		partitionSeries := make(map[int32][]uint32)

		err = ring.DoBatchWithOptions(ctx, ring.WriteNoExtend, partitionRing, seriesKeys,
			func(partition ring.InstanceDesc, indexes []int) error {
				partitionID, err := strconv.ParseInt(partition.Id, 10, 32)
				if err != nil {
					return err
				}

				// Collect series hashes for this partition
				for _, idx := range indexes {
					partitionSeries[int32(partitionID)] = append(
						partitionSeries[int32(partitionID)],
						seriesKeys[idx],
					)
				}

				return nil
			},
			ring.DoBatchOptions{},
		)

		if err != nil {
			return err
		}

		// For each partition, get current state and update with new series
		updates := make([]hlltracker.PartitionUpdate, 0, len(partitionSeries))

		for partitionID, seriesHashes := range partitionSeries {
			// Get thread-safe copies of HLL state
			state := d.hllTracker.GetCurrentState(partitionID)

			// Add new series to current HLL copy
			for _, hash := range seriesHashes {
				state.CurrentCopy.Add(hash)
			}

			// Merge historical and updated current to get total estimate
			merged := state.MergedHistorical.Clone()
			merged.Merge(state.CurrentCopy)

			// Check if limit would be exceeded
			count := merged.Count()
			if count > uint64(limit) {
				// Limit exceeded - reject request
				return newPartitionSeriesLimitError(
					userID,
					partitionID,
					limit,
					count,
				)
			}

			// Limit OK - save update for commit
			updates = append(updates, hlltracker.PartitionUpdate{
				PartitionID: partitionID,
				UpdatedHLL:  state.CurrentCopy,
			})
		}

		// All partitions within limit - commit updates
		d.hllTracker.UpdateCurrent(updates)

		// Continue to next middleware
		return next(ctx, pushReq)
	}
}

// newPartitionSeriesLimitError creates an error for partition series limit exceeded.
func newPartitionSeriesLimitError(
	userID string,
	partitionID int32,
	limit int,
	estimated uint64,
) error {
	msg := globalerror.DistributorMaxSeriesPerPartition.Message(
		fmt.Sprintf("the partition %d has reached its global series limit of %d (estimated: %d series across all tenants in the current time window); "+
			"this is a partition capacity limit similar to ingester instance limits; "+
			"consider increasing -distributor.partition-series-tracker.max-series-per-partition or "+
			"reducing -distributor.partition-series-tracker.time-window-minutes",
			partitionID, limit, estimated),
	)
	return httpgrpc.Errorf(http.StatusTooManyRequests, "%s", msg)
}
