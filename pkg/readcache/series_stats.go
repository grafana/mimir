// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"

	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/nautilus/loadstats"
)

// tickSampleRates advances every per-(partition, range)
// samples-per-second EwmaRate by one tick. Must be called every
// loadstats.TickInterval so the EWMA's configured half-life remains
// accurate.
//
// This runs synchronously on the running() goroutine because each
// EwmaRate.Tick is a single atomic Swap + a short mutex acquire;
// even thousands of (partition, range) pairs complete in well under
// a millisecond. Going async would just add scheduling overhead and
// would let multiple ticks queue up if the previous one was slow.
func (r *Readcache) tickSampleRates() {
	r.partitionMu.RLock()
	parts := make([]*partitionState, 0, len(r.partitions))
	for _, p := range r.partitions {
		parts = append(parts, p)
	}
	r.partitionMu.RUnlock()

	for _, p := range parts {
		p.ranges.tickSampleRates()
	}
}

// refreshPartitionSeriesCounts updates the cheap per-partition head
// series totals used by HashRangeStats. These totals stay on the
// load-stats cadence because the rebalancer uses them to distinguish a
// genuinely idle partition from a non-empty partition whose sample-rate
// EWMA is temporarily zero.
func (r *Readcache) refreshPartitionSeriesCounts() {
	r.partitionMu.RLock()
	parts := make([]*partitionState, 0, len(r.partitions))
	for _, p := range r.partitions {
		parts = append(parts, p)
	}
	r.partitionMu.RUnlock()

	partitionCounts := make(map[int32]int64, len(parts))
	for _, p := range parts {
		var partitionTotal int64
		p.tenantsMu.RLock()
		for _, db := range p.tenants {
			partitionTotal += int64(db.Head().NumSeries())
		}
		p.tenantsMu.RUnlock()
		partitionCounts[p.partitionID] = partitionTotal
	}
	r.partitionSeries.SetCounts(partitionCounts)
}

// refreshSeriesStats walks owned partition TSDB heads and updates the
// per-(partition, hash range) counts used by HashRangeStats. The walk
// runs on a slower fallback cadence and after events that can change
// range attribution or residue; it is deliberately decoupled from the
// 15-second EWMA tick.
//
// Per-partition bucketing is important for residue accounting: when a
// hash range moves from partition P_old to partition P_new, P_old's
// head still holds the series for up to one compaction interval, and
// those series must be reported against P_old (not summed onto
// P_new's growing count). Each partition has its own currentRanges /
// historicalRanges and its own rangeCounts map; this loop tallies one
// head walk per (tenant, partition) head into the matching partition's
// rangeCounts.
func (r *Readcache) refreshSeriesStats(ctx context.Context) {
	if !r.seriesWalkMu.TryLock() {
		return
	}
	defer r.seriesWalkMu.Unlock()

	r.partitionMu.RLock()
	parts := make([]*partitionState, 0, len(r.partitions))
	for _, p := range r.partitions {
		parts = append(parts, p)
	}
	r.partitionMu.RUnlock()

	for _, p := range parts {
		if err := ctx.Err(); err != nil {
			return
		}

		type tenantDB struct {
			tenantID string
			db       *partitionTSDB
		}
		var dbs []tenantDB
		seenTenants := make(map[string]struct{})

		p.tenantsMu.RLock()
		for tenantID, db := range p.tenants {
			dbs = append(dbs, tenantDB{tenantID: tenantID, db: db})
			seenTenants[tenantID] = struct{}{}
		}
		p.tenantsMu.RUnlock()
		// Configured tenants with no live TSDB still need a zero-count
		// result so periodic refreshes maintain the current-range
		// bookkeeping without walking another tenant's head.
		for _, tenantID := range p.ranges.trackedTenantIDs() {
			if _, ok := seenTenants[tenantID]; !ok {
				dbs = append(dbs, tenantDB{tenantID: tenantID})
			}
		}

		for _, td := range dbs {
			if err := ctx.Err(); err != nil {
				return
			}
			// Snapshot and walk only this tenant's buckets. If
			// SetHashRanges changes this tenant while its head is being
			// walked, applyWalkResultForTenant rejects the stale result.
			bucketRanges := p.ranges.rangesSnapshotForTenant(td.tenantID)
			if len(bucketRanges) == 0 {
				continue
			}
			counts := make([]int64, len(bucketRanges))
			// examples is parallel to bucketRanges. At most one
			// labels string is captured per tenant and range.
			examples := make([]string, len(bucketRanges))
			if td.db != nil {
				if _, err := loadstats.CountSeriesByHashRange(ctx, td.db.Head(), bucketRanges, counts, examples); err != nil {
					level.Warn(r.logger).Log(
						"msg", "hash range series walk failed",
						"partition", p.partitionID,
						"tenant", td.tenantID,
						"err", err,
					)
				}
			}
			if !p.ranges.applyWalkResultForTenant(td.tenantID, bucketRanges, counts, examples) {
				level.Debug(r.logger).Log(
					"msg", "discarded stale hash range series walk",
					"partition", p.partitionID,
					"tenant", td.tenantID,
				)
			}
		}
	}
}
