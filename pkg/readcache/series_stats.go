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

		// Snapshot the partition's bucket set ONCE at the start of
		// the walk. If SetHashRanges fires while we're walking, the
		// applyWalkResult call will detect the mismatch and discard
		// this round; the next tick uses the new snapshot.
		bucketRanges := p.ranges.rangesSnapshot()
		counts := make([]int64, len(bucketRanges))
		// examples is parallel to bucketRanges. The walker writes
		// one labels.Labels.String() per range (first series seen
		// wins) so the readcache admin page can show a concrete
		// example next to each hash range. The cost is bounded:
		// at most one allocation per range per walk, regardless of
		// head size.
		examples := make([]string, len(bucketRanges))

		type tenantDB struct {
			tenantID string
			db       *partitionTSDB
		}
		var dbs []tenantDB

		p.tenantsMu.RLock()
		for tenantID, db := range p.tenants {
			if len(bucketRanges) > 0 {
				dbs = append(dbs, tenantDB{tenantID: tenantID, db: db})
			}
		}
		p.tenantsMu.RUnlock()

		for _, td := range dbs {
			if err := ctx.Err(); err != nil {
				return
			}
			if _, err := loadstats.CountSeriesByHashRange(ctx, td.db.Head(), bucketRanges, counts, examples); err != nil {
				level.Warn(r.logger).Log(
					"msg", "hash range series walk failed",
					"partition", p.partitionID,
					"tenant", td.tenantID,
					"err", err,
				)
			}
		}

		if len(bucketRanges) > 0 {
			if !p.ranges.applyWalkResult(bucketRanges, counts, examples) {
				level.Debug(r.logger).Log(
					"msg", "discarded stale hash range series walk",
					"partition", p.partitionID,
				)
			}
		}
	}
}
