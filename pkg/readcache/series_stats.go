// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"

	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/nautilus/loadstats"
)

// refreshSeriesStats walks owned partition TSDB heads and updates the
// per-partition series and per-(partition, hash range) counts used by
// HashRangeStats. It runs on the load-stats ticker, not on the RPC
// path, so rebalancer polls cannot block ingest behind partitionMu.
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

	partitionCounts := make(map[int32]int64, len(parts))

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
		var partitionTotal int64

		p.tenantsMu.RLock()
		for tenantID, db := range p.tenants {
			partitionTotal += int64(db.Head().NumSeries())
			if len(bucketRanges) > 0 {
				dbs = append(dbs, tenantDB{tenantID: tenantID, db: db})
			}
		}
		p.tenantsMu.RUnlock()

		partitionCounts[p.partitionID] = partitionTotal

		for _, td := range dbs {
			if err := ctx.Err(); err != nil {
				return
			}
			if _, err := loadstats.CountSeriesByHashRange(ctx, td.tenantID, td.db.Head(), bucketRanges, counts, examples); err != nil {
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

	r.partitionSeries.SetCounts(partitionCounts)
}
