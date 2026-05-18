// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"

	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/nautilus/loadstats"
)

// refreshSeriesStats walks owned partition TSDB heads and updates the
// precomputed partition- and hash-range series counts used by
// HashRangeStats. It runs on the load-stats ticker, not on the RPC
// path, so rebalancer polls cannot block ingest behind partitionMu.
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

	ranges := r.rangeSeries.Ranges()
	rangeCounts := make([]int64, len(ranges))
	partitionCounts := make(map[int32]int64, len(parts))

	for _, p := range parts {
		if err := ctx.Err(); err != nil {
			return
		}

		var partitionTotal int64
		type tenantDB struct {
			tenantID string
			db       *partitionTSDB
		}
		var dbs []tenantDB

		p.tenantsMu.RLock()
		for tenantID, db := range p.tenants {
			partitionTotal += int64(db.Head().NumSeries())
			if len(ranges) > 0 {
				dbs = append(dbs, tenantDB{tenantID: tenantID, db: db})
			}
		}
		p.tenantsMu.RUnlock()

		partitionCounts[p.partitionID] = partitionTotal

		for _, td := range dbs {
			if err := ctx.Err(); err != nil {
				return
			}
			if _, err := loadstats.CountSeriesByHashRange(ctx, td.tenantID, td.db.Head(), ranges, rangeCounts); err != nil {
				level.Warn(r.logger).Log(
					"msg", "hash range series walk failed",
					"partition", p.partitionID,
					"tenant", td.tenantID,
					"err", err,
				)
			}
		}
	}

	r.partitionSeries.SetCounts(partitionCounts)
	if len(ranges) > 0 {
		if !r.rangeSeries.SetCountsFor(ranges, rangeCounts) {
			level.Debug(r.logger).Log("msg", "discarded stale hash range series walk")
		}
	}
}
