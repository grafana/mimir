// SPDX-License-Identifier: AGPL-3.0-only

package loadstats

import (
	"sort"
	"sync"
)

// PartitionSeries tracks per-partition TSDB head-series counts for the
// partitions this readcache instance owns. Counts are refreshed by a
// background walk (see readcache.refreshSeriesStats); HashRangeStats
// reads them via Snapshot without touching TSDB heads.
type PartitionSeries struct {
	mu          sync.RWMutex
	byPartition map[int32]int64
	total       int64
}

// PartitionSeriesEntry is one partition's head-series count.
type PartitionSeriesEntry struct {
	PartitionID  int32
	ActiveSeries int64
}

// PartitionSeriesSnapshot is a consistent view for HashRangeStats.
type PartitionSeriesSnapshot struct {
	Partitions []PartitionSeriesEntry
	Total      int64
}

// NewPartitionSeries returns an empty PartitionSeries.
func NewPartitionSeries() *PartitionSeries {
	return &PartitionSeries{}
}

// SetCounts atomically replaces all per-partition counts. counts maps
// partition_id -> head series on this instance; total is their sum.
func (p *PartitionSeries) SetCounts(counts map[int32]int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.byPartition = make(map[int32]int64, len(counts))
	var total int64
	for pid, n := range counts {
		p.byPartition[pid] = n
		total += n
	}
	p.total = total
}

// Snapshot returns a copy of the latest counts, sorted by partition ID.
func (p *PartitionSeries) Snapshot() PartitionSeriesSnapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()

	entries := make([]PartitionSeriesEntry, 0, len(p.byPartition))
	for pid, n := range p.byPartition {
		entries = append(entries, PartitionSeriesEntry{
			PartitionID:  pid,
			ActiveSeries: n,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].PartitionID < entries[j].PartitionID
	})
	return PartitionSeriesSnapshot{
		Partitions: entries,
		Total:      p.total,
	}
}
