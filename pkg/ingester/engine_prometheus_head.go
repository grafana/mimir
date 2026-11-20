// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"slices"

	"github.com/grafana/dskit/ring"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

func (e prometheusEngine) Head() engineHead { return prometheusHead{e.db.Head()} }

// prometheusHead is the Prometheus TSDB's head as an engineHead.
type prometheusHead struct {
	*tsdb.Head
}

func (h prometheusHead) Sync() error {
	return h.FsyncWLSegments()
}

func (h prometheusHead) TimeBounds() timeBounds {
	return timeBounds{MinTime: h.MinTime(), MaxTime: h.MaxTime(), MinOOOTime: h.MinOOOTime(), MaxOOOTime: h.MaxOOOTime()}
}

func (h prometheusHead) MinInOrderAppendTime() (int64, bool) {
	return h.AppendableMinValidTime()
}

func (h prometheusHead) Ownership(owned ring.TokenRanges, nonOwned func(storage.SeriesRef)) int {
	var count int
	h.ForEachSecondaryHash(func(refs []chunks.HeadSeriesRef, secondaryHashes []uint32) {
		for i, hash := range secondaryHashes {
			if len(owned) > 0 && owned.IncludesKey(hash) {
				count++
			} else {
				nonOwned(storage.SeriesRef(refs[i]))
			}
		}
	})
	return count
}

func (h prometheusHead) ShardSeries(shardIndex, shardCount uint64) index.Postings {
	out := make([]storage.SeriesRef, 0, 128)
	h.ForEachShardHash(func(refs []storage.SeriesRef, shardHashes []uint64) {
		for i := range refs {
			if shardHashes[i]%shardCount == shardIndex {
				out = append(out, refs[i])
			}
		}
	})
	slices.Sort(out) // Postings consumers require increasing series references.
	return index.NewListPostings(out)
}
