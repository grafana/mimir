// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

// seriesChunkRefsSet holds a set of series (sorted by labels) and their chunk references.
type seriesChunkRefsSet struct {
	// Series sorted by labels.
	Series []seriesChunkRefs
	Stats  *safeQueryStats
}

func newSeriesChunkRefsSet(size int) seriesChunkRefsSet {
	return seriesChunkRefsSet{
		Series: make([]seriesChunkRefs, size),
		Stats:  newSafeQueryStats(),
	}
}

func (b seriesChunkRefsSet) len() int {
	return len(b.Series)
}

// seriesChunkRefs holds a series with a list of chunks.
type seriesChunkRefs struct {
	lset   labels.Labels
	chunks []seriesChunkRef
}

// seriesChunkRef holds the reference to a chunk in a given block.
type seriesChunkRef struct {
	BlockID          ulid.ULID
	Ref              chunks.ChunkRef
	MinTime, MaxTime int64
}

// Compare returns > 0 if m should be before other when sorting seriesChunkRef,
// 0 if they're equal or < 0 if m should be after other.
func (m seriesChunkRef) Compare(other seriesChunkRef) int {
	if m.MinTime < other.MinTime {
		return 1
	}
	if m.MinTime > other.MinTime {
		return -1
	}

	// Same min time.
	if m.MaxTime < other.MaxTime {
		return 1
	}
	if m.MaxTime > other.MaxTime {
		return -1
	}
	return 0
}
