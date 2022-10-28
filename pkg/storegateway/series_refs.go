// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

// seriesChunkRefsSet holds a set of series (sorted by labels) and their chunk references.
type seriesChunkRefsSet struct {
	// series sorted by labels.
	series []seriesChunkRefs
	stats  *safeQueryStats
}

func newSeriesChunkRefsSet(size int) seriesChunkRefsSet {
	return seriesChunkRefsSet{
		series: make([]seriesChunkRefs, size),
		stats:  newSafeQueryStats(),
	}
}

func (b seriesChunkRefsSet) len() int {
	return len(b.series)
}

// seriesChunkRefs holds a series with a list of chunks.
type seriesChunkRefs struct {
	lset   labels.Labels
	chunks []seriesChunkRef
}

// seriesChunkRef holds the reference to a chunk in a given block.
type seriesChunkRef struct {
	blockID          ulid.ULID
	ref              chunks.ChunkRef
	minTime, maxTime int64
}

// Compare returns > 0 if m should be before other when sorting seriesChunkRef,
// 0 if they're equal or < 0 if m should be after other.
func (m seriesChunkRef) Compare(other seriesChunkRef) int {
	if m.minTime < other.minTime {
		return 1
	}
	if m.minTime > other.minTime {
		return -1
	}

	// Same min time.
	if m.maxTime < other.maxTime {
		return 1
	}
	if m.maxTime > other.maxTime {
		return -1
	}
	return 0
}
