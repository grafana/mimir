// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

// seriesChunkRefsSetIterator is the interface implemented by an iterator over a sequence of seriesChunkRefsSet.
type seriesChunkRefsSetIterator interface {
	Next() bool
	At() seriesChunkRefsSet
	Err() error
}

// seriesChunkRefsSet holds a set of series (sorted by labels) and their chunk references.
type seriesChunkRefsSet struct {
	// series sorted by labels.
	series []seriesChunkRefs
}

func newSeriesChunkRefsSet(size int) seriesChunkRefsSet {
	return seriesChunkRefsSet{
		series: make([]seriesChunkRefs, size),
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

// seriesChunkRefsIterator implements an interator over seriesChunkRefsSet.
type seriesChunkRefsIterator struct {
	currentOffset int
	set           seriesChunkRefsSet
}

func newSeriesChunkRefsIterator(set seriesChunkRefsSet) *seriesChunkRefsIterator {
	return &seriesChunkRefsIterator{
		set:           set,
		currentOffset: -1,
	}
}

// reset replaces the current set with the provided one. There is no need to call Next() after reset().
func (c *seriesChunkRefsIterator) reset(set seriesChunkRefsSet) {
	c.set = set
	c.currentOffset = 0
}

func (c *seriesChunkRefsIterator) Next() bool {
	c.currentOffset++
	return !c.Done()
}

func (c *seriesChunkRefsIterator) Done() bool {
	return c.currentOffset < 0 || c.currentOffset >= c.set.len()
}

func (c *seriesChunkRefsIterator) At() seriesChunkRefs {
	if c.Done() {
		return seriesChunkRefs{}
	}
	return c.set.series[c.currentOffset]
}
