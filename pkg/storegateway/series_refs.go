// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

// seriesChunkRefsSetIterator is the interface implemented by an iterator returning a sequence of seriesChunkRefsSet.
//
//nolint:unused // dead code while we are working on PR 3355
type seriesChunkRefsSetIterator interface {
	Next() bool
	At() seriesChunkRefsSet
	Err() error
}

// seriesChunkRefsIterator is the interface implemented by an iterator returning a sequence of seriesChunkRefs.
//
//nolint:unused // dead code while we are working on PR 3355
type seriesChunkRefsIterator interface {
	Next() bool
	At() seriesChunkRefs
	Err() error
}

// seriesChunkRefsSet holds a set of a set of series (sorted by labels) and their chunk references.
//
//nolint:unused // dead code while we are working on PR 3355
type seriesChunkRefsSet struct {
	// series sorted by labels.
	series []seriesChunkRefs
}

//nolint:unused // dead code while we are working on PR 3355
func newSeriesChunkRefsSet(capacity int) seriesChunkRefsSet {
	return seriesChunkRefsSet{
		series: make([]seriesChunkRefs, 0, capacity),
	}
}

//nolint:unused // dead code while we are working on PR 3355
func (b seriesChunkRefsSet) len() int {
	return len(b.series)
}

// seriesChunkRefs holds a series with a list of chunk references.
//
//nolint:unused // dead code while we are working on PR 3355
type seriesChunkRefs struct {
	lset   labels.Labels
	chunks []seriesChunkRef
}

// seriesChunkRef holds the reference to a chunk in a given block.
//
//nolint:unused // dead code while we are working on PR 3355
type seriesChunkRef struct {
	blockID          ulid.ULID
	ref              chunks.ChunkRef
	minTime, maxTime int64
}

// Compare returns > 0 if m should be before other when sorting seriesChunkRef,
// 0 if they're equal or < 0 if m should be after other.
//
//nolint:unused // dead code while we are working on PR 3355
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

// seriesChunkRefsIteratorImpl implements an iterator returning a sequence of seriesChunkRefs.
type seriesChunkRefsIteratorImpl struct {
	currentOffset int
	set           seriesChunkRefsSet
}

func newSeriesChunkRefsIterator(set seriesChunkRefsSet) *seriesChunkRefsIteratorImpl {
	c := &seriesChunkRefsIteratorImpl{}
	c.reset(set)

	return c
}

// reset replaces the current set with the provided one. After calling reset() you
// must call Next() to advance the iterator to the first element.
func (c *seriesChunkRefsIteratorImpl) reset(set seriesChunkRefsSet) {
	c.set = set
	c.currentOffset = -1
}

func (c *seriesChunkRefsIteratorImpl) Next() bool {
	c.currentOffset++
	return !c.Done()
}

// Done returns true if the iterator trespassed the end and the item returned by At()
// is the zero value. If the iterator is on the last item, the value returned by At()
// is the actual item and Done() returns false.
func (c *seriesChunkRefsIteratorImpl) Done() bool {
	setLength := c.set.len()
	return setLength == 0 || c.currentOffset >= setLength
}

func (c *seriesChunkRefsIteratorImpl) At() seriesChunkRefs {
	if c.currentOffset < 0 || c.currentOffset >= c.set.len() {
		return seriesChunkRefs{}
	}
	return c.set.series[c.currentOffset]
}

func (c *seriesChunkRefsIteratorImpl) Err() error {
	return nil
}

type flattenedSeriesChunkRefsIterator struct {
	from     seriesChunkRefsSetIterator
	iterator *seriesChunkRefsIteratorImpl
}

func newFlattenedSeriesChunkRefsIterator(from seriesChunkRefsSetIterator) seriesChunkRefsIterator {
	return &flattenedSeriesChunkRefsIterator{
		from:     from,
		iterator: newSeriesChunkRefsIterator(seriesChunkRefsSet{}), // start with an empty batch and initialize on the first call to Next()
	}
}

func (c flattenedSeriesChunkRefsIterator) Next() bool {
	if c.iterator.Next() {
		return true
	}

	// The current iterator has no more elements. We check if there's another
	// iterator to fetch and then iterate on.
	if !c.from.Next() {
		return false
	}

	c.iterator.reset(c.from.At())

	// We've replaced the current iterator, so can recursively call Next()
	// to check if there's any item in the new iterator and further advance it if not.
	return c.Next()
}

func (c flattenedSeriesChunkRefsIterator) At() seriesChunkRefs {
	return c.iterator.At()
}

func (c flattenedSeriesChunkRefsIterator) Err() error {
	return c.from.Err()
}
