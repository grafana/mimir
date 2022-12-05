// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
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
		iterator: newSeriesChunkRefsIterator(seriesChunkRefsSet{}), // start with an empty set and initialize on the first call to Next()
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

type emptySeriesChunkRefsSetIterator struct {
}

func (emptySeriesChunkRefsSetIterator) Next() bool             { return false }
func (emptySeriesChunkRefsSetIterator) At() seriesChunkRefsSet { return seriesChunkRefsSet{} }
func (emptySeriesChunkRefsSetIterator) Err() error             { return nil }

//nolint:unused // dead code while we are working on PR 3355
func mergedSeriesChunkRefsSetIterators(mergedSize int, all ...seriesChunkRefsSetIterator) seriesChunkRefsSetIterator {
	switch len(all) {
	case 0:
		return emptySeriesChunkRefsSetIterator{}
	case 1:
		return newDeduplicatingSeriesChunkRefsSetIterator(mergedSize, all[0])
	}
	h := len(all) / 2

	return newMergedSeriesChunkRefsSet(
		mergedSize,
		mergedSeriesChunkRefsSetIterators(mergedSize, all[:h]...),
		mergedSeriesChunkRefsSetIterators(mergedSize, all[h:]...),
	)
}

type mergedSeriesChunkRefsSet struct {
	batchSize int

	a, b     seriesChunkRefsSetIterator
	aAt, bAt *seriesChunkRefsIteratorImpl
	current  seriesChunkRefsSet
}

func newMergedSeriesChunkRefsSet(mergedBatchSize int, a, b seriesChunkRefsSetIterator) *mergedSeriesChunkRefsSet {
	return &mergedSeriesChunkRefsSet{
		batchSize: mergedBatchSize,
		a:         a,
		b:         b,
		// start iterator on an empty set. It will be reset with a non-empty set when Next() is called
		aAt: newSeriesChunkRefsIterator(seriesChunkRefsSet{}),
		bAt: newSeriesChunkRefsIterator(seriesChunkRefsSet{}),
	}
}

func (s *mergedSeriesChunkRefsSet) Err() error {
	if err := s.a.Err(); err != nil {
		return err
	} else if err = s.b.Err(); err != nil {
		return err
	}
	return nil
}

func (s *mergedSeriesChunkRefsSet) Next() bool {
	next := newSeriesChunkRefsSet(s.batchSize)

	for i := 0; i < s.batchSize; i++ {
		if err := s.ensureItemAvailableToRead(s.aAt, s.a); err != nil {
			// Stop iterating on first error encountered.
			return false
		}

		if err := s.ensureItemAvailableToRead(s.bAt, s.b); err != nil {
			// Stop iterating on first error encountered.
			return false
		}

		nextSeries, ok := s.nextUniqueEntry(s.aAt, s.bAt)
		if !ok {
			break
		}
		next.series = append(next.series, nextSeries)
	}

	s.current = next
	return s.current.len() > 0
}

// ensureItemAvailableToRead ensures curr has an item available to read, unless we reached the
// end of all sets. If curr has no item available to read, it will advance the iterator, eventually
// picking the next one from the set.
func (s *mergedSeriesChunkRefsSet) ensureItemAvailableToRead(curr *seriesChunkRefsIteratorImpl, set seriesChunkRefsSetIterator) error {
	// Ensure curr has an item available, otherwise fetch the next set.
	for curr.Done() {
		if set.Next() {
			curr.reset(set.At())

			// Advance the iterator to the first element. If the iterator is empty,
			// it will be detected and handled by the outer for loop.
			curr.Next()
			continue
		}

		if set.Err() != nil {
			// Stop iterating on first error encountered.
			return set.Err()
		}

		// No more sets.
		return nil
	}

	return nil
}

// nextUniqueEntry returns the next unique entry from both a and b. If a.At() and b.At() have the same
// label set, nextUniqueEntry merges their chunks. The merged chunks are sorted by their MinTime and then by MaxTIme.
func (s *mergedSeriesChunkRefsSet) nextUniqueEntry(a, b *seriesChunkRefsIteratorImpl) (toReturn seriesChunkRefs, _ bool) {
	if a.Done() && b.Done() {
		return toReturn, false
	} else if a.Done() {
		toReturn = b.At()
		b.Next()
		return toReturn, true
	} else if b.Done() {
		toReturn = a.At()
		a.Next()
		return toReturn, true
	}

	aAt := a.At()
	lsetA, chksA := aAt.lset, aAt.chunks
	bAt := b.At()
	lsetB, chksB := bAt.lset, bAt.chunks

	if d := labels.Compare(lsetA, lsetB); d > 0 {
		toReturn = b.At()
		b.Next()
		return toReturn, true
	} else if d < 0 {
		toReturn = a.At()
		a.Next()
		return toReturn, true
	}

	// Both a and b contains the same series. Go through all chunk references and concatenate them from both
	// series sets. We best effortly assume chunk references are sorted by min time, so that the sorting by min
	// time is honored in the returned chunk references too.
	toReturn.lset = lsetA

	// Slice reuse is not generally safe with nested merge iterators.
	// We err on the safe side and create a new slice.
	toReturn.chunks = make([]seriesChunkRef, 0, len(chksA)+len(chksB))

	bChunksOffset := 0
Outer:
	for aChunksOffset := range chksA {
		for {
			if bChunksOffset >= len(chksB) {
				// No more b chunks.
				toReturn.chunks = append(toReturn.chunks, chksA[aChunksOffset:]...)
				break Outer
			}

			if chksA[aChunksOffset].Compare(chksB[bChunksOffset]) > 0 {
				toReturn.chunks = append(toReturn.chunks, chksA[aChunksOffset])
				break
			} else {
				toReturn.chunks = append(toReturn.chunks, chksB[bChunksOffset])
				bChunksOffset++
			}
		}
	}

	if bChunksOffset < len(chksB) {
		toReturn.chunks = append(toReturn.chunks, chksB[bChunksOffset:]...)
	}

	a.Next()
	b.Next()
	return toReturn, true
}

func (s *mergedSeriesChunkRefsSet) At() seriesChunkRefsSet {
	return s.current
}

type seriesSetWithoutChunks struct {
	from seriesChunkRefsSetIterator

	currentIterator *seriesChunkRefsIteratorImpl
}

func newSeriesSetWithoutChunks(batches seriesChunkRefsSetIterator) storepb.SeriesSet {
	return &seriesSetWithoutChunks{
		from:            batches,
		currentIterator: newSeriesChunkRefsIterator(seriesChunkRefsSet{}),
	}
}

func (s *seriesSetWithoutChunks) Next() bool {
	if s.currentIterator.Next() {
		return true
	}

	// The current iterator has no more elements. We check if there's another
	// iterator to fetch and then iterate on.
	if !s.from.Next() {
		return false
	}

	next := s.from.At()
	s.currentIterator.reset(next)

	// We've replaced the current iterator, so can recursively call Next()
	// to check if there's any item in the new iterator and further advance it if not.
	return s.Next()
}

func (s *seriesSetWithoutChunks) At() (labels.Labels, []storepb.AggrChunk) {
	return s.currentIterator.At().lset, nil
}

func (s *seriesSetWithoutChunks) Err() error {
	return s.from.Err()
}

// deduplicatingSeriesChunkRefsSetIterator implements seriesChunkRefsSetIterator, and merges together consecutive
// series in an underlying seriesChunkRefsSetIterator.
type deduplicatingSeriesChunkRefsSetIterator struct {
	batchSize int

	from    seriesChunkRefsIterator
	peek    *seriesChunkRefs
	current seriesChunkRefsSet
}

func newDeduplicatingSeriesChunkRefsSetIterator(batchSize int, wrapped seriesChunkRefsSetIterator) seriesChunkRefsSetIterator {
	return &deduplicatingSeriesChunkRefsSetIterator{
		batchSize: batchSize,
		from:      newFlattenedSeriesChunkRefsIterator(wrapped),
	}
}

func (s *deduplicatingSeriesChunkRefsSetIterator) Err() error {
	return s.from.Err()
}

func (s *deduplicatingSeriesChunkRefsSetIterator) At() seriesChunkRefsSet {
	return s.current
}

func (s *deduplicatingSeriesChunkRefsSetIterator) Next() bool {
	var firstSeries seriesChunkRefs
	if s.peek == nil {
		if !s.from.Next() {
			return false
		}
		firstSeries = s.from.At()
	} else {
		firstSeries = *s.peek
		s.peek = nil
	}
	nextSet := newSeriesChunkRefsSet(s.batchSize)
	nextSet.series = append(nextSet.series, firstSeries)

	var nextSeries seriesChunkRefs
	for i := 0; i < s.batchSize; {
		if !s.from.Next() {
			break
		}
		nextSeries = s.from.At()

		if labels.Equal(nextSet.series[i].lset, nextSeries.lset) {
			nextSet.series[i].chunks = append(nextSet.series[i].chunks, nextSeries.chunks...)
		} else {
			i++
			if i >= s.batchSize {
				s.peek = &nextSeries
				break
			}
			nextSet.series = append(nextSet.series, nextSeries)
		}
	}
	s.current = nextSet
	return true
}
