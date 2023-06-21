// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/golang/snappy"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"github.com/prometheus/prometheus/tsdb/index"
	"golang.org/x/exp/slices"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway/indexcache"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	util_math "github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/pool"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

var (
	seriesChunkRefsSetPool = pool.Interface(&sync.Pool{
		// Intentionally return nil if the pool is empty, so that the caller can preallocate
		// the slice with the right size.
		New: nil,
	})

	symbolizedSeriesChunkRefsSetsPool = pool.Interface(&sync.Pool{
		// Intentionally return nil if the pool is empty, so that the caller can preallocate
		// the slice with the right size.
		New: nil,
	})

	symbolizedLabelsPool = pool.Interface(&sync.Pool{
		// Intentionally return nil if the pool is empty, so that the caller can preallocate
		// the slice with the right size.
		New: nil,
	})
)

const (
	ErrSeriesLimitMessage = "exceeded series limit"
	ErrChunksLimitMessage = "exceeded chunks limit"
)

// seriesChunkRefsSetIterator is the interface implemented by an iterator returning a sequence of seriesChunkRefsSet.
type seriesChunkRefsSetIterator interface {
	Next() bool

	// At returns the current seriesChunkRefsSet. The caller should (but NOT must) invoke seriesChunkRefsSet.release()
	// on the returned set once it's guaranteed it will not be used anymore.
	At() seriesChunkRefsSet

	Err() error
}

// seriesChunkRefsIterator is the interface implemented by an iterator returning a sequence of seriesChunkRefs.
type seriesChunkRefsIterator interface {
	Next() bool
	At() seriesChunkRefs
	Err() error
}

// seriesChunkRefsSet holds a set of a set of series (sorted by labels) and their chunk references.
type seriesChunkRefsSet struct {
	// series sorted by labels.
	series []seriesChunkRefs

	// releasable holds whether the series slice (but not its content) can be released to a memory pool.
	releasable bool
}

type symbolizedSeriesChunkRefsSet struct {
	// series sorted by labels.
	series []symbolizedSeriesChunkRefs

	labelsPool *pool.SlabPool[symbolizedLabel]
}

func newSymbolizedSeriesChunkRefsSet(capacity int) symbolizedSeriesChunkRefsSet {
	var prealloc []symbolizedSeriesChunkRefs

	if reused := symbolizedSeriesChunkRefsSetsPool.Get(); reused != nil {
		prealloc = *(reused.(*[]symbolizedSeriesChunkRefs))
	}

	if prealloc == nil {
		prealloc = make([]symbolizedSeriesChunkRefs, 0, capacity)
	}

	return symbolizedSeriesChunkRefsSet{
		series:     prealloc,
		labelsPool: pool.NewSlabPool[symbolizedLabel](symbolizedLabelsPool, 1024),
	}
}

// release the internal series slice to a memory pool.
//
// This function is not idempotent. Calling it twice would introduce subtle bugs.
func (b symbolizedSeriesChunkRefsSet) release() {
	b.labelsPool.Release()
	reuse := b.series[:0]
	symbolizedSeriesChunkRefsSetsPool.Put(&reuse)
}

type symbolizedSeriesChunkRefs struct {
	lset         []symbolizedLabel
	chunksRanges []seriesChunkRefsRange
}

// newSeriesChunkRefsSet creates a new seriesChunkRefsSet with the given capacity.
// If releasable is true, then a subsequent call release() will put the internal
// series slices to a memory pool for reusing.
func newSeriesChunkRefsSet(capacity int, releasable bool) seriesChunkRefsSet {
	var prealloc []seriesChunkRefs

	// If it's releasable then we try to reuse a slice from the pool.
	if releasable {
		if reused := seriesChunkRefsSetPool.Get(); reused != nil {
			prealloc = *(reused.(*[]seriesChunkRefs))
		}
	}

	if prealloc == nil {
		prealloc = make([]seriesChunkRefs, 0, capacity)
	}

	return seriesChunkRefsSet{
		series:     prealloc,
		releasable: releasable,
	}
}

func (b seriesChunkRefsSet) len() int {
	return len(b.series)
}

// release the internal series slice to a memory pool. This function call has no effect
// if seriesChunkRefsSet was created to be not releasable.
//
// This function is not idempotent. Calling it twice would introduce subtle bugs.
func (b seriesChunkRefsSet) release() {
	if b.series == nil || !b.releasable {
		return
	}

	reuse := b.series[:0]
	seriesChunkRefsSetPool.Put(&reuse)
}

// seriesChunkRefs holds a series with a list of chunk references.
type seriesChunkRefs struct {
	lset         labels.Labels
	chunksRanges []seriesChunkRefsRange
}

func (s seriesChunkRefs) numChunks() (n int) {
	for _, r := range s.chunksRanges {
		n += len(r.refs)
	}
	return
}

// seriesChunkRefsRange contains chunks from the same block and the same segment file. They are ordered by their minTime
type seriesChunkRefsRange struct {
	blockID     ulid.ULID
	segmentFile uint32
	refs        []seriesChunkRef
}

func (g seriesChunkRefsRange) firstRef() chunks.ChunkRef {
	if len(g.refs) == 0 {
		return 0
	}
	return chunkRef(g.segmentFile, g.refs[0].segFileOffset)
}

func (g seriesChunkRefsRange) minTime() int64 {
	if len(g.refs) == 0 {
		return 0
	}
	// Since chunks in the groups are ordered by minTime, then we can just take the minTime of the first one.
	return g.refs[0].minTime
}

func (g seriesChunkRefsRange) maxTime() int64 {
	// Since chunks are only ordered by minTime, we have no guarantee for their maxTime, so we need to iterate all.
	maxT := int64(math.MinInt64)
	for _, c := range g.refs {
		if c.maxTime > maxT {
			maxT = c.maxTime
		}
	}
	return maxT
}

func (g seriesChunkRefsRange) Compare(other seriesChunkRefsRange) int {
	if g.minTime() < other.minTime() {
		return -1
	}
	if g.minTime() > other.minTime() {
		return 1
	}
	// Same min time.

	if g.maxTime() < other.maxTime() {
		return -1
	}
	if g.maxTime() > other.maxTime() {
		return 1
	}
	return 0
}

// seriesChunkRef holds the reference to a chunk in a given block.
type seriesChunkRef struct {
	// The order of these fields matters; having the uint32 on top makes the whole struct 24 bytes; in a different order the struct is 32B
	segFileOffset uint32
	// length will be 0 when the length of the chunk isn't known
	length uint32
	// minTime and maxTime are inclusive
	minTime, maxTime int64
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
//
// This function just reset the internal state and it does NOT invoke release()
// on the previous seriesChunkRefsSet.
func (c *seriesChunkRefsIteratorImpl) reset(set seriesChunkRefsSet) {
	c.set = set
	c.currentOffset = -1
}

// resetIteratorAndReleasePreviousSet is like reset() but also release the previous seriesChunkRefsSet
// hold internally. Invoke this function if none else except this iterator is holding a
// reference to the previous seriesChunkRefsSet.
func (c *seriesChunkRefsIteratorImpl) resetIteratorAndReleasePreviousSet(set seriesChunkRefsSet) {
	c.set.release()
	c.reset(set)
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
		// We can safely release the previous set because none retained it except the
		// iterator itself (which we're going to reset).
		c.iterator.resetIteratorAndReleasePreviousSet(seriesChunkRefsSet{})
		return false
	}

	// We can safely release the previous set because none retained it except the
	// iterator itself (which we're going to reset).
	c.iterator.resetIteratorAndReleasePreviousSet(c.from.At())

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

func mergedSeriesChunkRefsSetIterators(mergedBatchSize int, all ...seriesChunkRefsSetIterator) seriesChunkRefsSetIterator {
	switch len(all) {
	case 0:
		return emptySeriesChunkRefsSetIterator{}
	case 1:
		return newDeduplicatingSeriesChunkRefsSetIterator(mergedBatchSize, all[0])
	}
	h := len(all) / 2

	return newMergedSeriesChunkRefsSet(
		mergedBatchSize,
		mergedSeriesChunkRefsSetIterators(mergedBatchSize, all[:h]...),
		mergedSeriesChunkRefsSetIterators(mergedBatchSize, all[h:]...),
	)
}

type mergedSeriesChunkRefsSet struct {
	batchSize int

	a, b     seriesChunkRefsSetIterator
	aAt, bAt *seriesChunkRefsIteratorImpl
	current  seriesChunkRefsSet
	done     bool
}

func newMergedSeriesChunkRefsSet(mergedBatchSize int, a, b seriesChunkRefsSetIterator) *mergedSeriesChunkRefsSet {
	return &mergedSeriesChunkRefsSet{
		batchSize: mergedBatchSize,
		a:         a,
		b:         b,
		done:      false,
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

func (s *mergedSeriesChunkRefsSet) At() seriesChunkRefsSet {
	return s.current
}

func (s *mergedSeriesChunkRefsSet) Next() bool {
	if s.done {
		return false
	}

	// This can be released by the caller because mergedSeriesChunkRefsSet doesn't retain it
	// after Next() will be called again.
	next := newSeriesChunkRefsSet(s.batchSize, true)

	for i := 0; i < s.batchSize; i++ {
		if err := s.ensureItemAvailableToRead(s.aAt, s.a); err != nil {
			// Stop iterating on first error encountered.
			s.current = seriesChunkRefsSet{}
			s.done = true
			return false
		}

		if err := s.ensureItemAvailableToRead(s.bAt, s.b); err != nil {
			// Stop iterating on first error encountered.
			s.current = seriesChunkRefsSet{}
			s.done = true
			return false
		}

		nextSeries, ok := s.nextUniqueEntry(s.aAt, s.bAt)
		if !ok {
			break
		}
		next.series = append(next.series, nextSeries)
	}

	// We have reached the end of the iterator and next set is empty, so we can
	// directly release it.
	if next.len() == 0 {
		next.release()
		s.current = seriesChunkRefsSet{}
		s.done = true
		return false
	}

	s.current = next
	return true
}

// ensureItemAvailableToRead ensures curr has an item available to read, unless we reached the
// end of all sets. If curr has no item available to read, it will advance the iterator, eventually
// picking the next one from the set.
func (s *mergedSeriesChunkRefsSet) ensureItemAvailableToRead(curr *seriesChunkRefsIteratorImpl, set seriesChunkRefsSetIterator) error {
	// Ensure curr has an item available, otherwise fetch the next set.
	for curr.Done() {
		if set.Next() {
			// We can release the previous set because it hasn't been retained by anyone else.
			curr.resetIteratorAndReleasePreviousSet(set.At())

			// Advance the iterator to the first element. If the iterator is empty,
			// it will be detected and handled by the outer for loop.
			curr.Next()
			continue
		}

		// Release the previous set because won't be accessed anymore.
		curr.resetIteratorAndReleasePreviousSet(seriesChunkRefsSet{})

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
// label set, nextUniqueEntry merges their chunks ranges. The merged ranges are sorted by their MinTime and then by MaxTime.
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
	lsetA, chksA := aAt.lset, aAt.chunksRanges
	bAt := b.At()
	lsetB, chksB := bAt.lset, bAt.chunksRanges

	if d := labels.Compare(lsetA, lsetB); d > 0 {
		toReturn = b.At()
		b.Next()
		return toReturn, true
	} else if d < 0 {
		toReturn = a.At()
		a.Next()
		return toReturn, true
	}

	// Both a and b contains the same series. Go through all chunk ranges and concatenate them from both
	// series sets. We best effortly assume chunk ranges are sorted by min time. This means that
	// if the ranges overlap, then the resulting chunks will not be in min time order.
	// This is ok since the series API doesn't require us to return sorted chunks.
	toReturn.lset = lsetA

	// Slice reuse is not generally safe with nested merge iterators.
	// We err on the safe side and create a new slice.
	toReturn.chunksRanges = make([]seriesChunkRefsRange, 0, len(chksA)+len(chksB))

	bChunksOffset := 0
Outer:
	for aChunksOffset := range chksA {
		for {
			if bChunksOffset >= len(chksB) {
				// No more b chunks.
				toReturn.chunksRanges = append(toReturn.chunksRanges, chksA[aChunksOffset:]...)
				break Outer
			}

			if chksA[aChunksOffset].Compare(chksB[bChunksOffset]) < 0 {
				toReturn.chunksRanges = append(toReturn.chunksRanges, chksA[aChunksOffset])
				break
			}

			toReturn.chunksRanges = append(toReturn.chunksRanges, chksB[bChunksOffset])
			bChunksOffset++
		}
	}

	if bChunksOffset < len(chksB) {
		toReturn.chunksRanges = append(toReturn.chunksRanges, chksB[bChunksOffset:]...)
	}

	a.Next()
	b.Next()
	return toReturn, true
}

type seriesChunkRefsSeriesSet struct {
	from seriesChunkRefsSetIterator

	currentIterator *seriesChunkRefsIteratorImpl
}

func newSeriesChunkRefsSeriesSet(from seriesChunkRefsSetIterator) storepb.SeriesSet {
	return &seriesChunkRefsSeriesSet{
		from:            from,
		currentIterator: newSeriesChunkRefsIterator(seriesChunkRefsSet{}),
	}
}

func newSeriesSetWithoutChunks(ctx context.Context, iterator seriesChunkRefsSetIterator, stats *safeQueryStats) storepb.SeriesSet {
	iterator = newPreloadingAndStatsTrackingSetIterator[seriesChunkRefsSet](ctx, 1, iterator, stats)
	return newSeriesChunkRefsSeriesSet(iterator)
}

func (s *seriesChunkRefsSeriesSet) Next() bool {
	if s.currentIterator.Next() {
		return true
	}

	// The current iterator has no more elements. We check if there's another
	// iterator to fetch and then iterate on.
	if !s.from.Next() {
		// We can safely release the previous set because none retained it except the
		// iterator itself (which we're going to reset).
		s.currentIterator.resetIteratorAndReleasePreviousSet(seriesChunkRefsSet{})

		return false
	}

	next := s.from.At()

	// We can safely release the previous set because none retained it except the
	// iterator itself (which we're going to reset).
	s.currentIterator.resetIteratorAndReleasePreviousSet(next)

	// We've replaced the current iterator, so can recursively call Next()
	// to check if there's any item in the new iterator and further advance it if not.
	return s.Next()
}

func (s *seriesChunkRefsSeriesSet) At() (labels.Labels, []storepb.AggrChunk) {
	return s.currentIterator.At().lset, nil
}

func (s *seriesChunkRefsSeriesSet) Err() error {
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

	// This can be released by the caller because deduplicatingSeriesChunkRefsSetIterator doesn't retain it
	// after Next() will be called again.
	nextSet := newSeriesChunkRefsSet(s.batchSize, true)
	nextSet.series = append(nextSet.series, firstSeries)

	var nextSeries seriesChunkRefs
	for i := 0; i < s.batchSize; {
		if !s.from.Next() {
			break
		}
		nextSeries = s.from.At()

		if labels.Equal(nextSet.series[i].lset, nextSeries.lset) {
			// We don't need to ensure that chunks are in any particular order. The querier will sort the chunks for a single series.
			nextSet.series[i].chunksRanges = append(nextSet.series[i].chunksRanges, nextSeries.chunksRanges...)
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

type limitingSeriesChunkRefsSetIterator struct {
	from          seriesChunkRefsSetIterator
	chunksLimiter ChunksLimiter
	seriesLimiter SeriesLimiter

	err          error
	currentBatch seriesChunkRefsSet
}

func newLimitingSeriesChunkRefsSetIterator(from seriesChunkRefsSetIterator, chunksLimiter ChunksLimiter, seriesLimiter SeriesLimiter) *limitingSeriesChunkRefsSetIterator {
	return &limitingSeriesChunkRefsSetIterator{
		from:          from,
		chunksLimiter: chunksLimiter,
		seriesLimiter: seriesLimiter,
	}
}

func (l *limitingSeriesChunkRefsSetIterator) Next() bool {
	if l.err != nil {
		return false
	}

	if !l.from.Next() {
		l.err = l.from.Err()
		return false
	}

	l.currentBatch = l.from.At()
	err := l.seriesLimiter.Reserve(uint64(l.currentBatch.len()))
	if err != nil {
		l.err = errors.Wrap(err, ErrSeriesLimitMessage)
		return false
	}

	var totalChunks int
	for _, s := range l.currentBatch.series {
		for _, r := range s.chunksRanges {
			totalChunks += len(r.refs)
		}
	}

	err = l.chunksLimiter.Reserve(uint64(totalChunks))
	if err != nil {
		l.err = errors.Wrap(err, ErrChunksLimitMessage)
		return false
	}
	return true
}

func (l *limitingSeriesChunkRefsSetIterator) At() seriesChunkRefsSet {
	return l.currentBatch
}

func (l *limitingSeriesChunkRefsSetIterator) Err() error {
	return l.err
}

type loadingSeriesChunkRefsSetIterator struct {
	ctx                  context.Context
	postingsSetIterator  *postingsSetsIterator
	indexr               *bucketIndexReader
	indexCache           indexcache.IndexCache
	stats                *safeQueryStats
	blockID              ulid.ULID
	shard                *sharding.ShardSelector
	seriesHasher         seriesHasher
	skipChunks           bool
	minTime, maxTime     int64
	tenantID             string
	chunkRangesPerSeries int
	logger               log.Logger

	chunkMetasBuffer []chunks.Meta

	err        error
	currentSet seriesChunkRefsSet
}

func openBlockSeriesChunkRefsSetsIterator(
	ctx context.Context,
	batchSize int,
	tenantID string,
	indexr *bucketIndexReader, // Index reader for block.
	indexCache indexcache.IndexCache,
	blockMeta *block.Meta,
	matchers []*labels.Matcher, // Series matchers.
	shard *sharding.ShardSelector, // Shard selector.
	seriesHasher seriesHasher,
	skipChunks bool, // If true chunks are not loaded and minTime/maxTime are ignored.
	minTime, maxTime int64, // Series must have data in this time range to be returned (ignored if skipChunks=true).
	chunkRangesPerSeries int,
	stats *safeQueryStats,
	logger log.Logger,
) (seriesChunkRefsSetIterator, error) {
	if batchSize <= 0 {
		return nil, errors.New("set size must be a positive number")
	}

	ps, pendingMatchers, err := indexr.ExpandedPostings(ctx, matchers, stats)
	if err != nil {
		return nil, errors.Wrap(err, "expanded matching postings")
	}

	var iterator seriesChunkRefsSetIterator
	iterator = newLoadingSeriesChunkRefsSetIterator(
		ctx,
		newPostingsSetsIterator(ps, batchSize),
		indexr,
		indexCache,
		stats,
		blockMeta,
		shard,
		seriesHasher,
		skipChunks,
		minTime,
		maxTime,
		tenantID,
		chunkRangesPerSeries,
		logger,
	)
	if len(pendingMatchers) > 0 {
		iterator = newFilteringSeriesChunkRefsSetIterator(pendingMatchers, iterator, stats)
	}

	return seriesStreamingFetchRefsDurationIterator(iterator, stats), nil
}

// seriesStreamingFetchRefsDurationIterator tracks the time spent loading series and chunk refs.
func seriesStreamingFetchRefsDurationIterator(iterator seriesChunkRefsSetIterator, stats *safeQueryStats) genericIterator[seriesChunkRefsSet] {
	return newNextDurationMeasuringIterator[seriesChunkRefsSet](iterator, func(duration time.Duration, _ bool) {
		stats.update(func(stats *queryStats) {
			stats.streamingSeriesFetchRefsDuration += duration
		})
	})
}

func newLoadingSeriesChunkRefsSetIterator(
	ctx context.Context,
	postingsSetIterator *postingsSetsIterator,
	indexr *bucketIndexReader,
	indexCache indexcache.IndexCache,
	stats *safeQueryStats,
	blockMeta *block.Meta,
	shard *sharding.ShardSelector,
	seriesHasher seriesHasher,
	skipChunks bool,
	minTime int64,
	maxTime int64,
	tenantID string,
	chunkRangesPerSeries int,
	logger log.Logger,
) *loadingSeriesChunkRefsSetIterator {
	if skipChunks {
		minTime, maxTime = blockMeta.MinTime, blockMeta.MaxTime
	}

	return &loadingSeriesChunkRefsSetIterator{
		ctx:                  ctx,
		postingsSetIterator:  postingsSetIterator,
		indexr:               indexr,
		indexCache:           indexCache,
		stats:                stats,
		blockID:              blockMeta.ULID,
		shard:                shard,
		seriesHasher:         seriesHasher,
		skipChunks:           skipChunks,
		minTime:              minTime,
		maxTime:              maxTime,
		tenantID:             tenantID,
		logger:               logger,
		chunkRangesPerSeries: chunkRangesPerSeries,
	}
}

func (s *loadingSeriesChunkRefsSetIterator) Next() bool {
	if s.err != nil {
		return false
	}
	if !s.postingsSetIterator.Next() {
		return false
	}

	defer func(startTime time.Time) {
		spanLog := spanlogger.FromContext(s.ctx, s.logger)
		level.Debug(spanLog).Log(
			"msg", "loaded series and chunk refs",
			"block_id", s.blockID.String(),
			"series_count", s.At().len(),
			"err", s.Err(),
			"duration", time.Since(startTime),
		)
	}(time.Now())

	nextPostings := s.postingsSetIterator.At()

	var cachedSeriesID cachedSeriesForPostingsID
	if s.skipChunks {
		var err error
		// Calculate the cache ID before we filter out anything from the postings,
		// so that the key doesn't depend on the series hash cache or any other filtering we do on the postings list.
		// Calculate the cache item ID only if we'll actually use it.
		cachedSeriesID.postingsKey = indexcache.CanonicalPostingsKey(nextPostings)
		cachedSeriesID.encodedPostings, err = diffVarintSnappyEncode(index.NewListPostings(nextPostings), len(nextPostings))
		if err != nil {
			level.Warn(s.logger).Log("msg", "could not encode postings for series cache key", "err", err)
		} else {
			if cachedSet, isCached := fetchCachedSeriesForPostings(s.ctx, s.tenantID, s.indexCache, s.blockID, s.shard, cachedSeriesID, s.logger); isCached {
				s.currentSet = cachedSet
				return true
			}
		}
	}

	// Track the series loading statistics in a not synchronized data structure to avoid locking for each series
	// and then merge before returning from the function.
	loadStats := &queryStats{}
	defer s.stats.merge(loadStats)

	// We can't compute the series hash yet because we're still missing the series labels.
	// However, if the hash is already in the cache, then we can remove all postings for series
	// not belonging to the shard.
	if s.shard != nil {
		nextPostings = filterPostingsByCachedShardHash(nextPostings, s.shard, s.seriesHasher, loadStats)
	}

	symbolizedSet, err := s.symbolizedSet(s.ctx, nextPostings, loadStats)
	if err != nil {
		s.err = err
		return false
	}
	defer symbolizedSet.release() // We only retain the slices of chunk ranges from this set. These are still not pooled, and it's ok to retain them.

	nextSet, err := s.stringifiedSet(symbolizedSet)
	if err != nil {
		s.err = err
		return false
	}

	nextSet = s.filterSeries(nextSet, nextPostings, loadStats)

	if len(nextSet.series) == 0 {
		// Try with the next set of postings.
		return s.Next()
	}

	s.currentSet = nextSet
	if s.skipChunks && cachedSeriesID.isSet() {
		storeCachedSeriesForPostings(s.ctx, s.indexCache, s.tenantID, s.blockID, s.shard, cachedSeriesID, nextSet, s.logger)
	}
	return true
}

func (s *loadingSeriesChunkRefsSetIterator) symbolizedSet(ctx context.Context, postings []storage.SeriesRef, stats *queryStats) (_ symbolizedSeriesChunkRefsSet, err error) {
	symbolizedSet := newSymbolizedSeriesChunkRefsSet(len(postings))
	defer func() {
		if err != nil {
			symbolizedSet.release()
		}
	}()

	loadedSeries, err := s.indexr.preloadSeries(ctx, postings, s.stats)
	if err != nil {
		return symbolizedSeriesChunkRefsSet{}, errors.Wrap(err, "preload series")
	}

	for _, id := range postings {
		var (
			metas  []chunks.Meta
			series symbolizedSeriesChunkRefs
		)
		series.lset, metas, err = s.loadSeries(id, loadedSeries, stats, symbolizedSet.labelsPool)
		if err != nil {
			return symbolizedSeriesChunkRefsSet{}, errors.Wrap(err, "read series")
		}
		if !s.skipChunks {
			clampLastChunkLength(symbolizedSet.series, metas)
			series.chunksRanges = metasToRanges(partitionChunks(metas, s.chunkRangesPerSeries, minChunksPerRange), s.blockID, s.minTime, s.maxTime)
		}
		symbolizedSet.series = append(symbolizedSet.series, series)
	}
	return symbolizedSet, nil
}

func (s *loadingSeriesChunkRefsSetIterator) stringifiedSet(symbolizedSet symbolizedSeriesChunkRefsSet) (seriesChunkRefsSet, error) {
	if len(symbolizedSet.series) > 256 {
		// This approach comes with some overhead in data structures.
		// It starts making more sense with more label values only.
		return s.singlePassStringify(symbolizedSet)
	}

	return s.multiLookupStringify(symbolizedSet)
}

// clampLastChunkLength checks the length of the last chunk in the last range of the last series.
// If the length of that chunk is larger than the difference with the first chunk ref in metas
// then the length is clamped at that difference.
// clampLastChunkLength assumes that the chunks are sorted by their refs
// (currently this is equivalent to also being sorted by their minTime) and that all series belong to the same block.
// clampLastChunkLength is a noop if metas or series is empty.
func clampLastChunkLength(series []symbolizedSeriesChunkRefs, nextSeriesChunkMetas []chunks.Meta) {
	if len(series) == 0 || len(nextSeriesChunkMetas) == 0 {
		return
	}
	var lastSeriesRanges = series[len(series)-1].chunksRanges
	if len(lastSeriesRanges) == 0 {
		return
	}
	var (
		lastRange       = lastSeriesRanges[len(lastSeriesRanges)-1]
		lastSeriesChunk = lastRange.refs[len(lastRange.refs)-1]
		firstRef        = nextSeriesChunkMetas[0].Ref
	)

	// We only compare the segment file of the series because they all come from the same block.
	if lastRange.segmentFile != uint32(chunkSegmentFile(firstRef)) {
		return
	}
	diffWithNextChunk := chunkOffset(firstRef) - lastSeriesChunk.segFileOffset
	// The diff should always be positive, but if for some reason it isn't (a bug?), we don't want to set length to a negative value.
	if diffWithNextChunk > 0 && lastSeriesChunk.length > diffWithNextChunk {
		lastRange.refs[len(lastRange.refs)-1].length = diffWithNextChunk
	}
}

// filterSeries filters out series that don't belong to this shard (if sharding is configured) or that don't have any
// chunk ranges and skipChunks=false. Empty chunks ranges indicates that the series doesn't have any chunk ranges in the
// requested time range.
func (s *loadingSeriesChunkRefsSetIterator) filterSeries(set seriesChunkRefsSet, postings []storage.SeriesRef, stats *queryStats) seriesChunkRefsSet {
	writeIdx := 0
	for sIdx, series := range set.series {
		// An empty label set means the series had no chunks in this block, so we skip it.
		// No chunk ranges means the series doesn't have a single chunk range in the requested range.
		if len(series.lset) == 0 || (!s.skipChunks && len(series.chunksRanges) == 0) {
			continue
		}
		if !shardOwned(s.shard, s.seriesHasher, postings[sIdx], series.lset, stats) {
			continue
		}
		set.series[writeIdx] = set.series[sIdx]
		writeIdx++
	}
	set.series = set.series[:writeIdx]
	return set
}

const (
	minChunksPerRange = 10
)

// partitionChunks creates a slice of []chunks.Meta for each range of chunks within the same segment file.
// The partitioning here should be fairly static and not depend on the actual Series() request because
// the resulting ranges may be used for caching, and we want our cache entries to be reusable between requests.
// partitionChunks tries to partition the metas into targetNumRanges ranges. If any of those ranges will have
// less than minChunksPerRange metas, then partitionChunks will return less ranges than targetNumRanges.
// Regardless of targetNumRanges and minChunksPerRange, partitionChunks will keep chunks from separate segment files
// in separate partitions.
func partitionChunks(chks []chunks.Meta, targetNumRanges, minChunksPerRange int) [][]chunks.Meta {
	if len(chks) == 0 {
		return nil
	}
	chunksPerRange := util_math.Max(minChunksPerRange, len(chks)/targetNumRanges)

	ranges := make([][]chunks.Meta, 0, util_math.Min(targetNumRanges, len(chks)/chunksPerRange))

	currentRangeFirstChunkIdx := 0
	for i := range chks {
		isDifferentSegmentFile := chunkSegmentFile(chks[currentRangeFirstChunkIdx].Ref) != chunkSegmentFile(chks[i].Ref)
		currentRangeIsFull := i-currentRangeFirstChunkIdx >= chunksPerRange
		atLastRange := len(ranges) == targetNumRanges-1

		if isDifferentSegmentFile || (currentRangeIsFull && !atLastRange) {
			ranges = append(ranges, chks[currentRangeFirstChunkIdx:i])
			currentRangeFirstChunkIdx = i
		}
	}

	ranges = append(ranges, chks[currentRangeFirstChunkIdx:])

	return ranges
}

// metasToRanges converts partitioned metas to ranges of chunk refs. It excludes any ranges that do not have
// at least one chunk which overlaps with minT and maxT
func metasToRanges(partitions [][]chunks.Meta, blockID ulid.ULID, minT, maxT int64) []seriesChunkRefsRange {
	someMetaOverlapsWithMinTMaxT := func(gr []chunks.Meta) bool {
		for _, m := range gr {
			if m.MinTime <= maxT && m.MaxTime >= minT {
				return true
			}
		}
		return false
	}

	rangesWithinTime := 0
	for _, gr := range partitions {
		if someMetaOverlapsWithMinTMaxT(gr) {
			rangesWithinTime++
		}
	}
	if rangesWithinTime == 0 {
		return nil
	}
	ranges := make([]seriesChunkRefsRange, 0, rangesWithinTime)

	for pIdx, partition := range partitions {
		if !someMetaOverlapsWithMinTMaxT(partition) {
			continue
		}

		chunkRefs := make([]seriesChunkRef, 0, len(partition))
		for cIdx, c := range partition {
			var chunkLen uint32
			// We can only calculate the length of this chunk, if we know the ref of the next chunk
			// and the two chunks are in the same segment file.
			// We do that by taking the difference between the chunk references. This works since the chunk references are offsets in a file.
			// If the chunks are in different segment files (unlikely, but possible),
			// then this chunk ends at the end of the segment file, and we don't know how big the segment file is.
			if nextRef, ok := nextChunkRef(partitions, pIdx, cIdx); ok && chunkSegmentFile(nextRef) == chunkSegmentFile(c.Ref) {
				chunkLen = chunkOffset(nextRef) - chunkOffset(c.Ref)
				if chunkLen > tsdb.EstimatedMaxChunkSize {
					// Clamp the length in case chunks are scattered across a segment file. This should never happen,
					// but if it does, we don't want to have an erroneously large length.
					chunkLen = tsdb.EstimatedMaxChunkSize
				}
			} else {
				chunkLen = tsdb.EstimatedMaxChunkSize
			}
			chunkRefs = append(chunkRefs, seriesChunkRef{
				segFileOffset: chunkOffset(c.Ref),
				minTime:       c.MinTime,
				maxTime:       c.MaxTime,
				length:        chunkLen,
			})
		}

		ranges = append(ranges, seriesChunkRefsRange{
			blockID: blockID,
			// We have a guarantee that each meta in a partition will be from the same segment file; we can just take the segment file of the first chunk.
			// The cast to uint32 is safe because the segment file seq must fit in the first 32 bytes of the chunk ref
			segmentFile: uint32(chunkSegmentFile(partition[0].Ref)),
			refs:        chunkRefs,
		})
	}
	return ranges
}

func nextChunkRef(metas [][]chunks.Meta, gIdx int, cIdx int) (chunks.ChunkRef, bool) {
	if cIdx+1 >= len(metas[gIdx]) && gIdx+1 >= len(metas) {
		return 0, false
	}

	if cIdx+1 < len(metas[gIdx]) {
		return metas[gIdx][cIdx+1].Ref, true
	}
	return metas[gIdx+1][0].Ref, true
}

func (s *loadingSeriesChunkRefsSetIterator) At() seriesChunkRefsSet {
	return s.currentSet
}

func (s *loadingSeriesChunkRefsSetIterator) Err() error {
	return s.err
}

// loadSeries returns a for chunks. It is not safe to use the returned []chunks.Meta after calling loadSeries again
func (s *loadingSeriesChunkRefsSetIterator) loadSeries(ref storage.SeriesRef, loadedSeries *bucketIndexLoadedSeries, stats *queryStats, lsetPool *pool.SlabPool[symbolizedLabel]) ([]symbolizedLabel, []chunks.Meta, error) {
	ok, lbls, err := loadedSeries.unsafeLoadSeries(ref, &s.chunkMetasBuffer, s.skipChunks, stats, lsetPool)
	if !ok || err != nil {
		return nil, nil, errors.Wrap(err, "loadSeries")
	}

	return lbls, s.chunkMetasBuffer, nil
}

func (s *loadingSeriesChunkRefsSetIterator) singlePassStringify(symbolizedSet symbolizedSeriesChunkRefsSet) (seriesChunkRefsSet, error) {
	// Some conservative map pre-allocation; the goal is to get an order of magnitude size of the map, so we minimize map growth.
	symbols := make(map[uint32]string, len(symbolizedSet.series)/2)
	maxLabelsPerSeries := 0

	for _, series := range symbolizedSet.series {
		if numLabels := len(series.lset); maxLabelsPerSeries < numLabels {
			maxLabelsPerSeries = numLabels
		}
		for _, symRef := range series.lset {
			symbols[symRef.value] = ""
			symbols[symRef.name] = ""
		}
	}

	allSymbols := make([]uint32, 0, len(symbols))
	for sym := range symbols {
		allSymbols = append(allSymbols, sym)
	}
	slices.Sort(allSymbols)

	symReader, err := s.indexr.indexHeaderReader.SymbolsReader()
	if err != nil {
		return seriesChunkRefsSet{}, err
	}
	defer symReader.Close()
	for _, sym := range allSymbols {
		symbols[sym], err = symReader.Read(sym)
		if err != nil {
			return seriesChunkRefsSet{}, err
		}
	}

	// This can be released by the caller because loadingSeriesChunkRefsSetIterator doesn't retain it after Next() is called again.
	set := newSeriesChunkRefsSet(len(symbolizedSet.series), true)

	labelsBuilder := labels.NewScratchBuilder(maxLabelsPerSeries)
	for _, series := range symbolizedSet.series {
		labelsBuilder.Reset()
		for _, symRef := range series.lset {
			labelsBuilder.Add(symbols[symRef.name], symbols[symRef.value])
		}

		set.series = append(set.series, seriesChunkRefs{
			lset:         labelsBuilder.Labels(),
			chunksRanges: series.chunksRanges,
		})
	}

	return set, nil
}

func (s *loadingSeriesChunkRefsSetIterator) multiLookupStringify(symbolizedSet symbolizedSeriesChunkRefsSet) (seriesChunkRefsSet, error) {
	// This can be released by the caller because loadingSeriesChunkRefsSetIterator doesn't retain it after Next() is called again.
	set := newSeriesChunkRefsSet(len(symbolizedSet.series), true)

	labelsBuilder := labels.NewScratchBuilder(16)
	for _, series := range symbolizedSet.series {
		lset, err := s.indexr.LookupLabelsSymbols(series.lset, &labelsBuilder)
		if err != nil {
			return seriesChunkRefsSet{}, err
		}

		set.series = append(set.series, seriesChunkRefs{
			lset:         lset,
			chunksRanges: series.chunksRanges,
		})
	}
	return set, nil
}

type filteringSeriesChunkRefsSetIterator struct {
	stats    *safeQueryStats
	from     seriesChunkRefsSetIterator
	matchers []*labels.Matcher

	current seriesChunkRefsSet
}

func newFilteringSeriesChunkRefsSetIterator(matchers []*labels.Matcher, from seriesChunkRefsSetIterator, stats *safeQueryStats) *filteringSeriesChunkRefsSetIterator {
	return &filteringSeriesChunkRefsSetIterator{
		stats:    stats,
		from:     from,
		matchers: matchers,
	}
}

func (m *filteringSeriesChunkRefsSetIterator) Next() bool {
	if !m.from.Next() {
		return false
	}

	next := m.from.At()
	writeIdx := 0

	for _, series := range next.series {
		matches := true
		for _, matcher := range m.matchers {
			if !matcher.Matches(series.lset.Get(matcher.Name)) {
				matches = false
				break
			}
		}
		if matches {
			next.series[writeIdx] = series
			writeIdx++
		}
	}
	m.stats.update(func(stats *queryStats) {
		stats.seriesOmitted += next.len() - writeIdx
	})
	next.series = next.series[:writeIdx]

	if next.len() == 0 {
		next.release()
		return m.Next()
	}
	m.current = next
	return true
}

func (m *filteringSeriesChunkRefsSetIterator) At() seriesChunkRefsSet {
	return m.current
}

func (m *filteringSeriesChunkRefsSetIterator) Err() error {
	return m.from.Err()
}

// cachedSeriesForPostingsID contains enough information to be able to tell whether a cache entry
// is the right cache entry that we are looking for. We store only the postingsKey in the
// cache key because the encoded postings are too big. We store the encoded postings within
// the item and compare them when we need to fetch postings.
type cachedSeriesForPostingsID struct {
	postingsKey     indexcache.PostingsKey
	encodedPostings []byte
}

func (i cachedSeriesForPostingsID) isSet() bool {
	return i.postingsKey != "" && len(i.encodedPostings) > 0
}

func fetchCachedSeriesForPostings(ctx context.Context, userID string, indexCache indexcache.IndexCache, blockID ulid.ULID, shard *sharding.ShardSelector, itemID cachedSeriesForPostingsID, logger log.Logger) (seriesChunkRefsSet, bool) {
	data, ok := indexCache.FetchSeriesForPostings(ctx, userID, blockID, shard, itemID.postingsKey)
	if !ok {
		return seriesChunkRefsSet{}, false
	}
	data, err := snappy.Decode(nil, data)
	if err != nil {
		logSeriesForPostingsCacheEvent(ctx, logger, userID, blockID, shard, itemID, "msg", "can't decode series cache snappy", "err", err)
		return seriesChunkRefsSet{}, false
	}
	var entry storepb.CachedSeries
	if err = entry.Unmarshal(data); err != nil {
		logSeriesForPostingsCacheEvent(ctx, logger, userID, blockID, shard, itemID, "msg", "can't decode series cache", "err", err)
		return seriesChunkRefsSet{}, false
	}

	if !bytes.Equal(itemID.encodedPostings, entry.DiffEncodedPostings) {
		logSeriesForPostingsCacheEvent(ctx, logger, userID, blockID, shard, itemID, "msg", "cached series postings doesn't match, possible collision", "cached_trimmed_postings", previewDiffEncodedPostings(entry.DiffEncodedPostings))
		return seriesChunkRefsSet{}, false
	}

	// This can be released by the caller because loadingSeriesChunkRefsSetIterator (where this function is called) doesn't retain it
	// after Next() will be called again.
	res := newSeriesChunkRefsSet(len(entry.Series), true)
	for _, lset := range entry.Series {
		res.series = append(res.series, seriesChunkRefs{
			lset: mimirpb.FromLabelAdaptersToLabels(lset.Labels),
		})
	}
	return res, true
}

func storeCachedSeriesForPostings(ctx context.Context, indexCache indexcache.IndexCache, userID string, blockID ulid.ULID, shard *sharding.ShardSelector, itemID cachedSeriesForPostingsID, set seriesChunkRefsSet, logger log.Logger) {
	data, err := encodeCachedSeriesForPostings(set, itemID.encodedPostings)
	if err != nil {
		logSeriesForPostingsCacheEvent(ctx, logger, userID, blockID, shard, itemID, "msg", "can't encode series for caching", "err", err)
		return
	}
	indexCache.StoreSeriesForPostings(userID, blockID, shard, itemID.postingsKey, data)
}

func encodeCachedSeriesForPostings(set seriesChunkRefsSet, diffEncodedPostings []byte) ([]byte, error) {
	entry := &storepb.CachedSeries{
		Series:              make([]mimirpb.PreallocatingMetric, set.len()),
		DiffEncodedPostings: diffEncodedPostings,
	}
	for i, s := range set.series {
		entry.Series[i].Metric.Labels = mimirpb.FromLabelsToLabelAdapters(s.lset)
	}

	uncompressed, err := entry.Marshal()
	if err != nil {
		return nil, err
	}
	return snappy.Encode(nil, uncompressed), nil
}

func logSeriesForPostingsCacheEvent(ctx context.Context, logger log.Logger, userID string, blockID ulid.ULID, shard *sharding.ShardSelector, itemID cachedSeriesForPostingsID, msgAndArgs ...any) {
	nonNilShard := maybeNilShard(shard)
	msgAndArgs = append(msgAndArgs,
		"tenant_id", userID,
		"block_ulid", blockID.String(),
		"requested_shard_index", nonNilShard.ShardIndex,
		"requested_shard_count", nonNilShard.ShardCount,
		"postings_key", itemID.postingsKey,
		"requested_trimmed_postings", previewDiffEncodedPostings(itemID.encodedPostings),
	)
	level.Warn(spanlogger.FromContext(ctx, logger)).Log(msgAndArgs...)
}

// previewDiffEncodedPostings truncates the postings to just 100 bytes and prints them as a golang byte slice
func previewDiffEncodedPostings(b []byte) string {
	if len(b) > 100 {
		b = b[:100]
	}
	return fmt.Sprintf("%v", b)
}

type seriesHasher interface {
	Hash(seriesID storage.SeriesRef, lset labels.Labels, stats *queryStats) uint64
	CachedHash(seriesID storage.SeriesRef, stats *queryStats) (uint64, bool)
}

type cachedSeriesHasher struct {
	cache *hashcache.BlockSeriesHashCache
}

func (b cachedSeriesHasher) CachedHash(seriesID storage.SeriesRef, stats *queryStats) (uint64, bool) {
	stats.seriesHashCacheRequests++
	hash, isCached := b.cache.Fetch(seriesID)
	if isCached {
		stats.seriesHashCacheHits++
	}
	return hash, isCached
}

func (b cachedSeriesHasher) Hash(id storage.SeriesRef, lset labels.Labels, stats *queryStats) uint64 {
	hash, ok := b.CachedHash(id, stats)
	if !ok {
		hash = labels.StableHash(lset)
		b.cache.Store(id, hash)
	}
	return hash
}

func shardOwned(shard *sharding.ShardSelector, hasher seriesHasher, id storage.SeriesRef, lset labels.Labels, stats *queryStats) bool {
	if shard == nil {
		return true
	}
	hash := hasher.Hash(id, lset, stats)

	return hash%shard.ShardCount == shard.ShardIndex
}

// postingsSetsIterator splits the provided postings into sets, while retaining their original order.
type postingsSetsIterator struct {
	postings []storage.SeriesRef

	batchSize               int
	nextBatchPostingsOffset int
	currentBatch            []storage.SeriesRef
}

func newPostingsSetsIterator(postings []storage.SeriesRef, batchSize int) *postingsSetsIterator {
	return &postingsSetsIterator{
		postings:  postings,
		batchSize: batchSize,
	}
}

func (s *postingsSetsIterator) Next() bool {
	if s.nextBatchPostingsOffset >= len(s.postings) {
		return false
	}

	end := s.nextBatchPostingsOffset + s.batchSize
	if end > len(s.postings) {
		end = len(s.postings)
	}
	s.currentBatch = s.postings[s.nextBatchPostingsOffset:end]
	s.nextBatchPostingsOffset += s.batchSize

	return true
}

func (s *postingsSetsIterator) At() []storage.SeriesRef {
	return s.currentBatch
}
