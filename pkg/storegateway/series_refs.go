// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"bytes"
	"context"
	"fmt"
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

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
	"github.com/grafana/mimir/pkg/storegateway/indexcache"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/pool"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

var (
	seriesChunkRefsSetPool = pool.Interface(&sync.Pool{
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
		totalChunks += len(s.chunks)
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
	ctx                 context.Context
	postingsSetIterator *postingsSetsIterator
	indexr              *bucketIndexReader
	indexCache          indexcache.IndexCache
	stats               *safeQueryStats
	blockID             ulid.ULID
	shard               *sharding.ShardSelector
	seriesHasher        seriesHasher
	skipChunks          bool
	minTime, maxTime    int64
	tenantID            string
	logger              log.Logger

	symbolizedLsetBuffer []symbolizedLabel
	chksBuffer           []chunks.Meta

	err        error
	currentSet seriesChunkRefsSet
}

func openBlockSeriesChunkRefsSetsIterator(
	ctx context.Context,
	batchSize int,
	tenantID string,
	indexr *bucketIndexReader, // Index reader for block.
	indexCache indexcache.IndexCache,
	blockMeta *metadata.Meta,
	matchers []*labels.Matcher, // Series matchers.
	shard *sharding.ShardSelector, // Shard selector.
	seriesHasher seriesHasher,
	skipChunks bool, // If true chunks are not loaded and minTime/maxTime are ignored.
	minTime, maxTime int64, // Series must have data in this time range to be returned (ignored if skipChunks=true).
	stats *safeQueryStats,
	logger log.Logger,
) (seriesChunkRefsSetIterator, error) {
	if batchSize <= 0 {
		return nil, errors.New("set size must be a positive number")
	}

	ps, err := indexr.ExpandedPostings(ctx, matchers, stats)
	if err != nil {
		return nil, errors.Wrap(err, "expanded matching postings")
	}

	iterator := newLoadingSeriesChunkRefsSetIterator(
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
		logger,
	)

	// Track the time spent loading series and chunk refs.
	return newNextDurationMeasuringIterator[seriesChunkRefsSet](iterator, func(duration time.Duration, _ bool) {
		stats.update(func(stats *queryStats) {
			stats.streamingSeriesFetchRefsDuration += duration
		})
	}), nil
}

func newLoadingSeriesChunkRefsSetIterator(
	ctx context.Context,
	postingsSetIterator *postingsSetsIterator,
	indexr *bucketIndexReader,
	indexCache indexcache.IndexCache,
	stats *safeQueryStats,
	blockMeta *metadata.Meta,
	shard *sharding.ShardSelector,
	seriesHasher seriesHasher,
	skipChunks bool,
	minTime int64,
	maxTime int64,
	tenantID string,
	logger log.Logger,
) *loadingSeriesChunkRefsSetIterator {
	if skipChunks {
		minTime, maxTime = blockMeta.MinTime, blockMeta.MaxTime
	}

	return &loadingSeriesChunkRefsSetIterator{
		ctx:                 ctx,
		postingsSetIterator: postingsSetIterator,
		indexr:              indexr,
		indexCache:          indexCache,
		stats:               stats,
		blockID:             blockMeta.ULID,
		shard:               shard,
		seriesHasher:        seriesHasher,
		skipChunks:          skipChunks,
		minTime:             minTime,
		maxTime:             maxTime,
		tenantID:            tenantID,
		logger:              logger,
	}
}

func (s *loadingSeriesChunkRefsSetIterator) Next() bool {
	if s.err != nil {
		return false
	}
	if !s.postingsSetIterator.Next() {
		return false
	}
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
		var unsafeStats queryStats
		nextPostings = filterPostingsByCachedShardHash(nextPostings, s.shard, s.seriesHasher, &unsafeStats)
		loadStats.merge(&unsafeStats)
	}

	loadedSeries, err := s.indexr.preloadSeries(s.ctx, nextPostings, s.stats)
	if err != nil {
		s.err = errors.Wrap(err, "preload series")
		return false
	}

	// This can be released by the caller because loadingSeriesChunkRefsSetIterator doesn't retain it
	// after Next() will be called again.
	nextSet := newSeriesChunkRefsSet(len(nextPostings), true)

	for _, id := range nextPostings {
		lset, chks, err := s.loadSeriesForTime(id, loadedSeries, loadStats)
		if err != nil {
			s.err = errors.Wrap(err, "read series")
			return false
		}
		if lset.Len() == 0 {
			// No matching chunks for this time duration, skip series
			continue
		}

		if !shardOwned(s.shard, s.seriesHasher, id, lset, loadStats) {
			continue
		}

		nextSet.series = append(nextSet.series, seriesChunkRefs{
			lset:   lset,
			chunks: chks,
		})
	}

	if nextSet.len() == 0 {
		// The next set we attempted to build is empty, so we can directly release it.
		nextSet.release()

		// Try with the next set of postings.
		return s.Next()
	}

	s.currentSet = nextSet
	if s.skipChunks && cachedSeriesID.isSet() {
		storeCachedSeriesForPostings(s.ctx, s.indexCache, s.tenantID, s.blockID, s.shard, cachedSeriesID, nextSet, s.logger)
	}
	return true
}

func (s *loadingSeriesChunkRefsSetIterator) At() seriesChunkRefsSet {
	return s.currentSet
}

func (s *loadingSeriesChunkRefsSetIterator) Err() error {
	return s.err
}

func (s *loadingSeriesChunkRefsSetIterator) loadSeriesForTime(ref storage.SeriesRef, loadedSeries *bucketIndexLoadedSeries, stats *queryStats) (labels.Labels, []seriesChunkRef, error) {
	ok, err := loadedSeries.unsafeLoadSeriesForTime(ref, &s.symbolizedLsetBuffer, &s.chksBuffer, s.skipChunks, s.minTime, s.maxTime, stats)
	if !ok || err != nil {
		return labels.EmptyLabels(), nil, errors.Wrap(err, "inflateSeriesForTime")
	}

	lset, err := s.indexr.LookupLabelsSymbols(s.symbolizedLsetBuffer)
	if err != nil {
		return labels.EmptyLabels(), nil, errors.Wrap(err, "lookup labels symbols")
	}

	var chks []seriesChunkRef
	if !s.skipChunks {
		chks = metasToChunks(s.blockID, s.chksBuffer)
	}

	return lset, chks, nil
}

func metasToChunks(blockID ulid.ULID, metas []chunks.Meta) []seriesChunkRef {
	chks := make([]seriesChunkRef, len(metas))
	for i, meta := range metas {
		chks[i] = seriesChunkRef{
			minTime: meta.MinTime,
			maxTime: meta.MaxTime,
			ref:     meta.Ref,
			blockID: blockID,
		}
	}
	return chks
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
	indexCache.StoreSeriesForPostings(ctx, userID, blockID, shard, itemID.postingsKey, data)
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
