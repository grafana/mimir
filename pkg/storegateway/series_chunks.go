// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/dennwc/varint"
	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/storegateway/chunkscache"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	util_math "github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/pool"
)

const (
	// Mimir compacts blocks up to 24h. Assuming a 5s scrape interval as worst case scenario,
	// and 120 samples per chunk, there could be 86400 * (1 / 5) * (1 / 120) = 144 chunks for
	// a series in the biggest block. Using a slab size of 1000 looks a good trade-off to support
	// high frequency scraping without wasting too much memory in case of queries hitting a low
	// number of chunks (across series).
	seriesChunksSlabSize = 1000

	// Selected so that chunks typically fit within the slab size (16 KiB)
	chunkBytesSlabSize = 16 * 1024
)

var (
	seriesEntrySlicePool = pool.Interface(&sync.Pool{
		// Intentionally return nil if the pool is empty, so that the caller can preallocate
		// the slice with the right size.
		New: nil,
	})

	seriesChunksSlicePool = pool.Interface(&sync.Pool{
		// Intentionally return nil if the pool is empty, so that the caller can preallocate
		// the slice with the right size.
		New: nil,
	})

	chunkBytesSlicePool = pool.Interface(&sync.Pool{
		// Intentionally return nil if the pool is empty, so that the caller can preallocate
		// the slice with the right size.
		New: nil,
	})
)

// seriesChunksSetIterator is the interface implemented by an iterator returning a sequence of seriesChunksSet.
type seriesChunksSetIterator interface {
	Next() bool

	// At returns the current seriesChunksSet. The caller should (but NOT must) invoke seriesChunksSet.release()
	// on the returned set once it's guaranteed it will not be used anymore.
	At() seriesChunksSet

	Err() error
}

// seriesChunksSet holds a set of series, each with its own chunks.
type seriesChunksSet struct {
	series           []seriesEntry
	seriesReleasable bool

	// It gets lazy initialized (only if required).
	seriesChunksPool *pool.SlabPool[storepb.AggrChunk]

	// chunksReleaser releases the memory used to allocate series chunks.
	chunksReleaser chunksReleaser
}

// newSeriesChunksSet creates a new seriesChunksSet. The series slice is pre-allocated with
// the provided seriesCapacity at least. This means this function GUARANTEES the series slice
// will have a capacity of at least seriesCapacity.
//
// If seriesReleasable is true, then a subsequent call release() will put the internal
// series slices to a memory pool for reusing.
func newSeriesChunksSet(seriesCapacity int, seriesReleasable bool) seriesChunksSet {
	var prealloc []seriesEntry

	// If it's releasable then we try to reuse a slice from the pool.
	if seriesReleasable {
		if reused := seriesEntrySlicePool.Get(); reused != nil {
			prealloc = *(reused.(*[]seriesEntry))

			// The capacity MUST be guaranteed. If it's smaller, then we forget it and will be
			// reallocated.
			if cap(prealloc) < seriesCapacity {
				prealloc = nil
			}
		}
	}

	if prealloc == nil {
		prealloc = make([]seriesEntry, 0, seriesCapacity)
	}

	return seriesChunksSet{
		series:           prealloc,
		seriesReleasable: seriesReleasable,
	}
}

type chunksReleaser interface {
	// Release the memory used to allocate series chunks.
	Release()
}

// release the internal series and chunks slices to a memory pool, and call the chunksReleaser.Release().
// The series and chunks slices won't be released to a memory pool if seriesChunksSet was created to be not releasable.
//
// This function is not idempotent. Calling it twice would introduce subtle bugs.
func (b *seriesChunksSet) release() {
	if b.chunksReleaser != nil {
		b.chunksReleaser.Release()
	}

	if b.seriesReleasable {
		// Reset series and chunk entries, before putting back to the pool.
		for i := range b.series {
			for c := range b.series[i].chks {
				b.series[i].chks[c].Reset()
			}

			b.series[i] = seriesEntry{}
		}

		if b.seriesChunksPool != nil {
			b.seriesChunksPool.Release()
		}

		reuse := b.series[:0]
		seriesEntrySlicePool.Put(&reuse)
	}
}

// newSeriesAggrChunkSlice returns a []storepb.AggrChunk guaranteed to have length and capacity
// equal to the provided size. The returned slice may be picked from a memory pool and then released
// back once release() gets invoked.
func (b *seriesChunksSet) newSeriesAggrChunkSlice(size int) []storepb.AggrChunk {
	if !b.seriesReleasable {
		return make([]storepb.AggrChunk, size)
	}

	// Lazy initialise the pool.
	if b.seriesChunksPool == nil {
		b.seriesChunksPool = pool.NewSlabPool[storepb.AggrChunk](seriesChunksSlicePool, seriesChunksSlabSize)
	}

	return b.seriesChunksPool.Get(size)
}

func (b *seriesChunksSet) len() int {
	return len(b.series)
}

type seriesChunksSeriesSet struct {
	from seriesChunksSetIterator

	currSet    seriesChunksSet
	currOffset int
}

func newSeriesChunksSeriesSet(from seriesChunksSetIterator) storepb.SeriesSet {
	return &seriesChunksSeriesSet{
		from: from,
	}
}

func newSeriesSetWithChunks(
	ctx context.Context,
	chunkReaders bucketChunkReaders,
	refsIterator seriesChunkRefsSetIterator,
	refsIteratorBatchSize int,
	stats *safeQueryStats,
	minT, maxT int64,
) storepb.SeriesSet {
	var iterator seriesChunksSetIterator
	iterator = newLoadingSeriesChunksSetIterator(chunkReaders, refsIterator, refsIteratorBatchSize, stats, minT, maxT)
	iterator = newPreloadingAndStatsTrackingSetIterator[seriesChunksSet](ctx, 1, iterator, stats)
	return newSeriesChunksSeriesSet(iterator)
}

// Next advances to the next item. Once the underlying seriesChunksSet has been fully consumed
// (which means the call to Next moves to the next set), the seriesChunksSet is released. This
// means that it's not safe to read from the values returned by At() after Next() is called again.
func (b *seriesChunksSeriesSet) Next() bool {
	b.currOffset++
	if b.currOffset >= b.currSet.len() {
		// The current set won't be accessed anymore because the iterator is moving to the next one,
		// so we can release it.
		b.currSet.release()

		if !b.from.Next() {
			b.currSet = seriesChunksSet{}
			return false
		}

		b.currSet = b.from.At()
		b.currOffset = 0
	}
	return true
}

// At returns the current series. The result from At() MUST not be retained after calling Next()
func (b *seriesChunksSeriesSet) At() (labels.Labels, []storepb.AggrChunk) {
	if b.currOffset >= b.currSet.len() {
		return nil, nil
	}

	return b.currSet.series[b.currOffset].lset, b.currSet.series[b.currOffset].chks
}

func (b *seriesChunksSeriesSet) Err() error {
	return b.from.Err()
}

// preloadedSeriesChunksSet holds the result of preloading the next set. It can either contain
// the preloaded set or an error, but not both.
type preloadedSeriesChunksSet[T any] struct {
	set T
	err error
}

type genericIterator[V any] interface {
	Next() bool
	At() V
	Err() error
}

type preloadingSetIterator[Set any] struct {
	ctx  context.Context
	from genericIterator[Set]

	current Set

	preloaded chan preloadedSeriesChunksSet[Set]
	err       error
}

func newPreloadingSetIterator[Set any](ctx context.Context, preloadedSetsCount int, from genericIterator[Set]) *preloadingSetIterator[Set] {
	preloadedSet := &preloadingSetIterator[Set]{
		ctx:       ctx,
		from:      from,
		preloaded: make(chan preloadedSeriesChunksSet[Set], preloadedSetsCount-1), // one will be kept outside the channel when the channel blocks
	}
	go preloadedSet.preload()
	return preloadedSet
}

func (p *preloadingSetIterator[Set]) preload() {
	defer close(p.preloaded)

	for p.from.Next() {
		select {
		case <-p.ctx.Done():
			// If the context is done, we should just stop the preloading goroutine.
			return
		case p.preloaded <- preloadedSeriesChunksSet[Set]{set: p.from.At()}:
		}
	}

	if p.from.Err() != nil {
		p.preloaded <- preloadedSeriesChunksSet[Set]{err: p.from.Err()}
	}
}

func (p *preloadingSetIterator[Set]) Next() bool {
	preloaded, ok := <-p.preloaded
	if !ok {
		// Iteration reached the end or context has been canceled.
		return false
	}

	p.current = preloaded.set
	p.err = preloaded.err

	return p.err == nil
}

func (p *preloadingSetIterator[Set]) At() Set {
	return p.current
}

func (p *preloadingSetIterator[Set]) Err() error {
	return p.err
}

func newPreloadingAndStatsTrackingSetIterator[Set any](ctx context.Context, preloadedSetsCount int, iterator genericIterator[Set], stats *safeQueryStats) genericIterator[Set] {
	// Track the time spent loading batches (including preloading).
	iterator = newNextDurationMeasuringIterator[Set](iterator, func(duration time.Duration, hasNext bool) {
		stats.update(func(stats *queryStats) {
			stats.streamingSeriesBatchLoadDuration += duration

			// This function is called for each Next() invocation, so we can use it to measure
			// into how many batches the request has been split.
			if hasNext {
				stats.streamingSeriesBatchCount++
			}
		})
	})

	iterator = newPreloadingSetIterator[Set](ctx, preloadedSetsCount, iterator)

	// Track the time step waiting until the next batch is loaded once the "reader" is ready to get it.
	return newNextDurationMeasuringIterator[Set](iterator, func(duration time.Duration, _ bool) {
		stats.update(func(stats *queryStats) {
			stats.streamingSeriesWaitBatchLoadedDuration += duration
		})
	})
}

type loadingSeriesChunksSetIterator struct {
	chunkReaders  bucketChunkReaders
	from          seriesChunkRefsSetIterator
	fromBatchSize int
	stats         *safeQueryStats

	current          seriesChunksSet
	err              error
	minTime, maxTime int64
}

func newLoadingSeriesChunksSetIterator(
	chunkReaders bucketChunkReaders,
	from seriesChunkRefsSetIterator,
	fromBatchSize int,
	stats *safeQueryStats,
	minT int64,
	maxT int64,
) *loadingSeriesChunksSetIterator {
	return &loadingSeriesChunksSetIterator{
		chunkReaders:  chunkReaders,
		from:          from,
		fromBatchSize: fromBatchSize,
		stats:         stats,
		minTime:       minT,
		maxTime:       maxT,
	}
}

func (c *loadingSeriesChunksSetIterator) Next() (retHasNext bool) {
	if c.err != nil {
		return false
	}

	if !c.from.Next() {
		c.err = c.from.Err()
		return false
	}

	nextUnloaded := c.from.At()

	// This data structure doesn't retain the seriesChunkRefsSet so it can be released once done.
	defer nextUnloaded.release()

	// Pre-allocate the series slice using the expected batchSize even if nextUnloaded has less elements,
	// so that there's a higher chance the slice will be reused once released.
	nextSet := newSeriesChunksSet(util_math.Max(c.fromBatchSize, nextUnloaded.len()), true)

	// Release the set if an error occurred.
	defer func() {
		if !retHasNext && c.err != nil {
			nextSet.release()
		}
	}()

	// The series slice is guaranteed to have at least the requested capacity,
	// so can safely expand it.
	nextSet.series = nextSet.series[:nextUnloaded.len()]

	c.chunkReaders.reset()

	for i, s := range nextUnloaded.series {
		nextSet.series[i].lset = s.lset
		nextSet.series[i].chks = nextSet.newSeriesAggrChunkSlice(s.numChunksWithinRange(c.minTime, c.maxTime))
		seriesChunkIdx := 0

		for _, chunksRange := range s.chunksRanges {
			for _, chunk := range chunksRange.refs {
				if chunk.minTime > c.maxTime || chunk.maxTime < c.minTime {
					continue
				}
				nextSet.series[i].chks[seriesChunkIdx].MinTime = chunk.minTime
				nextSet.series[i].chks[seriesChunkIdx].MaxTime = chunk.maxTime

				err := c.chunkReaders.addLoad(chunksRange.blockID, chunkRef(chunksRange.segmentFile, chunk.segFileOffset), i, seriesChunkIdx)
				if err != nil {
					c.err = errors.Wrap(err, "preloading chunks")
					return false
				}
				seriesChunkIdx++
			}
		}
	}

	// Create a batched memory pool that can be released all at once.
	chunksPool := pool.NewSafeSlabPool[byte](chunkBytesSlicePool, chunkBytesSlabSize)

	err := c.chunkReaders.load(nextSet.series, chunksPool, c.stats)
	if err != nil {
		c.err = errors.Wrap(err, "loading chunks")
		return false
	}

	nextSet.chunksReleaser = chunksPool
	c.current = nextSet
	return true
}

func (c *loadingSeriesChunksSetIterator) At() seriesChunksSet {
	return c.current
}

func (c *loadingSeriesChunksSetIterator) Err() error {
	return c.err
}

func newSeriesSetWithChunksAndCaching(
	ctx context.Context,
	l log.Logger,
	userID string,
	chunkReaders bucketChunkRangesReaders,
	refsIterator seriesChunkRefsSetIterator,
	refsIteratorBatchSize int,
	stats *safeQueryStats,
	cache chunkscache.Cache,
	minT, maxT int64,
	m *BucketStoreMetrics,
) storepb.SeriesSet {
	var iterator seriesChunksSetIterator
	iterator = newRangeLoadingSeriesChunksSetIterator(ctx, l, userID, chunkReaders, refsIterator, refsIteratorBatchSize, stats, cache, minT, maxT)
	iterator = newPreloadingAndStatsTrackingSetIterator[seriesChunksSet](ctx, 1, iterator, stats)
	return newSeriesChunksSeriesSet(iterator)
}

type rangeLoadingSeriesChunksSetIterator struct {
	ctx     context.Context
	l       log.Logger
	userID  string
	minTime int64
	maxTime int64

	cache         chunkscache.Cache
	chunkReaders  bucketChunkRangesReaders
	from          seriesChunkRefsSetIterator
	fromBatchSize int
	stats         *safeQueryStats

	current seriesChunksSet
	err     error
}

func newRangeLoadingSeriesChunksSetIterator(
	ctx context.Context,
	l log.Logger,
	userID string,
	chunkReaders bucketChunkRangesReaders,
	from seriesChunkRefsSetIterator,
	fromBatchSize int,
	stats *safeQueryStats,
	cache chunkscache.Cache,
	minT int64,
	maxT int64,
) *rangeLoadingSeriesChunksSetIterator {
	return &rangeLoadingSeriesChunksSetIterator{
		ctx:           ctx,
		l:             l,
		userID:        userID,
		chunkReaders:  chunkReaders,
		from:          from,
		fromBatchSize: fromBatchSize,
		stats:         stats,
		cache:         cache,
		minTime:       minT,
		maxTime:       maxT,
	}
}

func (c *rangeLoadingSeriesChunksSetIterator) Next() (retHasNext bool) {
	if c.err != nil {
		return false
	}

	if !c.from.Next() {
		c.err = c.from.Err()
		return false
	}

	nextUnloaded := c.from.At()

	// This data structure doesn't retain the seriesChunkRefsSet so it can be released once done.
	defer nextUnloaded.release()
	defer c.chunkReaders.reset()

	// Pre-allocate the series slice using the expected batchSize even if nextUnloaded has less elements,
	// so that there's a higher chance the slice will be reused once released.
	nextSet := newSeriesChunksSet(util_math.Max(c.fromBatchSize, nextUnloaded.len()), true)
	// The series slice is guaranteed to have at least the requested capacity,
	// so can safely expand it.
	nextSet.series = nextSet.series[:nextUnloaded.len()]

	// Release the set if an error occurred.
	defer func() {
		if !retHasNext && c.err != nil {
			nextSet.release()
		}
	}()

	partialSeries := newPartialSeries(nextUnloaded, nextSet.newSeriesAggrChunkSlice)

	fetchStartTime := time.Now()
	cachedRanges := c.cache.FetchMultiChunks(c.ctx, c.userID, toCacheKeys(nextUnloaded.series, countRanges(partialSeries)))
	c.recordCachedChunksStats(cachedRanges)

	// Collect the cached ranges bytes or prepare to fetch cache misses from the bucket.
	for sIdx, s := range partialSeries {
		for i, chunksRange := range s.refsRanges {
			if cachedBytes, ok := cachedRanges[toCacheKey(chunksRange)]; ok {
				s.rawRanges[i] = cachedBytes
				continue
			}
			err := c.chunkReaders.addLoadRange(chunksRange.blockID, chunksRange, sIdx, i)
			if err != nil {
				c.err = errors.Wrap(err, "preloading chunks")
				return false
			}
		}
	}

	err := c.chunkReaders.loadRanges(partialSeries, c.stats)
	if err != nil {
		c.err = errors.Wrap(err, "loading chunks")
		return false
	}
	c.recordFetchComplete(fetchStartTime)

	// Parse the bytes we have from the cache or the bucket. This returns the ranges for which we didn't have
	// enough fetched bytes. This may happen when a chunk's length was underestimated.
	underfetchedRanges, err := parseRanges(partialSeries)
	if err != nil {
		c.err = err
		return false
	}

	if len(underfetchedRanges) > 0 {
		c.chunkReaders.reset()
		err = c.refetchRanges(underfetchedRanges, partialSeries, nextSet)
		if err != nil {
			c.err = err
			return false
		}
	}

	c.recordFetchedChunks(partialSeries)

	c.storeChunkRanges(cachedRanges, partialSeries)

	for i, s := range partialSeries {
		// Since a range may contain more chunks that we need for the request,
		// go through all chunks and reslice to remove any chunks that are outside the request's MinT/MaxT
		nextSet.series[i].chks = removeChunksOutsideRange(s.parsedChunks, c.minTime, c.maxTime)
		nextSet.series[i].lset = nextUnloaded.series[i].lset
	}

	c.recordTouchedChunks(nextSet.series)

	c.current = nextSet
	return true
}

func countRanges(series []partialSeriesChunksSet) (n int) {
	for _, s := range series {
		for _, r := range s.refsRanges {
			n += len(r.refs)
		}
	}
	return
}

func (c *rangeLoadingSeriesChunksSetIterator) refetchRanges(underfetchedRanges map[int][]underfetchedChunksRangeIdx, partialSeries []partialSeriesChunksSet, nextSet seriesChunksSet) error {
	for sIdx, indices := range underfetchedRanges {
		for _, idx := range indices {
			err := c.chunkReaders.addLoadRange(idx.blockID, partialSeries[sIdx].refsRanges[idx.rangeIdx], sIdx, idx.rangeIdx)
			if err != nil {
				return fmt.Errorf("add load underfetched block %s first ref %d: %w", idx.blockID, partialSeries[sIdx].refsRanges[idx.rangeIdx].firstRef(), err)
			}
		}
	}

	refetchStartTime := time.Now()

	// Go to the bucket to fetch all ranges we undefetched.
	// We use a new stats instnace, so we can differentiate between the stats of fetching from those of refetching.
	refetchStats := newSafeQueryStats()
	err := c.chunkReaders.loadRanges(partialSeries, refetchStats)
	if err != nil {
		return errors.Wrap(err, "refetch ranges")
	}

	c.recordRefetchStats(refetchStartTime, underfetchedRanges, partialSeries, refetchStats)

	for sIdx, indices := range underfetchedRanges {
		for _, idx := range indices {
			err = partialSeries[sIdx].reparse(idx)
			if err != nil {
				return errors.Wrapf(err, "reparse ranges for series %s", nextSet.series[sIdx].lset)
			}
		}
	}
	return nil
}

func removeChunksOutsideRange(chks []storepb.AggrChunk, minT, maxT int64) []storepb.AggrChunk {
	writeIdx := 0
	for i, chk := range chks {
		if chk.MaxTime < minT || chk.MinTime > maxT {
			continue
		}
		if writeIdx != i {
			chks[i], chks[writeIdx] = chks[writeIdx], chks[i]
		}
		writeIdx++
	}

	return chks[:writeIdx]
}

func (c *rangeLoadingSeriesChunksSetIterator) storeChunkRanges(cachedRanges map[chunkscache.Range][]byte, partialSeries []partialSeriesChunksSet) {
	for _, s := range partialSeries {
		for i, g := range s.refsRanges {
			rangeKey := toCacheKey(g)
			if _, ok := cachedRanges[rangeKey]; ok {
				continue
			}
			// We don't copy the raw range because the raw range is not pooled. This means it will be freed by the GC.
			c.cache.StoreChunks(c.ctx, c.userID, rangeKey, s.rawRanges[i])
		}
	}
}

func toCacheKeys(series []seriesChunkRefs, totalRanges int) []chunkscache.Range {
	ranges := make([]chunkscache.Range, 0, totalRanges)
	for _, s := range series {
		for _, g := range s.chunksRanges {
			ranges = append(ranges, toCacheKey(g))
		}
	}
	return ranges
}

func toCacheKey(g seriesChunkRefsRange) chunkscache.Range {
	return chunkscache.Range{
		BlockID:   g.blockID,
		Start:     g.firstRef(),
		NumChunks: len(g.refs),
	}
}

func (c *rangeLoadingSeriesChunksSetIterator) At() seriesChunksSet {
	return c.current
}

func (c *rangeLoadingSeriesChunksSetIterator) Err() error {
	return c.err
}

type partialSeriesChunksSet struct {
	refsRanges   []seriesChunkRefsRange
	rawRanges    [][]byte
	parsedChunks []storepb.AggrChunk
}

func newPartialSeries(nextUnloaded seriesChunkRefsSet, getPooledChunks func(size int) []storepb.AggrChunk) []partialSeriesChunksSet {
	partialSeries := make([]partialSeriesChunksSet, len(nextUnloaded.series))

	for i, s := range nextUnloaded.series {
		numRanges := len(s.chunksRanges)
		partialSeries[i].refsRanges = s.chunksRanges
		partialSeries[i].rawRanges = make([][]byte, numRanges)
		chunksCount := 0
		for _, g := range s.chunksRanges {
			chunksCount += len(g.refs)
		}
		partialSeries[i].parsedChunks = getPooledChunks(chunksCount)
	}
	return partialSeries
}

type underfetchedChunksRangeIdx struct {
	blockID  ulid.ULID
	rangeIdx int
	parsed   []storepb.AggrChunk
}

// parse tries to parse the ranges in the partial set with the bytes it has in rawRanges.
// If the bytes for any of the ranges aren't enough, then parse will return an underfetchedChunksRangeIdx
// and will correct the length of all ranges which had a understimated length.
// Currently, parse will only correct the length of the last chunk, since this is the only chunk
// whose size we estimate.
// parse will also truncate the bytes in rawRanges in case there are extra unnecessary bytes there.
// parse will return an error when the bytes in rawRanges were underfetched before the last chunk or
// if the data in rawRanges is invalid.
func (s partialSeriesChunksSet) parse() ([]underfetchedChunksRangeIdx, error) {
	var underfetchedRanges []underfetchedChunksRangeIdx
	parsedChunksCount := 0
	for rIdx, r := range s.refsRanges {
		dst := s.parsedChunks[parsedChunksCount : parsedChunksCount+len(r.refs)]
		ok, err := s.populateRange(rIdx, dst)
		if err != nil {
			return nil, fmt.Errorf("parsing chunk range (block %s, first ref %d, num chunks %d): %w", r.blockID, r.firstRef(), len(r.refs), err)
		}
		if !ok {
			underfetchedRanges = append(underfetchedRanges, underfetchedChunksRangeIdx{
				blockID:  r.blockID,
				rangeIdx: rIdx,
				parsed:   dst,
			})
		}

		parsedChunksCount += len(r.refs)
	}
	return underfetchedRanges, nil
}

func convertChunkEncoding(storageEncoding chunkenc.Encoding) (storepb.Chunk_Encoding, bool) {
	switch storageEncoding {
	case chunkenc.EncXOR:
		return storepb.Chunk_XOR, true
	case chunkenc.EncHistogram:
		return storepb.Chunk_Histogram, true
	case chunkenc.EncFloatHistogram:
		return storepb.Chunk_FloatHistogram, true
	default:
		return 0, false
	}
}

func (s partialSeriesChunksSet) reparse(idx underfetchedChunksRangeIdx) error {
	refsRange := s.refsRanges[idx.rangeIdx]
	ok, err := s.populateRange(idx.rangeIdx, idx.parsed)
	if err != nil {
		return fmt.Errorf("parsing underfetched range (block %s first ref %d): %w", idx.blockID, refsRange.firstRef(), err)
	}
	if !ok {
		return fmt.Errorf("chunk length doesn't match after refetching (first ref %d)", refsRange.firstRef())
	}
	return nil
}

// parseRange also corrects the length of the last chunk to the size that it actually is.
// It slices away any extra bytes in the raw range.
func (s partialSeriesChunksSet) populateRange(rIdx int, dst []storepb.AggrChunk) (bool, error) {
	rawRange := s.rawRanges[rIdx]
	r := s.refsRanges[rIdx]
	for cIdx := range dst {
		dst[cIdx].MinTime = r.refs[cIdx].minTime
		dst[cIdx].MaxTime = r.refs[cIdx].maxTime
		if dst[cIdx].Raw == nil {
			// This may come as initialized from a pool or from a previous parse. Do an allocation only if it already isn't.
			dst[cIdx].Raw = &storepb.Chunk{}
		}
	}
	ok, lastChunkLen, totalRead, err := parseRange(rawRange, dst)
	if err != nil {
		return false, err
	}
	if !ok {
		// We estimate the length of the last chunk of a series.
		// Unfortunately, we got it wrong. We can set the length correctly because we now know it.
		r.refs[len(r.refs)-1].length = lastChunkLen
		return false, nil
	}
	s.rawRanges[rIdx] = s.rawRanges[rIdx][:totalRead]
	return true, nil
}

// parseRange parses the byte slice as concatenated encoded chunks. lastChunkLen is non-zero when allChunksComplete==false.
// An error is returned when gBytes are malformed or when more than the last chunk is incomplete.
func parseRange(rBytes []byte, chunks []storepb.AggrChunk) (allChunksComplete bool, lastChunkLen uint32, totalRead int, _ error) {
	rangeFullSize := len(rBytes)
	for i := range chunks {
		// ┌───────────────┬───────────────────┬──────────────┬────────────────┐
		// │ len <uvarint> │ encoding <1 byte> │ data <bytes> │ CRC32 <4 byte> │
		// └───────────────┴───────────────────┴──────────────┴────────────────┘
		chunkDataLen, n := varint.Uvarint(rBytes)
		if n == 0 {
			return false, 0, 0, fmt.Errorf("not enough bytes (%d) to read length of chunk %d/%d", len(rBytes), i, len(chunks))
		}
		if n < 0 {
			return false, 0, 0, fmt.Errorf("chunk length doesn't fit into uint64 %d/%d", i, len(chunks))
		}
		totalChunkLen := n + 1 + int(chunkDataLen) + crc32.Size
		if totalChunkLen > len(rBytes) {
			if i != len(chunks)-1 {
				return false, 0, 0, fmt.Errorf("underfetched before the last chunk, don't know what to do (chunk idx %d/%d, fetched %d/%d bytes)", i, len(chunks), len(rBytes), totalChunkLen)
			}
			return false, uint32(totalChunkLen), rangeFullSize - len(rBytes), nil
		}
		c := rawChunk(rBytes[n : n+1+int(chunkDataLen)])
		enc, ok := convertChunkEncoding(c.Encoding())
		if !ok {
			return false, 0, 0, fmt.Errorf("unknown encoding (%d, %s), don't know what to do", c.Encoding(), c.Encoding().String())
		}
		chunks[i].Raw.Type = enc
		chunks[i].Raw.Data = c.Bytes()
		// We ignore the crc32 because we assume that the chunk didn't get corrupted in transit or at rest.
		rBytes = rBytes[totalChunkLen:]
	}
	return true, 0, rangeFullSize - len(rBytes), nil
}

// parseRanges parses the passed bytes into nextSet. In case a range was underfetched, parseRanges will return an underfetchedChunksRangeIdx
// with the indices of the range; the keys in the map are the indices into partialSeries.
// parseRanges will also set the correct length of the last chunk in an underfetched range in case its estimated length was wrong.
func parseRanges(partialSeries []partialSeriesChunksSet) (map[int][]underfetchedChunksRangeIdx, error) {
	underfetchedSeries := make(map[int][]underfetchedChunksRangeIdx)
	for sIdx, series := range partialSeries {
		underfetched, err := series.parse()
		if err != nil {
			return nil, err
		}
		if len(underfetched) > 0 {
			underfetchedSeries[sIdx] = underfetched
		}
	}
	return underfetchedSeries, nil
}

func (c *rangeLoadingSeriesChunksSetIterator) recordFetchComplete(fetchStartTime time.Time) {
	c.stats.update(func(stats *queryStats) {
		stats.chunksFetchDurationSum += time.Since(fetchStartTime)
	})
}

func (c *rangeLoadingSeriesChunksSetIterator) recordCachedChunksStats(cachedRanges map[chunkscache.Range][]byte) {
	fetchedBytes := 0
	for _, b := range cachedRanges {
		fetchedBytes += len(b)
	}

	c.stats.update(func(stats *queryStats) {
		stats.chunksFetchedSizeSum += fetchedBytes
	})
}

func (c *rangeLoadingSeriesChunksSetIterator) recordRefetchStats(refetchStartTime time.Time, underfetchedRanges map[int][]underfetchedChunksRangeIdx, series []partialSeriesChunksSet, refetchStats *safeQueryStats) {
	refetchedChunks := 0
	for sIdx, indices := range underfetchedRanges {
		for _, idx := range indices {
			refetchedChunks += len(series[sIdx].refsRanges[idx.rangeIdx].refs)
		}
	}
	c.stats.update(func(stats *queryStats) {
		stats.chunksRefetched += refetchedChunks
		stats.chunksRefetchedSizeSum += refetchStats.export().chunksFetchedSizeSum
		stats.chunksFetchDurationSum += time.Since(refetchStartTime)
	})
}

func (c *rangeLoadingSeriesChunksSetIterator) recordFetchedChunks(series []partialSeriesChunksSet) {
	totalChunks := 0
	for _, s := range series {
		totalChunks += len(s.parsedChunks)
	}
	c.stats.update(func(stats *queryStats) {
		stats.chunksFetchCount++
		stats.chunksFetched += totalChunks
	})
}

func (c *rangeLoadingSeriesChunksSetIterator) recordTouchedChunks(series []seriesEntry) {
	touchedChunksBytes := 0
	touchedChunks := 0
	for _, s := range series {
		// We "reverse" calculate the size of chunks in the segment file. This was the size we touched from the
		// fetched range bytes. When parsing the range, we didn't have visibility into which bytes from the
		// fetched range were actually necessary. Now is the time we know what is actually touched.
		// We do this calculation, so it matches more closely the actual cost of the chunk as opposed to
		// only measuring the data length of the chunk and excluding the extra few bytes.
		// These extra few bytes may be significant if the data bytes are small.
		touchedChunksBytes += chunksSizeInSegmentFile(s.chks)
		touchedChunks += len(s.chks)
	}

	c.stats.update(func(stats *queryStats) {
		stats.chunksTouched += touchedChunks
		stats.chunksTouchedSizeSum += touchedChunksBytes
	})
}

func chunksSizeInSegmentFile(chks []storepb.AggrChunk) int {
	total := 0
	for _, c := range chks {
		dataLen := len(c.Raw.Data)
		total += varint.UvarintSize(uint64(dataLen)) + 1 + dataLen + crc32.Size
	}
	return total
}

type nextDurationMeasuringIterator[Set any] struct {
	from     genericIterator[Set]
	observer func(duration time.Duration, hasNext bool)
}

func newNextDurationMeasuringIterator[Set any](from genericIterator[Set], observer func(duration time.Duration, hasNext bool)) genericIterator[Set] {
	return &nextDurationMeasuringIterator[Set]{
		from:     from,
		observer: observer,
	}
}

func (m *nextDurationMeasuringIterator[Set]) Next() bool {
	start := time.Now()
	hasNext := m.from.Next()
	m.observer(time.Since(start), hasNext)
	return hasNext
}

func (m *nextDurationMeasuringIterator[Set]) At() Set {
	return m.from.At()
}

func (m *nextDurationMeasuringIterator[Set]) Err() error {
	return m.from.Err()
}
