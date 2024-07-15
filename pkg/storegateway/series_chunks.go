// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"hash/crc32"
	"sync"
	"time"

	"github.com/dennwc/varint"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
	util_math "github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/pool"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	// Mimir compacts blocks up to 24h. Assuming a 5s scrape interval as worst case scenario,
	// and 120 samples per chunk, there could be 86400 * (1 / 5) * (1 / 120) = 144 chunks for
	// a series in the biggest block. Using a slab size of 1000 looks a good trade-off to support
	// high frequency scraping without wasting too much memory in case of queries hitting a low
	// number of chunks (across series).
	seriesChunksSlabSize = 1000

	// Selected so that many chunks fit within the slab size with low fragmentation, because
	// byte slices are at most 16KB each as received by the caching bucket.
	chunkBytesSlabSize = 160 * 1024

	// Selected so that most series fit it and at the same time it's not too large for requests with few series.
	// Most series are less than 4096 B.
	seriesBytesSlabSize = 16 * 1024
)

var (
	seriesChunksSlicePool = pool.Interface(&sync.Pool{
		// Intentionally return nil if the pool is empty, so that the caller can preallocate
		// the slice with the right size.
		New: nil,
	})

	chunksSlicePool = pool.Interface(&sync.Pool{
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

type iterator[V any] interface {
	Next() bool
	At() V
	Err() error
}

// seriesChunksSet holds a set of series, each with its own chunks.
type seriesChunksSet struct {
	series           []seriesChunks
	seriesReleasable bool

	// It gets lazy initialized (only if required).
	seriesChunksPool *pool.SlabPool[storepb.AggrChunk]

	// chunksReleaser releases the memory used to allocate series chunks.
	chunksReleaser releaser
}

// newSeriesChunksSet creates a new seriesChunksSet. The series slice is pre-allocated with
// the provided seriesCapacity at least. This means this function GUARANTEES the series slice
// will have a capacity of at least seriesCapacity.
//
// If seriesReleasable is true, then a subsequent call release() will put the internal
// series slices to a memory pool for reusing.
func newSeriesChunksSet(seriesCapacity int, seriesReleasable bool) seriesChunksSet {
	var prealloc []seriesChunks

	// If it's releasable then we try to reuse a slice from the pool.
	if seriesReleasable {
		if reused := seriesChunksSlicePool.Get(); reused != nil {
			prealloc = *(reused.(*[]seriesChunks))

			// The capacity MUST be guaranteed. If it's smaller, then we forget it and will be
			// reallocated.
			if cap(prealloc) < seriesCapacity {
				prealloc = nil
			}
		}
	}

	if prealloc == nil {
		prealloc = make([]seriesChunks, 0, seriesCapacity)
	}

	return seriesChunksSet{
		series:           prealloc,
		seriesReleasable: seriesReleasable,
	}
}

type releaser interface {
	// Release should release resources associated with this releaser instance.
	// It is not safe to use any resources from this releaser after calling Release.
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

			b.series[i] = seriesChunks{}
		}

		if b.seriesChunksPool != nil {
			b.seriesChunksPool.Release()
		}

		reuse := b.series[:0]
		seriesChunksSlicePool.Put(&reuse)
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
		b.seriesChunksPool = pool.NewSlabPool[storepb.AggrChunk](chunksSlicePool, seriesChunksSlabSize)
	}

	return b.seriesChunksPool.Get(size)
}

func (b seriesChunksSet) len() int {
	return len(b.series)
}

type seriesChunksSeriesSet struct {
	from iterator[seriesChunksSet]

	currSet    seriesChunksSet
	currOffset int
}

func newSeriesChunksSeriesSet(from iterator[seriesChunksSet]) storepb.SeriesSet {
	return &seriesChunksSeriesSet{
		from: from,
	}
}

func newChunksPreloadingIterator(
	ctx context.Context,
	logger log.Logger,
	userID string,
	chunkReaders bucketChunkReaders,
	refsIterator iterator[seriesChunkRefsSet],
	refsIteratorBatchSize int,
	stats *safeQueryStats,
) iterator[seriesChunksSet] {
	var it iterator[seriesChunksSet]
	it = newLoadingSeriesChunksSetIterator(ctx, logger, userID, chunkReaders, refsIterator, refsIteratorBatchSize, stats)
	it = newPreloadingAndStatsTrackingSetIterator(ctx, 1, it, stats)
	return it
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
		return labels.EmptyLabels(), nil
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

type preloadingSetIterator[Set any] struct {
	ctx  context.Context
	from iterator[Set]

	current Set

	preloaded chan preloadedSeriesChunksSet[Set]
	err       error
}

func newPreloadingSetIterator[Set any](ctx context.Context, preloadedSetsCount int, from iterator[Set]) *preloadingSetIterator[Set] {
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
		select {
		case <-p.ctx.Done():
			// If the client gives up on reading from the stream, then we will never return this error
		case p.preloaded <- preloadedSeriesChunksSet[Set]{err: p.from.Err()}:
		}
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

func newPreloadingAndStatsTrackingSetIterator[Set any](ctx context.Context, preloadedSetsCount int, iterator iterator[Set], stats *safeQueryStats) iterator[Set] {
	// Track the time spent loading batches (including preloading).
	numBatches := 0
	iterator = newNextDurationMeasuringIterator[Set](iterator, func(duration time.Duration, hasNext bool) {
		stats.update(func(stats *queryStats) {
			stats.streamingSeriesBatchLoadDuration += duration

			// This function is called for each Next() invocation, so we can use it to measure
			// into how many batches the request has been split.
			if hasNext {
				numBatches++
			}
			stats.streamingSeriesBatchCount = numBatches
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

// loadingSeriesChunksSetIterator loads the chunks of many series from many TSDB blocks.
type loadingSeriesChunksSetIterator struct {
	ctx           context.Context
	logger        log.Logger
	userID        string
	chunkReaders  bucketChunkReaders
	from          iterator[seriesChunkRefsSet]
	fromBatchSize int
	stats         *safeQueryStats

	current seriesChunksSet
	err     error
}

func newLoadingSeriesChunksSetIterator(
	ctx context.Context,
	logger log.Logger,
	userID string,
	chunkReaders bucketChunkReaders,
	from iterator[seriesChunkRefsSet],
	fromBatchSize int,
	stats *safeQueryStats,
) *loadingSeriesChunksSetIterator {
	return &loadingSeriesChunksSetIterator{
		ctx:           ctx,
		logger:        logger,
		userID:        userID,
		chunkReaders:  chunkReaders,
		from:          from,
		fromBatchSize: fromBatchSize,
		stats:         stats,
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

	defer func(startTime time.Time) {
		spanLog := spanlogger.FromContext(c.ctx, c.logger)
		spanLog.DebugLog(
			"msg", "loaded chunks",
			"series_count", c.At().len(),
			"err", c.Err(),
			"duration", time.Since(startTime),
		)
	}(time.Now())

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

	// Create a batched memory pool that can be released all at once.
	chunksPool := pool.NewSafeSlabPool[byte](chunkBytesSlicePool, chunkBytesSlabSize)

	// The series slice is guaranteed to have at least the requested capacity,
	// so can safely expand it.
	nextSet.series = nextSet.series[:nextUnloaded.len()]

	c.chunkReaders.reset()
	for sIdx, s := range nextUnloaded.series {
		nextSet.series[sIdx].lset = s.lset
		nextSet.series[sIdx].chks = nextSet.newSeriesAggrChunkSlice(len(s.refs))
		initializeChunks(s.refs, nextSet.series[sIdx].chks)

		for cIdx, chunk := range s.refs {
			err := c.chunkReaders.addLoad(chunk.blockID, chunk.ref(), sIdx, cIdx, chunk.length)
			if err != nil {
				c.err = errors.Wrap(err, "preloading chunks")
				return false
			}
		}
	}

	err := c.chunkReaders.load(nextSet.series, chunksPool, c.stats)
	if err != nil {
		c.err = errors.Wrap(err, "loading chunks")
		return false
	}
	c.recordProcessedChunks(nextSet.series)
	c.recordReturnedChunks(nextSet.series)

	nextSet.chunksReleaser = chunksPool
	c.current = nextSet
	return true
}

func initializeChunks(refs []seriesChunkRef, chunks []storepb.AggrChunk) {
	for cIdx := range chunks {
		chunks[cIdx].MinTime = refs[cIdx].minTime
		chunks[cIdx].MaxTime = refs[cIdx].maxTime
	}
}

func (c *loadingSeriesChunksSetIterator) At() seriesChunksSet {
	return c.current
}

func (c *loadingSeriesChunksSetIterator) Err() error {
	return c.err
}

func (c *loadingSeriesChunksSetIterator) recordReturnedChunks(series []seriesChunks) {
	returnedChunks, returnedChunksBytes := chunkStats(series)

	c.stats.update(func(stats *queryStats) {
		stats.chunksReturned += returnedChunks
		stats.chunksReturnedSizeSum += returnedChunksBytes
	})
}

func (c *loadingSeriesChunksSetIterator) recordProcessedChunks(series []seriesChunks) {
	processedChunks, processedChunksBytes := chunkStats(series)

	c.stats.update(func(stats *queryStats) {
		stats.chunksProcessed += processedChunks
		stats.chunksProcessedSizeSum += processedChunksBytes
	})
}

func chunkStats(series []seriesChunks) (numChunks, totalSize int) {
	for _, s := range series {
		numChunks += len(s.chks)
		totalSize += chunksSizeInSegmentFile(s.chks)
	}
	return
}

// chunksSizeInSegmentFile "reverse" calculates the size of chunks in the segment file. This was the size we returned from the
// touched range bytes. We can measure only the data length of the chunk,
// but that would not account for the extra few bytes for data length, encoding and crc32.
// These extra few bytes may be significant if the data bytes are small.
func chunksSizeInSegmentFile(chks []storepb.AggrChunk) int {
	total := 0
	for _, c := range chks {
		dataLen := len(c.Raw.Data)
		total += varint.UvarintSize(uint64(dataLen)) + 1 + dataLen + crc32.Size
	}
	return total
}

type nextDurationMeasuringIterator[Set any] struct {
	from     iterator[Set]
	observer func(duration time.Duration, hasNext bool)
}

func newNextDurationMeasuringIterator[Set any](from iterator[Set], observer func(duration time.Duration, hasNext bool)) iterator[Set] {
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
