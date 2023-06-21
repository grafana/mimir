// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/dennwc/varint"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/storegateway/chunkscache"
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

	// Selected so that many chunks fit within the slab size with low fragmentation, either when
	// fine-grained chunks cache is enabled (byte slices have variable size and contain many chunks) or disabled (byte slices
	// are at most 16KB each).
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
	logger log.Logger,
	userID string,
	cache chunkscache.Cache,
	chunkReaders bucketChunkReaders,
	refsIterator seriesChunkRefsSetIterator,
	refsIteratorBatchSize int,
	stats *safeQueryStats,
	minT, maxT int64,
) storepb.SeriesSet {
	var iterator seriesChunksSetIterator
	iterator = newLoadingSeriesChunksSetIterator(ctx, logger, userID, cache, chunkReaders, refsIterator, refsIteratorBatchSize, stats, minT, maxT)
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
	ctx           context.Context
	logger        log.Logger
	userID        string
	cache         chunkscache.Cache
	chunkReaders  bucketChunkReaders
	from          seriesChunkRefsSetIterator
	fromBatchSize int
	stats         *safeQueryStats

	current          seriesChunksSet
	err              error
	minTime, maxTime int64
}

func newLoadingSeriesChunksSetIterator(
	ctx context.Context,
	logger log.Logger,
	userID string,
	cache chunkscache.Cache,
	chunkReaders bucketChunkReaders,
	from seriesChunkRefsSetIterator,
	fromBatchSize int,
	stats *safeQueryStats,
	minT int64,
	maxT int64,
) *loadingSeriesChunksSetIterator {
	return &loadingSeriesChunksSetIterator{
		ctx:           ctx,
		logger:        logger,
		userID:        userID,
		cache:         cache,
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

	defer func(startTime time.Time) {
		spanLog := spanlogger.FromContext(c.ctx, c.logger)
		level.Debug(spanLog).Log(
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

	var cachedRanges map[chunkscache.Range][]byte
	if c.cache != nil {
		cachedRanges = c.cache.FetchMultiChunks(c.ctx, c.userID, toCacheKeys(nextUnloaded.series), chunksPool)
		c.recordCachedChunks(cachedRanges)
	}
	c.chunkReaders.reset()

	for sIdx, s := range nextUnloaded.series {
		nextSet.series[sIdx].lset = s.lset
		nextSet.series[sIdx].chks = nextSet.newSeriesAggrChunkSlice(s.numChunks())
		seriesChunkIdx := 0

		for _, chunksRange := range s.chunksRanges {
			rangeChunks := nextSet.series[sIdx].chks[seriesChunkIdx : seriesChunkIdx+len(chunksRange.refs)]
			initializeChunks(chunksRange, rangeChunks)
			if cachedRange, ok := cachedRanges[toCacheKey(chunksRange)]; ok {
				err := parseChunksRange(cachedRange, rangeChunks)
				if err == nil {
					seriesChunkIdx += len(chunksRange.refs)
					continue
				}
				// we couldn't parse the chunk range form the cache, so we will fetch its chunks from the bucket.
				level.Warn(c.logger).Log("msg", "parsing cache chunks", "err", err)
			}

			for _, chunk := range chunksRange.refs {
				if c.cache == nil && (chunk.minTime > c.maxTime || chunk.maxTime < c.minTime) {
					// If the cache is not set, then we don't need to overfetch chunks that we know are outside minT/maxT.
					// If the cache is set, then we need to do that, so we can cache the complete chunks ranges; they will be filtered out after fetching.
					seriesChunkIdx++
					continue
				}
				err := c.chunkReaders.addLoad(chunksRange.blockID, chunkRef(chunksRange.segmentFile, chunk.segFileOffset), sIdx, seriesChunkIdx, chunk.length)
				if err != nil {
					c.err = errors.Wrap(err, "preloading chunks")
					return false
				}
				seriesChunkIdx++
			}
		}
	}

	err := c.chunkReaders.load(nextSet.series, chunksPool, c.stats)
	if err != nil {
		c.err = errors.Wrap(err, "loading chunks")
		return false
	}
	if c.cache != nil {
		c.storeRangesInCache(nextUnloaded.series, nextSet.series, cachedRanges)
	}
	c.recordProcessedChunks(nextSet.series)

	// We might have over-fetched some chunks that were outside minT/maxT because we fetch a whole
	// range of chunks. After storing the chunks in the cache, we should throw away the chunks that are outside
	// the requested time range.
	for sIdx := range nextSet.series {
		nextSet.series[sIdx].chks = removeChunksOutsideRange(nextSet.series[sIdx].chks, c.minTime, c.maxTime)
	}
	c.recordReturnedChunks(nextSet.series)

	nextSet.chunksReleaser = chunksPool
	c.current = nextSet
	return true
}

func initializeChunks(chunksRange seriesChunkRefsRange, chunks []storepb.AggrChunk) {
	for cIdx := range chunks {
		chunks[cIdx].MinTime = chunksRange.refs[cIdx].minTime
		chunks[cIdx].MaxTime = chunksRange.refs[cIdx].maxTime
		if chunks[cIdx].Raw == nil {
			chunks[cIdx].Raw = &storepb.Chunk{}
		}
	}
}

func toCacheKeys(series []seriesChunkRefs) []chunkscache.Range {
	totalRanges := 0
	for _, s := range series {
		totalRanges += len(s.chunksRanges)
	}
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

func parseChunksRange(rBytes []byte, chunks []storepb.AggrChunk) error {
	for i := range chunks {
		// ┌───────────────┬───────────────────┬──────────────┐
		// │ len <uvarint> │ encoding <1 byte> │ data <bytes> │
		// └───────────────┴───────────────────┴──────────────┘
		chunkDataLen, n := varint.Uvarint(rBytes)
		if n == 0 {
			return fmt.Errorf("not enough bytes (%d) to read length of chunk %d/%d", len(rBytes), i, len(chunks))
		}
		if n < 0 {
			return fmt.Errorf("chunk length doesn't fit into uint64 %d/%d", i, len(chunks))
		}
		totalChunkLen := n + 1 + int(chunkDataLen)
		// The length was estimated, but at this point we know the exact length of the chunk, so we can set it.
		if totalChunkLen > len(rBytes) {
			return fmt.Errorf("malformed cached chunk range")
		}
		encodingByte := rBytes[n]
		enc, ok := convertChunkEncoding(encodingByte)
		if !ok {
			return fmt.Errorf("unknown chunk encoding (%d)", encodingByte)
		}
		chunks[i].Raw.Type = enc
		chunks[i].Raw.Data = rBytes[n+1 : totalChunkLen]
		rBytes = rBytes[totalChunkLen:]
	}
	return nil
}

func convertChunkEncoding(storageEncoding byte) (storepb.Chunk_Encoding, bool) {
	converted := storepb.Chunk_Encoding(storageEncoding)
	_, exists := storepb.Chunk_Encoding_name[int32(converted)]
	return converted, exists
}

func (c *loadingSeriesChunksSetIterator) recordCachedChunks(cachedRanges map[chunkscache.Range][]byte) {
	fetchedChunks := 0
	fetchedBytes := 0
	for k, b := range cachedRanges {
		fetchedChunks += k.NumChunks
		fetchedBytes += len(b)
	}

	c.stats.update(func(stats *queryStats) {
		stats.chunksFetched += fetchedChunks
		stats.chunksFetchedSizeSum += fetchedBytes
	})
}

func removeChunksOutsideRange(chks []storepb.AggrChunk, minT, maxT int64) []storepb.AggrChunk {
	writeIdx := 0
	for i, chk := range chks {
		if chk.MaxTime < minT || chk.MinTime > maxT {
			continue
		}
		if writeIdx != i {
			chks[writeIdx] = chks[i]
		}
		writeIdx++
	}

	return chks[:writeIdx]
}

func (c *loadingSeriesChunksSetIterator) At() seriesChunksSet {
	return c.current
}

func (c *loadingSeriesChunksSetIterator) Err() error {
	return c.err
}

func encodeChunksForCache(chunks []storepb.AggrChunk) []byte {
	encodedSize := 0
	for _, chk := range chunks {
		dataLen := len(chk.Raw.Data)
		encodedSize += varint.UvarintSize(uint64(dataLen)) + 1 + dataLen
	}
	encoded := make([]byte, 0, encodedSize)
	for _, chk := range chunks {
		encoded = binary.AppendUvarint(encoded, uint64(len(chk.Raw.Data)))
		// The cast to byte() below is safe because the actual type of the chunk in the TSDB is a single byte,
		// so the type in our protos shouldn't take more than 1 byte.
		encoded = append(encoded, byte(chk.Raw.Type))
		encoded = append(encoded, chk.Raw.Data...)
	}
	return encoded
}

func (c *loadingSeriesChunksSetIterator) storeRangesInCache(seriesRefs []seriesChunkRefs, seriesChunks []seriesChunks, cacheHits map[chunkscache.Range][]byte) {
	// Count the number of ranges that were not previously cached, and so we need to store to the cache.
	cacheMisses := 0
	for _, s := range seriesRefs {
		for _, chunksRange := range s.chunksRanges {
			if _, ok := cacheHits[toCacheKey(chunksRange)]; !ok {
				cacheMisses++
			}
		}
	}

	toStore := make(map[chunkscache.Range][]byte, cacheMisses)
	for sIdx, s := range seriesRefs {
		seriesChunkIdx := 0
		for _, chunksRange := range s.chunksRanges {
			cacheKey := toCacheKey(chunksRange)
			if _, ok := cacheHits[cacheKey]; ok {
				seriesChunkIdx += len(chunksRange.refs)
				continue
			}
			rangeChunks := seriesChunks[sIdx].chks[seriesChunkIdx : seriesChunkIdx+len(chunksRange.refs)]
			toStore[cacheKey] = encodeChunksForCache(rangeChunks)

			seriesChunkIdx += len(chunksRange.refs)
		}
	}
	c.cache.StoreChunks(c.userID, toStore)
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
