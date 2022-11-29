// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

type bucketBatchSet struct {
	ctx      context.Context
	postings []storage.SeriesRef

	batchSize               int
	currBatchPostingsOffset int
	currentBatch            seriesChunkRefsSet
	err                     error

	blockID          ulid.ULID
	indexr           *bucketIndexReader      // Index reader for block.
	matchers         []*labels.Matcher       // Series matchers.
	shard            *sharding.ShardSelector // Shard selector.
	seriesHasher     seriesHasher            // Block-specific series hash cache (used only if shard selector is specified).
	chunksLimiter    ChunksLimiter           // Rate limiter for loading chunks.
	seriesLimiter    SeriesLimiter           // Rate limiter for loading series.
	skipChunks       bool                    // If true chunks are not loaded and minTime/maxTime are ignored.
	minTime, maxTime int64                   // Series must have data in this time range to be returned (ignored if skipChunks=true).
	loadAggregates   []storepb.Aggr          // List of aggregates to load when loading chunks.
	stats            *safeQueryStats
	logger           log.Logger
}

func unloadedBucketBatches(
	ctx context.Context,
	batchSize int,
	indexr *bucketIndexReader, // Index reader for block.
	blockID ulid.ULID,
	matchers []*labels.Matcher, // Series matchers.
	shard *sharding.ShardSelector, // Shard selector.
	seriesHashCache *hashcache.BlockSeriesHashCache, // Block-specific series hash cache (used only if shard selector is specified).
	chunksLimiter ChunksLimiter, // Rate limiter for loading chunks.
	seriesLimiter SeriesLimiter, // Rate limiter for loading series.
	skipChunks bool, // If true chunks are not loaded and minTime/maxTime are ignored.
	minTime, maxTime int64, // Series must have data in this time range to be returned (ignored if skipChunks=true).
	loadAggregates []storepb.Aggr, // List of aggregates to load when loading chunks.
	stats *safeQueryStats,
	logger log.Logger,
) (seriesChunkRefsSetIterator, error) {
	if batchSize <= 0 {
		return nil, errors.New("batch size must be a positive number")
	}

	ps, err := indexr.ExpandedPostings(ctx, matchers, stats)
	if err != nil {
		return nil, errors.Wrap(err, "expanded matching posting")
	}

	// We can't compute the series hash yet because we're still missing the series labels.
	// However, if the hash is already in the cache, then we can remove all postings for series
	// not belonging to the shard.
	if shard != nil {
		var unsafeStats queryStats
		ps, unsafeStats = filterPostingsByCachedShardHash(ps, shard, seriesHashCache)
		stats.merge(&unsafeStats)
	}

	return &bucketBatchSet{
		blockID:                 blockID,
		batchSize:               batchSize,
		currBatchPostingsOffset: -batchSize,
		ctx:                     ctx,
		postings:                ps,
		indexr:                  indexr,
		matchers:                matchers,
		shard:                   shard,
		seriesHasher:            cachedSeriesHasher{cache: seriesHashCache, stats: stats},
		chunksLimiter:           chunksLimiter,
		seriesLimiter:           seriesLimiter,
		skipChunks:              skipChunks,
		minTime:                 minTime,
		maxTime:                 maxTime,
		loadAggregates:          loadAggregates,
		stats:                   stats,
		logger:                  logger,
	}, nil
}

func (s *bucketBatchSet) Next() bool {
	if s.currBatchPostingsOffset >= len(s.postings)-1 || s.err != nil {
		return false
	}
	return s.loadBatch()
}

func (s *bucketBatchSet) loadBatch() bool {
	s.currBatchPostingsOffset += s.batchSize
	if s.currBatchPostingsOffset > len(s.postings) {
		return false
	}

	end := s.currBatchPostingsOffset + s.batchSize
	if end > len(s.postings) {
		end = len(s.postings)
	}
	s.currentBatch = newSeriesChunkRefsSet(s.batchSize)
	nextPostings := s.postings[s.currBatchPostingsOffset:end]

	loadedSeries, err := s.indexr.preloadSeries(s.ctx, nextPostings, s.stats)
	if err != nil {
		s.err = errors.Wrap(err, "preload series")
		return false
	}

	var (
		symbolizedLset []symbolizedLabel
		chks           []chunks.Meta
	)

	// Track the series loading statistics in a not synchronized data structure to avoid locking for each series
	// and then merge before returning from the function.
	loadStats := &queryStats{}
	defer s.stats.merge(loadStats)

	for _, id := range nextPostings {
		ok, err := loadedSeries.unsafeLoadSeriesForTime(id, &symbolizedLset, &chks, s.skipChunks, s.minTime, s.maxTime, loadStats)
		if err != nil {
			s.err = errors.Wrap(err, "read series")
			return false
		}
		if !ok {
			// No matching chunks for this time duration, skip series
			continue
		}

		lset, err := s.indexr.LookupLabelsSymbols(symbolizedLset)
		if err != nil {
			s.err = errors.Wrap(err, "lookup labels symbols")
			return false
		}

		if !shardOwned(s.shard, s.seriesHasher, id, lset) {
			continue
		}

		// Check series limit after filtering out series not belonging to the requested shard (if any).
		if err := s.seriesLimiter.Reserve(1); err != nil {
			s.err = errors.Wrap(err, "exceeded series limit")
			return false
		}

		entry := seriesChunkRefs{lset: lset}

		if !s.skipChunks {
			// Ensure sample limit through chunksLimiter if we return chunks.
			if err = s.chunksLimiter.Reserve(uint64(len(chks))); err != nil {
				s.err = errors.Wrap(err, "exceeded chunks limit")
				return false
			}
			entry.chunks = metasToChunks(s.blockID, chks)
		}

		s.currentBatch.series = append(s.currentBatch.series, entry)
	}

	if s.currentBatch.len() == 0 {
		return s.loadBatch() // we didn't find any suitable series in this batch, try with the next one
	}

	return true
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

func (s *bucketBatchSet) At() seriesChunkRefsSet {
	return s.currentBatch
}

func (s *bucketBatchSet) Err() error {
	return s.err
}

type seriesHasher interface {
	Hash(seriesID storage.SeriesRef, lset labels.Labels) uint64
}

type cachedSeriesHasher struct {
	cache *hashcache.BlockSeriesHashCache
	stats *safeQueryStats
}

func (b cachedSeriesHasher) Hash(id storage.SeriesRef, lset labels.Labels) uint64 {
	hash, ok := b.cache.Fetch(id)
	b.stats.update(func(stats *queryStats) {
		stats.seriesHashCacheRequests++
	})

	if !ok {
		hash = lset.Hash()
		b.cache.Store(id, hash)
	} else {
		b.stats.update(func(stats *queryStats) {
			stats.seriesHashCacheHits++
		})
	}
	return hash
}

func shardOwned(shard *sharding.ShardSelector, hasher seriesHasher, id storage.SeriesRef, lset labels.Labels) bool {
	if shard == nil {
		return true
	}
	hash := hasher.Hash(id, lset)

	return hash%shard.ShardCount == shard.ShardIndex
}

func (s *BucketStore) batchSetsForBlocks(ctx context.Context, req *storepb.SeriesRequest, blocks []*bucketBlock, indexReaders map[ulid.ULID]*bucketIndexReader, chunkReaders *chunkReaders, shardSelector *sharding.ShardSelector, matchers []*labels.Matcher, chunksLimiter ChunksLimiter, seriesLimiter SeriesLimiter, stats *safeQueryStats) (storepb.SeriesSet, *hintspb.SeriesResponseHints, func(), error) {
	resHints := &hintspb.SeriesResponseHints{}
	mtx := sync.Mutex{}
	batches := make([]seriesChunkRefsSetIterator, 0, len(blocks))
	g, ctx := errgroup.WithContext(ctx)
	cleanups := make([]func(), 0, len(blocks))

	for _, b := range blocks {
		b := b

		// Keep track of queried blocks.
		resHints.AddQueriedBlock(b.meta.ULID)
		indexr := indexReaders[b.meta.ULID]

		// If query sharding is enabled we have to get the block-specific series hash cache
		// which is used by blockSeries().
		var blockSeriesHashCache *hashcache.BlockSeriesHashCache
		if shardSelector != nil {
			blockSeriesHashCache = s.seriesHashCache.GetBlockCache(b.meta.ULID.String())
		}
		g.Go(func() error {
			var (
				part seriesChunkRefsSetIterator
				err  error
			)

			part, err = unloadedBucketBatches(
				ctx, s.maxSeriesPerBatch, indexr, b.meta.ULID, matchers, shardSelector, blockSeriesHashCache, chunksLimiter, seriesLimiter, req.SkipChunks, req.MinTime, req.MaxTime, req.Aggregates, stats, s.logger)
			if err != nil {
				return errors.Wrapf(err, "fetch series for block %s", b.meta.ULID)
			}

			mtx.Lock()
			batches = append(batches, part)
			mtx.Unlock()

			return nil
		})
	}

	cleanup := func() {
		for _, c := range cleanups {
			c()
		}
	}

	begin := time.Now()
	err := g.Wait()
	if err != nil {
		return nil, nil, cleanup, err
	}

	getAllDuration := time.Since(begin)
	stats.update(func(stats *queryStats) {
		stats.blocksQueried = len(batches)
		stats.getAllDuration = getAllDuration
	})
	s.metrics.seriesGetAllDuration.Observe(getAllDuration.Seconds())
	s.metrics.seriesBlocksQueried.Observe(float64(len(batches)))

	mergedBatches := mergedBatchSets(s.maxSeriesPerBatch, batches...)
	var set storepb.SeriesSet
	if chunkReaders != nil {
		set = newSeriesSetWithChunks(ctx, *chunkReaders, mergedBatches, stats)
	} else {
		set = newSeriesSetWithoutChunks(mergedBatches)
	}
	return set, resHints, cleanup, nil
}

type seriesSetWithoutChunks struct {
	from seriesChunkRefsSetIterator

	currentIterator *seriesChunkRefsIterator
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
	if !s.from.Next() {
		return false
	}

	next := s.from.At()
	s.currentIterator.reset(next)
	return !s.currentIterator.Done()
}

func (s *seriesSetWithoutChunks) At() (labels.Labels, []storepb.AggrChunk) {
	return s.currentIterator.At().lset, nil
}

func (s *seriesSetWithoutChunks) Err() error {
	return s.from.Err()
}

func newSeriesSetWithChunks(ctx context.Context, chunkReaders chunkReaders, batches seriesChunkRefsSetIterator, stats *safeQueryStats) storepb.SeriesSet {
	return &batchedSeriesSet{
		from:  newPreloadingBatchSet(ctx, 1, newLoadingBatchSet(chunkReaders, batches, stats)),
		stats: stats,
	}
}

type loadingBatchSet struct {
	chunkReaders chunkReaders
	from         seriesChunkRefsSetIterator
	stats        *safeQueryStats

	current seriesChunksSet
	err     error
}

func newLoadingBatchSet(chunkReaders chunkReaders, from seriesChunkRefsSetIterator, stats *safeQueryStats) *loadingBatchSet {
	return &loadingBatchSet{
		chunkReaders: chunkReaders,
		from:         from,
		stats:        stats,
	}
}

func (c *loadingBatchSet) Next() bool {
	if c.err != nil {
		return false
	}

	if !c.from.Next() {
		c.err = c.from.Err()
		return false
	}

	nextUnloaded := c.from.At()
	entries := make([]seriesEntry, nextUnloaded.len())
	c.chunkReaders.reset()
	for i, s := range nextUnloaded.series {
		entries[i].lset = s.lset
		entries[i].chks = make([]storepb.AggrChunk, len(s.chunks))

		for j, chunk := range s.chunks {
			entries[i].chks[j].MinTime = chunk.minTime
			entries[i].chks[j].MaxTime = chunk.maxTime

			err := c.chunkReaders.addLoad(chunk.blockID, chunk.ref, i, j)
			if err != nil {
				c.err = errors.Wrap(err, "preloading chunks")
				return false
			}
		}
	}

	err := c.chunkReaders.load(entries, c.stats)
	if err != nil {
		c.err = errors.Wrap(err, "loading chunks")
		return false
	}
	nextLoaded := seriesChunksSet{
		series:        entries,
		bytesReleaser: c.chunkReaders.chunkBytesReleaser,
	}
	c.current = nextLoaded
	return true
}

func (c *loadingBatchSet) At() seriesChunksSet {
	return c.current
}

func (c *loadingBatchSet) Err() error {
	return c.err
}

// preloadedBatch holds the result of preloading the next batch. It can either contain
// the preloaded batch or an error, but not both.
type preloadedBatch struct {
	batch seriesChunksSet
	err   error
}

type preloadingBatchSet struct {
	ctx     context.Context
	from    seriesChunksSetIterator
	current seriesChunksSet

	preloaded chan preloadedBatch
	err       error
}

func newPreloadingBatchSet(ctx context.Context, preloadNumberOfBatches int, from seriesChunksSetIterator) *preloadingBatchSet {
	preloadedSet := &preloadingBatchSet{
		ctx:       ctx,
		from:      from,
		preloaded: make(chan preloadedBatch, preloadNumberOfBatches-1), // one will be kept outside the channel when the channel blocks
	}
	go preloadedSet.preload()
	return preloadedSet
}

func (p *preloadingBatchSet) preload() {
	defer close(p.preloaded)

	for p.from.Next() {
		select {
		case <-p.ctx.Done():
			// If the context is done, we should just stop the preloading goroutine.
			return
		case p.preloaded <- preloadedBatch{batch: p.from.At()}:
		}
	}

	if p.from.Err() != nil {
		p.preloaded <- preloadedBatch{err: p.from.Err()}
	}
}

func (p *preloadingBatchSet) Next() bool {
	// TODO dimitarvdimitrov instrument the time we wait here

	preloaded, ok := <-p.preloaded
	if !ok {
		// Iteration reached the end or context has been canceled.
		return false
	}

	p.current = preloaded.batch
	p.err = preloaded.err

	return p.err == nil
}

func (p *preloadingBatchSet) At() seriesChunksSet {
	return p.current
}

func (p *preloadingBatchSet) Err() error {
	return p.err
}

type emptyBatchSet struct {
}

func (emptyBatchSet) Next() bool             { return false }
func (emptyBatchSet) At() seriesChunkRefsSet { return seriesChunkRefsSet{} }
func (emptyBatchSet) Err() error             { return nil }

func mergedBatchSets(mergedSize int, all ...seriesChunkRefsSetIterator) seriesChunkRefsSetIterator {
	switch len(all) {
	case 0:
		return emptyBatchSet{}
	case 1:
		return newDeduplicatingBatchSet(mergedSize, all[0])
	}
	h := len(all) / 2

	return newMergedBatchSet(
		mergedSize,
		mergedBatchSets(mergedSize, all[:h]...),
		mergedBatchSets(mergedSize, all[h:]...),
	)
}

type mergedBatchSet struct {
	batchSize int

	a, b     seriesChunkRefsSetIterator
	aAt, bAt *seriesChunkRefsIterator
	current  seriesChunkRefsSet
}

func newMergedBatchSet(mergedBatchSize int, a, b seriesChunkRefsSetIterator) *mergedBatchSet {
	return &mergedBatchSet{
		batchSize: mergedBatchSize,
		a:         a,
		b:         b,
		// start iterator on an empty batch. It will be reset with a non-empty batch next time Next() is called
		aAt: newSeriesChunkRefsIterator(seriesChunkRefsSet{}),
		bAt: newSeriesChunkRefsIterator(seriesChunkRefsSet{}),
	}
}

func (s *mergedBatchSet) Err() error {
	if err := s.a.Err(); err != nil {
		return err
	} else if err = s.b.Err(); err != nil {
		return err
	}
	return nil
}

func (s *mergedBatchSet) Next() bool {
	next := newSeriesChunkRefsSet(s.batchSize)
	for i := 0; i < next.len(); i++ {
		if s.aAt.Done() {
			if s.a.Next() {
				s.aAt.reset(s.a.At())
			} else if s.a.Err() != nil {
				// Stop iterating on first error encountered.
				return false
			}
		}
		if s.bAt.Done() {
			if s.b.Next() {
				s.bAt.reset(s.b.At())
			} else if s.b.Err() != nil {
				// Stop iterating on first error encountered.
				return false
			}
		}
		nextSeries, ok := nextUniqueEntry(s.aAt, s.bAt)
		if !ok {
			break
		}
		next.series = append(next.series, nextSeries)
	}

	s.current = next
	return s.current.len() > 0
}

// nextUniqueEntry returns the next unique entry from both a and b. If a.At() and b.At() have the same
// label set, nextUniqueEntry merges their chunks. The merged chunks are sorted by their MinTime and then by MaxTIme.
func nextUniqueEntry(a, b *seriesChunkRefsIterator) (toReturn seriesChunkRefs, _ bool) {
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

func (s *mergedBatchSet) At() seriesChunkRefsSet {
	return s.current
}

// deduplicatingBatchSet implements seriesChunkRefsSetIterator, and merges together consecutive
// series in an underlying seriesChunkRefsSetIterator.
type deduplicatingBatchSet struct {
	batchSize int

	from    *chainedBatchSetIterator
	peek    *seriesChunkRefs
	current seriesChunkRefsSet
}

func newDeduplicatingBatchSet(batchSize int, wrapped seriesChunkRefsSetIterator) seriesChunkRefsSetIterator {
	return &deduplicatingBatchSet{
		batchSize: batchSize,
		from:      newChainedSeriesSet(wrapped),
	}
}

func (s *deduplicatingBatchSet) Err() error {
	return s.from.Err()
}

func (s *deduplicatingBatchSet) At() seriesChunkRefsSet {
	return s.current
}

func (s *deduplicatingBatchSet) Next() bool {
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
	nextBatch := newSeriesChunkRefsSet(s.batchSize)
	nextBatch.series = append(nextBatch.series, firstSeries)

	var nextSeries seriesChunkRefs
	for i := 0; i < s.batchSize; {
		if !s.from.Next() {
			break
		}
		nextSeries = s.from.At()

		if labels.Equal(nextBatch.series[i].lset, nextSeries.lset) {
			nextBatch.series[i].chunks = append(nextBatch.series[i].chunks, nextSeries.chunks...)
		} else {
			i++
			if i >= s.batchSize {
				s.peek = &nextSeries
				break
			}
			nextBatch.series = append(nextBatch.series, nextSeries)
		}
	}
	s.current = nextBatch
	return true
}

type chainedBatchSetIterator struct {
	from     seriesChunkRefsSetIterator
	iterator *seriesChunkRefsIterator
}

func newChainedSeriesSet(from seriesChunkRefsSetIterator) *chainedBatchSetIterator {
	return &chainedBatchSetIterator{
		from:     from,
		iterator: newSeriesChunkRefsIterator(seriesChunkRefsSet{}), // start with an empty batch and initialize on the first call to Next()
	}
}

func (c chainedBatchSetIterator) Next() bool {
	if c.iterator.Next() {
		return true
	}
	if !c.from.Next() {
		return false
	}
	c.iterator.reset(c.from.At())
	return true
}

func (c chainedBatchSetIterator) At() seriesChunkRefs {
	return c.iterator.At()
}

func (c chainedBatchSetIterator) Err() error {
	return c.from.Err()
}

type batchedSeriesSet struct {
	from  seriesChunksSetIterator
	stats *safeQueryStats

	current         seriesChunksSet
	offsetInCurrent int
}

func (b *batchedSeriesSet) Next() bool {
	b.offsetInCurrent++
	if b.offsetInCurrent >= b.current.len() {
		if !b.from.Next() {
			return false
		}
		b.current.release()
		b.current = b.from.At()
		b.offsetInCurrent = 0
	}
	return true
}

// At returns the current series. The result from At() MUST not be retained after calling Next()
func (b *batchedSeriesSet) At() (labels.Labels, []storepb.AggrChunk) {
	return b.current.series[b.offsetInCurrent].lset, b.current.series[b.offsetInCurrent].chks
}

func (b *batchedSeriesSet) Err() error {
	return b.from.Err()
}
