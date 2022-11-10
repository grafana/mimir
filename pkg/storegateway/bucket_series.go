package storegateway

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/types"
	"github.com/oklog/ulid"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"github.com/thanos-io/objstore/tracing"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/dskit/runutil"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

func (s *BucketStore) SeriesStreaming(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) (err error) {
	matchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	// Check if matchers include the query shard selector.
	shardSelector, matchers, err := sharding.RemoveShardFromMatchers(matchers)
	if err != nil {
		return status.Error(codes.InvalidArgument, errors.Wrap(err, "parse query sharding label").Error())
	}

	var (
		ctx                = srv.Context()
		stats              = &queryStats{}
		matchingSeriesSets []*blockSeriesSet
		mtx                sync.Mutex
		g, gctx            = errgroup.WithContext(ctx)
		resHints           = &hintspb.SeriesResponseHints{}
		reqBlockMatchers   []*labels.Matcher
		//chunksLimiter    = s.chunksLimiterFactory(s.metrics.queriesDropped.WithLabelValues("chunks"))
		seriesLimiter = s.seriesLimiterFactory(s.metrics.queriesDropped.WithLabelValues("series"))
	)

	if req.Hints != nil {
		reqHints := &hintspb.SeriesRequestHints{}
		if err := types.UnmarshalAny(req.Hints, reqHints); err != nil {
			return status.Error(codes.InvalidArgument, errors.Wrap(err, "unmarshal series request hints").Error())
		}

		reqBlockMatchers, err = storepb.MatchersToPromMatchers(reqHints.BlockMatchers...)
		if err != nil {
			return status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request hints labels matchers").Error())
		}
	}

	gspan, gctx := tracing.StartSpan(gctx, "bucket_store_preload_all")

	s.mtx.RLock()

	blocks := s.blockSet.getFor(req.MinTime, req.MaxTime, req.MaxResolutionWindow, reqBlockMatchers)

	//
	// Stage 1: find all series matching the matchers in the given blocks. For each series, fetch the chunks
	// 			references but do not fetch chunks data.
	//
	for _, b := range blocks {
		b := b

		// Keep track of queried blocks.
		resHints.AddQueriedBlock(b.meta.ULID)

		// We must keep the readers open until all their data has been sent.
		indexr := b.indexReader()

		// Defer all closes to the end of Series method.
		defer runutil.CloseWithLogOnErr(s.logger, indexr, "series block")

		// If query sharding is enabled we have to get the block-specific series hash cache
		// which is used by blockSeries().
		var blockSeriesHashCache *hashcache.BlockSeriesHashCache
		if shardSelector != nil {
			blockSeriesHashCache = s.seriesHashCache.GetBlockCache(b.meta.ULID.String())
		}

		g.Go(func() error {
			part, pstats, err := findMatchingSeriesForBlock(
				gctx,
				b.meta.ULID,
				indexr,
				matchers,
				shardSelector,
				blockSeriesHashCache,
				seriesLimiter,
				req.SkipChunks,
				req.MinTime, req.MaxTime,
				s.logger,
			)
			if err != nil {
				return errors.Wrapf(err, "fetch series for block %s", b.meta.ULID)
			}

			mtx.Lock()
			matchingSeriesSets = append(matchingSeriesSets, part)
			stats = stats.merge(pstats.export())
			mtx.Unlock()

			return nil
		})
	}

	s.mtx.RUnlock()

	// Wait until done.
	err = g.Wait()
	gspan.Finish()

	if err != nil {
		code := codes.Aborted
		if s, ok := status.FromError(errors.Cause(err)); ok {
			code = s.Code()
		}
		return status.Error(code, err.Error())
	}

	// TODO For simplicity, all metrics tracking has been temporarily removed here.

	//
	// Stage 2: progressively load the series chunks and stream it to the client.
	//

	// NOTE: We "carefully" assume series and chunks are sorted within each SeriesSet. This should be guaranteed by
	// blockSeries method. In worst case deduplication logic won't deduplicate correctly, which will be accounted later.
	allSeries := newBlockSeriesSetBatcher(mergeBlockSeriesSet(matchingSeriesSets...))

	// TODO adding a preloader with this design should be easier because it just requires to wrap allSeries
	// 		with a new struct which does the preloading. When you call Next() it enqueues the request to preload
	// 		the next next batch too.
	for allSeries.Next() {
		batch := allSeries.At()

		// TODO Load all the chunks for all the series in the batch
		// TODO Call srv.Send() for each series
	}

	//
	// Final: send response hints and statistics.
	//

	var anyHints *types.Any
	if anyHints, err = types.MarshalAny(resHints); err != nil {
		err = status.Error(codes.Unknown, errors.Wrap(err, "marshal series response hints").Error())
		return
	}

	if err = srv.Send(storepb.NewHintsSeriesResponse(anyHints)); err != nil {
		err = status.Error(codes.Unknown, errors.Wrap(err, "send series response hints").Error())
		return
	}

	if err = srv.Send(storepb.NewStatsResponse(stats.postingsFetchedSizeSum + stats.seriesFetchedSizeSum)); err != nil {
		err = status.Error(codes.Unknown, errors.Wrap(err, "sends series response stats").Error())
		return
	}

	return err
}

// blockSeries returns series matching given matchers, that have some data in given time range.
// If skipChunks is provided, then provided minTime and maxTime are ignored and search is performed over the entire
// block to make the result cacheable.
func findMatchingSeriesForBlock(
	ctx context.Context,
	blockID ulid.ULID,
	indexr *bucketIndexReader, // Index reader for block.
	matchers []*labels.Matcher, // Series matchers.
	shard *sharding.ShardSelector, // Shard selector.
	seriesHashCache *hashcache.BlockSeriesHashCache, // Block-specific series hash cache (used only if shard selector is specified).
	seriesLimiter SeriesLimiter, // Rate limiter for loading series.
	skipChunks bool, // If true, chunks are not loaded and minTime/maxTime are ignored.
	minTime, maxTime int64, // Series must have data in this time range to be returned (ignored if skipChunks=true).
	logger log.Logger,
) (*blockSeriesSet, *safeQueryStats, error) {
	span, ctx := tracing.StartSpan(ctx, "findMatchingSeriesForBlock()")
	span.LogKV(
		"block ID", indexr.block.meta.ULID.String(),
		"block min time", time.UnixMilli(indexr.block.meta.MinTime).UTC().Format(time.RFC3339Nano),
		"block max time", time.UnixMilli(indexr.block.meta.MinTime).UTC().Format(time.RFC3339Nano),
	)
	defer span.Finish()

	indexStats := newSafeQueryStats()

	//if skipChunks {
	//	span.LogKV("msg", "manipulating mint/maxt to cover the entire block as skipChunks=true")
	//	minTime, maxTime = indexr.block.meta.MinTime, indexr.block.meta.MaxTime
	//
	//	res, ok := fetchCachedSeries(ctx, indexr.block.userID, indexr.block.indexCache, indexr.block.meta.ULID, matchers, shard, logger)
	//	if ok {
	//		span.LogKV("msg", "using cached result", "len", len(res))
	//		return newBucketSeriesSet(res), indexStats, nil
	//	}
	//}

	ps, err := indexr.ExpandedPostings(ctx, matchers, indexStats)
	if err != nil {
		return nil, nil, errors.Wrap(err, "expanded matching posting")
	}

	// We can't compute the series hash yet because we're still missing the series labels.
	// However, if the hash is already in the cache, then we can remove all postings for series
	// not belonging to the shard.
	var seriesCacheStats queryStats
	if shard != nil {
		ps, seriesCacheStats = filterPostingsByCachedShardHash(ps, shard, seriesHashCache)
	}

	if len(ps) == 0 {
		return newBlockSeriesSet(blockID, nil), indexStats, nil
	}

	// Preload all series index data.
	// TODO(bwplotka): Consider not keeping all series in memory all the time.
	// TODO(bwplotka): Do lazy loading in one step as `ExpandingPostings` method.
	loadedSeries, err := indexr.preloadSeries(ctx, ps, indexStats)
	if err != nil {
		return nil, nil, errors.Wrap(err, "preload series")
	}

	// Transform all series into the response types and mark their relevant chunks
	// for preloading.
	var (
		res       []blockSeriesEntry
		lookupErr error
	)

	tracing.DoWithSpan(ctx, "findMatchingSeriesForBlock() lookup series", func(ctx context.Context, span opentracing.Span) {
		var (
			symbolizedLset []symbolizedLabel
			chks           []chunks.Meta
			postingsStats  = &queryStats{}
		)

		// Keep track of postings lookup stats in a dedicated stats structure that doesn't require lock
		// and then merge it once done. We do it to avoid the lock overhead because loadSeriesForTime()
		// may be called many times.
		defer func() {
			indexStats = indexStats.merge(postingsStats)
		}()

		for _, id := range ps {
			ok, err := loadedSeries.loadSeriesForTime(id, &symbolizedLset, &chks, skipChunks, minTime, maxTime, postingsStats)
			if err != nil {
				lookupErr = errors.Wrap(err, "read series")
				return
			}
			if !ok {
				// No matching chunks for this time duration, skip series.
				continue
			}

			lset, err := indexr.LookupLabelsSymbols(symbolizedLset)
			if err != nil {
				lookupErr = errors.Wrap(err, "lookup labels symbols")
				return
			}

			// Skip the series if it doesn't belong to the shard.
			if shard != nil {
				hash, ok := seriesHashCache.Fetch(id)
				seriesCacheStats.seriesHashCacheRequests++

				if !ok {
					hash = lset.Hash()
					seriesHashCache.Store(id, hash)
				} else {
					seriesCacheStats.seriesHashCacheHits++
				}

				if hash%shard.ShardCount != shard.ShardIndex {
					continue
				}
			}

			// Check series limit after filtering out series not belonging to the requested shard (if any).
			if err := seriesLimiter.Reserve(1); err != nil {
				lookupErr = errors.Wrap(err, "exceeded series limit")
				return
			}

			s := blockSeriesEntry{lset: lset}

			if !skipChunks {
				// Schedule loading chunks.
				s.refs = make([]chunks.ChunkRef, 0, len(chks))
				//s.chks = make([]storepb.AggrChunk, 0, len(chks))
				for /*j*/ _, meta := range chks {
					// seriesEntry s is appended to res, but not at every outer loop iteration,
					// therefore len(res) is the index we need here, not outer loop iteration number.
					//if err := chunkr.addLoad(meta.Ref, len(res), j); err != nil {
					//	lookupErr = errors.Wrap(err, "add chunk load")
					//	return
					//}
					//s.chks = append(s.chks, storepb.AggrChunk{
					//	MinTime: meta.MinTime,
					//	MaxTime: meta.MaxTime,
					//})
					s.refs = append(s.refs, meta.Ref)
				}

				// Ensure sample limit through chunksLimiter if we return chunks.
				//if err := chunksLimiter.Reserve(uint64(len(s.chks))); err != nil {
				//	lookupErr = errors.Wrap(err, "exceeded chunks limit")
				//	return
				//}
			}

			res = append(res, s)
		}
	})

	if lookupErr != nil {
		return nil, nil, lookupErr
	}

	//if skipChunks {
	//	storeCachedSeries(ctx, indexr.block.indexCache, indexr.block.userID, indexr.block.meta.ULID, matchers, shard, res, logger)
	//	return newBucketSeriesSet(res), indexStats.merge(&seriesCacheStats), nil
	//}

	//if err := chunkr.load(res, loadAggregates); err != nil {
	//	return nil, nil, errors.Wrap(err, "load chunks")
	//}

	return newBlockSeriesSet(blockID, res), indexStats.merge(&seriesCacheStats) /* removed merge(chunkr.stats) */, nil
}

// blockSeriesEntry stores the labels and references to the chunks for a given series in a specific block.
type blockSeriesEntry struct {
	lset labels.Labels
	refs []chunks.ChunkRef
}

// blockChunkRefs stores the references of chunks for a given series in a given block.
// The reference to the series is intentionally missing because it's expected to be stored by who use this struct.
type blockSeriesChunkRefs struct {
	blockID ulid.ULID
	refs    []chunks.ChunkRef
}

type blockSeriesSetInterface interface {
	Next() bool
	At() (labels.Labels, []blockSeriesChunkRefs)
	Err() error
}

// blockSeriesSet stores the series matching given label matchers in a specific block.
type blockSeriesSet struct {
	blockID ulid.ULID
	set     []blockSeriesEntry
	i       int
	err     error
}

func newBlockSeriesSet(blockID ulid.ULID, set []blockSeriesEntry) *blockSeriesSet {
	return &blockSeriesSet{
		blockID: blockID,
		set:     set,
		i:       -1,
	}
}

func (s *blockSeriesSet) Next() bool {
	if s.i >= len(s.set)-1 {
		return false
	}
	s.i++
	return true
}

func (s *blockSeriesSet) At() (labels.Labels, []blockSeriesChunkRefs) {
	return s.set[s.i].lset, []blockSeriesChunkRefs{{
		blockID: s.blockID,
		refs:    s.set[s.i].refs,
	}}
}

func (s *blockSeriesSet) Err() error {
	return s.err
}

type emptyBlockSeriesSet struct{}

func newEmptySeriesSet() *emptyBlockSeriesSet {
	return &emptyBlockSeriesSet{}
}

func (s *emptyBlockSeriesSet) Next() bool {
	return false
}

func (s *emptyBlockSeriesSet) At() (labels.Labels, []blockSeriesChunkRefs) {
	return nil, nil
}

func (s *emptyBlockSeriesSet) Err() error {
	return nil
}

func mergeBlockSeriesSet(all ...*blockSeriesSet) blockSeriesSetInterface {
	switch len(all) {
	case 0:
		return newEmptySeriesSet()
	case 1:
		return all[0]
	}
	h := len(all) / 2

	return newMergedMatchingSeriesSet(
		mergeBlockSeriesSet(all[:h]...),
		mergeBlockSeriesSet(all[h:]...),
	)
}

type mergedMatchingSeriesSet struct {
	a, b blockSeriesSetInterface

	lset         labels.Labels
	chunks       []blockSeriesChunkRefs
	adone, bdone bool
}

func newMergedMatchingSeriesSet(a, b blockSeriesSetInterface) blockSeriesSetInterface {
	s := &mergedMatchingSeriesSet{a: a, b: b}
	// Initialize first elements of both sets as Next() needs
	// one element look-ahead.
	s.adone = !s.a.Next()
	s.bdone = !s.b.Next()

	return s
}

func (s *mergedMatchingSeriesSet) At() (labels.Labels, []blockSeriesChunkRefs) {
	return s.lset, s.chunks
}

func (s *mergedMatchingSeriesSet) Err() error {
	if s.a.Err() != nil {
		return s.a.Err()
	}
	return s.b.Err()
}

func (s *mergedMatchingSeriesSet) compare() int {
	if s.adone {
		return 1
	}
	if s.bdone {
		return -1
	}
	lsetA, _ := s.a.At()
	lsetB, _ := s.b.At()
	return labels.Compare(lsetA, lsetB)
}

func (s *mergedMatchingSeriesSet) Next() bool {
	if s.adone && s.bdone || s.Err() != nil {
		return false
	}

	d := s.compare()
	if d > 0 {
		s.lset, s.chunks = s.b.At()
		s.bdone = !s.b.Next()
		return true
	}
	if d < 0 {
		s.lset, s.chunks = s.a.At()
		s.adone = !s.a.Next()
		return true
	}

	// Both a and b contains the same series. Merge all chunks refs together, while preserving the reference to their
	// respective block (required to fetch chunks later on).
	lset, chksA := s.a.At()
	_, chksB := s.b.At()
	s.lset = lset

	// Slice reuse is not generally safe with nested merge iterators.
	// We err on the safe side an create a new slice.
	s.chunks = make([]blockSeriesChunkRefs, 0, len(chksA)+len(chksB))
	s.chunks = append(s.chunks, chksA...)
	s.chunks = append(s.chunks, chksB...)

	s.adone = !s.a.Next()
	s.bdone = !s.b.Next()
	return true
}

type blockSeriesSetBatch struct {
}

type blockSeriesSetBatcher struct {
	allSeries blockSeriesSetInterface
}

func newBlockSeriesSetBatcher(allSeries blockSeriesSetInterface) *blockSeriesSetBatcher {
	return &blockSeriesSetBatcher{
		allSeries: allSeries,
	}
}

func (b *blockSeriesSetBatcher) Next() bool {
	// TODO read some series from b.AllSeries up to the batch limit (e.g. number of series now, later based on memory estimation)
	return false
}

func (b *blockSeriesSetBatcher) At() *blockSeriesSetBatch {
	// TODO return the current batch
	return nil
}
