// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/storage/indexheader"
	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/indexcache"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// startCount waits for a slot on the count gate and returns the function that frees it.
// A store-gateway too busy to start in time answers Unavailable, so the querier asks another.
func (s *BucketStore) startCount(ctx context.Context) (func(), error) {
	if err := s.countGate.Start(ctx); err != nil {
		if errors.Is(err, errGateTimeout) {
			return nil, status.Error(codes.Unavailable, "too many cardinality estimate count requests running: "+err.Error())
		}
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return s.countGate.Done, nil
}

// loadCountIndexHeaders loads the index-header of each block and reports which failed to load.
// A failed block is left out of the answered block IDs so the querier asks another store-gateway.
// The load doesn't pin the index-header, so it may be loaded again when the block is read.
func (s *BucketStore) loadCountIndexHeaders(ctx context.Context, blocks []*bucketBlock, stats *safeQueryStats) ([]bool, error) {
	failed := make([]bool, len(blocks))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.countBlockConcurrency)
	for i, b := range blocks {
		g.Go(func() error {
			// Concurrent requests waiting on the same load each count it.
			loaded := indexHeaderLoaded(b.indexHeaderReader)
			start := time.Now()
			err := b.loadIndexHeader(gctx, stats)
			if !loaded && err == nil {
				s.metrics.cardinalityEstimateIndexHeaderLoads.Inc()
				s.metrics.cardinalityEstimateIndexHeaderLoadDuration.Observe(time.Since(start).Seconds())
			}
			if err != nil {
				if gctx.Err() != nil {
					return status.FromContextError(gctx.Err()).Err()
				}
				level.Warn(spanlogger.FromContext(gctx, b.logger)).Log("msg", "index-header did not load, block left uncounted for cardinality estimate", "err", err)
				failed[i] = true
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return failed, nil
}

// indexHeaderLoaded reports whether r is loaded; a reader that isn't lazy always is.
func indexHeaderLoaded(r indexheader.Reader) bool {
	if lazy, ok := r.(*indexheader.LazyBinaryReader); ok {
		return lazy.LoadedLastUse() != 0
	}
	return true
}

// recordCountFetchedBytes adds what a count request fetched to the count metrics.
func (s *BucketStore) recordCountFetchedBytes(postings, series, offsetsTable int64) {
	for dataType, n := range map[string]int64{"postings": postings, "series": series, "offsets_table": offsetsTable} {
		s.metrics.cardinalityEstimateFetchedBytes.WithLabelValues(dataType).Add(float64(n))
	}
}

// seriesCountsState accumulates one SeriesCounts request and prices what it reads and holds against its limits.
type seriesCountsState struct {
	// matchers is empty when every series is wanted.
	matchers []*labels.Matcher
	// opts is what is counted, as the counter validated it.
	opts    cardinality.CounterOptions
	counter *cardinality.SeriesCounter
	// ranges is scratch for one series' chunk ranges.
	ranges []cardinality.ChunkRange
	// maxIndexBytes caps postings, series and offsets table bytes read, 0 for none.
	maxIndexBytes int64
	// tableBytes is the group-by label's postings offsets table read so far, priced from the index-header's samples.
	tableBytes atomic.Int64
	// walkedTableBytes is the part of tableBytes whose walk finished, for metrics.
	walkedTableBytes atomic.Int64
	// groupBytes is the memory of the per-bucket counts held with step_ms, bounded by max_index_bytes.
	groupBytes atomic.Int64
	// maxHashes caps the distinct hashes held, 0 for none.
	maxHashes int64
	// shard is the request's query shard, nil for none.
	shard *sharding.ShardSelector
	// matchersKey is matchers' key in the expanded postings cache.
	matchersKey indexcache.LabelMatchersKey
	// stats is what the request read.
	stats *safeQueryStats
}

func newSeriesCountsState(req *storegatewaypb.SeriesCountsRequest) (*seriesCountsState, error) {
	reqMatchers := make([]storepb.LabelMatcher, 0, len(req.Matchers))
	for _, m := range req.Matchers {
		reqMatchers = append(reqMatchers, *m)
	}
	matchers, err := storepb.MatchersToPromMatchers(reqMatchers...)
	if err != nil {
		return nil, err
	}
	opts := cardinality.CounterOptions{
		MinT:        req.MinTime,
		MaxT:        req.MaxTime,
		Step:        req.StepMs,
		Window:      req.WindowMs,
		GroupBy:     req.GroupBy,
		Hashes:      req.Hashes,
		SketchAbove: req.SketchAbove,
	}
	counter, err := cardinality.NewSeriesCounter(opts)
	if err != nil {
		return nil, err
	}
	opts.GroupBy = counter.GroupBy()
	c := &seriesCountsState{
		matchers:      matchers,
		opts:          opts,
		counter:       counter,
		maxIndexBytes: req.MaxIndexBytes,
		maxHashes:     req.MaxHashes,
	}
	if c.maxIndexBytes < 0 {
		return nil, errors.New("max_index_bytes can't be negative")
	}
	if c.maxHashes < 0 {
		return nil, errors.New("max_hashes can't be negative")
	}
	if req.MaxGroups < 0 {
		return nil, errors.New("max_groups can't be negative")
	}
	return c, nil
}

// indexBytes is the index read so far, as the response's index_bytes.
func indexBytes(stats *safeQueryStats) int64 {
	st := stats.export()
	return int64(st.postingsTouchedSizeSum + st.seriesProcessedSizeSum)
}

// limitedBytes is what max_index_bytes bounds.
func (c *seriesCountsState) limitedBytes(stats *safeQueryStats) int64 {
	return indexBytes(stats) + c.tableBytes.Load() + c.groupBytes.Load()
}

// groupCountsBytes is the memory of the per-bucket counts of n groups, charged only with step_ms.
func (c *seriesCountsState) groupCountsBytes(n int64) int64 {
	if c.counter.Buckets() <= 1 {
		return 0
	}
	return n * int64(c.counter.Buckets()) * 8
}

// checkTable refuses before a block's postings offsets table is walked if it would pass max_index_bytes.
func (c *seriesCountsState) checkTable(read, table int64) error {
	if c.maxIndexBytes == 0 || read+table <= c.maxIndexBytes {
		return nil
	}
	return cardinality.Refusalf(cardinality.LimitMaxIndexBytes, "the request would read about %d index bytes (%d of postings, then %d of the %s postings offsets table), more than max_index_bytes %d",
		read+table, read, table, c.opts.GroupBy, c.maxIndexBytes)
}

// countPostingsPlansMaxBytes caps the posting groups one request keeps between pricing and fetching.
var countPostingsPlansMaxBytes int64 = 64 << 20

// checkPostings refuses before any postings list is fetched if their priced size would pass max_index_bytes.
func (c *seriesCountsState) checkPostings(read, postings, blocks int64) error {
	if c.maxIndexBytes == 0 || read+postings <= c.maxIndexBytes {
		return nil
	}
	lists := "postings lists its matchers select"
	if len(c.matchers) == 0 {
		lists = "all-postings lists"
	}
	return cardinality.Refusalf(cardinality.LimitMaxIndexBytes, "the request would read about %d index bytes (%d already read, then %d of %s over %d blocks, priced from the postings offsets table), more than max_index_bytes %d",
		read+postings, read, postings, lists, blocks, c.maxIndexBytes)
}

// checkPlan refuses before any series entry is read if the planned read would pass max_index_bytes.
func (c *seriesCountsState) checkPlan(read int64, series int, seriesBytes, postings, counts int64) error {
	if c.maxIndexBytes == 0 {
		return nil
	}
	estimate := read + seriesBytes + postings + counts
	if estimate <= c.maxIndexBytes {
		return nil
	}
	avg := int64(0)
	if series > 0 {
		avg = seriesBytes / int64(series)
	}
	return cardinality.Refusalf(cardinality.LimitMaxIndexBytes, "the request would read about %d index bytes (%d of postings and postings offsets table read, then %d series at an estimated %d bytes each and %d bytes of postings, holding %d bytes of counts), more than max_index_bytes %d",
		estimate, read, series, avg, postings, counts, c.maxIndexBytes)
}

// allPostingsBytes is the size of the all-postings list of a block with n series.
func allPostingsBytes(n int64) int64 {
	return 4 * n
}

// minSeriesEntryEstimate is the smallest estimate seriesEntryEstimate returns.
const minSeriesEntryEstimate = 32

// seriesEntryEstimate prices a series entry from meta.json's index size per series, capped at
// tsdb.EstimatedSeriesP99Size, which is also the fallback.
func seriesEntryEstimate(m *block.Meta) int64 {
	if m.Stats.NumSeries == 0 {
		return tsdb.EstimatedSeriesP99Size
	}
	for _, f := range m.Thanos.Files {
		if f.RelPath == block.IndexFilename && f.SizeBytes > 0 {
			return max(minSeriesEntryEstimate, min(tsdb.EstimatedSeriesP99Size, f.SizeBytes/int64(m.Stats.NumSeries)))
		}
	}
	return tsdb.EstimatedSeriesP99Size
}

// seriesEntryPlanningBytes is a typical series entry size, used to choose between series entries and group-by postings.
const seriesEntryPlanningBytes = 128

// postingsOnly reports whether a block's counts can come from postings alone.
func (c *seriesCountsState) postingsOnly(m *block.Meta, shard *sharding.ShardSelector, pending []*labels.Matcher) bool {
	return c.opts.Step == 0 && !c.opts.Hashes && shard == nil && len(pending) == 0 && m.MinTime >= c.opts.MinT && m.MaxTime <= c.opts.MaxT
}

// addCounts adds counts per group value, and without as the empty value.
func (c *seriesCountsState) addCounts(counts map[string]int64, without int64) error {
	if without > 0 {
		counts[""] += without
	}
	before := c.counter.GroupCount()
	for v, n := range counts {
		if err := c.counter.AddCount(v, n); err != nil {
			return err
		}
	}
	c.chargeNewGroups(before)
	return nil
}

// chargeNewGroups charges the counts of groups added since before to groupBytes.
func (c *seriesCountsState) chargeNewGroups(before int) {
	if added := c.counter.GroupCount() - before; added > 0 {
		c.groupBytes.Add(c.groupCountsBytes(int64(added)))
	}
}

// checkRead refuses once what was read, plus the counts held, passes max_index_bytes.
func (c *seriesCountsState) checkRead(read int64) error {
	if c.maxIndexBytes == 0 || read <= c.maxIndexBytes {
		return nil
	}
	return cardinality.Refusalf(cardinality.LimitMaxIndexBytes, "the request read %d index bytes, %d of them counts held, after %d series, more than max_index_bytes %d", read, c.groupBytes.Load(), c.counter.Counted(), c.maxIndexBytes)
}

// strategy picks the cheapest series iterator for one block.
func (c *seriesCountsState) strategy(m *block.Meta) seriesIteratorStrategy {
	switch {
	case c.opts.Step > 0:
		return defaultStrategy
	case m.MinTime >= c.opts.MinT && m.MaxTime <= c.opts.MaxT:
		return noChunkRefs
	default:
		return noChunkRefs | overlapMintMaxt
	}
}

// add counts one series. Without a step the iterator already kept only series in the window.
func (c *seriesCountsState) add(s seriesChunkRefs) {
	v := s.lset.Get(c.opts.GroupBy)
	var hash uint64
	if c.opts.Hashes {
		hash = labels.StableHash(s.lset)
	}
	before := c.counter.GroupCount()
	if c.opts.Step == 0 {
		c.counter.AddGroupInWindow(v, hash)
	} else {
		c.ranges = c.ranges[:0]
		for _, r := range s.refs {
			// Chunk times are inclusive.
			c.ranges = append(c.ranges, cardinality.ChunkRange{MinTime: r.minTime, MaxTime: r.maxTime})
		}
		c.counter.AddGroup(v, hash, c.ranges)
	}
	c.chargeNewGroups(before)
}

// SeriesCounts implements the storegatewaypb.StoreGatewayServer interface.
func (s *BucketStore) SeriesCounts(req *storegatewaypb.SeriesCountsRequest, srv storegatewaypb.StoreGateway_SeriesCountsServer) error {
	resp, err := s.countSeries(srv.Context(), req)
	if err != nil {
		return err
	}
	return storegatewaypb.SendSeriesCountsResponse(srv, resp)
}

// countBlock is one block of a SeriesCounts request, open until the request ends.
type countBlock struct {
	b        *bucketBlock
	indexr   *bucketIndexReader
	postings []storage.SeriesRef
	pending  []*labels.Matcher
	// seriesCount is how many series match; with no matchers it is read from the offsets table.
	seriesCount int64
	// groupOffsets is set when the block is counted from the group-by label's postings.
	groupOffsets []streamindex.PostingListOffset
	// groupPostingsBytes is what reading those postings costs.
	groupPostingsBytes int64
	// expanded is set once postings and pending hold the matching series; otherwise plan, if set,
	// is the posting groups to fetch.
	expanded bool
	plan     *postingsPlan
}

// countSeries answers a SeriesCounts request as one response; every refusal comes before anything is sent.
func (s *BucketStore) countSeries(ctx context.Context, req *storegatewaypb.SeriesCountsRequest) (_ *storegatewaypb.SeriesCountsResponse, returnErr error) {
	c, err := s.newCountRequest(req)
	if err != nil {
		return nil, err
	}
	done, err := s.startCount(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	blocks, err := s.openCountBlocks(req.BlockIds)
	if err != nil {
		return nil, err
	}
	// Index readers keep the blocks open until done.
	defer func() {
		for _, o := range blocks {
			runutil.CloseWithLogOnErr(o.b.logger, o.indexr, "close block index reader")
		}
	}()
	// Counted whether or not the request is answered, and not in the Series metrics.
	defer func() {
		st := c.stats.export()
		s.recordCountFetchedBytes(int64(st.postingsFetchedSizeSum), int64(st.seriesFetchedSizeSum), c.walkedTableBytes.Load())
	}()
	// One span per phase: plan, postings, walk. Ending a span twice is a no-op.
	planCtx, planSpan := tracer.Start(ctx, "BucketStore.SeriesCounts plan")
	defer planSpan.End()
	current := planSpan
	defer func() {
		if returnErr != nil {
			current.RecordError(returnErr)
		}
	}()
	planSpan.SetAttributes(attribute.Int("blocks", len(blocks)))
	loaded, err := s.loadCountBlocks(planCtx, c, blocks)
	if err != nil {
		return nil, err
	}
	planSpan.SetAttributes(attribute.Int("blocks_loaded", len(loaded)))
	groupsLowerBound, err := c.boundGroups(planCtx, loaded, req.MaxGroups)
	if err != nil {
		return nil, err
	}
	if c.maxIndexBytes > 0 {
		price, priced, err := s.priceCountPostings(planCtx, c, loaded)
		if err != nil {
			return nil, err
		}
		planSpan.SetAttributes(attribute.Int64("postings_price_bytes", price))
		if err := c.checkPostings(c.limitedBytes(c.stats), price, priced); err != nil {
			return nil, err
		}
	}
	planSpan.End()

	postingsCtx, postingsSpan := tracer.Start(ctx, "BucketStore.SeriesCounts postings")
	defer postingsSpan.End()
	current = postingsSpan
	if err := s.resolveCountPostings(postingsCtx, c, loaded); err != nil {
		return nil, err
	}
	if c.opts.Hashes && c.maxHashes > 0 {
		matching := c.hashesLowerBound(loaded)
		postingsSpan.SetAttributes(attribute.Int64("hashes_lower_bound", matching))
		if matching > c.maxHashes {
			return nil, cardinality.MaxHashesRefusal(matching, c.maxHashes)
		}
	}
	series, matched, err := c.checkWalk(loaded, groupsLowerBound)
	postingsSpan.SetAttributes(attribute.Int("blocks", len(loaded)), attribute.Int64("series_matched_summed_over_blocks", matched), attribute.Int("series_entries_to_read", series))
	if err != nil {
		return nil, err
	}
	postingsSpan.End()

	walkCtx, walkSpan := tracer.Start(ctx, "BucketStore.SeriesCounts walk")
	defer walkSpan.End()
	current = walkSpan
	resp, err := s.walkCountBlocks(walkCtx, c, loaded)
	if err != nil {
		return nil, err
	}
	if err := c.finishResponse(resp, req.MaxGroups); err != nil {
		return nil, err
	}
	walkSpan.SetAttributes(attribute.Int("blocks", len(loaded)), attribute.Int64("blocks_from_postings", resp.BlocksFromPostings), attribute.Int64("series_walked", resp.SeriesCounted))
	return resp, nil
}

// newCountRequest validates req and bounds it by the tenant's limits.
func (s *BucketStore) newCountRequest(req *storegatewaypb.SeriesCountsRequest) (*seriesCountsState, error) {
	c, err := newSeriesCountsState(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// The tenant's limits bound the request whatever it sets.
	if s.cardinalityEstimateMaxIndexBytes != nil {
		c.maxIndexBytes = cardinality.BoundMaxIndexBytes(c.maxIndexBytes, s.cardinalityEstimateMaxIndexBytes())
	}
	if s.cardinalityEstimateMaxMemoryBytes != nil {
		c.maxHashes = cardinality.BoundMaxHashes(c.maxHashes, cardinality.HashBudget(s.cardinalityEstimateMaxMemoryBytes()))
	}
	if s.cardinalityEstimateMaxGroups != nil {
		req.MaxGroups = cardinality.BoundMaxGroups(req.MaxGroups, s.cardinalityEstimateMaxGroups())
	}
	// A __query_shard__ matcher, as sent to Series, counts only the series in that shard.
	shard, matchers, err := sharding.RemoveShardFromMatchers(c.matchers)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	c.matchers, c.shard = matchers, shard
	c.matchersKey = indexcache.CanonicalLabelMatchersKey(c.matchers)
	c.stats = newSafeQueryStats()
	return c, nil
}

// openCountBlocks takes an index reader on each requested block held here, in block-ID order. The caller closes them.
func (s *BucketStore) openCountBlocks(ids []string) ([]countBlock, error) {
	want := make(map[ulid.ULID]bool, len(ids))
	for _, id := range ids {
		parsed, err := ulid.Parse(id)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "block ID %q: %v", id, err)
		}
		want[parsed] = true
	}
	var blocks []countBlock
	s.blockSet.forEach(func(b *bucketBlock) {
		if want[b.meta.ULID] {
			blocks = append(blocks, countBlock{b: b, indexr: b.indexReader(s.postingsStrategy)})
		}
	})
	slices.SortFunc(blocks, func(x, y countBlock) int { return x.b.meta.ULID.Compare(y.b.meta.ULID) })
	return blocks, nil
}

// loadCountBlocks loads the blocks' index-headers and returns the blocks that can be counted.
// A block whose index-header didn't load, or has no all-postings entry (corrupt), is left out so the
// querier asks another store-gateway. With no matchers the series count is read from the offsets table.
func (s *BucketStore) loadCountBlocks(ctx context.Context, c *seriesCountsState, blocks []countBlock) ([]countBlock, error) {
	toLoad := make([]*bucketBlock, len(blocks))
	for i, o := range blocks {
		toLoad[i] = o.b
	}
	failed, err := s.loadCountIndexHeaders(ctx, toLoad, c.stats)
	if err != nil {
		return nil, err
	}
	loaded := make([]countBlock, 0, len(blocks))
	for i, o := range blocks {
		if !failed[i] {
			loaded = append(loaded, o)
		}
	}
	if len(c.matchers) > 0 {
		return loaded, nil
	}
	dropped := make([]bool, len(loaded))
	ag, actx := errgroup.WithContext(ctx)
	ag.SetLimit(s.countBlockConcurrency)
	for i := range loaded {
		o := &loaded[i]
		ag.Go(func() error {
			n, err := allPostingsCount(actx, o.b.indexHeaderReader)
			if errors.Is(err, indexheader.NotFoundRangeErr) {
				level.Warn(spanlogger.FromContext(actx, o.b.logger)).Log("msg", "index-header has no all-postings entry, block left uncounted for cardinality estimate", "err", err)
				dropped[i] = true
				return nil
			}
			if err != nil {
				return status.Errorf(codes.Internal, "block %s: %v", o.b.meta.ULID, err)
			}
			o.seriesCount = n
			return nil
		})
	}
	if err := ag.Wait(); err != nil {
		return nil, err
	}
	counted := loaded[:0]
	for i, o := range loaded {
		if !dropped[i] {
			counted = append(counted, o)
		}
	}
	return counted, nil
}

// boundGroups refuses when one block alone is known to hold more groups than maxGroups, and returns
// the largest block's lower bound on the groups. It applies only with no matchers and no query shard,
// to blocks inside the window.
func (c *seriesCountsState) boundGroups(ctx context.Context, loaded []countBlock, maxGroups int64) (int64, error) {
	if (maxGroups <= 0 && c.maxIndexBytes <= 0) || len(c.matchers) > 0 || c.shard != nil {
		return 0, nil
	}
	var lowerBound int64
	for _, o := range loaded {
		if o.b.meta.MinTime < c.opts.MinT || o.b.meta.MaxTime > c.opts.MaxT {
			continue
		}
		stats, err := o.b.indexHeaderReader.LabelValuesStats(ctx, c.opts.GroupBy)
		if err != nil {
			return 0, status.Errorf(codes.Internal, "block %s: %v", o.b.meta.ULID, err)
		}
		n := int64(stats.CountLowerBound)
		if maxGroups > 0 && n > maxGroups {
			return 0, cardinality.MaxGroupsRefusal(fmt.Sprintf("block %s holds at least", o.b.meta.ULID), n, c.opts.GroupBy, maxGroups)
		}
		lowerBound = max(lowerBound, n)
	}
	return lowerBound, nil
}

// priceCountPostings prices every block's postings from the postings offsets table before any is fetched,
// and returns the price and the number of blocks priced. With matchers, posting groups are kept for the
// fetch while under countPostingsPlansMaxBytes, and rebuilt then otherwise.
func (s *BucketStore) priceCountPostings(ctx context.Context, c *seriesCountsState, loaded []countBlock) (price, priced int64, _ error) {
	var priceSum, kept, pricedSum atomic.Int64
	ag, actx := errgroup.WithContext(ctx)
	ag.SetLimit(s.countBlockConcurrency)
	for i := range loaded {
		o := &loaded[i]
		if len(c.matchers) == 0 {
			if !c.postingsOnly(o.b.meta, c.shard, nil) {
				pricedSum.Add(1)
				priceSum.Add(allPostingsBytes(o.seriesCount))
			}
			continue
		}
		ag.Go(func() error {
			if refs, pending, ok := o.indexr.fetchCachedExpandedPostings(actx, o.b.userID, c.matchersKey, c.stats); ok {
				o.postings, o.pending, o.expanded = refs, pending, true
				return nil
			}
			plan, err := o.indexr.planPostings(actx, c.matchers)
			if err != nil {
				return status.Errorf(codes.Internal, "block %s: plan postings: %v", o.b.meta.ULID, err)
			}
			if len(plan.groups) == 0 {
				o.indexr.cacheExpandedPostings(o.b.userID, c.matchersKey, nil, nil)
				o.expanded = true
				return nil
			}
			pricedSum.Add(1)
			priceSum.Add(o.indexr.plannedFetchBytes(plan))
			if size := plan.heapBytes(); kept.Add(size) <= countPostingsPlansMaxBytes {
				o.plan = &plan
			} else {
				kept.Add(-size)
			}
			return nil
		})
	}
	if err := ag.Wait(); err != nil {
		return 0, 0, err
	}
	return priceSum.Load(), pricedSum.Load(), nil
}

// resolveCountPostings resolves each block's matching series, and picks the blocks counted from the
// group-by label's postings instead of series entries.
func (s *BucketStore) resolveCountPostings(ctx context.Context, c *seriesCountsState, loaded []countBlock) error {
	pg, pctx := errgroup.WithContext(ctx)
	pg.SetLimit(s.countBlockConcurrency)
	for i := range loaded {
		o := &loaded[i]
		pg.Go(func() error {
			// With no matchers the all-postings list is fetched at the walk.
			if len(c.matchers) > 0 {
				var err error
				switch {
				case o.expanded:
				case o.plan != nil:
					// A kept plan skips the ExpandedPostings promise shared with concurrent requests.
					o.postings, o.pending, err = o.indexr.expandPlannedPostings(pctx, *o.plan, c.stats)
					o.plan = nil
					if err == nil {
						o.indexr.cacheExpandedPostings(o.b.userID, c.matchersKey, o.postings, o.pending)
					}
				default:
					o.postings, o.pending, err = o.indexr.ExpandedPostings(pctx, c.matchers, c.stats)
				}
				if err != nil {
					return status.Errorf(codes.Internal, "block %s: expanded postings: %v", o.b.meta.ULID, err)
				}
				o.seriesCount = int64(len(o.postings))
			}
			if !c.postingsOnly(o.b.meta, c.shard, o.pending) {
				return nil
			}
			// Skip when the offsets table alone costs more than reading the series entries.
			labelStats, err := o.b.indexHeaderReader.LabelValuesStats(pctx, c.opts.GroupBy)
			if err != nil {
				return status.Errorf(codes.Internal, "block %s: %s offsets table: %v", o.b.meta.ULID, c.opts.GroupBy, err)
			}
			tableBytes := labelStats.OffsetsTableBytes
			if tableBytes >= o.seriesCount*seriesEntryPlanningBytes {
				return nil
			}
			if err := c.checkTable(indexBytes(c.stats), c.tableBytes.Add(tableBytes)); err != nil {
				return err
			}
			offsets, err := o.b.indexHeaderReader.LabelValuesOffsets(pctx, c.opts.GroupBy, "", nil)
			if err != nil {
				return status.Errorf(codes.Internal, "block %s: %s offsets: %v", o.b.meta.ULID, c.opts.GroupBy, err)
			}
			c.walkedTableBytes.Add(tableBytes)
			var cost int64
			if len(c.matchers) > 0 {
				for _, off := range offsets {
					cost += off.Off.End - off.Off.Start
				}
			}
			if tableBytes+cost < o.seriesCount*seriesEntryPlanningBytes {
				o.groupOffsets, o.groupPostingsBytes = offsets, cost
				if o.groupOffsets == nil {
					o.groupOffsets = []streamindex.PostingListOffset{}
				}
			}
			return nil
		})
	}
	return pg.Wait()
}

// hashesLowerBound is a lower bound on the distinct series a request asking for hashes will hold, from
// blocks inside the window with no pending matchers. Blocks of distinct shards add up; blocks without a
// shard ID, such as replicas, may overlap, so the largest time range is taken.
func (c *seriesCountsState) hashesLowerBound(loaded []countBlock) int64 {
	type blockRange struct{ minT, maxT int64 }
	shards := map[blockRange]map[string]int64{}
	for _, o := range loaded {
		m := o.b.meta
		if m.MinTime < c.opts.MinT || m.MaxTime > c.opts.MaxT || len(o.pending) > 0 {
			continue
		}
		r := blockRange{m.MinTime, m.MaxTime}
		if shards[r] == nil {
			shards[r] = map[string]int64{}
		}
		id := m.Thanos.Labels[block.CompactorShardIDExternalLabel]
		shards[r][id] = max(shards[r][id], o.seriesCount)
	}
	var matching int64
	for _, byShard := range shards {
		var sum int64
		for id, n := range byShard {
			if id != "" {
				sum += n
			}
		}
		matching = max(matching, sum, byShard[""])
	}
	if c.shard != nil && c.shard.ShardCount > 1 {
		matching /= int64(c.shard.ShardCount)
	}
	return matching
}

// checkWalk prices what is still to read before the walk and refuses a request over max_index_bytes.
// It returns the series entries to read and the series matched.
func (c *seriesCountsState) checkWalk(loaded []countBlock, groupsLowerBound int64) (series int, matched int64, _ error) {
	var (
		seriesBytes int64
		postings    int64
	)
	for _, o := range loaded {
		matched += o.seriesCount
		if o.groupOffsets != nil {
			postings += o.groupPostingsBytes
			continue
		}
		if len(c.matchers) == 0 {
			postings += allPostingsBytes(o.seriesCount)
		}
		series += int(o.seriesCount)
		seriesBytes += o.seriesCount * seriesEntryEstimate(o.b.meta)
	}
	var counts int64
	if !c.opts.Hashes {
		counts = c.groupCountsBytes(groupsLowerBound)
	}
	return series, matched, c.checkPlan(c.limitedBytes(c.stats), series, seriesBytes, postings, counts)
}

// walkCountBlocks counts the blocks and returns the response with the block IDs counted.
func (s *BucketStore) walkCountBlocks(ctx context.Context, c *seriesCountsState, loaded []countBlock) (*storegatewaypb.SeriesCountsResponse, error) {
	resp := &storegatewaypb.SeriesCountsResponse{}
	var mtx sync.Mutex
	sg, sctx := errgroup.WithContext(ctx)
	sg.SetLimit(s.countBlockConcurrency)
	for _, o := range loaded {
		if o.groupOffsets != nil {
			resp.BlocksFromPostings++
			// With no matchers this reads only the offsets table.
			if len(c.matchers) == 0 {
				resp.BlocksFromOffsetsTable++
			}
		}
		sg.Go(func() error {
			var err error
			if o.groupOffsets != nil {
				err = addBlockCountsFromPostings(sctx, o.b, o.indexr, o.postings, o.seriesCount, o.groupOffsets, len(c.matchers) == 0, c, &mtx, c.stats)
			} else {
				if len(c.matchers) == 0 {
					// Every check has passed, so read the all-postings list now.
					if o.postings, err = allPostingsForBlock(sctx, o.indexr, c.stats); err != nil {
						return status.Errorf(codes.Internal, "block %s: expanded postings: %v", o.b.meta.ULID, err)
					}
				}
				err = s.addBlockSeriesCounts(sctx, o.b, o.indexr, o.postings, o.pending, c.shard, c, &mtx, c.stats)
			}
			if err != nil {
				if _, ok := grpcutil.ErrorToStatus(err); ok {
					return err
				}
				return status.Errorf(codes.Internal, "block %s: %v", o.b.meta.ULID, err)
			}
			return nil
		})
		resp.BlockIds = append(resp.BlockIds, o.b.meta.ULID.String())
	}
	if err := sg.Wait(); err != nil {
		return nil, err
	}
	return resp, nil
}

// finishResponse fills resp and refuses more groups than maxGroups.
func (c *seriesCountsState) finishResponse(resp *storegatewaypb.SeriesCountsResponse, maxGroups int64) error {
	var err error
	if resp.Groups, err = c.counter.Groups(); err != nil {
		return status.Errorf(codes.Internal, "sketch: %v", err)
	}
	if maxGroups > 0 && int64(len(resp.Groups)) > maxGroups {
		return cardinality.MaxGroupsRefusal("the blocks hold", int64(len(resp.Groups)), c.opts.GroupBy, maxGroups)
	}
	resp.SeriesCounted = c.counter.Counted()
	st := c.stats.export()
	resp.PostingsFetchedBytes = int64(st.postingsFetchedSizeSum)
	resp.SeriesFetchedBytes = int64(st.seriesFetchedSizeSum)
	resp.IndexBytes = int64(st.postingsTouchedSizeSum + st.seriesProcessedSizeSum)
	resp.PostingsOffsetsTableBytes = c.tableBytes.Load()
	resp.WindowMs = c.opts.Window
	return nil
}

// addBlockSeriesCounts adds one block's matching series to c and fails once max_index_bytes is passed.
// Additions to c hold mtx.
func (s *BucketStore) addBlockSeriesCounts(ctx context.Context, b *bucketBlock, indexr *bucketIndexReader, postings []storage.SeriesRef, pending []*labels.Matcher, shard *sharding.ShardSelector, c *seriesCountsState, mtx *sync.Mutex, stats *safeQueryStats) error {
	hasher := cachedSeriesHasher{nil}
	if shard != nil {
		hasher = cachedSeriesHasher{s.seriesHashCache.GetBlockCache(b.meta.ULID.String())}
	}
	var it iterator[seriesChunkRefsSet] = newLoadingSeriesChunkRefsSetIterator(
		ctx, newPostingsSetsIterator(postings, s.maxSeriesPerBatch), indexr, b.indexCache, stats, b.meta,
		shard, hasher, c.strategy(b.meta), c.opts.MinT, c.opts.MaxT-1, b.userID, b.logger,
	)
	if len(pending) > 0 {
		it = newFilteringSeriesChunkRefsSetIterator(pending, it, stats)
	}
	for it.Next() {
		mtx.Lock()
		for _, series := range it.At().series {
			c.add(series)
		}
		held := c.counter.HashesHeld()
		mtx.Unlock()
		if c.maxHashes > 0 && held > c.maxHashes {
			return cardinality.MaxHashesRefusal(held, c.maxHashes)
		}
		if err := c.checkRead(c.limitedBytes(stats)); err != nil {
			return err
		}
	}
	return it.Err()
}

// addBlockCountsFromPostings counts one block's matching series per group-by value from postings.
// With no matchers each count is the postings list's length. A series has at most one value, so the
// matching series in no list are those without it.
func addBlockCountsFromPostings(ctx context.Context, b *bucketBlock, indexr *bucketIndexReader, matching []storage.SeriesRef, total int64, offsets []streamindex.PostingListOffset, all bool, c *seriesCountsState, mtx *sync.Mutex, stats *safeQueryStats) error {
	counts := make(map[string]int64, len(offsets))
	var inGroups int64
	if all {
		for _, off := range offsets {
			size := off.Off.End - off.Off.Start - 4
			if size < 0 || size%4 != 0 {
				return status.Errorf(codes.Internal, "block %s: postings range for %s=%q spans %d bytes, which is not a count field plus 4-byte entries",
					b.meta.ULID, c.opts.GroupBy, off.LabelValue, off.Off.End-off.Off.Start)
			}
			counts[off.LabelValue] = size / 4
			inGroups += size / 4
		}
	} else if len(offsets) > 0 {
		keys := make([]labelPostingOffset, len(offsets))
		for i, off := range offsets {
			keys[i] = labelPostingOffset{Label: labels.Label{Name: c.opts.GroupBy, Value: off.LabelValue}, off: off.Off}
		}
		lists, err := indexr.fetchPostings(ctx, keys, stats)
		if err != nil {
			return fmt.Errorf("fetch %s postings: %w", c.opts.GroupBy, err)
		}
		// From index version 2 on, postings hold the entry offset divided by 16.
		version, err := indexr.indexHeaderReader.IndexVersion(ctx)
		if err != nil {
			return fmt.Errorf("get index version: %w", err)
		}
		scale := storage.SeriesRef(1)
		if version >= 2 {
			scale = 16
		}
		for i, p := range lists {
			n, err := countIntersection(p, matching, scale)
			if err != nil {
				return err
			}
			if n > 0 {
				counts[offsets[i].LabelValue] = n
				inGroups += n
			}
		}
	}
	mtx.Lock()
	err := c.addCounts(counts, total-inGroups)
	mtx.Unlock()
	if err != nil {
		return err
	}
	return c.checkRead(c.limitedBytes(stats))
}

// countIntersection counts the refs of p, multiplied by scale, in sorted.
func countIntersection(p index.Postings, sorted []storage.SeriesRef, scale storage.SeriesRef) (int64, error) {
	var (
		n int64
		i int
	)
	for p.Next() && i < len(sorted) {
		ref := p.At() * scale
		for i < len(sorted) && sorted[i] < ref {
			i++
		}
		if i < len(sorted) && sorted[i] == ref {
			n++
			i++
		}
	}
	return n, p.Err()
}

// allPostingsCount returns how many series a block holds from the all-postings range in the postings
// offsets table, without reading the list.
func allPostingsCount(ctx context.Context, r indexheader.Reader) (int64, error) {
	off, err := r.PostingsOffset(ctx, allPostingsKey.Name, allPostingsKey.Value)
	if err != nil {
		// NotFoundRangeErr means a corrupt index-header, not an empty block.
		return 0, fmt.Errorf("all postings offset: %w", err)
	}
	size := off.End - off.Start - 4
	if size < 0 || size%4 != 0 {
		return 0, fmt.Errorf("the all-postings range spans %d bytes, which is not a count field plus 4-byte entries", off.End-off.Start)
	}
	return size / 4, nil
}

// allPostingsForBlock reads a block's all-postings list in one range read.
func allPostingsForBlock(ctx context.Context, indexr *bucketIndexReader, stats *safeQueryStats) ([]storage.SeriesRef, error) {
	off, err := indexr.indexHeaderReader.PostingsOffset(ctx, allPostingsKey.Name, allPostingsKey.Value)
	if err != nil {
		// As in allPostingsCount, NotFoundRangeErr means a corrupt index-header.
		return nil, fmt.Errorf("all postings offset: %w", err)
	}
	ps, err := indexr.fetchPostings(ctx, []labelPostingOffset{{Label: allPostingsKey, off: off}}, stats)
	if err != nil {
		return nil, fmt.Errorf("fetch all postings: %w", err)
	}
	refs, err := index.ExpandPostings(ps[0])
	if err != nil {
		return nil, err
	}
	// As in ExpandedPostings: from index version 2 on, postings hold the entry offset divided by 16.
	version, err := indexr.indexHeaderReader.IndexVersion(ctx)
	if err != nil {
		return nil, fmt.Errorf("get index version: %w", err)
	}
	if version >= 2 {
		for i, id := range refs {
			refs[i] = id * 16
		}
	}
	return refs, nil
}
