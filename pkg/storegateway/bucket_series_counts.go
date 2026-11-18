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
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/indexcache"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// startCount waits for a slot on the count gate and returns the function that frees it.
// A store-gateway too busy to start in time answers Unavailable, so the querier asks another.
func (s *BucketStore) startCount(ctx context.Context) (func(), error) {
	if err := s.countGate.Start(ctx); err != nil {
		if errors.Is(err, errGateTimeout) {
			return nil, cardinality.Busyf("too many cardinality estimate count requests running: %s", err.Error())
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
				s.metrics.cardinalityEstimateIndexHeaderLoadDuration.Observe(time.Since(start).Seconds())
			}
			if err != nil {
				if gctx.Err() != nil {
					return status.FromContextError(gctx.Err()).Err()
				}
				level.Warn(spanlogger.FromContext(gctx, b.logger)).Log("msg", "index-header did not load, block left uncounted for cardinality estimate", "err", err)
				s.metrics.cardinalityEstimateBlocksUncounted.WithLabelValues("index_header_load_failed").Inc()
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

// seriesCountsState accumulates one SeriesCounts request and bounds what it holds by the tenant's
// cardinality estimate memory limit.
type seriesCountsState struct {
	// matchers is empty when every series is wanted.
	matchers []*labels.Matcher
	// opts is what is counted, as the counter validated it.
	opts    cardinality.CounterOptions
	counter *cardinality.SeriesCounter
	// tableBytes is the postings offsets table walked, for the group-by label and the labels of
	// lazyMatchers, priced from the index-header's samples.
	tableBytes atomic.Int64
	// maxGroups caps the distinct group-by values, 0 for none.
	maxGroups int64
	// lazyMatchers are the matchers planned by walking their label's postings offsets table.
	lazyMatchers []*labels.Matcher
	// matchersKey is matchers' key in the expanded postings cache.
	matchersKey indexcache.LabelMatchersKey
	// stats is what the request read.
	stats *safeQueryStats
	// mem is what the request holds: expanded postings, groups and hashes.
	mem countMemory
}

func newSeriesCountsState(req *storegatewaypb.SeriesCountsRequest, maxMemoryBytes uint64) (*seriesCountsState, error) {
	opts, counter, matchers, err := cardinality.CounterForQuery(&req.CountQuery, req.MaxGroups)
	if err != nil {
		return nil, err
	}
	return &seriesCountsState{
		matchers:  matchers,
		opts:      opts,
		counter:   counter,
		maxGroups: req.MaxGroups,
		mem:       countMemory{limit: maxMemoryBytes},
	}, nil
}

// What a count request holds, as countMemory charges it.
const (
	// countRefBytes is one expanded series ref.
	countRefBytes = 8
	// countRawPostingBytes is one entry of a postings list as fetched.
	countRawPostingBytes = 4
	// countGroupBytes is one group of counts, before its value.
	countGroupBytes = 64
)

// countMemory is what one count request holds. take refuses before the memory is taken when it
// would pass the limit. Safe for concurrent use.
type countMemory struct {
	// limit is the tenant's cardinality estimate memory limit, 0 for none.
	limit uint64
	held  atomic.Uint64
}

func (m *countMemory) take(b uint64, what string) error {
	if b == 0 {
		return nil
	}
	if n := m.held.Add(b); m.limit > 0 && n > m.limit {
		m.held.Sub(b)
		return cardinality.Refusalf(cardinality.LimitMemory, "the request would hold more than %d bytes in the store-gateway while counting, the tenant's cardinality estimate memory limit, with %s", m.limit, what)
	}
	return nil
}

func (m *countMemory) release(b uint64) {
	if b > 0 {
		m.held.Sub(b)
	}
}

// indexBytes is the postings and series bytes read so far. Postings count the larger of the bytes
// touched and the bytes fetched, since a fetch also reads the gaps between the lists it merges into
// one range. Series count as processed: their fetched bytes include the tsdb.MaxSeriesSize each
// range asks for past its last series, which is not read.
func indexBytes(stats *safeQueryStats) int64 {
	st := stats.export()
	return int64(max(st.postingsTouchedSizeSum, st.postingsFetchedSizeSum) + st.seriesProcessedSizeSum)
}

// measureLazyMatchers adds the postings offsets tables that planning a block's postings walks, one
// per lazy matcher, to tableBytes. A matcher with a prefix walks less than its label's whole table.
func (c *seriesCountsState) measureLazyMatchers(ctx context.Context, o *countBlock) error {
	for _, m := range c.lazyMatchers {
		labelStats, err := o.b.indexHeaderReader.LabelValuesStats(ctx, m.Name)
		if err != nil {
			return status.Errorf(codes.Internal, "block %s: %s offsets table: %v", o.b.meta.ULID, m.Name, err)
		}
		c.tableBytes.Add(labelStats.OffsetsTableBytes)
	}
	return nil
}

// seriesEntryPlanningBytes is a typical series entry size, used to choose between series entries and group-by postings.
const seriesEntryPlanningBytes = 128

// postingsOnly reports whether a block's counts can come from postings alone.
func (c *seriesCountsState) postingsOnly(m *block.Meta, pending []*labels.Matcher) bool {
	return !c.opts.Hashes && len(pending) == 0 && m.MinTime >= c.opts.MinT && m.MaxTime <= c.opts.MaxT
}

// addCounts adds counts per group value, and without as the empty value.
func (c *seriesCountsState) addCounts(counts map[string]int64, without int64) error {
	if without > 0 {
		counts[""] += without
	}
	for v, n := range counts {
		before := c.counter.GroupCount()
		if err := c.counter.AddCount(v, n); err != nil {
			return err
		}
		if err := c.chargeAdded(before, 0, v); err != nil {
			return err
		}
	}
	return nil
}

// chargeAdded checks max_groups and charges the memory of what the counter added since it held
// groupsBefore groups and hashesBefore hashes, value being the last group-by value added. Callers
// charge after each series, so the counter holds at most one entry past either limit.
func (c *seriesCountsState) chargeAdded(groupsBefore int, hashesBefore int64, value string) error {
	groups := c.counter.GroupCount()
	if n := int64(groups); c.maxGroups > 0 && n > c.maxGroups {
		return cardinality.MaxGroupsRefusal("the blocks hold at least", n, c.opts.GroupBy, c.maxGroups)
	}
	var b uint64
	if groups > groupsBefore {
		if c.opts.Hashes {
			b += uint64(cardinality.GroupBytes + len(value))
		} else {
			b += uint64(countGroupBytes + len(value))
		}
	}
	if c.opts.Hashes {
		b += uint64(c.counter.HashesHeld()-hashesBefore) * cardinality.HashBytes
	}
	if b == 0 {
		return nil
	}
	return c.mem.take(b, "the groups and hashes counted")
}

// strategy picks the cheapest series iterator for one block.
func (c *seriesCountsState) strategy(m *block.Meta) seriesIteratorStrategy {
	switch {
	case m.MinTime >= c.opts.MinT && m.MaxTime <= c.opts.MaxT:
		return noChunkRefs
	default:
		return noChunkRefs | overlapMintMaxt
	}
}

// add counts one series, refuses a group past max_groups and charges what the counter holds. The
// iterator already kept only series in the window.
func (c *seriesCountsState) add(s seriesChunkRefs) error {
	var hash uint64
	if c.opts.Hashes {
		hash = labels.StableHash(s.lset)
	}
	groups, hashes := c.counter.GroupCount(), c.counter.HashesHeld()
	v := s.lset.Get(c.opts.GroupBy)
	c.counter.AddGroupInWindow(v, hash)
	return c.chargeAdded(groups, hashes, v)
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
	// held is what postings is charged to the request's memory.
	held uint64
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
		s.recordCountFetchedBytes(int64(st.postingsFetchedSizeSum), int64(st.seriesFetchedSizeSum), c.tableBytes.Load())
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
	if err := c.checkBeforeLoad(blocks, s.countBlockConcurrency); err != nil {
		return nil, err
	}
	loaded, err := s.loadCountBlocks(planCtx, c, blocks)
	if err != nil {
		return nil, err
	}
	planSpan.SetAttributes(attribute.Int("blocks_loaded", len(loaded)))
	if err := c.boundGroups(planCtx, loaded); err != nil {
		return nil, err
	}
	planSpan.End()

	postingsCtx, postingsSpan := tracer.Start(ctx, "BucketStore.SeriesCounts postings")
	defer postingsSpan.End()
	current = postingsSpan
	if err := s.resolveCountPostings(postingsCtx, c, loaded); err != nil {
		return nil, err
	}
	postingsSpan.SetAttributes(attribute.Int("blocks", len(loaded)), attribute.Int64("held_bytes", int64(c.mem.held.Load())))
	postingsSpan.End()

	walkCtx, walkSpan := tracer.Start(ctx, "BucketStore.SeriesCounts walk")
	defer walkSpan.End()
	current = walkSpan
	resp, err := s.walkCountBlocks(walkCtx, c, loaded)
	if err != nil {
		return nil, err
	}
	c.finishResponse(resp)
	walkSpan.SetAttributes(attribute.Int("blocks", len(loaded)), attribute.Int64("blocks_from_postings", resp.BlocksFromPostings), attribute.Int64("series_walked", resp.SeriesCounted))
	return resp, nil
}

// newCountRequest validates req. max_groups is the caller's to set: the querier bounds it by the tenant's.
// The memory limit is the tenant's.
func (s *BucketStore) newCountRequest(req *storegatewaypb.SeriesCountsRequest) (*seriesCountsState, error) {
	var maxMemory uint64
	if s.countMaxMemoryBytes != nil {
		maxMemory = s.countMaxMemoryBytes()
	}
	c, err := newSeriesCountsState(req, maxMemory)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	for _, m := range c.matchers {
		if toRawPostingGroup(m).isLazy {
			c.lazyMatchers = append(c.lazyMatchers, m)
		}
	}
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
	if notHeld := len(want) - len(blocks); notHeld > 0 {
		s.metrics.cardinalityEstimateBlocksUncounted.WithLabelValues("not_held").Add(float64(notHeld))
	}
	return blocks, nil
}

// checkBeforeLoad refuses a request with no matchers that the series counts in the blocks' meta.json
// already put over the memory limit, before any index-header is loaded through the gate shared with
// Series requests: the all-postings lists of the largest blocks walked at once, or the hashes of the
// series one block range is known to hold. With matchers nothing is known until the index-headers
// are read.
func (c *seriesCountsState) checkBeforeLoad(blocks []countBlock, concurrency int) error {
	if len(c.matchers) > 0 || c.mem.limit == 0 {
		return nil
	}
	var all []countBlock
	var walked []uint64
	for _, o := range blocks {
		o.seriesCount = int64(o.b.meta.Stats.NumSeries)
		all = append(all, o)
		// The other blocks may be counted from the group-by label's postings.
		if !c.postingsOnly(o.b.meta, nil) {
			walked = append(walked, uint64(o.seriesCount))
		}
	}
	slices.Sort(walked)
	var postings uint64
	for _, n := range walked[max(0, len(walked)-concurrency):] {
		postings += n * (countRefBytes + countRawPostingBytes)
	}
	var hashes uint64
	if c.opts.Hashes {
		hashes = uint64(c.hashesLowerBound(all)) * cardinality.HashBytes
	}
	if postings+hashes > c.mem.limit {
		return cardinality.Refusalf(cardinality.LimitMemory, "the request would hold more than %d bytes in the store-gateway while counting, the tenant's cardinality estimate memory limit: %d bytes of postings for the largest blocks read at once and %d bytes of hashes, from the series counts in meta.json", c.mem.limit, postings, hashes)
	}
	return nil
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
				s.metrics.cardinalityEstimateBlocksUncounted.WithLabelValues("no_all_postings").Inc()
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

// boundGroups refuses when one block alone is known to hold more groups than max_groups. It applies
// only with no matchers, to blocks inside the window.
func (c *seriesCountsState) boundGroups(ctx context.Context, loaded []countBlock) error {
	maxGroups := c.maxGroups
	if maxGroups <= 0 || len(c.matchers) > 0 {
		return nil
	}
	for _, o := range loaded {
		if o.b.meta.MinTime < c.opts.MinT || o.b.meta.MaxTime > c.opts.MaxT {
			continue
		}
		stats, err := o.b.indexHeaderReader.LabelValuesStats(ctx, c.opts.GroupBy)
		if err != nil {
			return status.Errorf(codes.Internal, "block %s: %v", o.b.meta.ULID, err)
		}
		if n := int64(stats.CountLowerBound); n > maxGroups {
			return cardinality.MaxGroupsRefusal(fmt.Sprintf("block %s holds at least", o.b.meta.ULID), n, c.opts.GroupBy, maxGroups)
		}
	}
	return nil
}

// resolveCountPostings resolves each block's matching series, and picks the blocks counted from the
// group-by label's postings instead of series entries. Expanded postings are charged to the request's
// memory before they are read: with matchers at an upper value from the planned postings lists, then
// at their size.
func (s *BucketStore) resolveCountPostings(ctx context.Context, c *seriesCountsState, loaded []countBlock) error {
	pg, pctx := errgroup.WithContext(ctx)
	pg.SetLimit(s.countBlockConcurrency)
	for i := range loaded {
		o := &loaded[i]
		pg.Go(func() error {
			// With no matchers the all-postings list is fetched at the walk.
			if len(c.matchers) > 0 {
				if err := c.expandMatchingPostings(pctx, o); err != nil {
					return err
				}
			}
			if !c.postingsOnly(o.b.meta, o.pending) {
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
			offsets, err := o.b.indexHeaderReader.LabelValuesOffsets(pctx, c.opts.GroupBy, "", nil)
			if err != nil {
				return status.Errorf(codes.Internal, "block %s: %s offsets: %v", o.b.meta.ULID, c.opts.GroupBy, err)
			}
			c.tableBytes.Add(tableBytes)
			var cost int64
			if len(c.matchers) > 0 {
				for _, off := range offsets {
					cost += off.Off.End - off.Off.Start
				}
			}
			if tableBytes+cost < o.seriesCount*seriesEntryPlanningBytes {
				o.groupOffsets = offsets
				if o.groupOffsets == nil {
					o.groupOffsets = []streamindex.PostingListOffset{}
				}
			}
			return nil
		})
	}
	return pg.Wait()
}

// expandMatchingPostings sets o's postings and pending matchers for the request's matchers, from the
// expanded postings cache or the index, charged to the request's memory.
func (c *seriesCountsState) expandMatchingPostings(ctx context.Context, o *countBlock) error {
	if refs, pending, ok := o.indexr.fetchCachedExpandedPostings(ctx, o.b.userID, c.matchersKey, c.stats); ok {
		if err := c.holdPostings(o, uint64(len(refs))*countRefBytes); err != nil {
			return err
		}
		o.postings, o.pending, o.seriesCount = refs, pending, int64(len(refs))
		return nil
	}
	if err := c.measureLazyMatchers(ctx, o); err != nil {
		return err
	}
	plan, err := o.indexr.planPostings(ctx, c.matchers)
	if err != nil {
		return status.Errorf(codes.Internal, "block %s: plan postings: %v", o.b.meta.ULID, err)
	}
	if len(plan.groups) == 0 {
		o.indexr.cacheExpandedPostings(o.b.userID, c.matchersKey, nil, nil)
		o.seriesCount = 0
		return nil
	}
	// The fetched lists, and the refs expanded from them: at most one ref per entry fetched.
	fetched := uint64(max(0, o.indexr.plannedFetchBytes(plan)))
	upper := fetched + fetched/countRawPostingBytes*countRefBytes
	if err := c.mem.take(upper, fmt.Sprintf("about %d bytes of postings lists to expand in block %s", fetched, o.b.meta.ULID)); err != nil {
		return err
	}
	o.postings, o.pending, err = o.indexr.expandPlannedPostings(ctx, plan, c.stats)
	c.mem.release(upper)
	if err != nil {
		return status.Errorf(codes.Internal, "block %s: expanded postings: %v", o.b.meta.ULID, err)
	}
	o.indexr.cacheExpandedPostings(o.b.userID, c.matchersKey, o.postings, o.pending)
	o.seriesCount = int64(len(o.postings))
	return c.holdPostings(o, uint64(len(o.postings))*countRefBytes)
}

// holdPostings charges b bytes of o's postings to the request's memory until the block is counted.
func (c *seriesCountsState) holdPostings(o *countBlock, b uint64) error {
	if err := c.mem.take(b, fmt.Sprintf("the expanded postings of block %s", o.b.meta.ULID)); err != nil {
		return err
	}
	o.held += b
	return nil
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
	return matching
}

// walkCountBlocks counts the blocks and returns the response with the block IDs counted.
func (s *BucketStore) walkCountBlocks(ctx context.Context, c *seriesCountsState, loaded []countBlock) (*storegatewaypb.SeriesCountsResponse, error) {
	resp := &storegatewaypb.SeriesCountsResponse{}
	var mtx sync.Mutex
	sg, sctx := errgroup.WithContext(ctx)
	sg.SetLimit(s.countBlockConcurrency)
	for i := range loaded {
		o := &loaded[i]
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
					// The list as fetched and its expanded refs, charged before the read.
					n := uint64(o.seriesCount)
					if err := c.mem.take(n*(countRawPostingBytes+countRefBytes), fmt.Sprintf("the all-postings list of block %s, %d series", o.b.meta.ULID, n)); err != nil {
						return err
					}
					o.postings, err = allPostingsForBlock(sctx, o.indexr, c.stats)
					c.mem.release(n * countRawPostingBytes)
					o.held += n * countRefBytes
					if err != nil {
						return status.Errorf(codes.Internal, "block %s: expanded postings: %v", o.b.meta.ULID, err)
					}
				}
				err = s.addBlockSeriesCounts(sctx, o.b, o.indexr, o.postings, o.pending, c, &mtx, c.stats)
			}
			// The block is counted, so what it resolved is not held while the others are.
			o.postings, o.pending, o.groupOffsets = nil, nil, nil
			c.mem.release(o.held)
			o.held = 0
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

// finishResponse fills resp.
func (c *seriesCountsState) finishResponse(resp *storegatewaypb.SeriesCountsResponse) {
	resp.Groups = c.counter.Groups()
	resp.SeriesCounted = c.counter.Counted()
	st := c.stats.export()
	resp.PostingsFetchedBytes = int64(st.postingsFetchedSizeSum)
	resp.SeriesFetchedBytes = int64(st.seriesFetchedSizeSum)
	resp.IndexBytes = indexBytes(c.stats)
	resp.PostingsOffsetsTableBytes = c.tableBytes.Load()
}

// addBlockSeriesCounts adds one block's matching series to c. Additions to c hold mtx.
func (s *BucketStore) addBlockSeriesCounts(ctx context.Context, b *bucketBlock, indexr *bucketIndexReader, postings []storage.SeriesRef, pending []*labels.Matcher, c *seriesCountsState, mtx *sync.Mutex, stats *safeQueryStats) error {
	var it iterator[seriesChunkRefsSet] = newLoadingSeriesChunkRefsSetIterator(
		ctx, newPostingsSetsIterator(postings, s.maxSeriesPerBatch), indexr, b.indexCache, stats, b.meta,
		nil, cachedSeriesHasher{nil}, c.strategy(b.meta), c.opts.MinT, c.opts.MaxT-1, b.userID, b.logger,
	)
	if len(pending) > 0 {
		it = newFilteringSeriesChunkRefsSetIterator(pending, it, stats)
	}
	for it.Next() {
		mtx.Lock()
		var err error
		for _, series := range it.At().series {
			if err = c.add(series); err != nil {
				break
			}
		}
		mtx.Unlock()
		if err != nil {
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
			n, err := countIntersection(ctx, p, matching, scale)
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
	defer mtx.Unlock()
	return c.addCounts(counts, total-inGroups)
}

// countIntersectionCheckEvery is how many steps countIntersection takes between context checks.
const countIntersectionCheckEvery = 1 << 12

// countIntersection counts the refs of p, multiplied by scale, in sorted. It seeks p and gallops
// through sorted, so the cost follows the shorter of the two rather than their product with the
// number of lists a caller intersects.
func countIntersection(ctx context.Context, p index.Postings, sorted []storage.SeriesRef, scale storage.SeriesRef) (int64, error) {
	var n int64
	i := 0
	ok := p.Next()
	for steps := 0; ok && i < len(sorted); steps++ {
		if steps%countIntersectionCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}
		ref := p.At() * scale
		switch {
		case sorted[i] < ref:
			i = gallop(sorted, i, ref)
		case sorted[i] > ref:
			ok = p.Seek((sorted[i] + scale - 1) / scale)
		default:
			n++
			i++
			ok = p.Next()
		}
	}
	return n, p.Err()
}

// gallop returns the first index after i whose value is at least ref, given sorted[i] < ref.
func gallop(sorted []storage.SeriesRef, i int, ref storage.SeriesRef) int {
	lo, step := i, 1
	hi := lo + step
	for hi < len(sorted) && sorted[hi] < ref {
		lo = hi
		step *= 2
		hi = lo + step
	}
	hi = min(hi, len(sorted))
	j, _ := slices.BinarySearch(sorted[lo+1:hi], ref)
	return lo + 1 + j
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
