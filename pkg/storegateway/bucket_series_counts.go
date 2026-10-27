// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/axiomhq/hyperloglog"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	"go.opentelemetry.io/otel/attribute"
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

// SeriesCountsSketchPrecision is the HLL precision of a group's sketch:
// 2^11 registers, a standard error of about 2.3%.
const SeriesCountsSketchPrecision = cardinality.SketchPrecision

// startCount waits for a slot on the count gate, which cardinality estimate count requests take instead of
// the query gate. It returns the function that frees the slot. A store-gateway too busy to start the
// request in time answers Unavailable, so the querier asks another one for the same blocks.
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

// loadCountIndexHeaders loads the index-header of each block, countBlockConcurrency at a time, and reports
// which did not load. A count request leaves such a block out of the block IDs it answers with, so the querier
// asks another store-gateway for it, as for a store-gateway that is unavailable, and lists it as missing when no
// replica counts it. A cancelled request fails instead. The load doesn't pin the index-header, so one unloaded
// for idleness before the block is read is loaded again then, and a failure there fails the request.
func (s *BucketStore) loadCountIndexHeaders(ctx context.Context, blocks []*bucketBlock, stats *safeQueryStats) ([]bool, error) {
	failed := make([]bool, len(blocks))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.countBlockConcurrency)
	for i, b := range blocks {
		g.Go(func() error {
			// Another request can load the same index-header at the same
			// time, so a load is counted by each request that waited for it.
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

// indexHeaderLoaded reports whether r is loaded: a lazy index-header that
// is not loaded says so, and any other is loaded when the block is.
func indexHeaderLoaded(r indexheader.Reader) bool {
	if lazy, ok := r.(*indexheader.LazyBinaryReader); ok {
		return lazy.LoadedLastUse() != 0
	}
	return true
}

// recordCountFetchedBytes adds what a count request fetched to the
// cardinality estimate count metrics.
func (s *BucketStore) recordCountFetchedBytes(postings, series, offsetsTable int64) {
	for dataType, n := range map[string]int64{"postings": postings, "series": series, "offsets_table": offsetsTable} {
		s.metrics.cardinalityEstimateFetchedBytes.WithLabelValues(dataType).Add(float64(n))
	}
}

// seriesCounts accumulates one SeriesCounts request over several blocks.
type seriesCounts struct {
	// matchers is empty when every series is wanted; then the block's all-postings list is read directly
	// instead of fetching one postings list per metric name.
	matchers   []*labels.Matcher
	minT, maxT int64 // [minT, maxT)
	step       int64
	// window is each bucket's width, 0 for step; see cardinality.Buckets.
	window  int64
	buckets int
	groupBy string
	hashes  bool
	// maxIndexBytes caps postings, series entry and postings offsets table
	// bytes read, 0 for none.
	maxIndexBytes int64
	// tableBytes is the group-by label's postings offsets table read so far,
	// priced from the index-header's samples. With index-header version 2
	// these are object-storage reads.
	tableBytes atomic.Int64
	// walkedTableBytes is the part of tableBytes whose walk finished, for
	// the count metrics: a block refused before its walk is charged but not
	// read.
	walkedTableBytes atomic.Int64
	// groupBytes is the memory of the per-bucket counts held with step_ms,
	// 8 bytes per group per bucket, which max_index_bytes bounds with the
	// index read. Without a step max_groups bounds the groups, and hashes
	// are bounded by max_hashes.
	groupBytes atomic.Int64
	// sketchAbove turns a hash set larger than this into an HLL sketch.
	sketchAbove int64
	// maxHashes, with hashes, caps the distinct hashes held, 0 for none.
	maxHashes  int64
	hashesHeld int64

	counted int
	counts  map[string][]int64
	sets    map[string]map[uint64]struct{}
	marks   *cardinality.BucketMarks // scratch for one series' buckets
}

func newSeriesCounts(req *storegatewaypb.SeriesCountsRequest) (*seriesCounts, error) {
	reqMatchers := make([]storepb.LabelMatcher, 0, len(req.Matchers))
	for _, m := range req.Matchers {
		reqMatchers = append(reqMatchers, *m)
	}
	matchers, err := storepb.MatchersToPromMatchers(reqMatchers...)
	if err != nil {
		return nil, err
	}
	if req.MaxTime <= req.MinTime {
		return nil, fmt.Errorf("max_time %d must be after min_time %d", req.MaxTime, req.MinTime)
	}
	c := &seriesCounts{
		matchers:      matchers,
		minT:          req.MinTime,
		maxT:          req.MaxTime,
		step:          req.StepMs,
		window:        req.WindowMs,
		buckets:       1,
		groupBy:       req.GroupBy,
		hashes:        req.Hashes,
		maxIndexBytes: req.MaxIndexBytes,
		sketchAbove:   req.SketchAbove,
		maxHashes:     req.MaxHashes,
		counts:        map[string][]int64{},
		sets:          map[string]map[uint64]struct{}{},
	}
	if c.groupBy == "" {
		c.groupBy = labels.MetricName
	}
	if c.step < 0 {
		return nil, errors.New("step_ms can't be negative")
	}
	if c.window != 0 && c.step == 0 {
		return nil, errors.New("window_ms needs step_ms")
	}
	if c.step > 0 {
		if c.hashes {
			return nil, errors.New("hashes can't be combined with step_ms")
		}
		if c.buckets, err = cardinality.Buckets(c.minT, c.maxT, c.step, c.window); err != nil {
			return nil, err
		}
		c.marks = cardinality.NewBucketMarks(c.buckets)
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
	if c.sketchAbove < 0 {
		return nil, errors.New("sketch_above can't be negative")
	}
	return c, nil
}

// indexBytes is the index read so far, measured as the response's index_bytes.
func indexBytes(stats *safeQueryStats) int64 {
	st := stats.export()
	return int64(st.postingsTouchedSizeSum + st.seriesProcessedSizeSum)
}

// limitedBytes is what max_index_bytes bounds: index_bytes plus the postings
// offsets table read, plus the per-bucket counts held with step_ms.
func (c *seriesCounts) limitedBytes(stats *safeQueryStats) int64 {
	return indexBytes(stats) + c.tableBytes.Load() + c.groupBytes.Load()
}

// groupCountsBytes is the memory of the per-bucket counts of n groups, one
// int64 per bucket each, which a request with step_ms holds per group on
// top of the group itself. Without a step the groups are bounded by
// max_groups and nothing is charged.
func (c *seriesCounts) groupCountsBytes(n int64) int64 {
	if c.buckets <= 1 {
		return 0
	}
	return n * int64(c.buckets) * 8
}

// checkTable rejects the request before a block's postings offsets table is
// walked when the index already read, plus the table bytes priced so far,
// would pass max_index_bytes.
func (c *seriesCounts) checkTable(read, table int64) error {
	if c.maxIndexBytes == 0 || read+table <= c.maxIndexBytes {
		return nil
	}
	return status.Errorf(codes.ResourceExhausted, "the request would read about %d index bytes (%d of postings, then %d of the %s postings offsets table), more than max_index_bytes %d",
		read+table, read, table, c.groupBy, c.maxIndexBytes)
}

// countPostingsPlansMaxBytes caps the posting groups one SeriesCounts
// request keeps from pricing its blocks' postings to fetching them.
var countPostingsPlansMaxBytes int64 = 64 << 20

// checkPostings rejects the request before any postings list is fetched when
// the index already read, plus the postings lists its matchers select over
// blocks, or with no matchers the all-postings lists of the blocks it walks,
// priced from the postings offsets table, would pass max_index_bytes.
func (c *seriesCounts) checkPostings(read, postings, blocks int64) error {
	if c.maxIndexBytes == 0 || read+postings <= c.maxIndexBytes {
		return nil
	}
	lists := "postings lists its matchers select"
	if len(c.matchers) == 0 {
		lists = "all-postings lists"
	}
	return status.Errorf(codes.ResourceExhausted, "the request would read about %d index bytes (%d already read, then %d of %s over %d blocks, priced from the postings offsets table), more than max_index_bytes %d",
		read+postings, read, postings, lists, blocks, c.maxIndexBytes)
}

// checkPlan rejects the request before any series entry is read when the
// postings and postings offsets table already read, plus the matching series
// priced at their blocks' estimated entry size (seriesEntryEstimate), plus
// the postings still to read, plus the counts of the groups the blocks are
// known to hold, would pass max_index_bytes.
func (c *seriesCounts) checkPlan(read int64, series int, seriesBytes, postings, counts int64) error {
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
	return status.Errorf(codes.ResourceExhausted, "the request would read about %d index bytes (%d of postings and postings offsets table read, then %d series at an estimated %d bytes each and %d bytes of postings, holding %d bytes of counts), more than max_index_bytes %d",
		estimate, read, series, avg, postings, counts, c.maxIndexBytes)
}

// allPostingsBytes is the size of the all-postings list of a block with n
// series: 4 bytes per series, as allPostingsCount reads it.
func allPostingsBytes(n int64) int64 {
	return 4 * n
}

// minSeriesEntryEstimate is the smallest per-series size seriesEntryEstimate
// returns: a series entry holds at least its label references and one
// chunk reference.
const minSeriesEntryEstimate = 32

// seriesEntryEstimate is the size to price one of the block's series
// entries at: the index file's size divided by its series, from the block's
// meta.json, which overstates an entry by the postings and symbols but is
// usually far below the planner's p99. Without both figures it is the p99,
// tsdb.EstimatedSeriesP99Size, and it is never above it.
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

// seriesEntryPlanningBytes is the series entry size used to choose between
// reading series entries and reading the group-by label's postings for a
// block inside the window. It is a typical entry, not the p99 the limit
// prices at: synthetic tenants measured 54 to 155 bytes per series.
const seriesEntryPlanningBytes = 128

// postingsOnly reports whether a block's counts can come from postings
// alone: the block sits inside the window, so every matching series counts
// without its chunk metas, and nothing needs a series' labels, which
// hashes, query shards and matchers the postings could not apply do.
func (c *seriesCounts) postingsOnly(m *block.Meta, shard *sharding.ShardSelector, pending []*labels.Matcher) bool {
	return c.step == 0 && !c.hashes && shard == nil && len(pending) == 0 && m.MinTime >= c.minT && m.MaxTime <= c.maxT
}

// addCounts adds counts per group value and the series without the group-by
// label, which count under the empty value as add does.
func (c *seriesCounts) addCounts(counts map[string]int64, without int64) {
	if without > 0 {
		counts[""] += without
	}
	for v, n := range counts {
		sum := c.counts[v]
		if sum == nil {
			sum = c.newGroupCounts(v)
		}
		sum[0] += n
		c.counted += int(n)
	}
}

// newGroupCounts adds a group and charges its counts to groupBytes.
func (c *seriesCounts) newGroupCounts(v string) []int64 {
	counts := make([]int64, c.buckets)
	c.counts[strings.Clone(v)] = counts
	c.groupBytes.Add(c.groupCountsBytes(1))
	return counts
}

// checkRead stops the request once what it has read, plus the counts it
// holds, is more than max_index_bytes, which happens when series entries are
// larger than the plan assumed or the groups are more than it knew of.
func (c *seriesCounts) checkRead(read int64) error {
	if c.maxIndexBytes == 0 || read <= c.maxIndexBytes {
		return nil
	}
	return status.Errorf(codes.ResourceExhausted, "the request read %d index bytes, %d of them counts held, after %d series, more than max_index_bytes %d", read, c.groupBytes.Load(), c.counted, c.maxIndexBytes)
}

// strategy picks the cheapest series iterator for one block. A block that
// sits inside the window needs no chunk metas checked; per-bucket counts
// need each chunk's time range.
func (c *seriesCounts) strategy(m *block.Meta) seriesIteratorStrategy {
	switch {
	case c.step > 0:
		return defaultStrategy
	case m.MinTime >= c.minT && m.MaxTime <= c.maxT:
		return noChunkRefs
	default:
		return noChunkRefs | overlapMintMaxt
	}
}

func (c *seriesCounts) add(s seriesChunkRefs) {
	c.counted++
	// The value is a slice of the series' label data, so a new group's key
	// is cloned to keep only the value alive, not the whole label set.
	v := s.lset.Get(c.groupBy)
	if c.hashes {
		set := c.sets[v]
		if set == nil {
			set = map[uint64]struct{}{}
			c.sets[strings.Clone(v)] = set
		}
		h := labels.StableHash(s.lset)
		if _, ok := set[h]; !ok {
			set[h] = struct{}{}
			c.hashesHeld++
		}
		return
	}
	counts := c.counts[v]
	if counts == nil {
		counts = c.newGroupCounts(v)
	}
	if c.step == 0 {
		counts[0]++
		return
	}
	// Chunk times are inclusive. A series counts once per bucket even if
	// several of its chunks touch that bucket.
	for _, r := range s.refs {
		c.marks.Mark(cardinality.BucketSpan(c.minT, c.step, c.window, c.buckets, r.minTime, r.maxTime))
	}
	c.marks.Flush(counts)
}

func (c *seriesCounts) groups() ([]*storegatewaypb.SeriesCountGroup, error) {
	out := make([]*storegatewaypb.SeriesCountGroup, 0, len(c.counts)+len(c.sets))
	for v, counts := range c.counts {
		out = append(out, &storegatewaypb.SeriesCountGroup{Value: v, Counts: counts})
	}
	for v, set := range c.sets {
		g := &storegatewaypb.SeriesCountGroup{Value: v}
		if c.sketchAbove > 0 && int64(len(set)) > c.sketchAbove {
			sk, err := hyperloglog.NewSketch(SeriesCountsSketchPrecision, false)
			if err != nil {
				return nil, err
			}
			for h := range set {
				sk.InsertHash(h)
			}
			if g.Sketch, err = sk.MarshalBinary(); err != nil {
				return nil, err
			}
		} else {
			g.Hashes = make([]uint64, 0, len(set))
			for h := range set {
				g.Hashes = append(g.Hashes, h)
			}
		}
		out = append(out, g)
	}
	slices.SortFunc(out, func(a, b *storegatewaypb.SeriesCountGroup) int { return strings.Compare(a.Value, b.Value) })
	return out, nil
}

// SeriesCounts implements the storegatewaypb.StoreGatewayServer interface.
// It prices and resolves every block's postings first, so max_index_bytes
// and max_hashes are checked against the whole request before any
// all-postings list or series entry is read, then reads the blocks in
// block-ID order. A block whose index-header doesn't load is left out of the
// block IDs answered, so the querier asks another store-gateway for it.
func (s *BucketStore) SeriesCounts(req *storegatewaypb.SeriesCountsRequest, srv storegatewaypb.StoreGateway_SeriesCountsServer) error {
	resp, err := s.countSeries(srv.Context(), req)
	if err != nil {
		return err
	}
	return storegatewaypb.SendSeriesCountsResponse(srv, resp)
}

// countSeries answers a SeriesCounts request as one response, which
// SeriesCounts streams; every refusal comes before anything is sent.
func (s *BucketStore) countSeries(ctx context.Context, req *storegatewaypb.SeriesCountsRequest) (_ *storegatewaypb.SeriesCountsResponse, returnErr error) {
	c, err := newSeriesCounts(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// The tenant's limit also bounds a call that sets no max_index_bytes, or a larger one, whoever sends it.
	if s.cardinalityEstimateMaxIndexBytes != nil {
		c.maxIndexBytes = cardinality.BoundMaxIndexBytes(c.maxIndexBytes, s.cardinalityEstimateMaxIndexBytes())
	}
	// A __query_shard__ matcher, as query sharding sends to Series, counts only
	// the series whose label-set hash falls in that shard. Shards share no
	// series, so their counts add up and their hashes union to the total.
	shard, matchers, err := sharding.RemoveShardFromMatchers(c.matchers)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	c.matchers = matchers
	done, err := s.startCount(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	want := make(map[ulid.ULID]bool, len(req.BlockIds))
	for _, id := range req.BlockIds {
		parsed, err := ulid.Parse(id)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "block ID %q: %v", id, err)
		}
		want[parsed] = true
	}

	type openBlock struct {
		b        *bucketBlock
		indexr   *bucketIndexReader
		postings []storage.SeriesRef
		pending  []*labels.Matcher
		// seriesCount is how many series match: len(postings), or, with no
		// matchers, the all-postings list's length read from the offsets
		// table without fetching the list.
		seriesCount int64
		// groupOffsets is set when the block is counted from the group-by
		// label's postings instead of series entries.
		groupOffsets []streamindex.PostingListOffset
		// groupPostingsBytes is what reading those postings costs; 0 when
		// there are no matchers, since the offsets alone give the counts.
		groupPostingsBytes int64
		// With matchers, expanded is set once postings and pending hold the
		// block's matching series, from the expanded postings cache or
		// because the matchers select none. Otherwise plan, when set, is
		// the posting groups to fetch, kept from pricing.
		expanded bool
		plan     *postingsPlan
	}
	var blocks []openBlock
	// Taking the index reader here keeps the block open until we're done.
	s.blockSet.forEach(func(b *bucketBlock) {
		if want[b.meta.ULID] {
			blocks = append(blocks, openBlock{b: b, indexr: b.indexReader(s.postingsStrategy)})
		}
	})
	defer func() {
		for _, o := range blocks {
			runutil.CloseWithLogOnErr(o.b.logger, o.indexr, "close block index reader")
		}
	}()
	slices.SortFunc(blocks, func(x, y openBlock) int { return x.b.meta.ULID.Compare(y.b.meta.ULID) })

	stats := newSafeQueryStats()
	// What a request read counts whether or not it is answered, and not in
	// the Series metrics.
	defer func() {
		st := stats.export()
		s.recordCountFetchedBytes(int64(st.postingsFetchedSizeSum), int64(st.seriesFetchedSizeSum), c.walkedTableBytes.Load())
	}()
	// One span per phase: plan (index-header loads and the postings price),
	// postings (resolving each block's matching series) and walk (counting
	// them). Ending a span twice is a no-op, so the deferred End covers
	// every return.
	planCtx, planSpan := tracer.Start(ctx, "BucketStore.SeriesCounts plan")
	defer planSpan.End()
	current := planSpan
	defer func() {
		if returnErr != nil {
			current.RecordError(returnErr)
		}
	}()
	planSpan.SetAttributes(attribute.Int("blocks", len(blocks)))
	toLoad := make([]*bucketBlock, len(blocks))
	for i, o := range blocks {
		toLoad[i] = o.b
	}
	failed, err := s.loadCountIndexHeaders(planCtx, toLoad, stats)
	if err != nil {
		return nil, err
	}
	loaded := make([]openBlock, 0, len(blocks))
	for i, o := range blocks {
		if !failed[i] {
			loaded = append(loaded, o)
		}
	}
	// With no matchers a block's series count is the all-postings list's
	// length, read from the offsets table alone. An index-header with no
	// all-postings entry is corrupt, since the index writer always writes
	// one, and its block is left uncounted like one whose index-header did
	// not load, so the querier asks another store-gateway for it.
	if len(c.matchers) == 0 {
		dropped := make([]bool, len(loaded))
		ag, actx := errgroup.WithContext(planCtx)
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
		loaded = counted
	}
	planSpan.SetAttributes(attribute.Int("blocks_loaded", len(loaded)))
	// Refuse before walking any postings offsets table when one block alone is
	// known to hold more groups than max_groups. Only with no matchers and no
	// query shard, and for a block inside the window, does each of the
	// block's group-by values make a group, so only then is its count of
	// values, from the index-header's samples, a lower bound on the groups.
	// The largest block's bound also prices the counts the groups will hold
	// against max_index_bytes before the walk.
	var groupsLowerBound int64
	if (req.MaxGroups > 0 || c.maxIndexBytes > 0) && len(c.matchers) == 0 && shard == nil {
		for _, o := range loaded {
			if o.b.meta.MinTime < c.minT || o.b.meta.MaxTime > c.maxT {
				continue
			}
			n, err := o.b.indexHeaderReader.LabelValuesCountLowerBound(planCtx, c.groupBy)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "block %s: %v", o.b.meta.ULID, err)
			}
			if req.MaxGroups > 0 && int64(n) > req.MaxGroups {
				return nil, status.Errorf(codes.ResourceExhausted, "block %s holds at least %d distinct values of %s, more than max_groups %d", o.b.meta.ULID, n, c.groupBy, req.MaxGroups)
			}
			groupsLowerBound = max(groupsLowerBound, int64(n))
		}
	}
	// Every block's postings are priced from the postings offsets table
	// before any is fetched, so a selector that matches most series is
	// refused before it reads their postings. With no matchers the price is
	// the all-postings list, 4 bytes per series, of each block that will be
	// walked; the list is fetched only at the walk. With matchers, a block's
	// posting groups are kept for the fetch while the plans kept stay under
	// countPostingsPlansMaxBytes; past that a block keeps only its price and
	// builds its groups again when it is fetched. Without a limit there is
	// nothing to price, and every block is expanded as a query's is.
	key := indexcache.CanonicalLabelMatchersKey(c.matchers)
	if c.maxIndexBytes > 0 {
		var price, kept, priced atomic.Int64
		ag, actx := errgroup.WithContext(planCtx)
		ag.SetLimit(s.countBlockConcurrency)
		for i := range loaded {
			o := &loaded[i]
			if len(c.matchers) == 0 {
				if !c.postingsOnly(o.b.meta, shard, nil) {
					priced.Add(1)
					price.Add(allPostingsBytes(o.seriesCount))
				}
				continue
			}
			ag.Go(func() error {
				if refs, pending, ok := o.indexr.fetchCachedExpandedPostings(actx, o.b.userID, key, stats); ok {
					o.postings, o.pending, o.expanded = refs, pending, true
					return nil
				}
				plan, err := o.indexr.planPostings(actx, c.matchers)
				if err != nil {
					return status.Errorf(codes.Internal, "block %s: plan postings: %v", o.b.meta.ULID, err)
				}
				if len(plan.groups) == 0 {
					// Cached as ExpandedPostings caches it.
					o.indexr.cacheExpandedPostings(o.b.userID, key, nil, nil)
					o.expanded = true
					return nil
				}
				priced.Add(1)
				price.Add(o.indexr.plannedFetchBytes(plan))
				if size := plan.heapBytes(); kept.Add(size) <= countPostingsPlansMaxBytes {
					o.plan = &plan
				} else {
					kept.Add(-size)
				}
				return nil
			})
		}
		if err := ag.Wait(); err != nil {
			return nil, err
		}
		planSpan.SetAttributes(attribute.Int64("postings_price_bytes", price.Load()))
		if err := c.checkPostings(c.limitedBytes(stats), price.Load(), priced.Load()); err != nil {
			return nil, err
		}
	}
	planSpan.End()
	postingsCtx, postingsSpan := tracer.Start(ctx, "BucketStore.SeriesCounts postings")
	defer postingsSpan.End()
	current = postingsSpan
	pg, pctx := errgroup.WithContext(postingsCtx)
	pg.SetLimit(s.countBlockConcurrency)
	for i := range loaded {
		o := &loaded[i]
		pg.Go(func() error {
			// With no matchers the block's series count came from the offsets
			// table above, and its all-postings list, if the block is walked,
			// is fetched at the walk, once every check has passed.
			if len(c.matchers) > 0 {
				var err error
				switch {
				case o.expanded:
				case o.plan != nil:
					// A kept plan is fetched without the promise ExpandedPostings
					// shares with concurrent requests for the same matchers; the
					// index cache still serves each postings list once it holds it.
					o.postings, o.pending, err = o.indexr.expandPlannedPostings(pctx, *o.plan, stats)
					o.plan = nil
					if err == nil {
						o.indexr.cacheExpandedPostings(o.b.userID, key, o.postings, o.pending)
					}
				default:
					o.postings, o.pending, err = o.indexr.ExpandedPostings(pctx, c.matchers, stats)
				}
				if err != nil {
					return status.Errorf(codes.Internal, "block %s: expanded postings: %v", o.b.meta.ULID, err)
				}
				o.seriesCount = int64(len(o.postings))
			}
			if !c.postingsOnly(o.b.meta, shard, o.pending) {
				return nil
			}
			// The walk reads the group-by label's postings offsets table, from
			// object storage with index-header version 2. Skip it when the table
			// alone costs more than reading the series entries would.
			tableBytes, err := o.b.indexHeaderReader.LabelValuesOffsetsTableBytes(pctx, c.groupBy)
			if err != nil {
				return status.Errorf(codes.Internal, "block %s: %s offsets table: %v", o.b.meta.ULID, c.groupBy, err)
			}
			if tableBytes >= o.seriesCount*seriesEntryPlanningBytes {
				return nil
			}
			if err := c.checkTable(indexBytes(stats), c.tableBytes.Add(tableBytes)); err != nil {
				return err
			}
			offsets, err := o.b.indexHeaderReader.LabelValuesOffsets(pctx, c.groupBy, "", nil)
			if err != nil {
				return status.Errorf(codes.Internal, "block %s: %s offsets: %v", o.b.meta.ULID, c.groupBy, err)
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
	if err := pg.Wait(); err != nil {
		return nil, err
	}
	// Asked for hashes, the store-gateway holds one per distinct matching
	// series with a chunk in the window until it answers. Before any series
	// entry is read it refuses a request whose blocks already match more such
	// series than max_hashes, counting only blocks whose postings are exactly
	// what they hash: blocks inside the window, so every series has a chunk
	// there, with no matcher left to apply to the series. In one time range,
	// blocks of distinct shards share no series and add up, while blocks
	// without a shard ID, such as replicas, may share all of theirs; a series
	// can be in every range. The largest range is then a lower bound on the
	// distinct series. A query shard hashes about its share of them. The
	// walk then refuses once the distinct hashes held pass max_hashes.
	if c.hashes && c.maxHashes > 0 {
		type blockRange struct{ minT, maxT int64 }
		shards := map[blockRange]map[string]int64{}
		for _, o := range loaded {
			m := o.b.meta
			if m.MinTime < c.minT || m.MaxTime > c.maxT || len(o.pending) > 0 {
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
		if shard != nil && shard.ShardCount > 1 {
			matching /= int64(shard.ShardCount)
		}
		postingsSpan.SetAttributes(attribute.Int64("hashes_lower_bound", matching))
		if matching > c.maxHashes {
			return nil, status.Error(codes.ResourceExhausted, cardinality.MaxHashesRefusal(matching, c.maxHashes))
		}
	}
	// postings is what is still to read before any series entry: a walked
	// block's all-postings list with no matchers, and the group-by postings
	// of a block counted from them with matchers.
	var (
		series      int
		seriesBytes int64
		postings    int64
	)
	for _, o := range loaded {
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
	var matched int64
	for _, o := range loaded {
		matched += o.seriesCount
	}
	postingsSpan.SetAttributes(attribute.Int("blocks", len(loaded)), attribute.Int64("series_matched_summed_over_blocks", matched), attribute.Int("series_entries_to_read", series))
	var counts int64
	if !c.hashes {
		counts = c.groupCountsBytes(groupsLowerBound)
	}
	if err := c.checkPlan(c.limitedBytes(stats), series, seriesBytes, postings, counts); err != nil {
		return nil, err
	}
	postingsSpan.End()
	walkCtx, walkSpan := tracer.Start(ctx, "BucketStore.SeriesCounts walk")
	defer walkSpan.End()
	current = walkSpan
	resp := &storegatewaypb.SeriesCountsResponse{}
	var mtx sync.Mutex
	sg, sctx := errgroup.WithContext(walkCtx)
	sg.SetLimit(s.countBlockConcurrency)
	for _, o := range loaded {
		if o.groupOffsets != nil {
			resp.BlocksFromPostings++
			// With no matchers, a block counted from postings reads only
			// the offsets table.
			if len(c.matchers) == 0 {
				resp.BlocksFromOffsetsTable++
			}
		}
		sg.Go(func() error {
			var err error
			if o.groupOffsets != nil {
				err = addBlockCountsFromPostings(sctx, o.b, o.indexr, o.postings, o.seriesCount, o.groupOffsets, len(c.matchers) == 0, c, &mtx, stats)
			} else {
				if len(c.matchers) == 0 {
					// Every check has passed, so the walked block's
					// all-postings list is read now.
					if o.postings, err = allPostingsForBlock(sctx, o.indexr, stats); err != nil {
						return status.Errorf(codes.Internal, "block %s: expanded postings: %v", o.b.meta.ULID, err)
					}
				}
				err = s.addBlockSeriesCounts(sctx, o.b, o.indexr, o.postings, o.pending, shard, c, &mtx, stats)
			}
			if err != nil {
				if _, ok := status.FromError(err); ok {
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
	if resp.Groups, err = c.groups(); err != nil {
		return nil, status.Errorf(codes.Internal, "sketch: %v", err)
	}
	if req.MaxGroups > 0 && int64(len(resp.Groups)) > req.MaxGroups {
		return nil, status.Errorf(codes.ResourceExhausted, "the blocks hold %d distinct values of %s, more than max_groups %d", len(resp.Groups), c.groupBy, req.MaxGroups)
	}
	resp.SeriesCounted = int64(c.counted)
	walkSpan.SetAttributes(attribute.Int("blocks", len(loaded)), attribute.Int64("blocks_from_postings", resp.BlocksFromPostings), attribute.Int64("series_walked", resp.SeriesCounted))
	st := stats.export()
	resp.PostingsFetchedBytes = int64(st.postingsFetchedSizeSum)
	resp.SeriesFetchedBytes = int64(st.seriesFetchedSizeSum)
	resp.IndexBytes = int64(st.postingsTouchedSizeSum + st.seriesProcessedSizeSum)
	resp.PostingsOffsetsTableBytes = c.tableBytes.Load()
	resp.WindowMs = c.window
	return resp, nil
}

// addBlockSeriesCounts adds one block's matching series to c, from postings
// already resolved, and fails once max_index_bytes is passed. Blocks run
// concurrently, so additions to c hold mtx; reading the block does not.
func (s *BucketStore) addBlockSeriesCounts(ctx context.Context, b *bucketBlock, indexr *bucketIndexReader, postings []storage.SeriesRef, pending []*labels.Matcher, shard *sharding.ShardSelector, c *seriesCounts, mtx *sync.Mutex, stats *safeQueryStats) error {
	hasher := cachedSeriesHasher{nil}
	if shard != nil {
		hasher = cachedSeriesHasher{s.seriesHashCache.GetBlockCache(b.meta.ULID.String())}
	}
	var it iterator[seriesChunkRefsSet] = newLoadingSeriesChunkRefsSetIterator(
		ctx, newPostingsSetsIterator(postings, s.maxSeriesPerBatch), indexr, b.indexCache, stats, b.meta,
		shard, hasher, c.strategy(b.meta), c.minT, c.maxT-1, b.userID, b.logger,
	)
	if len(pending) > 0 {
		it = newFilteringSeriesChunkRefsSetIterator(pending, it, stats)
	}
	for it.Next() {
		mtx.Lock()
		for _, series := range it.At().series {
			c.add(series)
		}
		held := c.hashesHeld
		mtx.Unlock()
		if c.maxHashes > 0 && held > c.maxHashes {
			return status.Error(codes.ResourceExhausted, cardinality.MaxHashesRefusal(held, c.maxHashes))
		}
		if err := c.checkRead(c.limitedBytes(stats)); err != nil {
			return err
		}
	}
	return it.Err()
}

// addBlockCountsFromPostings counts one block's matching series per value of
// the group-by label from postings, without reading series entries. With no
// matchers every series matches, so each value's count is its postings
// list's length, read from the offsets alone. Otherwise each value's
// postings are read and intersected with the matching series. A series
// carries at most one value of a label, so the matching series not in any
// value's list are the ones without it.
func addBlockCountsFromPostings(ctx context.Context, b *bucketBlock, indexr *bucketIndexReader, matching []storage.SeriesRef, total int64, offsets []streamindex.PostingListOffset, all bool, c *seriesCounts, mtx *sync.Mutex, stats *safeQueryStats) error {
	counts := make(map[string]int64, len(offsets))
	var inGroups int64
	if all {
		for _, off := range offsets {
			size := off.Off.End - off.Off.Start - 4
			if size < 0 || size%4 != 0 {
				return status.Errorf(codes.Internal, "block %s: postings range for %s=%q spans %d bytes, which is not a count field plus 4-byte entries",
					b.meta.ULID, c.groupBy, off.LabelValue, off.Off.End-off.Off.Start)
			}
			counts[off.LabelValue] = size / 4
			inGroups += size / 4
		}
	} else if len(offsets) > 0 {
		keys := make([]labelPostingOffset, len(offsets))
		for i, off := range offsets {
			keys[i] = labelPostingOffset{Label: labels.Label{Name: c.groupBy, Value: off.LabelValue}, off: off.Off}
		}
		lists, err := indexr.fetchPostings(ctx, keys, stats)
		if err != nil {
			return fmt.Errorf("fetch %s postings: %w", c.groupBy, err)
		}
		// From index version 2 on, postings hold the series entry offset
		// divided by 16, and the matching refs are already multiplied back.
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
	c.addCounts(counts, total-inGroups)
	mtx.Unlock()
	return c.checkRead(c.limitedBytes(stats))
}

// countIntersection counts the refs of p, multiplied by scale, that are in
// sorted.
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

// allPostingsCount returns how many series a block holds, from the all-postings
// list's range in the postings offsets table and without reading the list: the
// range starts at the list's 4-byte entry count and ends before its CRC, so it
// holds 4 bytes per series after the count field.
func allPostingsCount(ctx context.Context, r indexheader.Reader) (int64, error) {
	off, err := r.PostingsOffset(ctx, allPostingsKey.Name, allPostingsKey.Value)
	if err != nil {
		// The index writer always writes the all-postings entry, so a
		// NotFoundRangeErr means a corrupt index-header, not an empty block.
		return 0, fmt.Errorf("all postings offset: %w", err)
	}
	size := off.End - off.Start - 4
	if size < 0 || size%4 != 0 {
		return 0, fmt.Errorf("the all-postings range spans %d bytes, which is not a count field plus 4-byte entries", off.End-off.Start)
	}
	return size / 4, nil
}

// allPostingsForBlock reads a block's all-postings list, one range read, which is
// what a request with no matchers needs. A matcher such as __name__=~".+"
// would instead fetch every metric name's postings list.
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
	// As in ExpandedPostings: from index version 2 on, series entries are 16-byte aligned and the postings
	// hold the entry offset divided by 16.
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
