// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/axiomhq/hyperloglog"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/storage/indexheader"
	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// SeriesCountsSketchPrecision is the HLL precision of a group's sketch:
// 2^11 registers, a standard error of about 2.3%.
const SeriesCountsSketchPrecision = cardinality.SketchPrecision

// countBlockConcurrency is how many blocks one count request reads at once.
// HEADCOUNT_BLOCK_CONCURRENCY overrides it; 1 reads blocks one after another,
// as the prototype did before 2026-10-12.
var countBlockConcurrency = func() int {
	if n, err := strconv.Atoi(os.Getenv("HEADCOUNT_BLOCK_CONCURRENCY")); err == nil && n > 0 {
		return n
	}
	return 16
}()

// maxSeriesCountsBuckets caps step_ms so a request can't ask for a huge
// count slice per group.
const maxSeriesCountsBuckets = 1440

// seriesCounts accumulates one SeriesCounts request over several blocks.
type seriesCounts struct {
	// matchers is empty when every series is wanted; then the block's all-postings list is read directly
	// instead of fetching one postings list per metric name.
	matchers   []*labels.Matcher
	minT, maxT int64 // [minT, maxT)
	step       int64
	buckets    int
	groupBy    string
	hashes     bool
	// maxIndexBytes caps postings plus series entry bytes read, 0 for none.
	maxIndexBytes int64
	// sketchAbove turns a hash set larger than this into an HLL sketch.
	sketchAbove int64

	counted int
	counts  map[string][]int64
	sets    map[string]map[uint64]struct{}
	seen    []bool // scratch, one flag per bucket
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
		buckets:       1,
		groupBy:       req.GroupBy,
		hashes:        req.Hashes,
		maxIndexBytes: req.MaxIndexBytes,
		sketchAbove:   req.SketchAbove,
		counts:        map[string][]int64{},
		sets:          map[string]map[uint64]struct{}{},
	}
	if c.groupBy == "" {
		c.groupBy = labels.MetricName
	}
	if c.step < 0 {
		return nil, errors.New("step_ms can't be negative")
	}
	if c.step > 0 {
		if c.hashes {
			return nil, errors.New("hashes can't be combined with step_ms")
		}
		if (c.maxT-c.minT)%c.step != 0 {
			return nil, fmt.Errorf("step_ms %d doesn't divide the window [%d, %d)", c.step, c.minT, c.maxT)
		}
		c.buckets = int((c.maxT - c.minT) / c.step)
		if c.buckets > maxSeriesCountsBuckets {
			return nil, fmt.Errorf("the window has %d buckets of step_ms, more than %d", c.buckets, maxSeriesCountsBuckets)
		}
		c.seen = make([]bool, c.buckets)
	}
	if c.maxIndexBytes < 0 {
		return nil, errors.New("max_index_bytes can't be negative")
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

// checkPlan rejects the request before any series entry is read when the
// postings already read, plus the matching series priced at their blocks'
// estimated entry size (seriesEntryEstimate), plus the group-by postings
// still to read, would pass max_index_bytes.
func (c *seriesCounts) checkPlan(read int64, series int, seriesBytes, postings int64) error {
	if c.maxIndexBytes == 0 {
		return nil
	}
	estimate := read + seriesBytes + postings
	if estimate <= c.maxIndexBytes {
		return nil
	}
	avg := int64(0)
	if series > 0 {
		avg = seriesBytes / int64(series)
	}
	return status.Errorf(codes.ResourceExhausted, "the request would read about %d index bytes (%d of postings read, then %d series at an estimated %d bytes each and %d bytes of group-by postings), more than max_index_bytes %d",
		estimate, read, series, avg, postings, c.maxIndexBytes)
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
			sum = make([]int64, c.buckets)
			c.counts[v] = sum
		}
		sum[0] += n
		c.counted += int(n)
	}
}

// checkRead stops the request once it has read more than max_index_bytes,
// which happens when series entries are larger than the plan assumed.
func (c *seriesCounts) checkRead(read int64) error {
	if c.maxIndexBytes == 0 || read <= c.maxIndexBytes {
		return nil
	}
	return status.Errorf(codes.ResourceExhausted, "the request read %d index bytes after %d series, more than max_index_bytes %d", read, c.counted, c.maxIndexBytes)
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
	v := s.lset.Get(c.groupBy)
	if c.hashes {
		set := c.sets[v]
		if set == nil {
			set = map[uint64]struct{}{}
			c.sets[v] = set
		}
		set[labels.StableHash(s.lset)] = struct{}{}
		return
	}
	counts := c.counts[v]
	if counts == nil {
		counts = make([]int64, c.buckets)
		c.counts[v] = counts
	}
	if c.step == 0 {
		counts[0]++
		return
	}
	// Chunk times are inclusive. A series counts once per bucket even if
	// several of its chunks touch that bucket.
	clear(c.seen)
	for _, r := range s.refs {
		first := max(0, (r.minTime-c.minT)/c.step)
		last := min(int64(c.buckets-1), (r.maxTime-c.minT)/c.step)
		for i := first; i <= last; i++ {
			c.seen[i] = true
		}
	}
	for i, hit := range c.seen {
		if hit {
			counts[i]++
		}
	}
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
// It resolves every block's postings first, so max_index_bytes is checked
// against the whole request before any series entry is read, then reads the
// blocks one at a time in block-ID order.
func (s *BucketStore) SeriesCounts(ctx context.Context, req *storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
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
		// groupOffsets is set when the block is counted from the group-by
		// label's postings instead of series entries.
		groupOffsets []streamindex.PostingListOffset
		// groupPostingsBytes is what reading those postings costs; 0 when
		// there are no matchers, since the offsets alone give the counts.
		groupPostingsBytes int64
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
	defer func() {
		st := stats.export()
		s.recordPostingsStats(st)
		s.recordSeriesStats(st)
	}()
	pg, pctx := errgroup.WithContext(ctx)
	pg.SetLimit(countBlockConcurrency)
	for i := range blocks {
		o := &blocks[i]
		pg.Go(func() error {
			o.b.ensureIndexHeaderLoaded(pctx, stats)
			var err error
			if len(c.matchers) == 0 {
				o.postings, err = allPostingsForBlock(pctx, o.indexr, stats)
			} else {
				o.postings, o.pending, err = o.indexr.ExpandedPostings(pctx, c.matchers, stats)
			}
			if err != nil {
				return status.Errorf(codes.Internal, "block %s: expanded postings: %v", o.b.meta.ULID, err)
			}
			if !c.postingsOnly(o.b.meta, shard, o.pending) {
				return nil
			}
			offsets, err := o.b.indexHeaderReader.LabelValuesOffsets(pctx, c.groupBy, "", nil)
			if err != nil {
				return status.Errorf(codes.Internal, "block %s: %s offsets: %v", o.b.meta.ULID, c.groupBy, err)
			}
			var cost int64
			if len(c.matchers) > 0 {
				for _, off := range offsets {
					cost += off.Off.End - off.Off.Start
				}
			}
			if cost < int64(len(o.postings))*seriesEntryPlanningBytes {
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
	var (
		series        int
		seriesBytes   int64
		groupPostings int64
	)
	for _, o := range blocks {
		if o.groupOffsets != nil {
			groupPostings += o.groupPostingsBytes
			continue
		}
		series += len(o.postings)
		seriesBytes += int64(len(o.postings)) * seriesEntryEstimate(o.b.meta)
	}
	if err := c.checkPlan(indexBytes(stats), series, seriesBytes, groupPostings); err != nil {
		return nil, err
	}
	resp := &storegatewaypb.SeriesCountsResponse{}
	var mtx sync.Mutex
	sg, sctx := errgroup.WithContext(ctx)
	sg.SetLimit(countBlockConcurrency)
	for _, o := range blocks {
		if o.groupOffsets != nil {
			resp.BlocksFromPostings++
		}
		sg.Go(func() error {
			var err error
			if o.groupOffsets != nil {
				err = addBlockCountsFromPostings(sctx, o.b, o.indexr, o.postings, o.groupOffsets, len(c.matchers) == 0, c, &mtx, stats)
			} else {
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
	resp.SeriesCounted = int64(c.counted)
	st := stats.export()
	resp.PostingsFetchedBytes = int64(st.postingsFetchedSizeSum)
	resp.SeriesFetchedBytes = int64(st.seriesFetchedSizeSum)
	resp.IndexBytes = int64(st.postingsTouchedSizeSum + st.seriesProcessedSizeSum)
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
		mtx.Unlock()
		if err := c.checkRead(indexBytes(stats)); err != nil {
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
func addBlockCountsFromPostings(ctx context.Context, b *bucketBlock, indexr *bucketIndexReader, matching []storage.SeriesRef, offsets []streamindex.PostingListOffset, all bool, c *seriesCounts, mtx *sync.Mutex, stats *safeQueryStats) error {
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
	c.addCounts(counts, int64(len(matching))-inGroups)
	mtx.Unlock()
	return c.checkRead(indexBytes(stats))
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

// allPostingsForBlock reads a block's all-postings list, one range read, which is
// what a request with no matchers needs. A matcher such as __name__=~".+"
// would instead fetch every metric name's postings list.
func allPostingsForBlock(ctx context.Context, indexr *bucketIndexReader, stats *safeQueryStats) ([]storage.SeriesRef, error) {
	off, err := indexr.indexHeaderReader.PostingsOffset(ctx, allPostingsKey.Name, allPostingsKey.Value)
	if errors.Is(err, indexheader.NotFoundRangeErr) {
		return nil, nil
	}
	if err != nil {
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
