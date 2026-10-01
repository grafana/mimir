// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// maxSeriesCountsBuckets caps step_ms so a request can't ask for a huge
// count slice per group.
const maxSeriesCountsBuckets = 1440

// seriesCounts accumulates one SeriesCounts request over several blocks.
type seriesCounts struct {
	matchers   []*labels.Matcher
	minT, maxT int64 // [minT, maxT)
	step       int64
	buckets    int
	groupBy    string
	hashes     bool
	maxSeries  int

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
	if len(matchers) == 0 {
		matchers = []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+")}
	}
	if req.MaxTime <= req.MinTime {
		return nil, fmt.Errorf("max_time %d must be after min_time %d", req.MaxTime, req.MinTime)
	}
	c := &seriesCounts{
		matchers:  matchers,
		minT:      req.MinTime,
		maxT:      req.MaxTime,
		step:      req.StepMs,
		buckets:   1,
		groupBy:   req.GroupBy,
		hashes:    req.Hashes,
		maxSeries: int(req.MaxSeries),
		counts:    map[string][]int64{},
		sets:      map[string]map[uint64]struct{}{},
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
	return c, nil
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

func (c *seriesCounts) groups() []*storegatewaypb.SeriesCountGroup {
	out := make([]*storegatewaypb.SeriesCountGroup, 0, len(c.counts)+len(c.sets))
	for v, counts := range c.counts {
		out = append(out, &storegatewaypb.SeriesCountGroup{Value: v, Counts: counts})
	}
	for v, set := range c.sets {
		g := &storegatewaypb.SeriesCountGroup{Value: v, Hashes: make([]uint64, 0, len(set))}
		for h := range set {
			g.Hashes = append(g.Hashes, h)
		}
		out = append(out, g)
	}
	slices.SortFunc(out, func(a, b *storegatewaypb.SeriesCountGroup) int { return strings.Compare(a.Value, b.Value) })
	return out
}

// SeriesCounts implements the storegatewaypb.StoreGatewayServer interface.
// Blocks are read one at a time in block-ID order, so a request that hits
// max_series always stops at the same series.
func (s *BucketStore) SeriesCounts(ctx context.Context, req *storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
	c, err := newSeriesCounts(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	want := make(map[ulid.ULID]bool, len(req.BlockIds))
	for _, id := range req.BlockIds {
		parsed, err := ulid.Parse(id)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "block ID %q: %v", id, err)
		}
		want[parsed] = true
	}

	type openBlock struct {
		b      *bucketBlock
		indexr *bucketIndexReader
	}
	var blocks []openBlock
	// Taking the index reader here keeps the block open until we're done.
	s.blockSet.forEach(func(b *bucketBlock) {
		if want[b.meta.ULID] {
			blocks = append(blocks, openBlock{b, b.indexReader(s.postingsStrategy)})
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
	resp := &storegatewaypb.SeriesCountsResponse{}
	for _, o := range blocks {
		finished, err := s.addBlockSeriesCounts(ctx, o.b, o.indexr, c, stats)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "block %s: %v", o.b.meta.ULID, err)
		}
		if !finished {
			resp.LowerBound = true
			break
		}
		resp.BlockIds = append(resp.BlockIds, o.b.meta.ULID.String())
	}
	resp.Groups = c.groups()
	resp.SeriesCounted = int64(c.counted)
	st := stats.export()
	resp.PostingsFetchedBytes = int64(st.postingsFetchedSizeSum)
	resp.SeriesFetchedBytes = int64(st.seriesFetchedSizeSum)
	return resp, nil
}

// addBlockSeriesCounts adds one block's matching series to c. It returns
// false if max_series was reached before the block was finished.
func (s *BucketStore) addBlockSeriesCounts(ctx context.Context, b *bucketBlock, indexr *bucketIndexReader, c *seriesCounts, stats *safeQueryStats) (bool, error) {
	b.ensureIndexHeaderLoaded(ctx, stats)
	postings, pending, err := indexr.ExpandedPostings(ctx, c.matchers, stats)
	if err != nil {
		return false, fmt.Errorf("expanded postings: %w", err)
	}
	var it iterator[seriesChunkRefsSet] = newLoadingSeriesChunkRefsSetIterator(
		ctx, newPostingsSetsIterator(postings, s.maxSeriesPerBatch), indexr, b.indexCache, stats, b.meta,
		nil, nil, c.strategy(b.meta), c.minT, c.maxT-1, b.userID, b.logger,
	)
	if len(pending) > 0 {
		it = newFilteringSeriesChunkRefsSetIterator(pending, it, stats)
	}
	for it.Next() {
		for _, series := range it.At().series {
			if c.maxSeries > 0 && c.counted >= c.maxSeries {
				return false, nil
			}
			c.add(series)
		}
	}
	return true, it.Err()
}
