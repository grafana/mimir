// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// headCardinality is the part of the distributor that answers the head rows of a bounds answer.
type headCardinality interface {
	LabelValuesCardinality(ctx context.Context, labelNames []model.LabelName, matchers []*labels.Matcher, countMethod cardinality.CountMethod) (uint64, *client.LabelValuesCardinalityResponse, error)
	// IngestStorageEnabled reports whether a partition is owned by one ingester per zone, so that the
	// head counts are added up across partitions rather than estimated from zone replicas.
	IngestStorageEnabled() bool
}

// BoundsResult answers a bounds request: for each block range the window touches, a lower and an
// upper value per group, and the ingesters' head figures apart.
type BoundsResult struct {
	Ranges []*BoundsRange
	// NotCovered holds the parts of the window no block range answers. The part at or after
	// query-store-after is there whenever the window reaches it: the head rows have no window.
	NotCovered []estimateWindow
	// Head is nil when the window does not reach the ingesters.
	Head *BoundsHead

	StoreGateways             int
	Retries                   int
	SeriesCounted             int64
	IndexBytes                int64
	PostingsOffsetsTableBytes int64
	PostingsFetchedBytes      int64
	SeriesFetchedBytes        int64
	PeakMemoryBytes           uint64
}

// BoundsRange is one block range of a bounds answer. For each group, Lower <= the series with a
// chunk in [MinT, MaxT) <= Upper, unless LowerBound is set, when Upper may be short too.
type BoundsRange struct {
	MinT, MaxT int64
	// MainSet is set when the range's non-out-of-order blocks of exactly the range are distinct shards,
	// which share no series, so their counts add up to Lower.
	MainSet                 bool
	MainBlocks, OtherBlocks int
	Lower, Upper            map[string]int64
	// LowerBound is set when a block of the range or a shard of its main set was not counted.
	LowerBound    bool
	MissingBlocks []ulid.ULID
	MissingShards []missingShard
}

// BoundsHead is what the ingesters' heads hold, per group, from the label values cardinality RPC.
type BoundsHead struct {
	// InMemory is the series each value has in the heads: an upper value for any window that starts
	// at or after the heads' oldest sample, and under ingest storage only.
	InMemory map[string]int64
	// Active is the series the ingesters saw a sample for within their active series idle timeout,
	// by arrival time. It is not a bound: an out-of-order sample marks a series active.
	Active map[string]int64
	// Approximate is set without ingest storage, where both are estimated from zone replicas.
	Approximate bool
}

// boundsBlock is one block of a bounds request and how it is folded into its range.
type boundsBlock struct {
	b      *bucketindex.Block
	rng    *boundsPlanRange
	main   bool
	marked bool
	ooo    bool
}

// boundsPlanRange is a range being answered and what its blocks counted so far.
type boundsPlanRange struct {
	out *BoundsRange
	// mainSum is the main set's counts, other the other unmarked blocks', single the largest count of
	// one block.
	mainSum, other, single map[string]int64
	blocks                 []*boundsBlock
}

// isOutOfOrderBlock reports whether b holds out-of-order samples, as the compactor groups blocks: it
// keeps no hint on compacted blocks, only the external label.
func isOutOfOrderBlock(b *bucketindex.Block) bool {
	return b.OutOfOrder || b.Labels[block.OutOfOrderExternalLabel] == block.OutOfOrderExternalLabelValue
}

// planBounds splits blocks into ranges: each maximal block range, those that overlap merged. A range
// has a main set when it is one block range whose unmarked, in-order blocks of exactly that range are
// disjoint shards.
func planBounds(blocks bucketindex.Blocks, marked map[ulid.ULID]struct{}) ([]*boundsPlanRange, map[ulid.ULID]*boundsBlock) {
	type span struct{ minT, maxT int64 }
	var spans []span
	seen := map[span]bool{}
	for _, b := range blocks {
		if s := (span{b.MinTime, b.MaxTime}); !seen[s] {
			seen[s] = true
			spans = append(spans, s)
		}
	}
	var maximal []span
	for _, s := range spans {
		inside := slices.ContainsFunc(spans, func(o span) bool { return o != s && o.minT <= s.minT && o.maxT >= s.maxT })
		if !inside {
			maximal = append(maximal, s)
		}
	}
	slices.SortFunc(maximal, func(a, b span) int { return int(min(1, max(-1, a.minT-b.minT))) })
	var merged []span
	exact := map[int]bool{}
	for _, s := range maximal {
		if n := len(merged); n > 0 && s.minT < merged[n-1].maxT {
			merged[n-1].maxT = max(merged[n-1].maxT, s.maxT)
			exact[n-1] = false
			continue
		}
		merged = append(merged, s)
		exact[len(merged)-1] = true
	}
	ranges := make([]*boundsPlanRange, len(merged))
	for i, s := range merged {
		ranges[i] = &boundsPlanRange{
			out:     &BoundsRange{MinT: s.minT, MaxT: s.maxT},
			mainSum: map[string]int64{}, other: map[string]int64{}, single: map[string]int64{},
		}
	}
	byID := make(map[ulid.ULID]*boundsBlock, len(blocks))
	for _, b := range blocks {
		i, _ := slices.BinarySearchFunc(merged, b.MinTime, func(s span, t int64) int {
			if s.maxT <= t {
				return -1
			}
			if s.minT > t {
				return 1
			}
			return 0
		})
		_, isMarked := marked[b.ID]
		bb := &boundsBlock{b: b, rng: ranges[i], marked: isMarked, ooo: isOutOfOrderBlock(b)}
		ranges[i].blocks = append(ranges[i].blocks, bb)
		byID[b.ID] = bb
	}
	for i, r := range ranges {
		var candidates bucketindex.Blocks
		for _, bb := range r.blocks {
			if exact[i] && !bb.marked && !bb.ooo && bb.b.MinTime == r.out.MinT && bb.b.MaxTime == r.out.MaxT {
				candidates = append(candidates, bb.b)
			}
		}
		r.out.MainSet = len(candidates) > 0 && checkDisjointShards(candidates) == nil
		for _, bb := range r.blocks {
			bb.main = r.out.MainSet && slices.Contains(candidates, bb.b)
			if bb.main {
				r.out.MainBlocks++
			} else {
				r.out.OtherBlocks++
			}
		}
		if r.out.MainSet {
			r.out.MissingShards = missingShards(candidates)
		}
	}
	return ranges, byID
}

// fold adds one block's counts to its range. A marked block's counts are kept apart until finish
// knows whether the blocks that replaced it were counted.
func (bb *boundsBlock) fold(counts map[string]int64, markedCounts map[ulid.ULID]map[string]int64) {
	if bb.marked {
		markedCounts[bb.b.ID] = counts
		return
	}
	r := bb.rng
	into := r.other
	if bb.main {
		into = r.mainSum
	}
	for v, n := range counts {
		into[v] += n
		r.single[v] = max(r.single[v], n)
	}
}

// finishBounds writes each range's Lower and Upper from what was counted. A marked block is left out
// when the counted unmarked blocks of its kind that hold its series cover its time range; otherwise
// it adds to Upper. An uncounted block flags its range, unless counted marked blocks of its kind that
// hold its series cover it: they are its sources.
func finishBounds(ranges []*boundsPlanRange, counted map[ulid.ULID]bool, markedCounts map[ulid.ULID]map[string]int64) []*BoundsRange {
	out := make([]*BoundsRange, 0, len(ranges))
	for _, r := range ranges {
		kept := map[string]int64{}
		for _, bb := range r.blocks {
			switch {
			case bb.marked && counted[bb.b.ID]:
				if heldBy(r, bb, false, counted) {
					continue
				}
				for v, n := range markedCounts[bb.b.ID] {
					kept[v] += n
					r.single[v] = max(r.single[v], n)
				}
			case !bb.marked && !counted[bb.b.ID]:
				if heldBy(r, bb, true, counted) {
					continue
				}
				r.out.MissingBlocks = append(r.out.MissingBlocks, bb.b.ID)
			}
		}
		r.out.LowerBound = len(r.out.MissingBlocks) > 0 || len(r.out.MissingShards) > 0
		r.out.Lower, r.out.Upper = map[string]int64{}, map[string]int64{}
		for _, m := range []map[string]int64{r.mainSum, r.other, kept} {
			for v, n := range m {
				r.out.Upper[v] += n
			}
		}
		lower := r.mainSum
		if !r.out.MainSet {
			lower = r.single
		}
		for v := range r.out.Upper {
			r.out.Lower[v] = lower[v]
		}
		out = append(out, r.out)
	}
	return out
}

// heldBy reports whether the counted blocks of r that are marked as asked, of bb's kind, hold every
// series of bb over its time range. A block of a shard holds the series of that shard; a block with no
// shard ID holds all of them, and is held when the blocks of every shard of one count cover it.
func heldBy(r *boundsPlanRange, bb *boundsBlock, marked bool, counted map[ulid.ULID]bool) bool {
	byShard := map[string]bucketindex.Blocks{}
	for _, o := range r.blocks {
		if o != bb && o.ooo == bb.ooo && o.marked == marked && counted[o.b.ID] {
			byShard[o.b.CompactorShardID] = append(byShard[o.b.CompactorShardID], o.b)
		}
	}
	covers := func(bs bucketindex.Blocks) bool { return len(bs) > 0 && blocksCover(bs, bb.b.MinTime, bb.b.MaxTime) }
	if covers(byShard[""]) {
		return true
	}
	if bb.b.CompactorShardID != "" {
		return covers(byShard[bb.b.CompactorShardID])
	}
	var count uint64
	for id := range byShard {
		if id == "" {
			continue
		}
		_, of, err := sharding.ParseShardIDLabelValue(id)
		if err != nil || (count != 0 && of != count) {
			return false
		}
		count = of
	}
	if count == 0 || count > maxCheckedShardCount {
		return false
	}
	for i := uint64(0); i < count; i++ {
		if !covers(byShard[sharding.FormatShardIDLabelValue(i, count)]) {
			return false
		}
	}
	return true
}

// coverageHoles returns the parts of [minT, maxT) no block holds.
func coverageHoles(blocks bucketindex.Blocks, minT, maxT int64) []estimateWindow {
	sorted := slices.Clone(blocks)
	slices.SortFunc(sorted, func(a, b *bucketindex.Block) int { return int(min(1, max(-1, a.MinTime-b.MinTime))) })
	var holes []estimateWindow
	covered := minT
	for _, b := range sorted {
		if covered >= maxT {
			break
		}
		if b.MinTime > covered {
			holes = append(holes, estimateWindow{Start: covered, End: min(b.MinTime, maxT)})
		}
		covered = max(covered, b.MaxTime)
	}
	if covered < maxT {
		holes = append(holes, estimateWindow{Start: covered, End: maxT})
	}
	return holes
}

// Bounds answers req, which has no matchers, in bounds mode: the blocks before query-store-after per
// block range from their postings offsets tables, and, when the window reaches the ingesters, the
// heads' figures apart.
func (e *CardinalityEstimator) Bounds(ctx context.Context, tenantID string, req *CardinalityEstimateRequest) (res BoundsResult, err error) {
	if len(req.Matchers) > 0 {
		return BoundsResult{}, badEstimateRequest("bounds mode takes no matchers")
	}
	req.MaxGroups = cardinality.BoundByLimit(req.MaxGroups, e.limits.CardinalityEstimateMaxGroups(tenantID))
	tracker, terr := limiter.MemoryConsumptionTrackerFromContext(ctx)
	if terr != nil {
		rejected := e.blocks.metrics.estimateMemoryRefusalsOrDiscard()
		limit := estimateMemoryLimit(e.limits.CardinalityEstimateMaxMemoryBytes(tenantID), e.limits.MaxEstimatedMemoryConsumptionPerQuery(tenantID))
		tracker = limiter.NewMemoryConsumptionTracker(ctx, limit, rejected, "cardinality estimate")
		ctx = limiter.AddMemoryTrackerToContext(ctx, tracker)
		defer func() { res.PeakMemoryBytes = tracker.PeakEstimatedMemoryConsumptionBytes() }()
	}
	now := e.now()
	needIngesters := false
	if e.ingesters != nil {
		needIngesters = ShouldQueryIngesters(e.limits.QueryIngestersWithin(tenantID), now, req.MaxT)
	}
	storeMaxT := req.MaxT
	if qsa := e.blocks.queryStoreAfter; qsa > 0 && needIngesters {
		storeMaxT = min(req.MaxT, util.TimeToMillis(now.Add(-qsa)))
	}

	// The blocks and the heads are asked at once, each into its own result.
	var blocks BoundsResult
	var head *BoundsHead
	g, gCtx := errgroup.WithContext(ctx)
	if storeMaxT > req.MinT {
		g.Go(func() error { return e.blocksBounds(gCtx, tenantID, req, storeMaxT, &blocks) })
	}
	if needIngesters {
		hc, ok := e.ingesters.(headCardinality)
		if !ok {
			return BoundsResult{}, refusedEstimate("the ingesters can't answer the head rows of bounds mode")
		}
		g.Go(func() error {
			var err error
			head, err = headBounds(gCtx, hc, req.GroupBy, tracker)
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return BoundsResult{}, err
	}
	peak := res.PeakMemoryBytes
	res = blocks
	res.PeakMemoryBytes = peak
	res.Head = head
	if storeMaxT < req.MaxT {
		res.NotCovered = append(res.NotCovered, estimateWindow{Start: max(req.MinT, storeMaxT), End: req.MaxT})
	}
	slices.SortFunc(res.NotCovered, func(a, b estimateWindow) int { return int(min(1, max(-1, a.Start-b.Start))) })
	return res, nil
}

// blocksBounds asks the store-gateways for per-block counts of the blocks in [req.MinT, storeMaxT)
// and writes the ranges, the holes and what the calls read into res.
func (e *CardinalityEstimator) blocksBounds(ctx context.Context, tenantID string, req *CardinalityEstimateRequest, storeMaxT int64, res *BoundsResult) error {
	q := e.blocks
	parts, all, err := q.seriesCountsParts(ctx, tenantID, req.MinT, storeMaxT)
	if err != nil {
		return err
	}
	res.NotCovered = append(res.NotCovered, coverageHoles(all, req.MinT, storeMaxT)...)
	if len(all) == 0 {
		return nil
	}
	marked := map[ulid.ULID]struct{}{}
	for _, p := range parts {
		f, ok := p.c.finder.(blocksMarkedForDeletionFinder)
		if !ok {
			continue
		}
		m, err := f.BlocksMarkedForDeletion(ctx, tenantID)
		if err != nil {
			return bucketIndexLookupError(err)
		}
		for id := range m {
			marked[id] = struct{}{}
		}
	}
	ranges, byID := planBounds(all, marked)

	tracker, _ := limiter.MemoryConsumptionTrackerFromContext(ctx)
	var (
		mtx          sync.Mutex
		counted      = map[ulid.ULID]bool{}
		markedCounts = map[ulid.ULID]map[string]int64{}
	)
	cov, err := q.runSeriesCountsCalls(ctx, tenantID, parts, func(ctx context.Context, c estimateCall) ([]ulid.ULID, error) {
		stream, err := c.client.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{
			BlockIds:  convertULIDsToString(c.ids),
			StartMs:   req.MinT,
			EndMs:     storeMaxT,
			GroupBy:   req.GroupBy,
			MaxGroups: req.MaxGroups,
			PerBlock:  true,
		})
		// The whole answer is read before any of it is folded, so a retried call is not counted twice.
		var (
			resp    *storegatewaypb.SeriesCountsResponse
			release func()
		)
		if err == nil {
			resp, release, err = receiveSeriesCounts(stream, tracker)
		}
		if err != nil {
			if own := (estimateError{}); errors.As(err, &own) {
				return nil, err
			}
			return nil, classifyStoreGatewayError(c.client.RemoteAddress(), err)
		}
		defer release()
		ids, err := parseBlockIDs(c.client.RemoteAddress(), resp.BlockIds)
		if err != nil {
			return nil, err
		}
		asked := make(map[ulid.ULID]bool, len(c.ids))
		for _, id := range c.ids {
			asked[id] = true
		}
		perBlock := map[ulid.ULID]map[string]int64{}
		for _, grp := range resp.Groups {
			id, err := ulid.Parse(grp.BlockId)
			if err != nil || !asked[id] || len(grp.Counts) != 1 {
				return nil, withSourceAddr(c.client.RemoteAddress(), fmt.Errorf("a store-gateway answered a per-block count with a group for block %q and %d counts", grp.BlockId, len(grp.Counts)))
			}
			if perBlock[id] == nil {
				perBlock[id] = map[string]int64{}
			}
			perBlock[id][grp.Value] += grp.Counts[0]
		}
		mtx.Lock()
		defer mtx.Unlock()
		var answered []ulid.ULID
		for _, id := range ids {
			bb := byID[id]
			if bb == nil || !asked[id] || counted[id] {
				continue
			}
			counts := perBlock[id]
			// The range's tables, while folding and in the answer, hold up to six entries per value.
			var b uint64
			for v := range counts {
				if _, ok := bb.rng.single[v]; !ok {
					b += 6 * uint64(estimateGroupBytes+len(v))
				}
			}
			if tracker != nil && b > 0 {
				if err := tracker.IncreaseMemoryConsumption(b, limiter.CardinalityEstimateGroups); err != nil {
					return nil, refusedByLimit(refusedByMemory, "the bounds this estimate holds in the querier: %w", err)
				}
			}
			bb.fold(counts, markedCounts)
			counted[id] = true
			answered = append(answered, id)
		}
		res.SeriesCounted += resp.SeriesCounted
		res.IndexBytes += resp.IndexBytes
		res.PostingsOffsetsTableBytes += resp.PostingsOffsetsTableBytes
		res.PostingsFetchedBytes += resp.PostingsFetchedBytes
		res.SeriesFetchedBytes += resp.SeriesFetchedBytes
		return answered, nil
	})
	if err != nil {
		return err
	}
	if len(counted) == 0 {
		return errNothingCounted(len(all), cov)
	}
	res.StoreGateways, res.Retries = cov.StoreGateways, cov.Retries
	res.Ranges = finishBounds(ranges, counted, markedCounts)
	return nil
}

// headBounds asks the ingesters for the in-memory and the active series of each value of groupBy.
func headBounds(ctx context.Context, hc headCardinality, groupBy string, tracker *limiter.MemoryConsumptionTracker) (*BoundsHead, error) {
	head := &BoundsHead{Approximate: !hc.IngestStorageEnabled()}
	g, gCtx := errgroup.WithContext(ctx)
	for _, m := range []struct {
		method cardinality.CountMethod
		into   *map[string]int64
	}{{cardinality.InMemoryMethod, &head.InMemory}, {cardinality.ActiveMethod, &head.Active}} {
		g.Go(func() error {
			_, resp, err := hc.LabelValuesCardinality(gCtx, []model.LabelName{model.LabelName(groupBy)}, nil, m.method)
			if err != nil {
				return classifyIngesterError(err)
			}
			out := map[string]int64{}
			var b uint64
			for _, item := range resp.Items {
				if item.LabelName != groupBy {
					continue
				}
				for v, n := range item.LabelValueSeries {
					out[v] = int64(n)
					b += uint64(estimateGroupBytes + len(v))
				}
			}
			if tracker != nil && b > 0 {
				if err := tracker.IncreaseMemoryConsumption(b, limiter.CardinalityEstimateGroups); err != nil {
					return refusedByLimit(refusedByMemory, "the head rows this estimate holds in the querier: %w", err)
				}
			}
			*m.into = out
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return head, nil
}
