// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// CardinalityEstimateRequest asks for series counts over [MinT, MaxT), see
// storegatewaypb.SeriesCountsRequest for the fields.
type CardinalityEstimateRequest struct {
	Matchers   []*labels.Matcher
	MinT, MaxT int64
	Step       int64
	GroupBy    string
	// MaxIndexBytes caps the index bytes the whole request reads, 0 for
	// none. It is split evenly across the store-gateway calls.
	MaxIndexBytes int64
	// SketchAbove has each store-gateway send an HLL sketch instead of
	// hashes for a group with more series than this, when deduplicating.
	// 0 keeps every group exact.
	SketchAbove int64
	// Snap widens a window inside one block range to that range.
	Snap bool
	// Shards splits every store-gateway call into this many calls, each
	// counting one series-hash shard, as query sharding does for Series.
	// 0 or 1 sends one call.
	Shards int
}

// CardinalityEstimateResult holds one count per bucket for each group.
type CardinalityEstimateResult struct {
	// Read is readIndexHeader, readFullIndex or readHeadPostings.
	Read string
	// Source is sourceBlocks or sourceHead.
	Source string
	// HeadWhole is set when the counts are every series in the ingesters'
	// heads, since head postings can't be filtered by time.
	HeadWhole bool
	// HeadFallback, when set, is why the head walk was refused and the head
	// was counted as one bucket from its postings instead.
	HeadFallback string
	// CoverageGap is set when neither the blocks nor the ingesters hold part
	// of the window, so the counts are a lower bound.
	CoverageGap bool
	// HeadMinTime, when set, is the time from which every ingester still
	// holds data, after the window's start, so the counts are a lower bound.
	HeadMinTime int64
	Snapped     bool
	Counts      map[string][]int64
	// Dedup is set when the blocks may share series, so the counts come
	// from a union of label-set hashes rather than a sum.
	Dedup  bool
	Blocks []ulid.ULID
	// LowerBound is set when some blocks were not counted by any
	// store-gateway after every retry; MissingBlocks lists them. The counts
	// then leave out those blocks' series.
	LowerBound    bool
	MissingBlocks []ulid.ULID
	// Retries is the number of attempts after the first.
	Retries              int
	StoreGateways        int
	SeriesCounted        int64
	PostingsFetchedBytes int64
	SeriesFetchedBytes   int64
	// IndexBytes is measured the way the Series call measures it.
	IndexBytes int64
	// BlocksFromPostings is how many blocks were counted from the group-by
	// label's postings without reading series entries.
	BlocksFromPostings int64
	// Estimated holds the groups counted from an HLL sketch, not exactly.
	Estimated map[string]bool
	// HashBytes and SketchBytes are the dedup payload the store-gateways sent.
	HashBytes, SketchBytes int64
}

// SeriesCounts counts the matching series with a chunk in the window, from
// the store-gateways. When every block covering the window is in one block
// range and the blocks share no series, the store-gateways' counts are
// added up. Otherwise, for example over several days, each store-gateway
// returns label-set hashes and the counts are the size of their union.
// Per-bucket counts need the first case.
func (q *BlocksStoreQueryable) SeriesCounts(ctx context.Context, tenantID string, req CardinalityEstimateRequest) (CardinalityEstimateResult, error) {
	parts, all, err := q.estimateParts(ctx, tenantID, req.MinT, req.MaxT)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	return q.seriesCounts(ctx, tenantID, req, parts, all)
}

// seriesCounts answers req from the blocks in parts (all is the same blocks
// flattened).
func (q *BlocksStoreQueryable) seriesCounts(ctx context.Context, tenantID string, req CardinalityEstimateRequest, parts []estimatePart, all bucketindex.Blocks) (CardinalityEstimateResult, error) {
	dedup := !inOneRangeWithoutSharedSeries(all)
	m := newEstimateMerger(ctx, dedup, q.estimateCfg.MaxDedupSeries)
	res, err := q.seriesCountsInto(ctx, tenantID, req, parts, all, dedup, m)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	if err := m.finish(&res); err != nil {
		return CardinalityEstimateResult{}, err
	}
	return res, nil
}

// seriesCountsInto sends req to the store-gateways for the blocks in parts
// and adds every group they return to m: label-set hashes and sketches when
// dedup is set, counts otherwise. The result carries everything but the
// counts, which m.finish writes, so a caller can add another source's
// groups to m first.
func (q *BlocksStoreQueryable) seriesCountsInto(ctx context.Context, tenantID string, req CardinalityEstimateRequest, parts []estimatePart, all bucketindex.Blocks, dedup bool, m *estimateMerger) (CardinalityEstimateResult, error) {
	res := CardinalityEstimateResult{Counts: map[string][]int64{}, Dedup: dedup}
	if res.Dedup && req.Step > 0 {
		return CardinalityEstimateResult{}, badEstimateRequest("per-bucket counts need a window inside one block range whose blocks share no series")
	}
	matchers, err := storepb.PromMatchersToMatchers(req.Matchers...)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	pbMatchers := make([]*storepb.LabelMatcher, len(matchers))
	for i := range matchers {
		pbMatchers[i] = &matchers[i]
	}
	shards := max(req.Shards, 1)
	// One matcher list per shard. Shards share no series, so their counts
	// add up and their hashes union to the same total as one call.
	shardMatchers := make([][]*storepb.LabelMatcher, shards)
	for i := range shards {
		if shards == 1 {
			shardMatchers[i] = pbMatchers
			continue
		}
		sel := sharding.ShardSelector{ShardIndex: uint64(i), ShardCount: uint64(shards)}
		m := sel.Matcher()
		pm, err := storepb.PromMatchersToMatchers(m)
		if err != nil {
			return CardinalityEstimateResult{}, err
		}
		shardMatchers[i] = append(append([]*storepb.LabelMatcher{}, pbMatchers...), &pm[0])
	}

	var mtx sync.Mutex
	merge := func(addr string, resp *storegatewaypb.SeriesCountsResponse) error {
		mtx.Lock()
		defer mtx.Unlock()
		for _, g := range resp.Groups {
			if err := m.add("store-gateway "+addr, g.Value, g.Counts, g.Hashes, g.Sketch); err != nil {
				return err
			}
		}
		res.SeriesCounted += resp.SeriesCounted
		res.PostingsFetchedBytes += resp.PostingsFetchedBytes
		res.SeriesFetchedBytes += resp.SeriesFetchedBytes
		res.IndexBytes += resp.IndexBytes
		res.BlocksFromPostings += resp.BlocksFromPostings
		return nil
	}

	cov, err := q.runEstimateCalls(ctx, tenantID, parts, shards, req.MaxIndexBytes, func(ctx context.Context, c estimateCall, perCall int64, calls int) (estimateCallResult, error) {
		resp, err := c.client.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{
			BlockIds:      convertULIDsToString(c.ids),
			Matchers:      shardMatchers[c.shard],
			MinTime:       req.MinT,
			MaxTime:       req.MaxT,
			StepMs:        req.Step,
			GroupBy:       req.GroupBy,
			Hashes:        res.Dedup,
			MaxIndexBytes: perCall,
			SketchAbove:   req.SketchAbove,
		})
		if err != nil {
			err = classifyStoreGatewayError(c.client.RemoteAddress(), err)
			if estimateHTTPStatus(err) == http.StatusUnprocessableEntity && req.MaxIndexBytes > 0 {
				return estimateCallResult{}, refusedEstimate("max_index_bytes %d, split into %d bytes for each of %d store-gateway calls: %w", req.MaxIndexBytes, perCall, calls, err)
			}
			return estimateCallResult{}, err
		}
		counted, err := parseBlockIDs(c.client.RemoteAddress(), resp.BlockIds)
		if err != nil {
			return estimateCallResult{}, err
		}
		if err := merge(c.client.RemoteAddress(), resp); err != nil {
			return estimateCallResult{}, err
		}
		return estimateCallResult{counted: counted, indexBytes: resp.IndexBytes}, nil
	})
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	if err := res.setCoverage(all, cov); err != nil {
		return CardinalityEstimateResult{}, err
	}

	return res, nil
}

// setCoverage records which of the blocks in all were counted. A missing
// block makes the counts a lower bound; when no block was counted at all
// there is nothing to bound, and the request fails.
func (r *CardinalityEstimateResult) setCoverage(all bucketindex.Blocks, cov estimateCoverage) error {
	missing := make(map[ulid.ULID]bool, len(cov.Missing))
	for _, id := range cov.Missing {
		missing[id] = true
	}
	if len(all) > 0 && len(missing) == len(all) {
		return errNothingCounted(len(all), cov)
	}
	for _, b := range all {
		if !missing[b.ID] {
			r.Blocks = append(r.Blocks, b.ID)
		}
	}
	r.MissingBlocks = cov.Missing
	r.LowerBound = len(cov.Missing) > 0
	r.StoreGateways = cov.StoreGateways
	r.Retries = cov.Retries
	return nil
}

// parseBlockIDs parses the block IDs a store-gateway reported as counted.
func parseBlockIDs(addr string, ids []string) ([]ulid.ULID, error) {
	out := make([]ulid.ULID, 0, len(ids))
	for _, id := range ids {
		parsed, err := ulid.Parse(id)
		if err != nil {
			return nil, fmt.Errorf("store-gateway %s returned block ID %q: %w", addr, id, err)
		}
		out = append(out, parsed)
	}
	return out, nil
}

// splitIndexBytes gives each of n calls an equal share of limit, at least one
// byte, so the request's total stays under limit. 0 stays unlimited.
func splitIndexBytes(limit int64, n int) int64 {
	if limit <= 0 || n <= 1 {
		return limit
	}
	return max(1, limit/int64(n))
}

// inOneRangeWithoutSharedSeries reports whether every block covers the same
// range and the blocks are a single block or disjoint shards.
func inOneRangeWithoutSharedSeries(blocks bucketindex.Blocks) bool {
	for _, b := range blocks {
		if b.MinTime != blocks[0].MinTime || b.MaxTime != blocks[0].MaxTime {
			return false
		}
	}
	return checkDisjointShards(blocks) == nil
}

// Reads a cardinality estimate can use, as reported in the response and in
// the duration metric's path label.
const (
	readIndexHeader = "index_header"
	readFullIndex   = "full_index"
	readChunks      = "chunks" // the compare=true reference only
)

// CardinalityEstimate answers req with the cheapest read. With Snap, a
// window inside one block range is first widened to that range, and req is
// updated to say so. A window of exactly one block range whose blocks share
// no series, asked for every metric name with no matchers and no step, is
// counted from the index-headers. Anything else reads the full index.
func (q *BlocksStoreQueryable) CardinalityEstimate(ctx context.Context, tenantID string, req *CardinalityEstimateRequest) (CardinalityEstimateResult, error) {
	parts, blocks, err := q.estimateBlocks(ctx, tenantID, req.MinT, req.MaxT)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	snapped := false
	if req.Snap {
		lo, hi, err := blockRangeHolding(blocks, req.MinT, req.MaxT)
		if err != nil {
			return CardinalityEstimateResult{}, err
		}
		snapped = lo != req.MinT || hi != req.MaxT
		req.MinT, req.MaxT = lo, hi
		if parts, blocks, err = q.estimateBlocks(ctx, tenantID, lo, hi); err != nil {
			return CardinalityEstimateResult{}, err
		}
	}
	byName := req.GroupBy == "" || req.GroupBy == labels.MetricName
	if byName && len(req.Matchers) == 0 && req.Step == 0 && len(blocks) > 0 && checkMetricNameCountsBlocks(blocks, req.MinT, req.MaxT) == nil {
		mc, err := q.metricNameCounts(ctx, tenantID, parts)
		if err != nil {
			return CardinalityEstimateResult{}, err
		}
		res := CardinalityEstimateResult{
			Read: readIndexHeader, Snapped: snapped, Counts: make(map[string][]int64, len(mc.Counts)), Blocks: mc.Blocks, StoreGateways: mc.StoreGateways,
			LowerBound: len(mc.MissingBlocks) > 0, MissingBlocks: mc.MissingBlocks, Retries: mc.Retries,
		}
		for name, n := range mc.Counts {
			res.Counts[name] = []int64{n}
		}
		return res, nil
	}
	res, err := q.seriesCounts(ctx, tenantID, *req, parts, blocks)
	res.Read, res.Snapped = readFullIndex, snapped
	return res, err
}

// estimateBlocks finds the blocks for [minT, maxT). When they do not form
// one block range of series-disjoint blocks, but do once the blocks marked
// for deletion are left out, the marked blocks are dropped: right after a
// compaction the finder lists both the sources and their compacted output
// for -blocks-storage.bucket-store.ignore-deletion-marks-delay, and the
// output holds every series of the sources.
func (q *BlocksStoreQueryable) estimateBlocks(ctx context.Context, tenantID string, minT, maxT int64) ([]estimatePart, bucketindex.Blocks, error) {
	parts, blocks, err := q.estimateParts(ctx, tenantID, minT, maxT)
	if err != nil {
		return nil, nil, err
	}
	if inOneRangeWithoutSharedSeries(blocks) {
		return parts, blocks, nil
	}
	kept, keptBlocks, dropped, err := q.withoutMarkedForDeletion(ctx, tenantID, parts)
	if err != nil {
		return nil, nil, err
	}
	if dropped > 0 && len(keptBlocks) > 0 && inOneRangeWithoutSharedSeries(keptBlocks) {
		return kept, keptBlocks, nil
	}
	return parts, blocks, nil
}

func blockRangeHolding(blocks bucketindex.Blocks, minT, maxT int64) (int64, int64, error) {
	if len(blocks) == 0 {
		return minT, maxT, nil
	}
	lo, hi := blocks[0].MinTime, blocks[0].MaxTime
	for _, b := range blocks {
		if b.MinTime != lo || b.MaxTime != hi {
			return 0, 0, badEstimateRequest("the window [%d, %d) touches more than one block range, so it can't be snapped to one", minT, maxT)
		}
	}
	if lo > minT || hi < maxT {
		return 0, 0, badEstimateRequest("the blocks cover [%d, %d), which doesn't hold the whole window [%d, %d)", lo, hi, minT, maxT)
	}
	return lo, hi, nil
}
