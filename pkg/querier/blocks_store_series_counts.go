// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// CardinalityEstimateRequest asks for series counts over [MinT, MaxT), see
// storegatewaypb.SeriesCountsRequest for the fields.
type CardinalityEstimateRequest struct {
	Matchers   []*labels.Matcher
	MinT, MaxT int64
	Step       int64
	// Window is each bucket's width with Step, 0 for Step; see
	// cardinality.Buckets. [MinT, MaxT) is then the data read, and the
	// first bucket ends at MinT+Window.
	Window  int64
	GroupBy string
	// MaxIndexBytes caps the index bytes the whole request reads, 0 for
	// none. It is split evenly across the store-gateway calls.
	MaxIndexBytes int64
	// MaxHashes caps the label-set hashes a deduplicating request may hold,
	// 0 for none. Each source gets a share and refuses past it. Estimate sets
	// it from the tenant's memory limit, at estimateHashBytes per hash.
	MaxHashes int64
	// SketchAbove has each store-gateway send an HLL sketch instead of
	// hashes for a group with more series than this, when deduplicating.
	// 0 keeps every group exact.
	SketchAbove int64
	// Snap widens a window inside one block range to that range.
	Snap bool
	// StreamNames has the groups of a request over one block range, for any
	// GroupBy, come back as an open table merged from the store-gateways'
	// SeriesCounts streams, in the result's names, instead of in Counts, so
	// the caller can merge it without holding it. The caller must close it.
	StreamNames bool
}

// windowMs is the window_ms to send: 0 when the buckets are the step wide,
// so a source that doesn't know the field counts them the same way.
func (r CardinalityEstimateRequest) windowMs() int64 {
	if r.Step == 0 || r.Window == r.Step {
		return 0
	}
	return r.Window
}

// checkWindowEcho refuses an answer counted with a different window_ms than
// asked, which comes from a source that doesn't know the field, such as an
// older version during a rolling update: it counts adjacent step-wide
// buckets instead.
func checkWindowEcho(source string, asked, got int64) error {
	if asked == got {
		return nil
	}
	return olderVersionEstimate("%s counted with window_ms %d instead of the %d asked for; it may run an older version, retry once every %s is updated", aSource(source), got, asked, source)
}

// expectedBuckets is how many counts each group of the answer carries: the
// buckets of the step and window, or 0 without a step. It fails when the step
// doesn't cut the window into whole buckets, which the handler doesn't check
// with snap, since a snapped window's buckets are known only once it is
// widened to a block range.
func (r CardinalityEstimateRequest) expectedBuckets() (int, error) {
	if r.Step <= 0 {
		return 0, nil
	}
	n, err := cardinality.Buckets(r.MinT, r.MaxT, r.Step, r.Window)
	if err != nil {
		return 0, badEstimateRequest("step: %v", err)
	}
	return n, nil
}

// CardinalityEstimateResult holds one count per bucket for each group.
type CardinalityEstimateResult struct {
	// Read is one of the Read values: how the sources were read.
	Read string
	// Source is one of the Source values: where the counts come from.
	Source string
	// HeadWhole is set when the counts are every series in the ingesters'
	// heads, since head postings can't be filtered by time.
	HeadWhole bool
	// HeadFallback, when set, is one of the HeadFallback values: why the
	// head walk was refused and the head was counted as one bucket from its
	// postings instead; headFallbackErr is that refusal, kept for the log
	// line and for what its status detail says.
	HeadFallback    string
	headFallbackErr error
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
	// store-gateway after every retry, which MissingBlocks lists, or when a
	// block range lacks some of its shard blocks, which MissingShards
	// lists. The counts then leave out those blocks' series.
	LowerBound    bool
	MissingBlocks []ulid.ULID
	MissingShards []MissingShard
	// missingReason is the last store-gateway failure that left blocks
	// uncounted, for the request's log line; nil when MissingBlocks is empty
	// or no store-gateway failed.
	missingReason error
	// Retries is the number of attempts after the first.
	Retries              int
	StoreGateways        int
	SeriesCounted        int64
	PostingsFetchedBytes int64
	SeriesFetchedBytes   int64
	// IndexBytes is measured the way the Series call measures it.
	IndexBytes int64
	// PostingsOffsetsTableBytes is the postings offsets table the
	// store-gateways walked, which max_index_bytes bounds with IndexBytes.
	// With index-header version 2 it is read from object storage.
	PostingsOffsetsTableBytes int64
	// BlocksFromPostings is how many blocks were counted from the group-by
	// label's postings without reading series entries, and
	// BlocksFromOffsetsTable how many of those from the postings offsets
	// table alone, reading no postings either.
	BlocksFromPostings     int64
	BlocksFromOffsetsTable int64
	// Estimated holds the groups counted from an HLL sketch, not exactly.
	Estimated map[string]bool
	// HashBytes and SketchBytes are the dedup payload the store-gateways sent.
	HashBytes, SketchBytes int64
	// DedupHashes is the distinct label-set hashes the querier held to
	// deduplicate, the most of any bucket answered on its own.
	DedupHashes int64
	// names, when set, holds the counts instead of Counts: see
	// CardinalityEstimateRequest.StreamNames.
	names *nameTable
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
	m := newEstimateMerger(ctx, dedup)
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
	buckets, err := req.expectedBuckets()
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	m.buckets = buckets
	if res.Dedup && req.Step > 0 {
		return CardinalityEstimateResult{}, badEstimateRequest("%w: the window spans more than one block range, or blocks that may share series", errStepNeedsOneSource)
	}
	pbMatchers, err := toStoreMatchers(req.Matchers)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	tracker, _ := limiter.MemoryConsumptionTrackerFromContext(ctx)
	var mtx sync.Mutex
	merge := func(addr string, resp *storegatewaypb.SeriesCountsResponse) error {
		mtx.Lock()
		defer mtx.Unlock()
		if err := checkWindowEcho("store-gateway", req.windowMs(), resp.WindowMs); err != nil {
			return withSourceAddr(addr, err)
		}
		for _, g := range resp.Groups {
			if err := m.add("store-gateway", g.Value, g.Counts, g.Hashes, g.Sketch); err != nil {
				return withSourceAddr(addr, err)
			}
		}
		res.SeriesCounted += resp.SeriesCounted
		res.PostingsFetchedBytes += resp.PostingsFetchedBytes
		res.SeriesFetchedBytes += resp.SeriesFetchedBytes
		res.IndexBytes += resp.IndexBytes
		res.PostingsOffsetsTableBytes += resp.PostingsOffsetsTableBytes
		res.BlocksFromPostings += resp.BlocksFromPostings
		res.BlocksFromOffsetsTable += resp.BlocksFromOffsetsTable
		return nil
	}

	cov, err := q.runEstimateCalls(ctx, tenantID, parts, req.MaxIndexBytes, func(ctx context.Context, c estimateCall, perCall int64, calls int) (estimateCallResult, error) {
		// A store-gateway asked for hashes holds one per matching series,
		// so each call gets a share of the querier's limit on hashes: on a
		// retry, of what the hashes already merged leave of it, as
		// max_index_bytes is split from what earlier attempts left unread.
		var maxHashes int64
		if res.Dedup && req.MaxHashes > 0 {
			mtx.Lock()
			left := req.MaxHashes - m.held
			mtx.Unlock()
			if left <= 0 {
				return estimateCallResult{}, refusedByLimit(refusedByMaxHashes, "the %d hashes the tenant's cardinality estimate memory limit allows are held before every block was counted", req.MaxHashes)
			}
			maxHashes = splitIndexBytes(left, calls)
		}
		stream, err := c.client.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{
			BlockIds:      convertULIDsToString(c.ids),
			Matchers:      pbMatchers,
			MinTime:       req.MinT,
			MaxTime:       req.MaxT,
			StepMs:        req.Step,
			WindowMs:      req.windowMs(),
			GroupBy:       req.GroupBy,
			Hashes:        res.Dedup,
			MaxIndexBytes: perCall,
			SketchAbove:   req.SketchAbove,
			MaxHashes:     maxHashes,
			MaxGroups:     q.estimateCfg.MaxGroups,
		})
		// The whole answer is read before any of it is merged, so a call
		// that fails partway and is retried is not counted twice. What is
		// held while reading is charged message by message, and given back
		// once merged or when the call fails.
		var (
			resp    *storegatewaypb.SeriesCountsResponse
			release func()
		)
		if err == nil {
			resp, release, err = receiveSeriesCounts(stream, tracker)
		}
		if err != nil {
			err = classifyStoreGatewayError(c.client.RemoteAddress(), err)
			switch estimateRefusalLimit(err) {
			case refusedByMaxIndexBytes:
				if req.MaxIndexBytes > 0 {
					return estimateCallResult{}, refusedByLimit(refusedByMaxIndexBytes, "max_index_bytes %d, split into %d bytes for each of %d store-gateway calls: %w", req.MaxIndexBytes, perCall, calls, err)
				}
			case refusedByMaxHashes:
				return estimateCallResult{}, refusedByLimit(refusedByMaxHashes, "the %d hashes the tenant's cardinality estimate memory limit allows, split into %d for each of %d store-gateway calls: %w", req.MaxHashes, maxHashes, calls, err)
			}
			return estimateCallResult{}, err
		}
		counted, err := parseBlockIDs(c.client.RemoteAddress(), resp.BlockIds)
		if err != nil {
			return estimateCallResult{}, err
		}
		err = merge(c.client.RemoteAddress(), resp)
		release()
		if err != nil {
			return estimateCallResult{}, err
		}
		// max_index_bytes bounds the postings offsets table walked as well as index_bytes.
		return estimateCallResult{counted: counted, indexBytes: resp.IndexBytes + resp.PostingsOffsetsTableBytes}, nil
	})
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	if err := res.setCoverage(all, cov); err != nil {
		return CardinalityEstimateResult{}, err
	}

	return res, nil
}

// receiveSeriesCounts reads every message of a SeriesCounts stream into one
// response, charging each message's groups to tracker (which may be nil) as
// they arrive, so a reply larger than the tenant's memory limit is refused
// before it is held whole. On success the caller gives the charge back with
// release once the groups are merged; on an error nothing stays charged, so
// a retried call is not counted twice.
func receiveSeriesCounts(s storegatewaypb.StoreGateway_SeriesCountsClient, tracker *limiter.MemoryConsumptionTracker) (*storegatewaypb.SeriesCountsResponse, func(), error) {
	var (
		out     *storegatewaypb.SeriesCountsResponse
		charged uint64
	)
	release := func() {
		if tracker != nil && charged > 0 {
			tracker.DecreaseMemoryConsumption(charged, limiter.CardinalityEstimateGroups)
		}
		charged = 0
	}
	for {
		msg, err := s.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			release()
			return nil, nil, err
		}
		if tracker != nil {
			var b uint64
			for _, g := range msg.Groups {
				b += uint64(estimateGroupBytes + len(g.Value) + 8*len(g.Counts) + 8*len(g.Hashes) + len(g.Sketch))
			}
			if err := tracker.IncreaseMemoryConsumption(b, limiter.CardinalityEstimateGroups); err != nil {
				release()
				return nil, nil, refusedByLimit(refusedByMemory, "the store-gateway replies this estimate holds in the querier: %w", err)
			}
			charged += b
		}
		if out == nil {
			out = msg
			continue
		}
		out.Groups = append(out.Groups, msg.Groups...)
	}
	if out == nil {
		release()
		return nil, nil, errors.New("SeriesCounts stream ended without a message")
	}
	return out, release, nil
}

// setCoverage records which of the blocks in all were counted. A missing
// block, or a block range missing some of its shard blocks, makes the
// counts a lower bound; when no block was counted at all there is nothing
// to bound, and the request fails.
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
	if len(cov.Missing) > 0 {
		r.missingReason = cov.Unavailable
	}
	r.MissingShards = missingShards(all)
	r.LowerBound = len(cov.Missing) > 0 || len(r.MissingShards) > 0
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
			return nil, withSourceAddr(addr, fmt.Errorf("a store-gateway returned block ID %q: %w", id, err))
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

// CardinalityEstimate answers req from the store-gateways. With Snap, a
// window inside one block range is first widened to that range, and req is
// updated to say so. A window of one block range whose blocks share no
// series, with no step, is merged as the store-gateways stream it when
// StreamNames is set; anything else is collected. The store-gateways count a
// block inside the window with no matchers from its postings offsets table
// alone, and the answer's read says whether every block was.
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
	// Streams merge by summing each value's counts, which is right only for
	// blocks that share no series and one count per group.
	if req.StreamNames && req.Step == 0 && len(blocks) > 0 && inOneRangeWithoutSharedSeries(blocks) {
		names, res, err := q.openNameTable(ctx, tenantID, parts, blocks, *req)
		if err != nil {
			return CardinalityEstimateResult{}, err
		}
		res.names, res.Snapped = names, snapped
		return res, nil
	}
	res, err := q.seriesCounts(ctx, tenantID, *req, parts, blocks)
	res.Read, res.Snapped = estimateRead(res), snapped
	return res, err
}

// estimateRead is the read the store-gateways made for res: the
// index-headers alone when they counted every block from its postings
// offsets table, the full index otherwise.
func estimateRead(res CardinalityEstimateResult) string {
	if len(res.Blocks) > 0 && res.BlocksFromOffsetsTable == int64(len(res.Blocks)) {
		return ReadIndexHeader
	}
	return ReadFullIndex
}

// toStoreMatchers converts matchers to the store-gateway's.
func toStoreMatchers(matchers []*labels.Matcher) ([]*storepb.LabelMatcher, error) {
	ms, err := storepb.PromMatchersToMatchers(matchers...)
	if err != nil {
		return nil, err
	}
	out := make([]*storepb.LabelMatcher, len(ms))
	for i := range ms {
		out[i] = &ms[i]
	}
	return out, nil
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
