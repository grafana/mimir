// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/grafana/dskit/grpcutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// The values of the response's source field, also the source label of the estimate metrics.
const (
	// SourceBlocks: the store-gateways' blocks.
	SourceBlocks = "blocks"
	// SourceHead: the ingesters' heads.
	SourceHead = "head"
	// SourceBlocksAndHead: both.
	SourceBlocksAndHead = "blocks+head"
)

// The values of the response's read field, also the path label of the estimate metrics.
const (
	// ReadIndexHeader: every block counted from its index-header's postings offsets table.
	ReadIndexHeader = "index_header"
	// ReadFullIndex: some block counted from its series entries and chunk metas.
	ReadFullIndex = "full_index"
	// ReadHeadChunkMetas: the matching head series' chunk metas.
	ReadHeadChunkMetas = "head_chunk_metas"
	// ReadFullIndexAndHeadChunkMetas: blocks from their full index, the head from its chunk metas.
	ReadFullIndexAndHeadChunkMetas = "full_index+head_chunk_metas"
	// ReadPerBucket: each bucket answered as its own window.
	ReadPerBucket = "per_bucket"
)

// estimateIngesters is the part of the distributor the estimate uses.
type estimateIngesters interface {
	SeriesCounts(ctx context.Context, matchers []*labels.Matcher, req *client.SeriesCountsRequest) (distributor.SeriesCountsResult, error)
}

// estimateLimits are the per-tenant limits of an estimate.
type estimateLimits interface {
	CardinalityEstimateEnabled(userID string) bool
	CardinalityEstimateMaxIndexBytes(userID string) int64
	CardinalityEstimateMaxMemoryBytes(userID string) uint64
	CardinalityEstimateMaxBucketsAnsweredSeparately(userID string) int
	CardinalityEstimateMaxGroups(userID string) int64
	QueryIngestersWithin(userID string) time.Duration
	MaxQueryLookback(userID string) time.Duration
	CompactorBlocksRetentionPeriod(userID string) time.Duration
	MaxEstimatedMemoryConsumptionPerQuery(userID string) uint64
	// MaxLabelNameLength is the longest group_by.
	MaxLabelNameLength(userID string) int
}

// CardinalityEstimator answers a cardinality estimate from the sources a query over the same window would read.
type CardinalityEstimator struct {
	blocks    *BlocksStoreQueryable
	ingesters estimateIngesters
	limits    estimateLimits
	now       func() time.Time
}

// NewCardinalityEstimator returns an estimator over blocks and, when ingesters is not nil, the ingesters.
func NewCardinalityEstimator(blocks *BlocksStoreQueryable, ingesters estimateIngesters, limits estimateLimits) *CardinalityEstimator {
	return &CardinalityEstimator{blocks: blocks, ingesters: ingesters, limits: limits, now: time.Now}
}

// enabledFor reports whether tenantID may use the estimate.
func (e *CardinalityEstimator) enabledFor(tenantID string) bool {
	return e.limits.CardinalityEstimateEnabled(tenantID)
}

// defaultMaxGroupByLength caps group_by when the tenant's label name length is unset.
const defaultMaxGroupByLength = 1024

// maxGroupByLength is the longest group_by the tenant can ask for.
func (e *CardinalityEstimator) maxGroupByLength(tenantID string) int {
	if n := e.limits.MaxLabelNameLength(tenantID); n > 0 {
		return n
	}
	return defaultMaxGroupByLength
}

// clampToLookback moves req.MinT up to the oldest time the tenant's queries may read, and records that time in req for a later snap. With a step, whole buckets are dropped so the ones left keep their boundaries. It reports false when none of the window is left.
func (e *CardinalityEstimator) clampToLookback(spanLog *spanlogger.SpanLogger, tenantID string, req *CardinalityEstimateRequest) bool {
	now := util.TimeToMillis(e.now())
	minT := clampMinTime(spanLog, req.MinT, now, -e.limits.MaxQueryLookback(tenantID), "max query lookback")
	minT = clampMinTime(spanLog, minT, now, -e.limits.CompactorBlocksRetentionPeriod(tenantID), "blocks retention period")
	for _, d := range []time.Duration{e.limits.MaxQueryLookback(tenantID), e.limits.CompactorBlocksRetentionPeriod(tenantID)} {
		if d > 0 {
			req.oldestT = max(req.oldestT, now-d.Milliseconds())
		}
	}
	// A snapped window is cut into buckets once widened to its block range.
	width := int64(1)
	if req.Step > 0 {
		minT = req.MinT + (minT-req.MinT+req.Step-1)/req.Step*req.Step
		if !req.Snap {
			width = max(req.Step, req.Window)
		}
	}
	if minT+width > req.MaxT {
		return false
	}
	req.MinT = minT
	return true
}

// Estimate answers req for tenantID. With Snap, req is widened to its block range and updated.
func (e *CardinalityEstimator) Estimate(ctx context.Context, tenantID string, req *CardinalityEstimateRequest) (CardinalityEstimateResult, error) {
	// The tenant's limit bounds max_index_bytes before it is split across the sources.
	req.MaxIndexBytes = cardinality.BoundMaxIndexBytes(req.MaxIndexBytes, e.limits.CardinalityEstimateMaxIndexBytes(tenantID))
	req.MaxGroups = cardinality.BoundMaxGroups(req.MaxGroups, e.limits.CardinalityEstimateMaxGroups(tenantID))
	// The groups and hashes held in the querier count against the memory limit.
	if _, err := limiter.MemoryConsumptionTrackerFromContext(ctx); err != nil {
		rejected := e.blocks.metrics.estimateMemoryRefusalsOrDiscard()
		limit := estimateMemoryLimit(e.limits.CardinalityEstimateMaxMemoryBytes(tenantID), e.limits.MaxEstimatedMemoryConsumptionPerQuery(tenantID))
		ctx = limiter.AddMemoryTrackerToContext(ctx, limiter.NewMemoryConsumptionTracker(ctx, limit, rejected, "cardinality estimate"))
	}
	// The same limit sets how many hashes each source may send. A lower budget already on the request is kept.
	if budget := estimateHashBudget(e.limits.CardinalityEstimateMaxMemoryBytes(tenantID), e.limits.MaxEstimatedMemoryConsumptionPerQuery(tenantID)); req.MaxHashes == 0 || (budget > 0 && budget < req.MaxHashes) {
		req.MaxHashes = budget
	}
	// Pick the sources the way a query does. Block metadata alone can't say the blocks are complete.
	now := e.now()
	needIngesters := false
	// heldFrom is where the ingesters are sure to hold everything written.
	heldFrom := int64(0)
	if e.ingesters != nil {
		within := e.limits.QueryIngestersWithin(tenantID)
		needIngesters = ShouldQueryIngesters(within, now, req.MaxT)
		if within > 0 {
			heldFrom = util.TimeToMillis(now.Add(-within))
		}
	}
	storeMaxT := req.MaxT
	if qsa := e.blocks.queryStoreAfter; qsa > 0 {
		storeMaxT = min(req.MaxT, util.TimeToMillis(now.Add(-qsa)))
	}

	trusted := needIngesters && e.compactedBlocksHold(ctx, tenantID, req, now)
	if trusted {
		needIngesters = false
	}

	var (
		res CardinalityEstimateResult
		err error
	)
	var source string
	switch {
	case !needIngesters:
		source = SourceBlocks
		res, err = e.blocksOnly(ctx, tenantID, req)
	case storeMaxT <= req.MinT:
		source = SourceHead
		res, err = e.headCounts(ctx, req, heldFrom)
	default:
		source = SourceBlocksAndHead
		res, err = e.blocksAndHeadCounts(ctx, tenantID, req, storeMaxT, heldFrom)
	}
	if req.Step > 0 && errors.Is(err, errStepNeedsOneSource) {
		// The blocks path may have snapped req before refusing the step.
		out, err := e.perBucket(ctx, tenantID, req)
		out.Snapped = res.Snapped
		return out, err
	}
	// A failed estimate still says which sources it asked, for the metrics.
	if err != nil && res.Source == "" {
		res.Source = source
	}
	res.TrustedCompactedBlocks = trusted
	return res, err
}

// estimateHashBudget is how many label-set hashes a deduplicating estimate may hold, or 0 for no limit.
func estimateHashBudget(estimate, query uint64) int64 {
	return cardinality.HashBudget(estimateMemoryLimit(estimate, query))
}

// estimateMemoryLimit is the memory an estimate may hold in the querier, the lower of the limits that are set.
func estimateMemoryLimit(estimate, query uint64) uint64 {
	if query > 0 && (estimate == 0 || query < estimate) {
		return query
	}
	return estimate
}

// maxPerBucketEstimates is the most buckets perBucket may answer one by one, 0 for no limit.
func (e *CardinalityEstimator) maxPerBucketEstimates(tenantID string) int {
	return e.limits.CardinalityEstimateMaxBucketsAnsweredSeparately(tenantID)
}

// perBucket answers a per-bucket count whose window crosses sources that may share series, by answering each step-wide bucket as its own window.
func (e *CardinalityEstimator) perBucket(ctx context.Context, tenantID string, req *CardinalityEstimateRequest) (CardinalityEstimateResult, error) {
	n, err := cardinality.Buckets(req.MinT, req.MaxT, req.Step, req.Window)
	if err != nil {
		return CardinalityEstimateResult{}, badEstimateRequest("%v", err)
	}
	width := req.Window
	if width == 0 {
		width = req.Step
	}
	if limit := e.maxPerBucketEstimates(tenantID); limit > 0 && n > limit {
		return CardinalityEstimateResult{}, refusedByLimit(refusedByMaxBucketsAnsweredSeparately, "the window crosses block ranges or the cut between the store-gateways and the ingesters, or the ingesters' counts can't be added up across them, so its %d buckets would be answered one by one, more than %d (the -querier.cardinality-estimate-max-buckets-answered-separately limit); use a larger step, or a window inside one block range or inside the ingesters' time", n, limit)
	}
	out := CardinalityEstimateResult{Read: ReadPerBucket, Counts: map[string][]int64{}}
	sources := map[string]bool{}
	seenBlocks := map[ulid.ULID]bool{}
	seenMissing := map[ulid.ULID]bool{}
	seenShards := map[missingShard]bool{}
	// Each bucket's groups and hashes are charged to the memory tracker and given back once its counts are in the table.
	tracker, _ := limiter.MemoryConsumptionTrackerFromContext(ctx)
	held := func() uint64 {
		if tracker == nil {
			return 0
		}
		return tracker.CurrentEstimatedMemoryConsumptionBytesBySource(limiter.CardinalityEstimateGroups)
	}
	for i := range n {
		sub := *req
		sub.MinT = req.MinT + int64(i)*req.Step
		sub.MaxT = sub.MinT + width
		sub.Step, sub.Window, sub.Snap = 0, 0, false
		if req.MaxIndexBytes > 0 {
			sub.MaxIndexBytes = max(1, req.MaxIndexBytes/int64(n))
		}
		before := held()
		// A bucket may hold the hash budget less what the tracker holds. A spent budget is the max_hashes refusal.
		if req.MaxHashes > 0 {
			heldHashes := int64((before + estimateHashBytes - 1) / estimateHashBytes)
			if heldHashes >= req.MaxHashes {
				return CardinalityEstimateResult{}, refusedByLimit(refusedByMaxHashes, "bucket [%d, %d): the %d hashes the tenant's cardinality estimate memory limit allows are held by the per-bucket table before this bucket was counted", sub.MinT, sub.MaxT, req.MaxHashes)
			}
			sub.MaxHashes = req.MaxHashes - heldHashes
		}
		res, err := e.Estimate(ctx, tenantID, &sub)
		if err != nil {
			return CardinalityEstimateResult{}, fmt.Errorf("bucket [%d, %d): %w", sub.MinT, sub.MaxT, err)
		}
		tableCharge := uint64(0)
		for v, c := range res.Counts {
			counts := out.Counts[v]
			if counts == nil {
				if err := chargeEstimateGroup(tracker, v, n); err != nil {
					return CardinalityEstimateResult{}, fmt.Errorf("bucket [%d, %d): %w", sub.MinT, sub.MaxT, err)
				}
				tableCharge += estimateGroupCharge(v, n)
				counts = make([]int64, n)
				out.Counts[v] = counts
			}
			counts[i] = c[0]
			if res.Estimated[v] {
				if out.Estimated == nil {
					out.Estimated = map[string]bool{}
				}
				out.Estimated[v] = true
			}
		}
		sources[res.Source] = true
		for _, id := range res.Blocks {
			if !seenBlocks[id] {
				seenBlocks[id] = true
				out.Blocks = append(out.Blocks, id)
			}
		}
		for _, id := range res.MissingBlocks {
			if !seenMissing[id] {
				seenMissing[id] = true
				out.MissingBlocks = append(out.MissingBlocks, id)
			}
		}
		if res.missingReason != nil {
			out.missingReason = res.missingReason
		}
		for _, s := range res.MissingShards {
			if !seenShards[s] {
				seenShards[s] = true
				out.MissingShards = append(out.MissingShards, s)
			}
		}
		out.LowerBound = out.LowerBound || res.LowerBound
		out.CoverageGap = out.CoverageGap || res.CoverageGap
		out.Dedup = out.Dedup || res.Dedup
		out.TrustedCompactedBlocks = out.TrustedCompactedBlocks || res.TrustedCompactedBlocks
		out.HeadMinTime = max(out.HeadMinTime, res.HeadMinTime)
		out.DedupHashes = max(out.DedupHashes, res.DedupHashes)
		out.StoreGateways = max(out.StoreGateways, res.StoreGateways)
		out.Retries += res.Retries
		out.SeriesCounted += res.SeriesCounted
		out.PostingsFetchedBytes += res.PostingsFetchedBytes
		out.SeriesFetchedBytes += res.SeriesFetchedBytes
		out.IndexBytes += res.IndexBytes
		out.PostingsOffsetsTableBytes += res.PostingsOffsetsTableBytes
		out.BlocksFromPostings += res.BlocksFromPostings
		out.HashBytes += res.HashBytes
		out.SketchBytes += res.SketchBytes
		// Give back what the bucket's estimate holds beyond the table's groups.
		if after := held(); after > before+tableCharge {
			tracker.DecreaseMemoryConsumption(after-before-tableCharge, limiter.CardinalityEstimateGroups)
		}
	}
	out.Source = unionSource(sources)
	return out, nil
}

// unionSource is the source of an answer made of parts counted from sources.
func unionSource(sources map[string]bool) string {
	blocks := sources[SourceBlocks] || sources[SourceBlocksAndHead]
	head := sources[SourceHead] || sources[SourceBlocksAndHead]
	switch {
	case blocks && head:
		return SourceBlocksAndHead
	case head:
		return SourceHead
	default:
		return SourceBlocks
	}
}

// compactedBlocksHold reports whether blocks written by the compactor cover a window that ended long enough ago to answer it alone. Under ingest storage they can miss a lagging partition, hence the default of 0.
func (e *CardinalityEstimator) compactedBlocksHold(ctx context.Context, tenantID string, req *CardinalityEstimateRequest, now time.Time) bool {
	trust := e.blocks.estimateCfg.TrustCompactedBlocksAfter
	if trust <= 0 || req.MaxT > util.TimeToMillis(now.Add(-trust)) {
		return false
	}
	_, blocks, err := e.blocks.seriesCountsBlocks(ctx, tenantID, req.MinT, req.MaxT)
	if err != nil || len(blocks) == 0 {
		return false
	}
	for _, b := range blocks {
		if b.CompactionLevel < 2 {
			return false
		}
	}
	if !req.Snap {
		return blocksCover(blocks, req.MinT, req.MaxT)
	}
	// A snapped window is counted over its whole block range, which must have ended long enough ago too. A window that can't be snapped is left to the ingesters.
	_, hi, err := blockRangeHolding(blocks, req.MinT, req.MaxT)
	return err == nil && hi <= util.TimeToMillis(now.Add(-trust))
}

// blocksOnly answers a window that ends before the ingesters' range from the blocks. Partial coverage makes the counts a lower bound.
func (e *CardinalityEstimator) blocksOnly(ctx context.Context, tenantID string, req *CardinalityEstimateRequest) (CardinalityEstimateResult, error) {
	_, blocks, err := e.blocks.seriesCountsBlocks(ctx, tenantID, req.MinT, req.MaxT)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	if len(blocks) == 0 {
		// No block is listed, so the empty table is a lower bound and is not cached.
		return CardinalityEstimateResult{Source: SourceBlocks, Read: ReadFullIndex, Counts: map[string][]int64{}, LowerBound: true, CoverageGap: true}, nil
	}
	covered := req.Snap || blocksCover(blocks, req.MinT, req.MaxT)
	res, err := e.blocks.cardinalityEstimate(ctx, tenantID, req)
	if err != nil {
		// A step refused for blocks that may share series is answered bucket by bucket from the window as snapped.
		return CardinalityEstimateResult{Snapped: res.Snapped}, err
	}
	res.Source = SourceBlocks
	if !covered {
		res.LowerBound, res.CoverageGap = true, true
	}
	return res, nil
}

// blocksAndHeadCounts answers a window that reaches into the ingesters' range and starts before query-store-after. The querier unions both sources' label-set hashes, so a series in a block and an ingester counts once. Per-bucket counts are refused, since hashes don't carry buckets.
func (e *CardinalityEstimator) blocksAndHeadCounts(ctx context.Context, tenantID string, req *CardinalityEstimateRequest, storeMaxT, heldFrom int64) (CardinalityEstimateResult, error) {
	if req.Step > 0 {
		return CardinalityEstimateResult{}, badEstimateRequest("%w: the window is held partly by blocks and partly by the ingesters", errStepNeedsOneSource)
	}
	half := req.MaxIndexBytes
	if half > 0 {
		half = max(1, half/2)
	}
	// The two sides share the hashes budget too.
	halfHashes := req.MaxHashes
	if halfHashes > 0 {
		halfHashes = max(1, halfHashes/2)
	}
	blocksReq := *req
	blocksReq.MaxIndexBytes = half
	blocksReq.MaxHashes = halfHashes
	blocksReq.MaxT = storeMaxT

	m := newEstimateMerger(ctx, true)
	parts, blocks, err := e.blocks.seriesCountsBlocks(ctx, tenantID, blocksReq.MinT, blocksReq.MaxT)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	matchers, err := client.ToLabelMatchers(req.Matchers)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}

	// Both sides are asked at once, and the first to fail cancels the other. The merger is not safe for concurrent use, so the ingesters' groups are added once the store-gateways' are in.
	res := CardinalityEstimateResult{Counts: map[string][]int64{}}
	var sr distributor.SeriesCountsResult
	g, gCtx := errgroup.WithContext(ctx)
	if len(blocks) > 0 {
		g.Go(func() error {
			var err error
			res, err = e.blocks.seriesCountsInto(gCtx, tenantID, blocksReq, parts, blocks, true, m)
			return err
		})
	}
	g.Go(func() error {
		var err error
		sr, err = e.ingesters.SeriesCounts(gCtx, req.Matchers, &client.SeriesCountsRequest{
			Matchers:         matchers,
			StartTimestampMs: req.MinT,
			EndTimestampMs:   req.MaxT,
			GroupBy:          req.GroupBy,
			Hashes:           true,
			SketchAbove:      req.SketchAbove,
			MaxIndexBytes:    half,
			MaxHashes:        halfHashes,
			MaxGroups:        req.MaxGroups,
		})
		if err != nil {
			return classifyIngesterError(err)
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return CardinalityEstimateResult{}, err
	}
	// The answers are charged to the memory tracker until merged.
	defer sr.Release()
	ingestersMinT := int64(0)
	for _, resp := range sr.Responses {
		for _, g := range resp.Groups {
			if err := m.add("ingester", g.Value, g.Counts, g.Hashes, g.Sketch); err != nil {
				return CardinalityEstimateResult{}, err
			}
		}
		res.SeriesCounted += resp.SeriesCounted
		if resp.MinTimeMs > 0 {
			ingestersMinT = max(ingestersMinT, resp.MinTimeMs)
		}
	}
	sr.Release()
	if err := m.finish(&res); err != nil {
		return CardinalityEstimateResult{}, err
	}

	res.Source, res.Read, res.Dedup = SourceBlocksAndHead, ReadFullIndexAndHeadChunkMetas, true
	// The blocks are trusted up to storeMaxT, the ingesters from what they still hold.
	covered := make(bucketindex.Blocks, 0, len(blocks)+1)
	for _, b := range blocks {
		covered = append(covered, &bucketindex.Block{MinTime: b.MinTime, MaxTime: min(b.MaxTime, storeMaxT)})
	}
	from := ingestersCoverFrom(ingestersMinT, heldFrom, req.MaxT)
	covered = append(covered, &bucketindex.Block{MinTime: from, MaxTime: req.MaxT})
	if !blocksCover(covered, req.MinT, req.MaxT) {
		res.LowerBound, res.CoverageGap = true, true
		res.HeadMinTime = from
	}
	return res, nil
}

// headCounts counts series per value of req.GroupBy in the ingesters' heads and local blocks over the window, from the series' chunk metas.
// A window that starts before heldFrom and before the oldest data the ingesters report may miss series, so the counts are then a lower bound.
func (e *CardinalityEstimator) headCounts(ctx context.Context, req *CardinalityEstimateRequest, heldFrom int64) (CardinalityEstimateResult, error) {
	matchers, err := client.ToLabelMatchers(req.Matchers)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	// The handler doesn't check the buckets of a request with snap.
	buckets, err := req.expectedBuckets()
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	sr, err := e.ingesters.SeriesCounts(ctx, req.Matchers, &client.SeriesCountsRequest{
		Matchers:         matchers,
		StartTimestampMs: req.MinT,
		EndTimestampMs:   req.MaxT,
		StepMs:           req.Step,
		WindowMs:         req.windowMs(),
		GroupBy:          req.GroupBy,
		SketchAbove:      req.SketchAbove,
		MaxIndexBytes:    req.MaxIndexBytes,
		MaxHashes:        req.MaxHashes,
		MaxGroups:        req.MaxGroups,
	})
	if err != nil {
		return CardinalityEstimateResult{}, classifyIngesterError(err)
	}

	// The answers are charged to the memory tracker until merged.
	defer sr.Release()
	res := CardinalityEstimateResult{Source: SourceHead, Read: ReadHeadChunkMetas, Counts: map[string][]int64{}, Dedup: sr.Hashed}
	m := newEstimateMerger(ctx, sr.Hashed)
	m.buckets = buckets
	var headMinT int64
	for _, resp := range sr.Responses {
		for _, g := range resp.Groups {
			if err := m.add("ingester", g.Value, g.Counts, g.Hashes, g.Sketch); err != nil {
				return CardinalityEstimateResult{}, err
			}
		}
		res.SeriesCounted += resp.SeriesCounted
		// An ingester that holds nothing for the tenant reports 0 and is left out.
		if resp.MinTimeMs > 0 {
			headMinT = max(headMinT, resp.MinTimeMs)
		}
	}
	sr.Release()
	if err := m.finish(&res); err != nil {
		return CardinalityEstimateResult{}, err
	}
	if from := ingestersCoverFrom(headMinT, heldFrom, req.MaxT); from > req.MinT {
		res.LowerBound = true
		res.HeadMinTime = from
	}
	return res, nil
}

// classifyIngesterError turns an ingester's gRPC status, as the distributor passes it on, into an estimate error.
func classifyIngesterError(err error) error {
	if errors.Is(err, distributor.ErrSeriesCountsNeedOneIngesterPerZone) {
		// Estimate answers the buckets one by one.
		return badEstimateRequest("%w: ingesters: %v", errStepNeedsOneSource, err)
	}
	if errors.Is(err, distributor.ErrSeriesCountsOlderIngester) {
		return olderVersionEstimate("%w", err)
	}
	if errors.Is(err, distributor.ErrSeriesCountsOverMemoryLimit) {
		return refusedByLimit(refusedByMemory, "the ingester replies this estimate holds in the querier: %w", err)
	}
	for e := err; e != nil; e = errors.Unwrap(e) {
		st, ok := grpcutil.ErrorToStatus(e)
		if !ok {
			continue
		}
		switch st.Code() {
		case codes.InvalidArgument:
			return badEstimateRequest("ingesters: %s", st.Message())
		case codes.Unimplemented:
			return refusedEstimate("ingesters %w yet, they may run an older version", errCountsNotSupported)
		case codes.ResourceExhausted:
			if limit := sourceRefusalLimit(err); limit == refusedByMaxHashes {
				return refusedByLimit(limit, "%w", sourceRefusal{msg: "ingesters, given the hashes the tenant's cardinality estimate memory limit allows, shared among them: " + st.Message(), err: err})
			} else if limit != "" {
				return refusedByLimit(limit, "%w", sourceRefusal{msg: "ingesters: " + st.Message(), err: err})
			}
		case codes.Unavailable:
			if cardinality.IsBusy(err) {
				return busyEstimate("ingesters", err)
			}
		}
		break
	}
	return redactFailure("ingesters", err)
}

// ingestersCoverFrom is the start of the time the ingesters answer for. reported is the latest start among them, 0 when none holds anything. heldFrom is 0 when -querier.query-ingesters-within is 0, and then only reported counts.
func ingestersCoverFrom(reported, heldFrom, maxT int64) int64 {
	if heldFrom == 0 {
		if reported > 0 {
			return reported
		}
		return maxT
	}
	if reported > 0 && reported < heldFrom {
		return reported
	}
	return heldFrom
}

// blocksCover reports whether the blocks' ranges hold all of [minT, maxT).
func blocksCover(blocks bucketindex.Blocks, minT, maxT int64) bool {
	sorted := slices.Clone(blocks)
	slices.SortFunc(sorted, func(a, b *bucketindex.Block) int {
		switch {
		case a.MinTime < b.MinTime:
			return -1
		case a.MinTime > b.MinTime:
			return 1
		}
		return 0
	})
	covered := minT
	for _, b := range sorted {
		if b.MinTime > covered {
			return false
		}
		covered = max(covered, b.MaxTime)
		if covered >= maxT {
			return true
		}
	}
	return covered >= maxT
}
