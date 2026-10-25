// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// Sources a cardinality estimate can count from, as reported in the
// response's source field.
const (
	sourceBlocks = "blocks"
	sourceHead   = "head"
	sourceBoth   = "blocks+head"
)

// Reads on the ingesters: readHeadPostings counts head postings with no
// time filter, readHeadChunkMetas walks the matching head series' chunk
// metas so only series with a chunk in the window count.
const (
	readHeadPostings   = "head_postings"
	readHeadChunkMetas = "head_chunk_metas"
	// readPerBucket says each bucket was answered as its own window, with
	// the read each one needed.
	readPerBucket = "per_bucket"
)

// estimateIngesters is the part of the distributor the cardinality
// estimate uses to count series in the ingesters.
type estimateIngesters interface {
	LabelValuesCardinality(ctx context.Context, labelNames []model.LabelName, matchers []*labels.Matcher, countMethod cardinality.CountMethod) (uint64, *client.LabelValuesCardinalityResponse, error)
	SeriesCounts(ctx context.Context, matchers []*labels.Matcher, req *client.SeriesCountsRequest) (distributor.SeriesCountsResult, error)
}

// estimateLimits are the per-tenant limits that decide which sources a
// window reaches.
type estimateLimits interface {
	CardinalityEstimateEnabled(userID string) bool
	CardinalityEstimateMaxIndexBytes(userID string) int64
	CardinalityEstimateMaxMemoryBytes(userID string) uint64
	QueryIngestersWithin(userID string) time.Duration
	MaxEstimatedMemoryConsumptionPerQuery(userID string) uint64
}

// CardinalityEstimator answers a cardinality estimate from the sources a
// query over the same window would read: the store-gateways alone when the
// window ends before query-ingesters-within, or when compacted blocks cover
// a window that ended long enough ago; the ingesters alone when it starts
// after query-store-after; and both, deduplicated by label-set hash,
// otherwise.
type CardinalityEstimator struct {
	blocks    *BlocksStoreQueryable
	ingesters estimateIngesters
	limits    estimateLimits
	now       func() time.Time
	// headAlwaysHolds is the recent time every ingester head holds even
	// right after a head compaction: half the TSDB block range, since the
	// head is cut once it spans 1.5 block ranges.
	headAlwaysHolds time.Duration
}

// defaultHeadAlwaysHolds is headAlwaysHolds for the default 2h block range.
const defaultHeadAlwaysHolds = time.Hour

// NewCardinalityEstimator returns an estimator over blocks and, when
// ingesters is not nil, the ingesters. limits may be nil only when
// ingesters is; then no per-tenant limit applies.
func NewCardinalityEstimator(blocks *BlocksStoreQueryable, ingesters estimateIngesters, limits estimateLimits) *CardinalityEstimator {
	return &CardinalityEstimator{blocks: blocks, ingesters: ingesters, limits: limits, now: time.Now, headAlwaysHolds: defaultHeadAlwaysHolds}
}

// WithIngesterBlockRange sets the ingesters' TSDB block range, from which
// the estimator knows how much recent time a head always holds.
func (e *CardinalityEstimator) WithIngesterBlockRange(blockRange time.Duration) *CardinalityEstimator {
	if blockRange > 0 {
		e.headAlwaysHolds = blockRange / 2
	}
	return e
}

// enabledFor reports whether tenantID may use the estimate. Without
// per-tenant limits every tenant may.
func (e *CardinalityEstimator) enabledFor(tenantID string) bool {
	return e.limits == nil || e.limits.CardinalityEstimateEnabled(tenantID)
}

// Estimate answers req for tenantID. With Snap, a window inside one block
// range is widened to it and req is updated, and so is its MaxIndexBytes
// once the tenant's limit is applied.
func (e *CardinalityEstimator) Estimate(ctx context.Context, tenantID string, req *CardinalityEstimateRequest) (CardinalityEstimateResult, error) {
	// The tenant's limit bounds a request that sets no max_index_bytes and
	// cuts one that asks for more, before it is split across the sources.
	if e.limits != nil {
		req.MaxIndexBytes = cardinality.BoundMaxIndexBytes(req.MaxIndexBytes, e.limits.CardinalityEstimateMaxIndexBytes(tenantID))
	}
	// The groups and hashes held in the querier count against the tenant's
	// memory limit for an estimate, or for a query when that is lower.
	if e.limits != nil {
		if _, err := limiter.MemoryConsumptionTrackerFromContext(ctx); err != nil {
			rejected := e.blocks.metrics.estimateMemoryRejectedOrDiscard()
			limit := estimateMemoryLimit(e.limits.CardinalityEstimateMaxMemoryBytes(tenantID), e.limits.MaxEstimatedMemoryConsumptionPerQuery(tenantID))
			ctx = limiter.AddMemoryTrackerToContext(ctx, limiter.NewMemoryConsumptionTracker(ctx, limit, rejected, "cardinality estimate"))
		}
	}
	// Pick the sources the way a query does: the ingesters for anything
	// after query-ingesters-within, the store-gateways only for data older
	// than query-store-after. Block metadata alone can't say the blocks are
	// complete: a block being built for the current range can claim the
	// range before all of its data is in.
	now := e.now()
	needIngesters := false
	// heldFrom is where the ingesters are sure to hold everything written:
	// they keep at least query-ingesters-within, so a series missing from
	// them after that was never written.
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

	if needIngesters && e.compactedBlocksHold(ctx, tenantID, req, now) {
		needIngesters = false
	}

	var (
		res CardinalityEstimateResult
		err error
	)
	switch {
	case !needIngesters:
		res, err = e.blocksOnly(ctx, tenantID, req)
	case storeMaxT <= req.MinT:
		res, err = e.headCounts(ctx, req, heldFrom)
	default:
		res, err = e.blocksAndHeadCounts(ctx, tenantID, req, storeMaxT, heldFrom)
	}
	if req.Step > 0 && errors.Is(err, errStepNeedsOneSource) {
		return e.perBucket(ctx, tenantID, req)
	}
	return res, err
}

// estimateMemoryLimit is the memory an estimate may hold in the querier:
// the estimate's own limit, or the per-query limit when that is set and
// lower. 0 for either means it doesn't apply.
func estimateMemoryLimit(estimate, query uint64) uint64 {
	if query > 0 && (estimate == 0 || query < estimate) {
		return query
	}
	return estimate
}

// maxPerBucketEstimates caps the buckets perBucket answers one by one, since
// each is a full estimate with its own fan-out.
const maxPerBucketEstimates = 48

// perBucket answers a per-bucket count whose window crosses sources that
// may share series, by answering each step-wide bucket as its own window.
// A bucket inside one source, or one block range of series-disjoint blocks,
// is a plain count with no deduplication; a bucket that crosses the
// blocks/ingesters cut is deduplicated by hash on its own. The buckets
// share max_index_bytes evenly and the request's memory limit.
func (e *CardinalityEstimator) perBucket(ctx context.Context, tenantID string, req *CardinalityEstimateRequest) (CardinalityEstimateResult, error) {
	n, err := cardinality.Buckets(req.MinT, req.MaxT, req.Step, req.Window)
	if err != nil {
		return CardinalityEstimateResult{}, badEstimateRequest("%v", err)
	}
	width := req.Window
	if width == 0 {
		width = req.Step
	}
	if n > maxPerBucketEstimates {
		return CardinalityEstimateResult{}, badEstimateRequest("%w; answering the %d buckets one by one is capped at %d, so use a larger step or a window inside one source", errStepNeedsOneSource, n, maxPerBucketEstimates)
	}
	out := CardinalityEstimateResult{Read: readPerBucket, Counts: map[string][]int64{}}
	sources := map[string]bool{}
	seenBlocks := map[ulid.ULID]bool{}
	for i := range n {
		sub := *req
		sub.MinT = req.MinT + int64(i)*req.Step
		sub.MaxT = sub.MinT + width
		// A bucket's counts are read from its result, so its name table
		// must come back in Counts, not as an open stream.
		sub.Step, sub.Window, sub.Snap, sub.StreamNames = 0, 0, false, false
		if req.MaxIndexBytes > 0 {
			sub.MaxIndexBytes = max(1, req.MaxIndexBytes/int64(n))
		}
		res, err := e.Estimate(ctx, tenantID, &sub)
		if err != nil {
			return CardinalityEstimateResult{}, fmt.Errorf("bucket [%d, %d): %w", sub.MinT, sub.MaxT, err)
		}
		for v, c := range res.Counts {
			counts := out.Counts[v]
			if counts == nil {
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
		out.MissingBlocks = append(out.MissingBlocks, res.MissingBlocks...)
		out.LowerBound = out.LowerBound || res.LowerBound
		out.CoverageGap = out.CoverageGap || res.CoverageGap
		out.Dedup = out.Dedup || res.Dedup
		out.HeadWhole = out.HeadWhole || res.HeadWhole
		out.HeadMinTime = max(out.HeadMinTime, res.HeadMinTime)
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
	}
	names := make([]string, 0, len(sources))
	for s := range sources {
		names = append(names, s)
	}
	slices.Sort(names)
	out.Source = strings.Join(names, ",")
	return out, nil
}

// compactedBlocksHold reports whether compacted blocks can answer req alone:
// the window ended at least -querier.cardinality-estimate.trust-compacted-blocks-after
// ago, and blocks that the compactor wrote, not the ingesters or the
// block-builder, cover it. The compactor merges a range only once newer
// blocks exist, but it does not wait for every partition's block-builder
// block, so under ingest storage its output can miss a partition that lags.
// That is why the default is 0.
func (e *CardinalityEstimator) compactedBlocksHold(ctx context.Context, tenantID string, req *CardinalityEstimateRequest, now time.Time) bool {
	trust := e.blocks.estimateCfg.TrustCompactedBlocksAfter
	if trust <= 0 || req.MaxT > util.TimeToMillis(now.Add(-trust)) {
		return false
	}
	_, blocks, err := e.blocks.estimateBlocks(ctx, tenantID, req.MinT, req.MaxT)
	if err != nil || len(blocks) == 0 {
		return false
	}
	for _, b := range blocks {
		if b.CompactionLevel < 2 {
			return false
		}
	}
	return req.Snap || blocksCover(blocks, req.MinT, req.MaxT)
}

// blocksOnly answers a window that ends before the ingesters' range from the
// blocks. When they cover only part of it the counts are a lower bound.
func (e *CardinalityEstimator) blocksOnly(ctx context.Context, tenantID string, req *CardinalityEstimateRequest) (CardinalityEstimateResult, error) {
	_, blocks, err := e.blocks.estimateBlocks(ctx, tenantID, req.MinT, req.MaxT)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	if len(blocks) == 0 {
		// No block is listed for a window the ingesters no longer answer for.
		// The bucket index may not list a block yet, or a block-builder may
		// lag, so an empty table is a lower bound and is not cached.
		return CardinalityEstimateResult{Source: sourceBlocks, Read: readFullIndex, Counts: map[string][]int64{}, LowerBound: true, CoverageGap: true}, nil
	}
	covered := req.Snap || blocksCover(blocks, req.MinT, req.MaxT)
	res, err := e.blocks.CardinalityEstimate(ctx, tenantID, req)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	res.Source = sourceBlocks
	if !covered {
		res.LowerBound, res.CoverageGap = true, true
	}
	return res, nil
}

// blocksAndHeadCounts answers a window that reaches into the ingesters'
// range and starts before query-store-after. The store-gateways count the
// part before storeMaxT and the ingesters the whole window, both as
// label-set hashes with sketches above sketch_above, and the querier unions
// them, so a series in a block and in an ingester counts once. max_index_bytes is split evenly between the two sources. Per-bucket
// counts are refused, since hashes don't carry buckets. When the blocks and
// what the ingesters hold leave part of the window uncovered, the counts are
// a lower bound.
func (e *CardinalityEstimator) blocksAndHeadCounts(ctx context.Context, tenantID string, req *CardinalityEstimateRequest, storeMaxT, heldFrom int64) (CardinalityEstimateResult, error) {
	if req.Step > 0 {
		return CardinalityEstimateResult{}, badEstimateRequest("%w: the window is held partly by blocks and partly by the ingesters", errStepNeedsOneSource)
	}
	half := req.MaxIndexBytes
	if half > 0 {
		half = max(1, half/2)
	}
	blocksReq := *req
	blocksReq.MaxIndexBytes = half
	blocksReq.MaxT = storeMaxT

	m := newEstimateMerger(ctx, true, e.blocks.estimateCfg.MaxDedupSeries)
	parts, blocks, err := e.blocks.estimateBlocks(ctx, tenantID, blocksReq.MinT, blocksReq.MaxT)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	res := CardinalityEstimateResult{Counts: map[string][]int64{}}
	if len(blocks) > 0 {
		if res, err = e.blocks.seriesCountsInto(ctx, tenantID, blocksReq, parts, blocks, true, m); err != nil {
			return CardinalityEstimateResult{}, err
		}
	}

	matchers, err := client.ToLabelMatchers(req.Matchers)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	sr, err := e.ingesters.SeriesCounts(ctx, req.Matchers, &client.SeriesCountsRequest{
		Matchers:         matchers,
		StartTimestampMs: req.MinT,
		EndTimestampMs:   req.MaxT,
		GroupBy:          req.GroupBy,
		Hashes:           true,
		SketchAbove:      req.SketchAbove,
		MaxIndexBytes:    half,
	})
	if err != nil {
		return CardinalityEstimateResult{}, classifyIngesterError(err)
	}
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
	if err := m.finish(&res); err != nil {
		return CardinalityEstimateResult{}, err
	}

	res.Source, res.Read, res.Dedup = sourceBoth, readFullIndex+"+"+readHeadChunkMetas, true
	// The blocks are trusted up to storeMaxT, the ingesters from what they
	// still hold.
	covered := make(bucketindex.Blocks, 0, len(blocks)+1)
	for _, b := range blocks {
		covered = append(covered, &bucketindex.Block{MinTime: b.MinTime, MaxTime: min(b.MaxTime, storeMaxT)})
	}
	from := ingestersCoverFrom(ingestersMinT, heldFrom)
	covered = append(covered, &bucketindex.Block{MinTime: from, MaxTime: req.MaxT})
	if !blocksCover(covered, req.MinT, req.MaxT) {
		res.LowerBound, res.CoverageGap = true, true
		res.HeadMinTime = from
	}
	return res, nil
}

// headCounts counts series per value of req.GroupBy in the ingesters'
// heads and local blocks over the window, from the series' chunk metas.
// The distributor asks for hashes whenever more than one ingester per zone
// answers, since a series can be in two partitions or on two ingesters of
// a zone; a per-bucket count it can't add up is refused and answered
// bucket by bucket.
// When that walk is over max_index_bytes, no step is asked for and the
// window is inside the time every head holds, the head is counted as one
// bucket from its postings instead (headWholeCounts) and the response says
// so; otherwise the refusal stands.
//
// The ingesters count their heads and their compacted local blocks. They
// keep everything written after heldFrom; a window that starts before that
// and before the oldest data they report may miss series they no longer
// hold, so the counts are then flagged as a lower bound.
func (e *CardinalityEstimator) headCounts(ctx context.Context, req *CardinalityEstimateRequest, heldFrom int64) (CardinalityEstimateResult, error) {
	matchers, err := client.ToLabelMatchers(req.Matchers)
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
	})
	if err != nil {
		err = classifyIngesterError(err)
		// The whole-head count reads only the head's postings, not the
		// ingesters' local blocks, so it stands in only for a window the
		// head is sure to hold; an older window would miss the series
		// that are only in local blocks.
		headOnly := req.MinT >= util.TimeToMillis(e.now().Add(-e.headAlwaysHolds))
		if estimateHTTPStatus(err) == http.StatusUnprocessableEntity && req.Step == 0 && headOnly {
			res, werr := e.headWholeCounts(ctx, req)
			if werr != nil {
				return CardinalityEstimateResult{}, werr
			}
			res.HeadFallback = err.Error()
			return res, nil
		}
		return CardinalityEstimateResult{}, err
	}

	res := CardinalityEstimateResult{Source: sourceHead, Read: readHeadChunkMetas, Counts: map[string][]int64{}, Dedup: sr.Hashed}
	m := newEstimateMerger(ctx, sr.Hashed, e.blocks.estimateCfg.MaxDedupSeries)
	m.buckets = req.expectedBuckets()
	var headMinT int64
	for _, resp := range sr.Responses {
		for _, g := range resp.Groups {
			if err := m.add("ingester", g.Value, g.Counts, g.Hashes, g.Sketch); err != nil {
				return CardinalityEstimateResult{}, err
			}
		}
		res.SeriesCounted += resp.SeriesCounted
		// An ingester with nothing in the window reports 0. The others
		// hold data back to min_time_ms, from their local blocks or head.
		if resp.MinTimeMs > 0 {
			headMinT = max(headMinT, resp.MinTimeMs)
		}
	}
	if err := m.finish(&res); err != nil {
		return CardinalityEstimateResult{}, err
	}
	if from := ingestersCoverFrom(headMinT, heldFrom); from > req.MinT {
		res.LowerBound = true
		res.HeadMinTime = from
	}
	return res, nil
}

// headWholeCounts counts series per value of req.GroupBy in the ingesters'
// heads from their postings. Head postings have no time axis, so the count
// covers every series an ingester holds in memory, including series whose
// samples are all outside the window but are not yet removed by a head
// compaction; the result says so with HeadWhole. Under ingest storage the
// distributor takes one zone's answer per partition and adds up the
// partitions; with classic replication it takes the largest zone's sum. A
// series that moved between partitions or ingesters counts in each.
func (e *CardinalityEstimator) headWholeCounts(ctx context.Context, req *CardinalityEstimateRequest) (CardinalityEstimateResult, error) {
	groupBy := req.GroupBy
	if groupBy == "" {
		groupBy = labels.MetricName
	}
	_, resp, err := e.ingesters.LabelValuesCardinality(ctx, []model.LabelName{model.LabelName(groupBy)}, req.Matchers, cardinality.InMemoryMethod)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	res := CardinalityEstimateResult{Source: sourceHead, Read: readHeadPostings, HeadWhole: true, Counts: map[string][]int64{}}
	tracker, _ := limiter.MemoryConsumptionTrackerFromContext(ctx)
	for _, item := range resp.GetItems() {
		if item.LabelName != groupBy {
			continue
		}
		for v, n := range item.LabelValueSeries {
			if err := chargeEstimateGroup(tracker, v); err != nil {
				return CardinalityEstimateResult{}, err
			}
			res.Counts[v] = []int64{int64(n)}
			res.SeriesCounted += int64(n)
		}
	}
	if limit := e.blocks.estimateCfg.MaxMetricNames; groupBy == labels.MetricName && limit > 0 && int64(len(res.Counts)) > limit {
		return CardinalityEstimateResult{}, refusedEstimate("the ingesters hold more than %d metric names (the -querier.cardinality-estimate.max-metric-names limit)", limit)
	}
	return res, nil
}

// classifyIngesterError turns an ingester's gRPC status, as the
// distributor passes it on, into an estimate error.
func classifyIngesterError(err error) error {
	if errors.Is(err, distributor.ErrSeriesCountsNeedOneIngesterPerZone) {
		// Estimate answers the buckets one by one, each deduplicated by
		// hash.
		return badEstimateRequest("%w: ingesters: %v", errStepNeedsOneSource, err)
	}
	for e := err; e != nil; e = errors.Unwrap(e) {
		st, ok := status.FromError(e)
		if !ok {
			continue
		}
		switch st.Code() {
		case codes.InvalidArgument:
			return badEstimateRequest("ingesters: %s", st.Message())
		case codes.ResourceExhausted:
			if strings.Contains(st.Message(), "max_index_bytes") {
				return refusedEstimate("ingesters: %s", st.Message())
			}
		}
		break
	}
	return fmt.Errorf("ingesters: %w", err)
}

// ingestersCoverFrom is the start of the time the ingesters answer for:
// everything after heldFrom, which they are sure to keep, and anything
// older they reported holding (reported is 0 when they hold nothing in the
// window).
func ingestersCoverFrom(reported, heldFrom int64) int64 {
	if reported > 0 && reported < heldFrom {
		return reported
	}
	return heldFrom
}

// blocksCover reports whether the union of the blocks' ranges holds all of
// [minT, maxT).
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
