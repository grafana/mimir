// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/grafana/dskit/grpcutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/limiter"
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
	// ReadHeadPostings: the ingesters' head postings, with no time filter.
	ReadHeadPostings = "head_postings"
	// ReadHeadChunkMetas: the matching head series' chunk metas.
	ReadHeadChunkMetas = "head_chunk_metas"
	// ReadFullIndexAndHeadChunkMetas: blocks from their full index, the head from its chunk metas.
	ReadFullIndexAndHeadChunkMetas = "full_index+head_chunk_metas"
	// ReadPerBucket: each bucket answered as its own window.
	ReadPerBucket = "per_bucket"
)

// The values of the response's head_fallback field: the limit that refused the head walk, or HeadFallbackRefused.
const (
	HeadFallbackMaxIndexBytes = refusedByMaxIndexBytes
	HeadFallbackMaxHashes     = refusedByMaxHashes
	HeadFallbackRefused       = "refused"
)

// headFallbackReason is the head_fallback value for the refusal err.
func headFallbackReason(err error) string {
	if limit := estimateRefusalLimit(err); limit != "" {
		return limit
	}
	return HeadFallbackRefused
}

// estimateIngesters is the part of the distributor the estimate uses.
type estimateIngesters interface {
	LabelValuesCardinality(ctx context.Context, labelNames []model.LabelName, matchers []*labels.Matcher, countMethod cardinality.CountMethod) (uint64, *client.LabelValuesCardinalityResponse, error)
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
	// headAlwaysHolds is the recent time every ingester head holds: half the TSDB block range.
	headAlwaysHolds time.Duration
}

// defaultHeadAlwaysHolds is headAlwaysHolds for the default 2h block range.
const defaultHeadAlwaysHolds = time.Hour

// NewCardinalityEstimator returns an estimator over blocks and, when ingesters is not nil, the ingesters. A zero ingesterBlockRange means the default 2h.
func NewCardinalityEstimator(blocks *BlocksStoreQueryable, ingesters estimateIngesters, limits estimateLimits, ingesterBlockRange time.Duration) *CardinalityEstimator {
	e := &CardinalityEstimator{blocks: blocks, ingesters: ingesters, limits: limits, now: time.Now, headAlwaysHolds: defaultHeadAlwaysHolds}
	if ingesterBlockRange > 0 {
		e.headAlwaysHolds = ingesterBlockRange / 2
	}
	return e
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

	if needIngesters && e.compactedBlocksHold(ctx, tenantID, req, now) {
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
		// A bucket's counts are read from its result, not an open stream.
		sub.Step, sub.Window, sub.Snap, sub.StreamNames = 0, 0, false, false
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
		out.HeadWhole = out.HeadWhole || res.HeadWhole
		if out.HeadFallback == "" {
			out.HeadFallback, out.headFallbackErr = res.HeadFallback, res.headFallbackErr
		}
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
	return req.Snap || blocksCover(blocks, req.MinT, req.MaxT)
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
		MaxHashes:        halfHashes,
		MaxGroups:        req.MaxGroups,
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
// When that walk is over max_index_bytes, no step is asked for and the window is inside the time every head holds, the head is counted as one bucket from its postings (headWholeCounts) instead; otherwise the refusal stands.
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
		err = classifyIngesterError(err)
		// The whole-head count reads only the head's postings, so it stands in only for a window the head is sure to hold, and not for a max_groups refusal.
		headOnly := req.MinT >= util.TimeToMillis(e.now().Add(-e.headAlwaysHolds))
		if estimateHTTPStatus(err) == http.StatusUnprocessableEntity && !errors.Is(err, errCountsNotSupported) && estimateRefusalLimit(err) != refusedByMaxGroups && req.Step == 0 && headOnly {
			res, werr := e.headWholeCounts(ctx, req)
			if werr != nil {
				if estimateHTTPStatus(werr) != http.StatusInternalServerError {
					// The fallback's own limit names the refusal, the one it stood in for stays in the text.
					var fe estimateError
					if errors.As(werr, &fe) {
						return CardinalityEstimateResult{}, estimateError{class: fe.class, limit: fe.limit, err: fmt.Errorf("%w; it stood in for the refusal: %s", werr, err.Error())}
					}
					return CardinalityEstimateResult{}, werr
				}
				// Answer with the refusal it stood in for, and keep the failure for the log.
				return CardinalityEstimateResult{}, redactedError{msg: err.Error() + "; the whole-head count it falls back to failed too", err: errors.Join(err, werr)}
			}
			res.HeadFallback, res.headFallbackErr = headFallbackReason(err), err
			return res, nil
		}
		return CardinalityEstimateResult{}, err
	}

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
	if err := m.finish(&res); err != nil {
		return CardinalityEstimateResult{}, err
	}
	if from := ingestersCoverFrom(headMinT, heldFrom, req.MaxT); from > req.MinT {
		res.LowerBound = true
		res.HeadMinTime = from
	}
	return res, nil
}

// headWholeCounts counts series per value of req.GroupBy in the ingesters' heads from their postings. Head postings have no time axis, so the count covers every series in memory, which the result says with HeadWhole. A series that moved between ingesters counts in each.
func (e *CardinalityEstimator) headWholeCounts(ctx context.Context, req *CardinalityEstimateRequest) (CardinalityEstimateResult, error) {
	groupBy := req.GroupBy
	if groupBy == "" {
		groupBy = model.MetricNameLabel
	}
	_, resp, err := e.ingesters.LabelValuesCardinality(ctx, []model.LabelName{model.LabelName(groupBy)}, req.Matchers, cardinality.InMemoryMethod)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	res := CardinalityEstimateResult{Source: SourceHead, Read: ReadHeadPostings, HeadWhole: true, Counts: map[string][]int64{}}
	tracker, _ := limiter.MemoryConsumptionTrackerFromContext(ctx)
	for _, item := range resp.GetItems() {
		if item.LabelName != groupBy {
			continue
		}
		for v, n := range item.LabelValueSeries {
			if err := chargeEstimateGroup(tracker, v, 1); err != nil {
				return CardinalityEstimateResult{}, err
			}
			res.Counts[v] = []int64{int64(n)}
			res.SeriesCounted += int64(n)
		}
	}
	if limit := req.MaxGroups; limit > 0 && int64(len(res.Counts)) > limit {
		return CardinalityEstimateResult{}, errOverMaxGroups("the ingesters", limit, groupBy)
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
