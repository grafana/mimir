// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/grafana/dskit/grpcutil"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
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
)

// estimateIngesters is the part of the distributor the estimate uses.
type estimateIngesters interface {
	SeriesCounts(ctx context.Context, matchers []*labels.Matcher, req *client.SeriesCountsRequest) (distributor.SeriesCountsResult, error)
}

// estimateLimits are the per-tenant limits of an estimate.
type estimateLimits interface {
	CardinalityEstimateEnabled(userID string) bool
	CardinalityEstimateMaxMemoryBytes(userID string) uint64
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

// clampToLookback moves req.MinT up to the oldest time the tenant's queries may read. It reports false when none of the window is left.
func (e *CardinalityEstimator) clampToLookback(spanLog *spanlogger.SpanLogger, tenantID string, req *CardinalityEstimateRequest) bool {
	now := util.TimeToMillis(e.now())
	minT := clampMinTime(spanLog, req.MinT, now, -e.limits.MaxQueryLookback(tenantID), "max query lookback")
	minT = clampMinTime(spanLog, minT, now, -e.limits.CompactorBlocksRetentionPeriod(tenantID), "blocks retention period")
	if minT >= req.MaxT {
		return false
	}
	req.MinT = minT
	return true
}

// Estimate answers req for tenantID.
func (e *CardinalityEstimator) Estimate(ctx context.Context, tenantID string, req *CardinalityEstimateRequest) (res CardinalityEstimateResult, err error) {
	req.MaxGroups = cardinality.BoundByLimit(req.MaxGroups, e.limits.CardinalityEstimateMaxGroups(tenantID))
	// The groups and hashes held in the querier count against the memory limit.
	if _, terr := limiter.MemoryConsumptionTrackerFromContext(ctx); terr != nil {
		rejected := e.blocks.metrics.estimateMemoryRefusalsOrDiscard()
		limit := estimateMemoryLimit(e.limits.CardinalityEstimateMaxMemoryBytes(tenantID), e.limits.MaxEstimatedMemoryConsumptionPerQuery(tenantID))
		tracker := limiter.NewMemoryConsumptionTracker(ctx, limit, rejected, "cardinality estimate")
		ctx = limiter.AddMemoryTrackerToContext(ctx, tracker)
		// The tracker is the estimate's own, so its peak is what the estimate held, refused or not.
		defer func() { res.PeakMemoryBytes = tracker.PeakEstimatedMemoryConsumptionBytes() }()
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
	// A failed estimate still says which sources it asked, for the metrics.
	if err != nil && res.Source == "" {
		res.Source = source
	}
	return res, err
}

// estimateMemoryLimit is the memory an estimate may hold in the querier, the lower of the limits that are set.
func estimateMemoryLimit(estimate, query uint64) uint64 {
	if query > 0 && (estimate == 0 || query < estimate) {
		return query
	}
	return estimate
}

// blocksOnly answers a window that ends before the ingesters' range from the blocks. Partial coverage makes the counts a lower bound.
func (e *CardinalityEstimator) blocksOnly(ctx context.Context, tenantID string, req *CardinalityEstimateRequest) (CardinalityEstimateResult, error) {
	_, blocks, err := e.blocks.seriesCountsBlocks(ctx, tenantID, req.MinT, req.MaxT)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	if len(blocks) == 0 {
		// No block is listed, so the empty table is a lower bound.
		return CardinalityEstimateResult{Source: SourceBlocks, Read: ReadFullIndex, Counts: map[string][]int64{}, LowerBound: true, CoverageGap: true}, nil
	}
	covered := blocksCover(blocks, req.MinT, req.MaxT)
	res, err := e.blocks.cardinalityEstimate(ctx, tenantID, req)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	res.Source = SourceBlocks
	if !covered {
		res.LowerBound, res.CoverageGap = true, true
	}
	return res, nil
}

// blocksAndHeadCounts answers a window that reaches into the ingesters' range and starts before query-store-after. The querier unions both sources' label-set hashes, so a series in a block and an ingester counts once.
func (e *CardinalityEstimator) blocksAndHeadCounts(ctx context.Context, tenantID string, req *CardinalityEstimateRequest, storeMaxT, heldFrom int64) (CardinalityEstimateResult, error) {
	blocksReq := *req
	blocksReq.MaxT = storeMaxT

	m := newEstimateMerger(ctx, true)
	parts, blocks, err := e.blocks.seriesCountsBlocks(ctx, tenantID, blocksReq.MinT, blocksReq.MaxT)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}

	// Both sides are asked at once, and the first to fail cancels the other. The merger is not safe for concurrent use, so the ingesters' groups are added once the store-gateways' are in.
	res := CardinalityEstimateResult{Counts: map[string][]int64{}}
	var sr distributor.SeriesCountsResult
	// The ingesters' answers are charged to the memory tracker until merged, or until the store-gateways fail.
	defer func() { sr.Release() }()
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
			Matchers:  cardinalitypb.ToLabelMatchers(req.Matchers),
			StartMs:   req.MinT,
			EndMs:     req.MaxT,
			GroupBy:   req.GroupBy,
			Hashes:    true,
			MaxGroups: req.MaxGroups,
		})
		if err != nil {
			return classifyIngesterError(err)
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return CardinalityEstimateResult{}, err
	}
	ingestersMinT := int64(0)
	for _, resp := range sr.Responses {
		for _, g := range resp.Groups {
			if err := m.add("ingester", g.Value, g.Counts, g.Hashes); err != nil {
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
	sr, err := e.ingesters.SeriesCounts(ctx, req.Matchers, &client.SeriesCountsRequest{
		Matchers:  cardinalitypb.ToLabelMatchers(req.Matchers),
		StartMs:   req.MinT,
		EndMs:     req.MaxT,
		GroupBy:   req.GroupBy,
		MaxGroups: req.MaxGroups,
	})
	if err != nil {
		return CardinalityEstimateResult{}, classifyIngesterError(err)
	}

	// The answers are charged to the memory tracker until merged.
	defer sr.Release()
	res := CardinalityEstimateResult{Source: SourceHead, Read: ReadHeadChunkMetas, Counts: map[string][]int64{}, Dedup: sr.Hashed, UpperBound: sr.UpperBound}
	m := newEstimateMerger(ctx, sr.Hashed)
	var headMinT int64
	for _, resp := range sr.Responses {
		for _, g := range resp.Groups {
			if err := m.add("ingester", g.Value, g.Counts, g.Hashes); err != nil {
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
			if limit := sourceRefusalLimit(err); limit != "" {
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
