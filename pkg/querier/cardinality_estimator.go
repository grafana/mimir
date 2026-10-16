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

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/util"
)

// Sources a cardinality estimate can count from, as reported in the
// response's source field.
const (
	sourceBlocks = "blocks"
	sourceHead   = "head"
)

// Reads on the ingesters: readHeadPostings counts head postings with no
// time filter, readHeadChunkMetas walks the matching head series' chunk
// metas so only series with a chunk in the window count.
const (
	readHeadPostings   = "head_postings"
	readHeadChunkMetas = "head_chunk_metas"
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
	QueryIngestersWithin(userID string) time.Duration
}

// CardinalityEstimator answers a cardinality estimate from the source that
// holds the window: the store-gateways when blocks cover the whole window,
// the ingesters when no block reaches into it and the window is recent
// enough for the ingesters to hold it. A window that needs both is refused
// for now, since counting it needs deduplication between the two.
type CardinalityEstimator struct {
	blocks    *BlocksStoreQueryable
	ingesters estimateIngesters
	limits    estimateLimits
	now       func() time.Time
}

// NewCardinalityEstimator returns an estimator over blocks and, when
// ingesters is not nil, the ingesters. limits may be nil only when
// ingesters is.
func NewCardinalityEstimator(blocks *BlocksStoreQueryable, ingesters estimateIngesters, limits estimateLimits) *CardinalityEstimator {
	return &CardinalityEstimator{blocks: blocks, ingesters: ingesters, limits: limits, now: time.Now}
}

// Estimate answers req for tenantID. With Snap, a window inside one block
// range is widened to it and req is updated.
func (e *CardinalityEstimator) Estimate(ctx context.Context, tenantID string, req *CardinalityEstimateRequest) (CardinalityEstimateResult, error) {
	_, blocks, err := e.blocks.estimateBlocks(ctx, tenantID, req.MinT, req.MaxT)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	if len(blocks) > 0 && (req.Snap || blocksCover(blocks, req.MinT, req.MaxT)) {
		res, err := e.blocks.CardinalityEstimate(ctx, tenantID, req)
		res.Source = sourceBlocks
		return res, err
	}

	if e.ingesters == nil {
		if len(blocks) > 0 {
			return CardinalityEstimateResult{}, badEstimateRequest("blocks cover only part of the window [%d, %d), and the ingesters are not consulted", req.MinT, req.MaxT)
		}
		return CardinalityEstimateResult{Source: sourceBlocks, Read: readFullIndex, Counts: map[string][]int64{}}, nil
	}

	now := e.now()
	within := e.limits.QueryIngestersWithin(tenantID)
	headMinT := int64(0)
	if within > 0 {
		headMinT = util.TimeToMillis(now.Add(-within))
	}
	switch {
	case len(blocks) == 0 && req.MinT >= headMinT:
		return e.headCounts(ctx, req)
	case len(blocks) == 0 && req.MaxT <= headMinT:
		// Older than the ingesters hold, and no block: nothing was written.
		return CardinalityEstimateResult{Source: sourceBlocks, Read: readFullIndex, Counts: map[string][]int64{}}, nil
	default:
		return CardinalityEstimateResult{}, badEstimateRequest("the window [%d, %d) is held partly by blocks and partly by the ingesters (blocks reach it, the ingesters hold data after %d); counting across both needs deduplication, which is not supported yet", req.MinT, req.MaxT, headMinT)
	}
}

// headCounts counts series per value of req.GroupBy in the ingesters'
// heads over the window, from the series' chunk metas. When that walk is
// over max_index_bytes and no step is asked for, the head is counted as one
// bucket from its postings instead (headWholeCounts) and the response says
// so; with a step the refusal stands.
//
// The ingesters' compacted blocks are not read, so a window that starts
// before the newest head's start may miss series that only had samples
// there; the counts are then flagged as a lower bound.
func (e *CardinalityEstimator) headCounts(ctx context.Context, req *CardinalityEstimateRequest) (CardinalityEstimateResult, error) {
	matchers, err := client.ToLabelMatchers(req.Matchers)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	sr, err := e.ingesters.SeriesCounts(ctx, req.Matchers, &client.SeriesCountsRequest{
		Matchers:         matchers,
		StartTimestampMs: req.MinT,
		EndTimestampMs:   req.MaxT,
		StepMs:           req.Step,
		GroupBy:          req.GroupBy,
		SketchAbove:      req.SketchAbove,
		MaxIndexBytes:    req.MaxIndexBytes,
	})
	if err != nil {
		err = classifyIngesterError(err)
		if estimateHTTPStatus(err) == http.StatusUnprocessableEntity && req.Step == 0 {
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
	m := newEstimateMerger(sr.Hashed, e.blocks.estimateCfg.MaxDedupSeries)
	var headMinT int64
	for _, resp := range sr.Responses {
		for _, g := range resp.Groups {
			if err := m.add("ingester", g.Value, g.Counts, g.Hashes, g.Sketch); err != nil {
				return CardinalityEstimateResult{}, err
			}
		}
		res.SeriesCounted += resp.SeriesCounted
		// An ingester with no data for the tenant reports an empty head.
		if resp.HeadMaxTimeMs > 0 {
			headMinT = max(headMinT, resp.HeadMinTimeMs)
		}
	}
	if err := m.finish(&res); err != nil {
		return CardinalityEstimateResult{}, err
	}
	if headMinT > req.MinT {
		res.LowerBound = true
		res.HeadMinTime = headMinT
	}
	return res, nil
}

// headWholeCounts counts series per value of req.GroupBy in the ingesters'
// heads from their postings. Head postings have no time axis, so the count
// covers every series an ingester holds in memory, including series whose
// samples are all outside the window but are not yet removed by a head
// compaction; the result says so with HeadWhole. Under ingest storage the
// distributor takes one zone's answer per partition and adds up the
// partitions, which share no series; with classic replication it takes the
// largest zone's sum.
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
	for _, item := range resp.GetItems() {
		if item.LabelName != groupBy {
			continue
		}
		for v, n := range item.LabelValueSeries {
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
	if strings.Contains(err.Error(), "need ingest storage or zone-aware replication") {
		return badEstimateRequest("ingesters: %v", err)
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
