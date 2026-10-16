// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"slices"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/cardinality"
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

// readHeadPostings is the read for counts taken from the ingesters' head
// postings, with no time filter.
const readHeadPostings = "head_postings"

// estimateIngesters is the part of the distributor the cardinality
// estimate uses to count series in the ingesters.
type estimateIngesters interface {
	LabelValuesCardinality(ctx context.Context, labelNames []model.LabelName, matchers []*labels.Matcher, countMethod cardinality.CountMethod) (uint64, *client.LabelValuesCardinalityResponse, error)
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
// heads. The head's postings have no time axis, so the count covers every
// series an ingester holds in memory, including series whose samples are
// all outside the window but are not yet removed by a head compaction; the
// result says so with HeadWhole. Under ingest storage the distributor takes
// one zone's answer per partition and adds up the partitions, which share
// no series; with classic replication it takes the largest zone's sum.
func (e *CardinalityEstimator) headCounts(ctx context.Context, req *CardinalityEstimateRequest) (CardinalityEstimateResult, error) {
	if req.Step > 0 {
		return CardinalityEstimateResult{}, badEstimateRequest("per-bucket counts on the ingesters are not supported yet")
	}
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
