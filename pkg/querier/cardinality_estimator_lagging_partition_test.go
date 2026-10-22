// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

// Ingest storage, two partitions. The window [96h, 98h) ended 2h ago: past
// a trust-compacted-blocks-after of 1h, inside query-ingesters-within (13h)
// and query-store-after (12h). Partition 0's block-builder blocks for the
// range were uploaded and compacted into a level-2 block (b1); partition
// 1's block-builder lags, so its level-1 block for the range is not in the
// bucket index yet. Partition 0 holds series 1, 2, 3; partition 1 holds
// series 4, 5. The ingesters of both partitions hold all five, which is
// what a PromQL query over the window reads (it asks the ingesters inside
// query-ingesters-within, and no store-gateway since the window is after
// query-store-after).
func TestCardinalityEstimator_CompactedBlocksWithLaggingPartition(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	b1 := ulid.MustNew(1, nil) // compactor output, partition 0 only
	b2 := ulid.MustNew(2, nil) // partition 1's late block-builder block
	up := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")}
	now := time.UnixMilli(100 * hour)

	estimate := func(t *testing.T, index bucketindex.Blocks, trust time.Duration) (CardinalityEstimateResult, *ingestersMock) {
		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(index, &bucketindex.Metadata{}, nil)
		gw := gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{3}, Hashes: []uint64{1, 2, 3}}}})
		ids := make([]ulid.ULID, 0, len(index))
		for _, b := range index {
			ids = append(ids, b.ID)
		}
		q := &BlocksStoreQueryable{
			compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
				map[BlocksStoreClient][][]ulid.ULID{gw: {ids}},
			}}}},
			queryStoreAfter: 12 * time.Hour,
			estimateCfg:     CardinalityEstimateConfig{TrustCompactedBlocksAfter: trust},
		}
		ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Hashed: true, Responses: []*client.SeriesCountsResponse{
			{Groups: []*client.SeriesCountGroup{{Value: "up", Hashes: []uint64{1, 2, 3}}}, MinTimeMs: 80 * hour}, // partition 0
			{Groups: []*client.SeriesCountGroup{{Value: "up", Hashes: []uint64{4, 5}}}, MinTimeMs: 80 * hour},    // partition 1
		}}}
		e := NewCardinalityEstimator(q, ing, ingestersWithinLimit(13*time.Hour))
		e.now = func() time.Time { return now }
		res, err := e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{Matchers: up, MinT: 96 * hour, MaxT: 98 * hour})
		require.NoError(t, err)
		return res, ing
	}
	compactedOnly := bucketindex.Blocks{{ID: b1, MinTime: 96 * hour, MaxTime: 98 * hour, CompactionLevel: 2}}

	t.Run("reference: the query path's sources (trust off) count both partitions", func(t *testing.T) {
		res, ing := estimate(t, compactedOnly, 0)
		assert.NotNil(t, ing.gotSeriesCounts)
		assert.Equal(t, map[string][]int64{"up": {5}}, res.Counts)
	})

	t.Run("control: once the late block is in the index the ingesters are asked", func(t *testing.T) {
		withLate := append(bucketindex.Blocks{{ID: b2, MinTime: 96 * hour, MaxTime: 98 * hour, CompactionLevel: 1}}, compactedOnly...)
		res, ing := estimate(t, withLate, time.Hour)
		assert.NotNil(t, ing.gotSeriesCounts)
		assert.Equal(t, map[string][]int64{"up": {5}}, res.Counts)
	})

	t.Run("default config: the ingesters are asked and both partitions count", func(t *testing.T) {
		var cfg CardinalityEstimateConfig
		flagext.DefaultValues(&cfg)
		res, ing := estimate(t, compactedOnly, cfg.TrustCompactedBlocksAfter)
		assert.NotNil(t, ing.gotSeriesCounts)
		assert.Equal(t, map[string][]int64{"up": {5}}, res.Counts)
	})

	// Why trust-compacted-blocks-after defaults to 0: when it is set, the
	// compacted block is trusted and partition 1's series are missing, with
	// no lower bound.
	t.Run("trust on: the lagging partition is missed", func(t *testing.T) {
		res, ing := estimate(t, compactedOnly, time.Hour)
		assert.Nil(t, ing.gotSeriesCounts)
		assert.Equal(t, map[string][]int64{"up": {3}}, res.Counts)
		assert.False(t, res.LowerBound)
	})
}
