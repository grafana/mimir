// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

func TestBlocksStoreQueryable_MetricNameCounts(t *testing.T) {
	const minT, maxT = int64(0), int64(24 * 60 * 60 * 1000)
	b1, b2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	shards := bucketindex.Blocks{
		{ID: b1, MinTime: minT, MaxTime: maxT, CompactorShardID: "1_of_2"},
		{ID: b2, MinTime: minT, MaxTime: maxT, CompactorShardID: "2_of_2"},
	}

	countsFor := func(id ulid.ULID, counts map[string]int64) func(*storegatewaypb.MetricNameCountsRequest) (*storegatewaypb.MetricNameCountsResponse, error) {
		return func(req *storegatewaypb.MetricNameCountsRequest) (*storegatewaypb.MetricNameCountsResponse, error) {
			if len(req.BlockIds) != 1 || req.BlockIds[0] != id.String() {
				return nil, errors.New("unexpected blocks")
			}
			resp := &storegatewaypb.MetricNameCountsResponse{BlockIds: req.BlockIds}
			for name, n := range counts {
				resp.Counts = append(resp.Counts, &storegatewaypb.MetricNameCount{Name: name, Count: n})
			}
			return resp, nil
		}
	}
	twoGateways := map[BlocksStoreClient][][]ulid.ULID{
		&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedMetricNameCounts: countsFor(b1, map[string]int64{"up": 3, "errors_total": 1})}: {{b1}},
		&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedMetricNameCounts: countsFor(b2, map[string]int64{"up": 2})}:                    {{b2}},
	}

	newQueryable := func(blocks bucketindex.Blocks, clients any) *BlocksStoreQueryable {
		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT-1).Return(blocks, &bucketindex.Metadata{}, nil)
		return &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{clients}}}}}
	}

	t.Run("sums the shards across store-gateways", func(t *testing.T) {
		res, err := newQueryable(shards, twoGateways).MetricNameCounts(context.Background(), "user-1", minT, maxT)
		require.NoError(t, err)
		assert.Equal(t, map[string]int64{"up": 5, "errors_total": 1}, res.Counts)
		assert.ElementsMatch(t, []ulid.ULID{b1, b2}, res.Blocks)
		assert.Equal(t, 2, res.StoreGateways)
	})

	t.Run("fails if a block is not counted", func(t *testing.T) {
		oneCounted := map[BlocksStoreClient][][]ulid.ULID{
			&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedMetricNameCounts: countsFor(b1, map[string]int64{"up": 3})}: {{b1}},
			&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedMetricNameCounts: func(req *storegatewaypb.MetricNameCountsRequest) (*storegatewaypb.MetricNameCountsResponse, error) {
				return &storegatewaypb.MetricNameCountsResponse{}, nil
			}}: {{b2}},
		}
		_, err := newQueryable(shards, oneCounted).MetricNameCounts(context.Background(), "user-1", minT, maxT)
		require.ErrorContains(t, err, "was not counted")
	})

	t.Run("no blocks", func(t *testing.T) {
		res, err := newQueryable(bucketindex.Blocks{}, map[BlocksStoreClient][][]ulid.ULID{}).MetricNameCounts(context.Background(), "user-1", minT, maxT)
		require.NoError(t, err)
		assert.Empty(t, res.Counts)
	})
}

func TestCheckMetricNameCountsBlocks(t *testing.T) {
	const minT, maxT = int64(0), int64(100)
	block := func(lo, hi int64, shard string) *bucketindex.Block {
		return &bucketindex.Block{ID: ulid.MustNew(uint64(lo+hi), nil), MinTime: lo, MaxTime: hi, CompactorShardID: shard}
	}
	assert.NoError(t, checkMetricNameCountsBlocks(bucketindex.Blocks{block(0, 100, "")}, minT, maxT))
	assert.NoError(t, checkMetricNameCountsBlocks(bucketindex.Blocks{block(0, 100, "1_of_2"), block(0, 100, "2_of_2")}, minT, maxT))
	assert.ErrorContains(t, checkMetricNameCountsBlocks(bucketindex.Blocks{block(0, 50, ""), block(50, 100, "")}, minT, maxT), "more than one block range")
	assert.ErrorContains(t, checkMetricNameCountsBlocks(bucketindex.Blocks{block(0, 200, "")}, minT, maxT), "cuts through")
	assert.ErrorContains(t, checkMetricNameCountsBlocks(bucketindex.Blocks{block(0, 100, ""), block(0, 100, "1_of_2")}, minT, maxT), "may share series")
	assert.ErrorContains(t, checkMetricNameCountsBlocks(bucketindex.Blocks{block(0, 100, "1_of_2"), block(0, 100, "1_of_4")}, minT, maxT), "shard counts")
	assert.ErrorContains(t, checkMetricNameCountsBlocks(bucketindex.Blocks{block(0, 100, "1_of_2"), block(0, 100, "1_of_2")}, minT, maxT), "appears twice")
}
