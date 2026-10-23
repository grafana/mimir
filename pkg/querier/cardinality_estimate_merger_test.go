// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

func TestEstimateMerger_CountsWhenHashesWereAsked(t *testing.T) {
	m := newEstimateMerger(context.Background(), true, 0)
	require.NoError(t, m.add("store-gateway 1.1.1.1", "up", nil, []uint64{1, 2}, nil))
	// An older store-gateway ignores the hashes field and sends counts.
	err := m.add("store-gateway 2.2.2.2", "up", []int64{3}, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store-gateway 2.2.2.2 sent counts instead of the label-set hashes")
	assert.Contains(t, err.Error(), "once every store-gateway is updated")

	// Without dedup, counts are what is asked for.
	m = newEstimateMerger(context.Background(), false, 0)
	require.NoError(t, m.add("ingester", "up", []int64{3}, nil, nil))
}

// A window held by blocks and ingesters is deduplicated by hash. A
// store-gateway that answers with counts must fail the estimate, not count
// its groups as 0.
func TestCardinalityEstimator_StoreGatewayIgnoresHashes(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	b1 := ulid.MustNew(1, nil)
	day := bucketindex.Blocks{{ID: b1, MinTime: 52 * hour, MaxTime: 76 * hour}}
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(day, &bucketindex.Metadata{}, nil)
	gw := gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{3}}}})
	q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][][]ulid.ULID{gw: {{b1}}},
	}}}}, queryStoreAfter: 12 * time.Hour}
	ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Hashed: true, Responses: []*client.SeriesCountsResponse{
		{Groups: []*client.SeriesCountGroup{{Value: "up", Hashes: []uint64{1}}}, MinTimeMs: 87 * hour},
	}}}
	e := NewCardinalityEstimator(q, ing, ingestersWithinLimit(13*time.Hour))
	e.now = func() time.Time { return time.UnixMilli(100 * hour) }

	_, err := e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 52 * hour, MaxT: 100 * hour})
	require.ErrorContains(t, err, "sent counts instead of the label-set hashes")
	assert.Equal(t, http.StatusInternalServerError, estimateHTTPStatus(err))
}
