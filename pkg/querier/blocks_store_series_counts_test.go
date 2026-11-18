// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/util/limiter"
)

const dayMs = int64(24 * 60 * 60 * 1000)

func seriesCountsQueryable(minT, maxT int64, blocks bucketindex.Blocks, clients map[BlocksStoreClient][][]ulid.ULID) *BlocksStoreQueryable {
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT-1).Return(blocks, &bucketindex.Metadata{}, nil)
	return &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{clients}}}}}
}

// gatewayAnswering returns a store-gateway mock that records the request it
// got and answers with resp. Unless resp sets BlockIds, every requested
// block is reported as counted.
func gatewayAnswering(addr string, got **storegatewaypb.SeriesCountsRequest, resp storegatewaypb.SeriesCountsResponse) *storeGatewayClientMock {
	return &storeGatewayClientMock{remoteAddr: addr, mockedSeriesCounts: func(req *storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
		if got != nil {
			*got = req
		}
		out := resp
		if out.BlockIds == nil {
			out.BlockIds = req.BlockIds
		}
		return &out, nil
	}}
}

// A store-gateway reply is charged to the memory tracker message by message
// while it is read, so a reply the tenant's limit can't hold is refused
// before the querier holds it whole, and nothing stays charged after it.
func TestBlocksStoreQueryable_SeriesCounts_ChargesEachMessage(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	// Five messages of 10 groups; the tracker holds about two of them.
	const perMessage = 10
	var msgs []*storegatewaypb.SeriesCountsResponse
	for i := range 5 {
		msg := &storegatewaypb.SeriesCountsResponse{}
		if i == 0 {
			msg.BlockIds = []string{b1.String()}
		}
		for j := range perMessage {
			msg.Groups = append(msg.Groups, &cardinalitypb.SeriesCountGroup{Value: fmt.Sprintf("m%02d_%02d", i, j), Counts: []int64{1}})
		}
		msgs = append(msgs, msg)
	}
	recvs := atomic.NewInt32(0)
	gw := &storeGatewayClientMock{remoteAddr: "1.1.1.1", seriesCountsRecvs: recvs, mockedSeriesCountsStream: func(*storegatewaypb.SeriesCountsRequest) ([]*storegatewaypb.SeriesCountsResponse, error) {
		return msgs, nil
	}}
	q := seriesCountsQueryable(0, dayMs, blocks, map[BlocksStoreClient][][]ulid.ULID{gw: {{b1}}})
	q.metrics = newBlocksStoreQueryableMetrics(compartments.Config{}, prometheus.NewPedanticRegistry())
	messageBytes := uint64(perMessage * (estimateGroupBytes + len("m00_00") + 8))
	tracker := limiter.NewMemoryConsumptionTracker(context.Background(), 2*messageBytes+messageBytes/2, prometheus.NewCounter(prometheus.CounterOpts{Name: "rejected"}), "test")
	ctx := limiter.AddMemoryTrackerToContext(context.Background(), tracker)

	_, err := q.seriesCountsInWindow(ctx, "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: dayMs, GroupBy: "__name__", Matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "x")}})
	require.Error(t, err)
	assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
	assert.Equal(t, refusedByMemory, estimateRefusalLimit(err))
	assert.Equal(t, int32(3), recvs.Load(), "refused on the third message, before the reply was held whole")
	assert.Zero(t, tracker.CurrentEstimatedMemoryConsumptionBytes(), "a failed call leaves nothing charged")

	// A reply that fits is held only while it is merged.
	recvs.Store(0)
	tracker = limiter.NewMemoryConsumptionTracker(context.Background(), 1<<20, prometheus.NewCounter(prometheus.CounterOpts{Name: "rejected"}), "test")
	ctx = limiter.AddMemoryTrackerToContext(context.Background(), tracker)
	q = seriesCountsQueryable(0, dayMs, blocks, map[BlocksStoreClient][][]ulid.ULID{gw: {{b1}}})
	res, err := q.seriesCountsInWindow(ctx, "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: dayMs, GroupBy: "__name__", Matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "x")}})
	require.NoError(t, err)
	assert.Len(t, res.Counts, 5*perMessage)
	assert.Equal(t, int32(5), recvs.Load())
	assert.GreaterOrEqual(t, tracker.PeakEstimatedMemoryConsumptionBytes(), 5*messageBytes, "the whole reply was charged while held")
	assert.Equal(t, uint64(5*perMessage*(estimateGroupBytes+len("m00_00")+8)), tracker.CurrentEstimatedMemoryConsumptionBytes(), "only the merged groups stay charged")
}

func TestBlocksStoreQueryable_SeriesCounts(t *testing.T) {
	b1, b2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	up := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")}

	t.Run("adds up shards of one range", func(t *testing.T) {
		shards := bucketindex.Blocks{
			{ID: b1, MinTime: 0, MaxTime: dayMs, CompactorShardID: "1_of_2"},
			{ID: b2, MinTime: 0, MaxTime: dayMs, CompactorShardID: "2_of_2"},
		}
		var got *storegatewaypb.SeriesCountsRequest
		clients := map[BlocksStoreClient][][]ulid.ULID{
			gatewayAnswering("1.1.1.1", &got, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{1}}}, SeriesFetchedBytes: 10}): {{b1}},
			gatewayAnswering("2.2.2.2", nil, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{3}}}, SeriesFetchedBytes: 5}):   {{b2}},
		}
		q := seriesCountsQueryable(0, 2*60*60*1000, shards, clients)
		res, err := q.seriesCountsInWindow(context.Background(), "user-1", CardinalityEstimateRequest{Matchers: up, MinT: 0, MaxT: 2 * 60 * 60 * 1000, GroupBy: "__name__"})
		require.NoError(t, err)
		assert.False(t, res.Dedup)
		assert.Equal(t, map[string][]int64{"up": {4}}, res.Counts)
		assert.Equal(t, int64(15), res.SeriesFetchedBytes)
		assert.Equal(t, 2, res.StoreGateways)
		require.NotNil(t, got)
		assert.False(t, got.Hashes)
		require.Len(t, got.Matchers, 1)
		assert.Equal(t, "up", got.Matchers[0].Value)
	})

	t.Run("fails on a store-gateway answer without one count per group", func(t *testing.T) {
		shards := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
		const hour = 60 * 60 * 1000
		req := CardinalityEstimateRequest{Matchers: up, MinT: 0, MaxT: 3 * hour, GroupBy: "__name__"}
		malformed := gatewayAnswering("2.2.2.2", nil, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{6, 1}}}})
		_, err := seriesCountsQueryable(0, 3*hour, shards, map[BlocksStoreClient][][]ulid.ULID{malformed: {{b1}}}).seriesCountsInWindow(context.Background(), "user-1", req)
		require.ErrorContains(t, err, "a store-gateway returned 2 counts for \"up\" instead of one")
		assert.NotContains(t, err.Error(), "2.2.2.2")
		assert.Equal(t, "2.2.2.2", estimateSourceAddr(err))
		assert.Equal(t, http.StatusInternalServerError, estimateHTTPStatus(err))
	})

	t.Run("unions hashes across block ranges", func(t *testing.T) {
		days := bucketindex.Blocks{
			{ID: b1, MinTime: 0, MaxTime: dayMs},
			{ID: b2, MinTime: dayMs, MaxTime: 2 * dayMs},
		}
		var got *storegatewaypb.SeriesCountsRequest
		clients := map[BlocksStoreClient][][]ulid.ULID{
			gatewayAnswering("1.1.1.1", &got, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{1, 2}}}}):                                   {{b1}},
			gatewayAnswering("2.2.2.2", nil, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{2, 3}}, {Value: "x", Hashes: []uint64{9}}}}): {{b2}},
		}
		res, err := seriesCountsQueryable(0, 2*dayMs, days, clients).seriesCountsInWindow(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: 2 * dayMs})
		require.NoError(t, err)
		assert.True(t, res.Dedup)
		assert.Equal(t, map[string][]int64{"up": {3}, "x": {1}}, res.Counts, "series 2 is in both days")
		assert.True(t, got.Hashes)
	})

	t.Run("a group sent in parts is unioned with the parts of another store-gateway", func(t *testing.T) {
		days := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}, {ID: b2, MinTime: dayMs, MaxTime: 2 * dayMs}}
		clients := map[BlocksStoreClient][][]ulid.ULID{
			gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{
				{Value: "up", Hashes: []uint64{0x1111111111111111, 0x2222222222222222}},
				{Value: "up", Hashes: []uint64{0x3333333333333333}},
			}}): {{b1}},
			gatewayAnswering("2.2.2.2", nil, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{
				{Value: "up", Hashes: []uint64{0x3333333333333333}},
				{Value: "up", Hashes: []uint64{0x4444444444444444}},
				{Value: "down", Hashes: []uint64{0x5555555555555555}},
			}}): {{b2}},
		}
		res, err := seriesCountsQueryable(0, 2*dayMs, days, clients).seriesCountsInWindow(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: 2 * dayMs})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {4}, "down": {1}}, res.Counts, "four distinct up series, one shared")
		assert.Equal(t, int64(48), res.HashBytes, "six hashes of 8 bytes")
	})

	t.Run("fails if a block is not counted", func(t *testing.T) {
		blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
		clients := map[BlocksStoreClient][][]ulid.ULID{
			gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{BlockIds: []string{}}): {{b1}},
		}
		_, err := seriesCountsQueryable(0, dayMs, blocks, clients).seriesCountsInWindow(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: dayMs})
		require.ErrorContains(t, err, "none of the 1 blocks for this window was counted")
	})

	t.Run("retries an unavailable store-gateway's blocks on another one", func(t *testing.T) {
		blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs, CompactorShardID: "1_of_2"}, {ID: b2, MinTime: 0, MaxTime: dayMs, CompactorShardID: "2_of_2"}}
		down := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			return nil, status.Error(codes.Unavailable, "connection refused")
		}}
		up := gatewayAnswering("2.2.2.2", nil, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}, IndexBytes: 100, PostingsOffsetsTableBytes: 50})
		q := seriesCountsQueryable(0, dayMs, blocks, nil)
		q.dynamicReplication = storegateway.NewNopDynamicReplication(3)
		var gotRetry *storegatewaypb.SeriesCountsRequest
		replica := gatewayAnswering("3.3.3.3", &gotRetry, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{5}}}, IndexBytes: 100})
		q.compartments[0].stores = &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][][]ulid.ULID{down: {{b1}}, up: {{b2}}},
			map[BlocksStoreClient][][]ulid.ULID{replica: {{b1}}},
		}}
		res, err := q.seriesCountsInWindow(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: dayMs})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {7}}, res.Counts, "b1 counted once, by the replica")
		assert.False(t, res.LowerBound)
		assert.Equal(t, 1, res.Retries)
		assert.Equal(t, 3, res.StoreGateways)
		require.NotNil(t, gotRetry)
		assert.Equal(t, []string{b1.String()}, gotRetry.BlockIds)
	})

	t.Run("a busy store-gateway is retried on another one", func(t *testing.T) {
		blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
		busy := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			return nil, cardinality.Busyf("the store-gateway runs as many counts as it is allowed to")
		}}
		var gotRetry *storegatewaypb.SeriesCountsRequest
		replica := gatewayAnswering("3.3.3.3", &gotRetry, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{5}}}})
		q := seriesCountsQueryable(0, dayMs, blocks, nil)
		q.dynamicReplication = storegateway.NewNopDynamicReplication(3)
		q.compartments[0].stores = &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][][]ulid.ULID{busy: {{b1}}},
			map[BlocksStoreClient][][]ulid.ULID{replica: {{b1}}},
		}}
		res, err := q.seriesCountsInWindow(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: dayMs})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {5}}, res.Counts)
		assert.False(t, res.LowerBound)
		assert.Equal(t, 1, res.Retries)
		require.NotNil(t, gotRetry)
	})

	t.Run("a store-gateway with the estimate disabled for the tenant fails the request", func(t *testing.T) {
		blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
		disabled := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			return nil, status.Error(codes.FailedPrecondition, cardinality.DisabledError("user-1"))
		}}
		var gotRetry *storegatewaypb.SeriesCountsRequest
		replica := gatewayAnswering("3.3.3.3", &gotRetry, storegatewaypb.SeriesCountsResponse{})
		q := seriesCountsQueryable(0, dayMs, blocks, nil)
		q.dynamicReplication = storegateway.NewNopDynamicReplication(3)
		q.compartments[0].stores = &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][][]ulid.ULID{disabled: {{b1}}},
			map[BlocksStoreClient][][]ulid.ULID{replica: {{b1}}},
		}}
		_, err := q.seriesCountsInWindow(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: dayMs})
		require.ErrorContains(t, err, "cardinality estimate is disabled for the tenant: user-1")
		assert.Equal(t, http.StatusBadRequest, estimateHTTPStatus(err))
		assert.Nil(t, gotRetry, "another store-gateway would answer the same way")
	})

	t.Run("a block a healthy store-gateway leaves out is retried on another one", func(t *testing.T) {
		blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs, CompactorShardID: "1_of_2"}, {ID: b2, MinTime: 0, MaxTime: dayMs, CompactorShardID: "2_of_2"}}
		// Asked for both blocks, this store-gateway answers for b2 alone, as
		// one whose index-header for b1 did not load does.
		partial := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			return &storegatewaypb.SeriesCountsResponse{BlockIds: []string{b2.String()}, Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}}, nil
		}}
		q := seriesCountsQueryable(0, dayMs, blocks, nil)
		q.dynamicReplication = storegateway.NewNopDynamicReplication(3)
		var gotRetry *storegatewaypb.SeriesCountsRequest
		replica := gatewayAnswering("3.3.3.3", &gotRetry, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{5}}}})
		q.compartments[0].stores = &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][][]ulid.ULID{partial: {{b1, b2}}},
			map[BlocksStoreClient][][]ulid.ULID{replica: {{b1}}},
		}}
		res, err := q.seriesCountsInWindow(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: dayMs})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {7}}, res.Counts, "b1 counted once, by the replica")
		assert.False(t, res.LowerBound)
		assert.Empty(t, res.MissingBlocks)
		assert.Equal(t, 1, res.Retries)
		require.NotNil(t, gotRetry)
		assert.Equal(t, []string{b1.String()}, gotRetry.BlockIds, "only the block left out is retried")
	})

	t.Run("a block no replica counts makes the counts a lower bound", func(t *testing.T) {
		blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs, CompactorShardID: "1_of_2"}, {ID: b2, MinTime: 0, MaxTime: dayMs, CompactorShardID: "2_of_2"}}
		down := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			return nil, status.Error(codes.Unavailable, "connection refused")
		}}
		up := gatewayAnswering("2.2.2.2", nil, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}})
		q := seriesCountsQueryable(0, dayMs, blocks, nil)
		q.dynamicReplication = storegateway.NewNopDynamicReplication(2)
		q.compartments[0].stores = &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][][]ulid.ULID{down: {{b1}}, up: {{b2}}},
			errors.New("no store-gateway left"),
		}}
		var logs bytes.Buffer
		q.logger = log.NewLogfmtLogger(&logs)
		res, err := q.seriesCountsInWindow(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: dayMs})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {2}}, res.Counts)
		assert.True(t, res.LowerBound)
		assert.Equal(t, []ulid.ULID{b1}, res.MissingBlocks)
		assert.Equal(t, []ulid.ULID{b2}, res.Blocks)
		// Both ways a block is left uncounted are logged with the address, the attempt and the error.
		assert.Contains(t, logs.String(), `level=warn`)
		assert.Contains(t, logs.String(), `store_gateway=1.1.1.1 attempt=1 blocks=1 err="rpc error: code = Unavailable desc = connection refused"`)
		assert.Contains(t, logs.String(), `attempt=2 blocks=1 err="no store-gateway left"`)
		require.Error(t, res.missingReason, "the last reason is kept for the request's log line")
		assert.Contains(t, res.missingReason.Error(), "Unavailable")
	})

	t.Run("a range with fewer shard blocks than its shard count is summed as a lower bound", func(t *testing.T) {
		blocks := bucketindex.Blocks{
			{ID: b1, MinTime: 0, MaxTime: dayMs, CompactorShardID: "1_of_3"},
			{ID: b2, MinTime: 0, MaxTime: dayMs, CompactorShardID: "3_of_3"},
		}
		var got *storegatewaypb.SeriesCountsRequest
		clients := map[BlocksStoreClient][][]ulid.ULID{
			gatewayAnswering("1.1.1.1", &got, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}}): {{b1}},
			gatewayAnswering("2.2.2.2", nil, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{3}}}}):  {{b2}},
		}
		res, err := seriesCountsQueryable(0, dayMs, blocks, clients).seriesCountsInWindow(context.Background(), "user-1", CardinalityEstimateRequest{Matchers: up, MinT: 0, MaxT: dayMs})
		require.NoError(t, err)
		assert.False(t, res.Dedup, "the shards present share no series, so they are added up")
		require.NotNil(t, got)
		assert.False(t, got.Hashes)
		assert.Equal(t, map[string][]int64{"up": {5}}, res.Counts)
		assert.True(t, res.LowerBound)
		assert.Equal(t, []missingShard{{Shard: "2_of_3", MinTime: 0, MaxTime: dayMs}}, res.MissingShards)
		assert.Empty(t, res.MissingBlocks)
	})
}

// A reply the querier's memory limit can't hold fails the request: the querier
// refused it, so another store-gateway is not asked and the block is not
// reported as missing.
func TestBlocksStoreQueryable_SeriesCounts_MemoryRefusalWhileReceivingIsNotRetried(t *testing.T) {
	b1, b2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs, CompactorShardID: "1_of_2"}, {ID: b2, MinTime: 0, MaxTime: dayMs, CompactorShardID: "2_of_2"}}
	var big []*cardinalitypb.SeriesCountGroup
	for i := range 500 {
		big = append(big, &cardinalitypb.SeriesCountGroup{Value: fmt.Sprintf("metric_%04d", i), Counts: []int64{1}})
	}
	small := gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}})
	large := gatewayAnswering("2.2.2.2", nil, storegatewaypb.SeriesCountsResponse{Groups: big})
	var gotRetry *storegatewaypb.SeriesCountsRequest
	replica := gatewayAnswering("3.3.3.3", &gotRetry, storegatewaypb.SeriesCountsResponse{Groups: big})
	q := seriesCountsQueryable(0, dayMs, blocks, nil)
	q.dynamicReplication = storegateway.NewNopDynamicReplication(2)
	q.compartments[0].stores = &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][][]ulid.ULID{small: {{b1}}, large: {{b2}}},
		map[BlocksStoreClient][][]ulid.ULID{replica: {{b2}}},
	}}
	tracker := limiter.NewMemoryConsumptionTracker(context.Background(), 4096, prometheus.NewCounter(prometheus.CounterOpts{Name: "rejected"}), "test")
	ctx := limiter.AddMemoryTrackerToContext(context.Background(), tracker)

	_, err := q.seriesCountsInWindow(ctx, "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: dayMs})
	require.Error(t, err)
	assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
	assert.Equal(t, refusedByMemory, estimateRefusalLimit(err))
	assert.ErrorContains(t, err, "the store-gateway replies this estimate holds in the querier", "the refusal is the querier's, not a store-gateway failure")
	assert.Empty(t, estimateSourceAddr(err))
	assert.Nil(t, gotRetry, "the refused answer is not asked of another store-gateway")
}

func TestShouldRetrySeriesCounts(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"a store-gateway failure":               {err: classifyStoreGatewayError("1.1.1.1", status.Error(codes.Internal, "boom")), want: true},
		"a store-gateway that can't be reached": {err: classifyStoreGatewayError("1.1.1.1", status.Error(codes.Unavailable, "connection refused")), want: true},
		"a busy store-gateway":                  {err: classifyStoreGatewayError("1.1.1.1", cardinality.Busyf("busy")), want: true},
		"a store-gateway's refusal":             {err: classifyStoreGatewayError("1.1.1.1", cardinality.Refusalf(cardinality.LimitMaxGroups, "too many")), want: false},
		"a bad request":                         {err: classifyStoreGatewayError("1.1.1.1", status.Error(codes.InvalidArgument, "bad")), want: false},
		"a tenant with the estimate disabled":   {err: classifyStoreGatewayError("1.1.1.1", status.Error(codes.FailedPrecondition, "disabled")), want: false},
		"the querier's refusal":                 {err: refusedByLimit(refusedByMemory, "over"), want: false},
		"a refusal inside a redacted failure":   {err: redactFailure("store-gateway", refusedByLimit(refusedByMemory, "over")), want: false},
		"a bad request inside a failure":        {err: redactFailure("store-gateway", badEstimateRequest("bad")), want: false},
		"a canceled call":                       {err: classifyStoreGatewayError("1.1.1.1", context.Canceled), want: false},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, shouldRetrySeriesCounts(ctx, tc.err))
		})
	}
}

// A store-gateway whose stream fails partway is retried on another one, and
// what it sent before failing is not counted.
func TestBlocksStoreQueryable_SeriesCounts_StreamFailingPartway(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	partway := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCountsStream: func(req *storegatewaypb.SeriesCountsRequest) ([]*storegatewaypb.SeriesCountsResponse, error) {
		return []*storegatewaypb.SeriesCountsResponse{{BlockIds: req.BlockIds, Groups: []*cardinalitypb.SeriesCountGroup{{Value: "a", Counts: []int64{5}}}}}, status.Error(codes.Unavailable, "connection reset")
	}}
	whole := &storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesCountsStream: func(req *storegatewaypb.SeriesCountsRequest) ([]*storegatewaypb.SeriesCountsResponse, error) {
		return []*storegatewaypb.SeriesCountsResponse{
			{BlockIds: req.BlockIds, Groups: []*cardinalitypb.SeriesCountGroup{{Value: "a", Counts: []int64{2}}}},
			{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "b", Counts: []int64{1}}}},
		}, nil
	}}
	q := seriesCountsQueryable(0, dayMs, blocks, nil)
	q.dynamicReplication = storegateway.NewNopDynamicReplication(2)
	q.compartments[0].stores = &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][][]ulid.ULID{partway: {{b1}}},
		map[BlocksStoreClient][][]ulid.ULID{whole: {{b1}}},
	}}
	res, err := q.seriesCountsInWindow(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: dayMs})
	require.NoError(t, err)
	assert.Equal(t, map[string][]int64{"a": {2}, "b": {1}}, res.Counts, "the groups of every message, and none of the failed stream's")
	assert.Equal(t, 1, res.Retries)
}

func TestParseSeriesCountsRequest(t *testing.T) {
	parse := func(query string) (CardinalityEstimateRequest, int, error) {
		r := httptest.NewRequest(http.MethodGet, "/x?"+query, nil)
		require.NoError(t, r.ParseForm())
		req, opts, err := parseCardinalityEstimateRequest(r, defaultMaxGroupByLength)
		return req, opts.limit, err
	}
	req, limit, err := parse(`start=0&end=86400&match[]={__name__="up"}&group_by=pod&limit=5`)
	require.NoError(t, err)
	assert.Equal(t, int64(86400000), req.MaxT)
	assert.Equal(t, "pod", req.GroupBy)
	assert.Equal(t, 5, limit)
	require.Len(t, req.Matchers, 1)

	req, _, err = parse("start=0&end=10")
	require.NoError(t, err)
	assert.Equal(t, "__name__", req.GroupBy)
	assert.Empty(t, req.Matchers)

	// Every series has a name, so a matcher on any name is no matcher.
	req, _, err = parse(`start=0&end=10&match[]={__name__=~".%2B"}`)
	require.NoError(t, err)
	assert.Nil(t, req.Matchers)
	req, _, err = parse(`start=0&end=10&match[]={__name__=~".*",job="x"}`)
	require.NoError(t, err)
	assert.Equal(t, []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "x")}, req.Matchers)
	for _, every := range []string{`{__name__!=""}`, `{__name__!~""}`} {
		req, _, err = parse("start=0&end=10&match[]=" + every)
		require.NoError(t, err)
		assert.Nil(t, req.Matchers, every)
	}
	req, _, err = parse(`start=0&end=10&match[]={__name__=~".%2Bx"}`)
	require.NoError(t, err)
	assert.Len(t, req.Matchers, 1, "a regex that matches only some names stays")

	for _, bad := range []string{
		"start=x&end=10",
		"start=10&end=5",
		"start=0&end=10&match[]=up{&",
		"start=0&end=10&match[]=a&match[]=b",
		`start=0&end=10&match[]={__name__="up",__query_shard__="1_of_2"}`,
		"start=0&end=10&limit=x",
	} {
		_, _, err := parse(bad)
		assert.Error(t, err, bad)
	}
}

func TestCardinalityEstimateHandler(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	clients := map[BlocksStoreClient][][]ulid.ULID{
		gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{
			{Value: "a", Counts: []int64{1}}, {Value: "b", Counts: []int64{4}}, {Value: "c", Counts: []int64{2}},
		}, IndexBytes: 300, PostingsOffsetsTableBytes: 40}): {{b1}},
	}
	q := seriesCountsQueryable(0, dayMs, blocks, clients)
	var logs bytes.Buffer
	q.logger = log.NewLogfmtLogger(&logs)
	reg := prometheus.NewPedanticRegistry()
	q.metrics = newBlocksStoreQueryableMetrics(compartments.Config{}, reg)
	h := NewCardinalityEstimateHandler(NewCardinalityEstimator(q, nil, ingestersWithinLimit(13*time.Hour)), CardinalityEstimateConfig{}, false)

	// A matcher sends it to the full index.
	r := httptest.NewRequest(http.MethodGet, `/prometheus/api/v1/cardinality/estimate?start=0&end=86400&limit=2&match[]={job="x"}`, nil)
	r = r.WithContext(user.InjectOrgID(r.Context(), "user-1"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out estimateResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, ReadFullIndex, out.Read)
	assert.Equal(t, 3, out.GroupCount)
	assert.Equal(t, int64(7), out.Total)
	assert.Equal(t, []estimateGroup{{Value: "b", Count: 4}, {Value: "c", Count: 2}}, out.Groups)
	// The field names are the API contract's: total, and the window counted.
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	assert.JSONEq(t, `7`, string(raw["total"]))
	assert.JSONEq(t, `{"start":0,"end":86400000}`, string(raw["window"]))
	for _, old := range []string{"series", "min_time", "max_time", "snapped"} {
		assert.NotContains(t, raw, old)
	}
	// What the estimate cost is in the log line and the metrics, not in the
	// response.
	for _, cost := range []string{"elapsed_ms", "index_bytes", "postings_fetched_bytes", "series_fetched_bytes", "postings_offsets_table_bytes", "series_counted", "hash_bytes", "blocks_from_postings", "retries", "store_gateways", "blocks"} {
		assert.NotContains(t, raw, cost)
	}
	line := logs.String()
	assert.Contains(t, line, `msg="cardinality estimate"`)
	assert.Contains(t, line, "index_bytes=300")
	assert.Contains(t, line, "postings_offsets_table_bytes=40")
	assert.Contains(t, line, "blocks=1")
	assert.Contains(t, line, "read=full_index")
	assert.Equal(t, 1, strings.Count(line, "\n"), "one line per request")
	assert.Equal(t, float64(300), testutil.ToFloat64(q.metrics.estimateBytes.WithLabelValues(ReadFullIndex, "index")))
	assert.Equal(t, float64(40), testutil.ToFloat64(q.metrics.estimateBytes.WithLabelValues(ReadFullIndex, "postings_offsets_table")))
	assert.Equal(t, float64(1), testutil.ToFloat64(q.metrics.estimateBlocks.WithLabelValues(ReadFullIndex)))
	assert.Equal(t, SourceBlocks, out.Source)
	assert.True(t, out.HasMore, "limit=2 cut one of three groups")
	assert.False(t, out.LowerBound)
	assert.Empty(t, out.MissingBlocks)

	// The blocks cover only the first day of a two-day window: the answer is
	// a lower bound.
	q.compartments[0].finder.(*blocksFinderMock).On("GetBlocks", mock.Anything, "user-1", int64(0), 2*dayMs-1).Return(blocks, &bucketindex.Metadata{}, nil)
	q.compartments[0].stores.(*blocksStoreSetMock).mockedResponses = append(q.compartments[0].stores.(*blocksStoreSetMock).mockedResponses, clients)
	r = httptest.NewRequest(http.MethodGet, `/prometheus/api/v1/cardinality/estimate?start=0&end=172800&match[]={job="x"}`, nil)
	r = r.WithContext(user.InjectOrgID(r.Context(), "user-1"))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"lower_bound":true`)

}

// A block range missing one of its shard blocks is answered from the
// shards present, as a lower bound that names the missing shard.
func TestCardinalityEstimateHandler_MissingShard(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs, CompactorShardID: "1_of_2"}}
	gateway := &storeGatewayClientMock{
		remoteAddr: "1.1.1.1",
		mockedSeriesCounts: func(req *storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			return &storegatewaypb.SeriesCountsResponse{BlockIds: req.BlockIds, BlocksFromOffsetsTable: int64(len(req.BlockIds)), Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{3}}}}, nil
		},
	}
	q := seriesCountsQueryable(0, dayMs, blocks, map[BlocksStoreClient][][]ulid.ULID{gateway: {{b1}}})
	h := NewCardinalityEstimateHandler(NewCardinalityEstimator(q, nil, ingestersWithinLimit(13*time.Hour)), CardinalityEstimateConfig{}, false)

	r := httptest.NewRequest(http.MethodGet, `/prometheus/api/v1/cardinality/estimate?start=0&end=86400`, nil)
	r = r.WithContext(user.InjectOrgID(r.Context(), "user-1"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	assert.JSONEq(t, `"index_header"`, string(raw["read"]))
	assert.JSONEq(t, `true`, string(raw["lower_bound"]))
	assert.JSONEq(t, `[{"shard":"2_of_2","start":0,"end":86400000}]`, string(raw["missing_shards"]))
	assert.NotContains(t, raw, "missing_blocks")
	assert.JSONEq(t, `3`, string(raw["total"]))
}

// The design spells "every metric name" as {__name__=~".+"}, which is read
// as no matcher and answered from the index-headers.
func TestCardinalityEstimateHandler_EveryNameIsNoMatcher(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	gateway := &storeGatewayClientMock{
		remoteAddr: "1.1.1.1",
		mockedSeriesCounts: func(req *storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			return &storegatewaypb.SeriesCountsResponse{BlockIds: req.BlockIds, BlocksFromOffsetsTable: int64(len(req.BlockIds)), Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{3}}}}, nil
		},
	}
	q := seriesCountsQueryable(0, dayMs, blocks, map[BlocksStoreClient][][]ulid.ULID{gateway: {{b1}}})
	h := NewCardinalityEstimateHandler(NewCardinalityEstimator(q, nil, ingestersWithinLimit(13*time.Hour)), CardinalityEstimateConfig{}, false)
	r := httptest.NewRequest(http.MethodGet, `/prometheus/api/v1/cardinality/estimate?start=0&end=86400&match[]={__name__=~".%2B"}`, nil)
	r = r.WithContext(user.InjectOrgID(r.Context(), "user-1"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"read":"index_header"`)
}

func TestBlocksStoreQueryable_CardinalityEstimate_PicksTheRead(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	day := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	gateway := &storeGatewayClientMock{
		remoteAddr: "1.1.1.1",
		// A window of a whole block with no matchers is counted from the
		// offsets table, anything else from series entries.
		mockedSeriesCounts: func(req *storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			if len(req.Matchers) == 0 && req.EndMs-req.StartMs == dayMs {
				return &storegatewaypb.SeriesCountsResponse{BlockIds: req.BlockIds, BlocksFromOffsetsTable: int64(len(req.BlockIds)), Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{3}}}}, nil
			}
			return &storegatewaypb.SeriesCountsResponse{BlockIds: req.BlockIds, Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}}, nil
		},
	}
	queryable := func(windows ...[2]int64) *BlocksStoreQueryable {
		finder := &blocksFinderMock{}
		for _, w := range windows {
			finder.On("GetBlocks", mock.Anything, "user-1", w[0], w[1]-1).Return(day, &bucketindex.Metadata{}, nil)
		}
		clients := map[BlocksStoreClient][][]ulid.ULID{gateway: {{b1}}}
		return &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{clients, clients}}}}}
	}
	up := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")}

	for name, tc := range map[string]struct {
		req    CardinalityEstimateRequest
		read   string
		counts map[string][]int64
	}{
		"every name over one block range": {CardinalityEstimateRequest{MinT: 0, MaxT: dayMs, GroupBy: "__name__"}, ReadIndexHeader, map[string][]int64{"up": {3}}},
		"a matcher":                       {CardinalityEstimateRequest{MinT: 0, MaxT: dayMs, Matchers: up}, ReadFullIndex, map[string][]int64{"up": {2}}},
		"a window inside the block":       {CardinalityEstimateRequest{MinT: 0, MaxT: dayMs / 2}, ReadFullIndex, map[string][]int64{"up": {2}}},
		"grouped by another label":        {CardinalityEstimateRequest{MinT: 0, MaxT: dayMs, GroupBy: "job"}, ReadIndexHeader, map[string][]int64{"up": {3}}},
	} {
		t.Run(name, func(t *testing.T) {
			req := tc.req
			res, err := queryable([2]int64{req.MinT, req.MaxT}, [2]int64{0, dayMs}).cardinalityEstimate(context.Background(), "user-1", &req)
			require.NoError(t, err)
			assert.Equal(t, tc.read, res.Read)
			assert.Equal(t, tc.counts, res.Counts)
		})
	}
}

// As on the Series path, a store-gateway failing with any gRPC code has its blocks retried on another
// one; a refusal is not retried, since another store-gateway would refuse the same.
func TestBlocksStoreQueryable_SeriesCounts_RetriesAnyStoreGatewayFailure(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	failing := func(calls *int, err error) *storeGatewayClientMock {
		return &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			*calls++
			return nil, err
		}}
	}
	queryable := func(first *storeGatewayClientMock, replica *storeGatewayClientMock) *BlocksStoreQueryable {
		q := seriesCountsQueryable(0, dayMs, blocks, nil)
		q.dynamicReplication = storegateway.NewNopDynamicReplication(2)
		q.compartments[0].stores = &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][][]ulid.ULID{first: {{b1}}},
			map[BlocksStoreClient][][]ulid.ULID{replica: {{b1}}},
		}}
		return q
	}

	for _, code := range []codes.Code{codes.Internal, codes.Unknown, codes.Unavailable, codes.Aborted} {
		t.Run(code.String()+" is retried", func(t *testing.T) {
			var failed int
			var gotRetry *storegatewaypb.SeriesCountsRequest
			replica := gatewayAnswering("2.2.2.2", &gotRetry, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{5}}}})
			res, err := queryable(failing(&failed, status.Error(code, "block 01ABC: expanded postings: boom")), replica).seriesCountsInWindow(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: dayMs})
			require.NoError(t, err)
			assert.Equal(t, map[string][]int64{"up": {5}}, res.Counts)
			assert.False(t, res.LowerBound)
			assert.Equal(t, 1, failed)
			assert.Equal(t, 1, res.Retries)
			require.NotNil(t, gotRetry)
		})
	}

	for name, refusal := range map[string]error{
		"a limit refusal": cardinality.Refusalf(cardinality.LimitMaxGroups, "the blocks hold at least 3 distinct values of pod, more than max_groups 2"),
		"a bad request":   status.Error(codes.InvalidArgument, "max_groups can't be negative"),
	} {
		t.Run(name+" is not retried", func(t *testing.T) {
			var failed int
			var gotRetry *storegatewaypb.SeriesCountsRequest
			replica := gatewayAnswering("2.2.2.2", &gotRetry, storegatewaypb.SeriesCountsResponse{})
			_, err := queryable(failing(&failed, refusal), replica).seriesCountsInWindow(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: dayMs})
			require.Error(t, err)
			assert.Equal(t, 1, failed)
			assert.Nil(t, gotRetry, "the replica is not asked")
		})
	}

	t.Run("a canceled request is not retried", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var gotRetry *storegatewaypb.SeriesCountsRequest
		canceling := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			cancel()
			return nil, status.Error(codes.Canceled, "context canceled")
		}}
		replica := gatewayAnswering("2.2.2.2", &gotRetry, storegatewaypb.SeriesCountsResponse{})
		_, err := queryable(canceling, replica).seriesCountsInWindow(ctx, "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: dayMs})
		require.Error(t, err)
		assert.Nil(t, gotRetry, "the replica is not asked")
	})
}

// A block ID a store-gateway reports without having been asked for it does not make that block
// counted: the store-gateway that was asked for it left it out, so it is retried.
func TestBlocksStoreQueryable_SeriesCounts_IgnoresBlockIDsNotAskedFor(t *testing.T) {
	b1, b2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs, CompactorShardID: "1_of_2"}, {ID: b2, MinTime: 0, MaxTime: dayMs, CompactorShardID: "2_of_2"}}
	overReporting := gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{BlockIds: []string{b1.String(), b2.String()}, Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}})
	leavingOut := gatewayAnswering("2.2.2.2", nil, storegatewaypb.SeriesCountsResponse{BlockIds: []string{}})
	var gotRetry *storegatewaypb.SeriesCountsRequest
	replica := gatewayAnswering("3.3.3.3", &gotRetry, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{5}}}})
	q := seriesCountsQueryable(0, dayMs, blocks, nil)
	q.dynamicReplication = storegateway.NewNopDynamicReplication(2)
	q.compartments[0].stores = &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][][]ulid.ULID{overReporting: {{b1}}, leavingOut: {{b2}}},
		map[BlocksStoreClient][][]ulid.ULID{replica: {{b2}}},
	}}
	res, err := q.seriesCountsInWindow(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: dayMs})
	require.NoError(t, err)
	require.NotNil(t, gotRetry, "b2 is retried")
	assert.Equal(t, []string{b2.String()}, gotRetry.BlockIds)
	assert.Equal(t, map[string][]int64{"up": {7}}, res.Counts)
	assert.False(t, res.LowerBound)
}
