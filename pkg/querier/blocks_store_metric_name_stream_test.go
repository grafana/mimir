// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/querier/worker"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// nameBatches returns a SeriesCounts stream answer over names, in batches
// of size, the first message naming the requested blocks as counted from
// their postings offsets tables.
func nameBatches(names []string, count int64, size int, err error) func(*storegatewaypb.SeriesCountsRequest) ([]*storegatewaypb.SeriesCountsResponse, error) {
	return func(req *storegatewaypb.SeriesCountsRequest) ([]*storegatewaypb.SeriesCountsResponse, error) {
		var out []*storegatewaypb.SeriesCountsResponse
		for start := 0; start == 0 || start < len(names); start += size {
			msg := &storegatewaypb.SeriesCountsResponse{}
			if start == 0 {
				msg.BlockIds = req.BlockIds
				msg.BlocksFromOffsetsTable = int64(len(req.BlockIds))
			}
			for _, n := range names[start:min(start+size, len(names))] {
				msg.Groups = append(msg.Groups, &storegatewaypb.SeriesCountGroup{Value: n, Counts: []int64{count}})
			}
			out = append(out, msg)
		}
		return out, err
	}
}

func namesFrom(from, to int, format string) []string {
	out := make([]string, 0, to-from)
	for i := from; i < to; i++ {
		out = append(out, fmt.Sprintf(format, i))
	}
	return out
}

func TestCardinalityEstimateHandler_StreamedNameTable(t *testing.T) {
	b1, b2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	shards := bucketindex.Blocks{
		{ID: b1, MinTime: 0, MaxTime: dayMs, CompactorShardID: "1_of_2"},
		{ID: b2, MinTime: 0, MaxTime: dayMs, CompactorShardID: "2_of_2"},
	}
	// Each store-gateway sends 12 names in batches of 2. They share n006 to
	// n011, so the table has 18 names.
	const batch = 2
	gateways := func() map[BlocksStoreClient][][]ulid.ULID {
		return map[BlocksStoreClient][][]ulid.ULID{
			&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCountsStream: nameBatches(namesFrom(0, 12, "n%03d"), 1, batch, nil)}: {{b1}},
			&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesCountsStream: nameBatches(namesFrom(6, 18, "n%03d"), 2, batch, nil)}: {{b2}},
		}
	}
	// What one held batch is charged.
	const batchBytes = batch * (estimateGroupBytes + 4)

	serve := func(t *testing.T, clients map[BlocksStoreClient][][]ulid.ULID, query string) (*http.Response, []byte, *limiter.MemoryConsumptionTracker) {
		tracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, prometheus.NewCounter(prometheus.CounterOpts{Name: "rejected"}), "test")
		ctx := limiter.AddMemoryTrackerToContext(context.Background(), tracker)
		rec := estimateRequest(t, seriesCountsQueryable(0, dayMs, shards, clients), CardinalityEstimateConfig{}, false, ctx, query)
		return rec.Result(), rec.Body.Bytes(), tracker
	}

	t.Run("without limit the table is merged in name order holding one batch per store-gateway", func(t *testing.T) {
		res, body, tracker := serve(t, gateways(), "start=0&end=86400")
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		var out estimateResponse
		require.NoError(t, json.Unmarshal(body, &out))
		assert.Equal(t, readIndexHeader, out.Read)
		require.Len(t, out.Groups, 18)
		for i, g := range out.Groups {
			want := int64(1)
			switch {
			case i >= 12:
				want = 2
			case i >= 6:
				want = 3
			}
			assert.Equal(t, estimateGroup{Value: fmt.Sprintf("n%03d", i), Count: want}, g)
		}
		assert.Equal(t, int64(36), out.Total)
		assert.Equal(t, 18, out.GroupCount)
		assert.False(t, out.HasMore)
		assert.LessOrEqual(t, tracker.PeakEstimatedMemoryConsumptionBytes(), uint64(2*batchBytes), "one batch per store-gateway, not the 18-name table")
		assert.Zero(t, tracker.CurrentEstimatedMemoryConsumptionBytes(), "every batch is released")
		assert.Empty(t, res.Header.Get("Cache-Control"), "a complete table that fits the buffer can be cached")
	})

	t.Run("any group_by is merged the same way, from the offsets tables", func(t *testing.T) {
		// The store-gateways are asked concurrently.
		var (
			askedMtx sync.Mutex
			asked    []string
		)
		record := func(next func(*storegatewaypb.SeriesCountsRequest) ([]*storegatewaypb.SeriesCountsResponse, error)) func(*storegatewaypb.SeriesCountsRequest) ([]*storegatewaypb.SeriesCountsResponse, error) {
			return func(req *storegatewaypb.SeriesCountsRequest) ([]*storegatewaypb.SeriesCountsResponse, error) {
				askedMtx.Lock()
				asked = append(asked, req.GroupBy)
				askedMtx.Unlock()
				return next(req)
			}
		}
		clients := map[BlocksStoreClient][][]ulid.ULID{
			&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCountsStream: record(nameBatches(namesFrom(0, 3, "pod-%d"), 1, batch, nil))}: {{b1}},
			&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesCountsStream: record(nameBatches(namesFrom(2, 4, "pod-%d"), 1, batch, nil))}: {{b2}},
		}
		res, body, _ := serve(t, clients, "start=0&end=86400&group_by=pod")
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		var out estimateResponse
		require.NoError(t, json.Unmarshal(body, &out))
		assert.Equal(t, readIndexHeader, out.Read)
		assert.Equal(t, []string{"pod", "pod"}, asked)
		assert.Equal(t, 4, out.GroupCount)
		assert.Equal(t, int64(5), out.Total, "pod-2 is in both shards")
	})

	t.Run("with limit only the largest groups are held", func(t *testing.T) {
		res, body, tracker := serve(t, gateways(), "start=0&end=86400&limit=4")
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		var out estimateResponse
		require.NoError(t, json.Unmarshal(body, &out))
		assert.Equal(t, []estimateGroup{{Value: "n006", Count: 3}, {Value: "n007", Count: 3}, {Value: "n008", Count: 3}, {Value: "n009", Count: 3}}, out.Groups)
		assert.True(t, out.HasMore)
		assert.Equal(t, int64(36), out.Total)
		assert.LessOrEqual(t, tracker.PeakEstimatedMemoryConsumptionBytes(), uint64(2*batchBytes+5*(estimateGroupBytes+4)), "the batches and at most limit+1 groups")
		assert.Equal(t, uint64(4*(estimateGroupBytes+4)), tracker.CurrentEstimatedMemoryConsumptionBytes(), "only the groups answered stay held")
	})

	t.Run("min_count is applied while merging", func(t *testing.T) {
		res, body, _ := serve(t, gateways(), "start=0&end=86400&min_count=3")
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		var out estimateResponse
		require.NoError(t, json.Unmarshal(body, &out))
		require.Len(t, out.Groups, 6)
		assert.Equal(t, "n006", out.Groups[0].Value)
		assert.Equal(t, int64(36), out.Total, "the total counts every group")
		assert.Equal(t, 18, out.GroupCount)
	})

	t.Run("a table past the buffer is streamed and not cached", func(t *testing.T) {
		names := namesFrom(0, 40_000, "metric_name_%08d")
		clients := map[BlocksStoreClient][][]ulid.ULID{
			&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCountsStream: nameBatches(names, 1, 10_000, nil)}: {{b1, b2}},
		}
		res, body, tracker := serve(t, clients, "start=0&end=86400")
		require.Equal(t, http.StatusOK, res.StatusCode)
		require.Greater(t, len(body), nameTableBufferBytes)
		assert.Equal(t, "no-store", res.Header.Get("Cache-Control"))
		assert.Equal(t, "true", res.Header.Get(worker.ResponseStreamingEnabledHeader))
		var out estimateResponse
		require.NoError(t, json.Unmarshal(body, &out))
		require.Len(t, out.Groups, len(names))
		assert.Equal(t, names[len(names)-1], out.Groups[len(names)-1].Value)
		assert.Equal(t, int64(len(names)), out.Total)
		assert.Equal(t, len(names), out.GroupCount)
		assert.LessOrEqual(t, tracker.PeakEstimatedMemoryConsumptionBytes(), uint64(10_000*(estimateGroupBytes+len(names[0]))))
	})

	t.Run("a failure after the table started ends it with an error", func(t *testing.T) {
		names := namesFrom(0, 40_000, "metric_name_%08d")
		clients := map[BlocksStoreClient][][]ulid.ULID{
			&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCountsStream: nameBatches(names, 1, 10_000, status.Error(codes.Internal, "read tcp 10.0.0.2:41234->10.0.0.1:9095: connection lost"))}: {{b1, b2}},
		}
		res, body, _ := serve(t, clients, "start=0&end=86400")
		require.Equal(t, http.StatusOK, res.StatusCode, "the status was sent with the first rows")
		var out struct {
			Groups []estimateGroup `json:"groups"`
			Error  string          `json:"error"`
			Total  *int64          `json:"total"`
		}
		require.NoError(t, json.Unmarshal(body, &out), "the body is still a JSON object")
		assert.Contains(t, out.Error, "store-gateway: the call failed with Internal")
		assert.NotContains(t, out.Error, "10.0.0.1", "the transport error's address is left out")
		assert.Nil(t, out.Total, "no total for a table that did not finish")
	})

	t.Run("a failure before anything is sent is an error status", func(t *testing.T) {
		clients := map[BlocksStoreClient][][]ulid.ULID{
			&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCountsStream: nameBatches(namesFrom(0, 10, "n%03d"), 1, 2, status.Error(codes.Internal, "read tcp 10.0.0.2:41234->10.0.0.1:9095: connection lost"))}: {{b1, b2}},
		}
		res, body, _ := serve(t, clients, "start=0&end=86400")
		assert.Equal(t, http.StatusInternalServerError, res.StatusCode)
		assert.Contains(t, string(body), "store-gateway: the call failed with Internal")
		assert.NotContains(t, string(body), "10.0.0.1")
	})

	t.Run("names out of order are refused", func(t *testing.T) {
		clients := map[BlocksStoreClient][][]ulid.ULID{
			&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCountsStream: nameBatches([]string{"b", "a"}, 1, 1, nil)}: {{b1, b2}},
		}
		res, body, _ := serve(t, clients, "start=0&end=86400")
		assert.Equal(t, http.StatusInternalServerError, res.StatusCode)
		assert.Contains(t, string(body), `sent group "a" after "b", out of order`)
	})
}

// A per-bucket estimate answers each bucket as its own estimate and reads its
// counts, so a bucket counted from the index-headers must not come back as an
// open name table.
func TestCardinalityEstimateHandler_PerBucketNameTableIsNotStreamed(t *testing.T) {
	b1, b2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	day1 := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	day2 := bucketindex.Blocks{{ID: b2, MinTime: dayMs, MaxTime: 2 * dayMs}}
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", int64(0), 2*dayMs-1).Return(append(slices.Clone(day1), day2...), &bucketindex.Metadata{}, nil)
	finder.On("GetBlocks", mock.Anything, "user-1", int64(0), dayMs-1).Return(day1, &bucketindex.Metadata{}, nil)
	finder.On("GetBlocks", mock.Anything, "user-1", dayMs, 2*dayMs-1).Return(day2, &bucketindex.Metadata{}, nil)
	gw1 := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCountsStream: nameBatches([]string{"errors_total", "up"}, 3, 10, nil)}
	gw2 := &storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesCountsStream: nameBatches([]string{"up"}, 2, 10, nil)}
	q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][][]ulid.ULID{gw1: {{b1}}},
		map[BlocksStoreClient][][]ulid.ULID{gw2: {{b2}}},
	}}}}}

	rec := estimateRequest(t, q, CardinalityEstimateConfig{}, false, context.Background(), "start=0&end=172800&step=1d")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var out estimateResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, readPerBucket, out.Read)
	assert.ElementsMatch(t, []estimateGroup{
		{Value: "up", Count: 3, Counts: []int64{3, 2}},
		{Value: "errors_total", Count: 3, Counts: []int64{3, 0}},
	}, out.Groups)
}
