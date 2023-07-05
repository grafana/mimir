// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestStoreGatewayStreamReader_HappyPaths(t *testing.T) {
	series0 := []storepb.AggrChunk{createChunk(t, 1000, 1)}
	series1 := []storepb.AggrChunk{createChunk(t, 1000, 2)}
	series2 := []storepb.AggrChunk{createChunk(t, 1000, 3)}
	series3 := []storepb.AggrChunk{createChunk(t, 1000, 4)}
	series4 := []storepb.AggrChunk{createChunk(t, 1000, 5)}

	testCases := map[string]struct {
		batches []storepb.StreamingChunksBatch
	}{
		"single series per batch": {
			batches: []storepb.StreamingChunksBatch{
				{Series: []*storepb.StreamingChunks{{SeriesIndex: 0, Chunks: series0}}},
				{Series: []*storepb.StreamingChunks{{SeriesIndex: 1, Chunks: series1}}},
				{Series: []*storepb.StreamingChunks{{SeriesIndex: 2, Chunks: series2}}},
				{Series: []*storepb.StreamingChunks{{SeriesIndex: 3, Chunks: series3}}},
				{Series: []*storepb.StreamingChunks{{SeriesIndex: 4, Chunks: series4}}},
			},
		},
		"multiple series per batch": {
			batches: []storepb.StreamingChunksBatch{
				{
					Series: []*storepb.StreamingChunks{
						{SeriesIndex: 0, Chunks: series0},
						{SeriesIndex: 1, Chunks: series1},
						{SeriesIndex: 2, Chunks: series2},
					},
				},
				{
					Series: []*storepb.StreamingChunks{
						{SeriesIndex: 3, Chunks: series3},
						{SeriesIndex: 4, Chunks: series4},
					},
				},
			},
		},
		"empty batches": {
			batches: []storepb.StreamingChunksBatch{
				{
					Series: []*storepb.StreamingChunks{
						{SeriesIndex: 0, Chunks: series0},
						{SeriesIndex: 1, Chunks: series1},
						{SeriesIndex: 2, Chunks: series2},
					},
				},
				{},
				{
					Series: []*storepb.StreamingChunks{
						{SeriesIndex: 3, Chunks: series3},
						{SeriesIndex: 4, Chunks: series4},
					},
				},
				{},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			mockClient := &mockStoreGatewayQueryStreamClient{ctx: context.Background(), batches: testCase.batches}
			reader := NewStoreGatewayStreamReader(mockClient, 5, limiter.NewQueryLimiter(0, 0, 0, nil), &stats.Stats{}, log.NewNopLogger())
			reader.StartBuffering()

			for i, expected := range [][]storepb.AggrChunk{series0, series1, series2, series3, series4} {
				actual, err := reader.GetChunks(uint64(i))
				require.NoError(t, err)
				require.Equalf(t, expected, actual, "received unexpected chunk for series index %v", i)
			}

			require.Eventually(t, func() bool {
				return mockClient.closed.Load()
			}, time.Second, 10*time.Millisecond)
		})
	}
}

func TestStoreGatewayStreamReader_AbortsWhenContextCancelled(t *testing.T) {
	// Ensure that the buffering goroutine is not leaked after context cancellation.
	test.VerifyNoLeak(t)

	// Create multiple batches to ensure that the buffering goroutine becomes blocked waiting to send further chunks to GetChunks().
	batches := []storepb.StreamingChunksBatch{
		{Series: []*storepb.StreamingChunks{{SeriesIndex: 0, Chunks: []storepb.AggrChunk{createChunk(t, 1000, 1.23)}}}},
		{Series: []*storepb.StreamingChunks{{SeriesIndex: 1, Chunks: []storepb.AggrChunk{createChunk(t, 1000, 4.56)}}}},
		{Series: []*storepb.StreamingChunks{{SeriesIndex: 2, Chunks: []storepb.AggrChunk{createChunk(t, 1000, 7.89)}}}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	mockClient := &mockStoreGatewayQueryStreamClient{ctx: ctx, batches: batches}

	reader := NewStoreGatewayStreamReader(mockClient, 3, limiter.NewQueryLimiter(0, 0, 0, nil), &stats.Stats{}, log.NewNopLogger())
	cancel()
	reader.StartBuffering()

	for i := 0; i < 3; i++ {
		_, err := reader.GetChunks(uint64(i))

		if errors.Is(err, context.Canceled) {
			break
		}

		require.NoError(t, err)

		if i == 2 {
			require.Fail(t, "expected GetChunks to report context cancellation error before reaching end of stream")
		}
	}

	require.True(t, mockClient.closed.Load(), "expected gRPC client to be closed after context cancelled")
}

func TestStoreGatewayStreamReader_ReadingSeriesOutOfOrder(t *testing.T) {
	batches := []storepb.StreamingChunksBatch{
		{Series: []*storepb.StreamingChunks{{SeriesIndex: 0, Chunks: []storepb.AggrChunk{createChunk(t, 1000, 1.23)}}}},
	}

	mockClient := &mockStoreGatewayQueryStreamClient{ctx: context.Background(), batches: batches}
	reader := NewStoreGatewayStreamReader(mockClient, 1, limiter.NewQueryLimiter(0, 0, 0, nil), &stats.Stats{}, log.NewNopLogger())
	reader.StartBuffering()

	s, err := reader.GetChunks(1)
	require.Nil(t, s)
	require.EqualError(t, err, "attempted to read series at index 1 from stream, but the stream has series with index 0")
}

func TestStoreGatewayStreamReader_ReadingMoreSeriesThanAvailable(t *testing.T) {
	firstSeries := []storepb.AggrChunk{createChunk(t, 1000, 1.23)}
	batches := []storepb.StreamingChunksBatch{
		{Series: []*storepb.StreamingChunks{{SeriesIndex: 0, Chunks: firstSeries}}},
	}

	mockClient := &mockStoreGatewayQueryStreamClient{ctx: context.Background(), batches: batches}
	reader := NewStoreGatewayStreamReader(mockClient, 1, limiter.NewQueryLimiter(0, 0, 0, nil), &stats.Stats{}, log.NewNopLogger())
	reader.StartBuffering()

	s, err := reader.GetChunks(0)
	require.NoError(t, err)
	require.Equal(t, s, firstSeries)

	s, err = reader.GetChunks(1)
	require.Nil(t, s)
	require.EqualError(t, err, "attempted to read series at index 1 from stream, but the stream has already been exhausted")
}

func TestStoreGatewayStreamReader_ReceivedFewerSeriesThanExpected(t *testing.T) {
	firstSeries := []storepb.AggrChunk{createChunk(t, 1000, 1.23)}
	batches := []storepb.StreamingChunksBatch{
		{Series: []*storepb.StreamingChunks{{SeriesIndex: 0, Chunks: firstSeries}}},
	}

	mockClient := &mockStoreGatewayQueryStreamClient{ctx: context.Background(), batches: batches}
	reader := NewStoreGatewayStreamReader(mockClient, 3, limiter.NewQueryLimiter(0, 0, 0, nil), &stats.Stats{}, log.NewNopLogger())
	reader.StartBuffering()

	s, err := reader.GetChunks(0)
	require.NoError(t, err)
	require.Equal(t, s, firstSeries)

	s, err = reader.GetChunks(1)
	require.Nil(t, s)
	require.EqualError(t, err, "attempted to read series at index 1 from stream, but the stream has failed: expected to receive 3 series, but got EOF after receiving 1 series")

	require.True(t, mockClient.closed.Load(), "expected gRPC client to be closed after failure")
}

func TestStoreGatewayStreamReader_ReceivedMoreSeriesThanExpected(t *testing.T) {
	batches := []storepb.StreamingChunksBatch{
		{
			Series: []*storepb.StreamingChunks{
				{SeriesIndex: 0, Chunks: []storepb.AggrChunk{createChunk(t, 1000, 1.23)}},
				{SeriesIndex: 1, Chunks: []storepb.AggrChunk{createChunk(t, 1000, 1.23)}},
				{SeriesIndex: 2, Chunks: []storepb.AggrChunk{createChunk(t, 1000, 1.23)}},
			},
		},
	}
	mockClient := &mockStoreGatewayQueryStreamClient{ctx: context.Background(), batches: batches}
	reader := NewStoreGatewayStreamReader(mockClient, 1, limiter.NewQueryLimiter(0, 0, 0, nil), &stats.Stats{}, log.NewNopLogger())
	reader.StartBuffering()

	s, err := reader.GetChunks(0)
	require.Nil(t, s)
	require.EqualError(t, err, "attempted to read series at index 0 from stream, but the stream has failed: expected to receive only 1 series, but received at least 3 series")

	require.True(t, mockClient.closed.Load(), "expected gRPC client to be closed after receiving more series than expected")
}

func TestStoreGatewayStreamReader_ChunksLimits(t *testing.T) {
	testCases := map[string]struct {
		maxChunks     int
		maxChunkBytes int
		expectedError string
	}{
		"query under both limits": {
			maxChunks:     4,
			maxChunkBytes: 200,
			expectedError: "",
		},
		"query selects too many chunks": {
			maxChunks:     2,
			maxChunkBytes: 200,
			expectedError: "the query exceeded the maximum number of chunks (limit: 2 chunks) (err-mimir-max-chunks-per-query). Consider reducing the time range and/or number of series selected by the query. One way to reduce the number of selected series is to add more label matchers to the query. Otherwise, to adjust the related per-tenant limit, configure -querier.max-fetched-chunks-per-query, or contact your service administrator.",
		},
		"query selects too many chunk bytes": {
			maxChunks:     4,
			maxChunkBytes: 50,
			expectedError: "the query exceeded the aggregated chunks size limit (limit: 50 bytes) (err-mimir-max-chunks-bytes-per-query). Consider reducing the time range and/or number of series selected by the query. One way to reduce the number of selected series is to add more label matchers to the query. Otherwise, to adjust the related per-tenant limit, configure -querier.max-fetched-chunk-bytes-per-query, or contact your service administrator.",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			batches := []storepb.StreamingChunksBatch{
				{Series: []*storepb.StreamingChunks{{SeriesIndex: 0, Chunks: []storepb.AggrChunk{
					createChunk(t, 1000, 1.23),
					createChunk(t, 1100, 1.23),
					createChunk(t, 1200, 1.23),
				}}}},
			}

			mockClient := &mockStoreGatewayQueryStreamClient{ctx: context.Background(), batches: batches}
			queryMetrics := stats.NewQueryMetrics(prometheus.NewPedanticRegistry())
			reader := NewStoreGatewayStreamReader(mockClient, 1, limiter.NewQueryLimiter(0, testCase.maxChunkBytes, testCase.maxChunks, queryMetrics), &stats.Stats{}, log.NewNopLogger())
			reader.StartBuffering()

			_, err := reader.GetChunks(0)

			if testCase.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, testCase.expectedError)
			}

			require.Eventually(t, mockClient.closed.Load, time.Second, 10*time.Millisecond, "expected gRPC client to be closed")
		})
	}
}

func createChunk(t *testing.T, time int64, value float64) storepb.AggrChunk {
	promChunk := chunkenc.NewXORChunk()
	app, err := promChunk.Appender()
	require.NoError(t, err)

	app.Append(time, value)

	return storepb.AggrChunk{
		MinTime: time,
		MaxTime: time,
		Raw: &storepb.Chunk{
			Type: storepb.Chunk_XOR,
			Data: promChunk.Bytes(),
		},
	}
}

type mockStoreGatewayQueryStreamClient struct {
	ctx     context.Context
	batches []storepb.StreamingChunksBatch
	closed  atomic.Bool
}

func (m *mockStoreGatewayQueryStreamClient) Recv() (*storepb.SeriesResponse, error) {
	if len(m.batches) == 0 {
		return nil, io.EOF
	}

	batch := m.batches[0]
	m.batches = m.batches[1:]

	return storepb.NewStreamingChunksResponse(&batch), nil
}

func (m *mockStoreGatewayQueryStreamClient) Header() (metadata.MD, error) {
	panic("not supported on mock")
}

func (m *mockStoreGatewayQueryStreamClient) Trailer() metadata.MD {
	panic("not supported on mock")
}

func (m *mockStoreGatewayQueryStreamClient) CloseSend() error {
	m.closed.Store(true)
	return nil
}

func (m *mockStoreGatewayQueryStreamClient) Context() context.Context {
	return m.ctx
}

func (m *mockStoreGatewayQueryStreamClient) SendMsg(interface{}) error {
	panic("not supported on mock")
}

func (m *mockStoreGatewayQueryStreamClient) RecvMsg(interface{}) error {
	panic("not supported on mock")
}
