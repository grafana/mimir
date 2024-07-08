// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestSeriesChunksStreamReader_HappyPaths(t *testing.T) {
	series0 := []Chunk{createTestChunk(t, 1000, 1)}
	series1 := []Chunk{createTestChunk(t, 1000, 2)}
	series2 := []Chunk{createTestChunk(t, 1000, 3)}
	series3 := []Chunk{createTestChunk(t, 1000, 4)}
	series4 := []Chunk{createTestChunk(t, 1000, 5)}

	testCases := map[string]struct {
		batches [][]QueryStreamSeriesChunks
	}{
		"single series per batch": {
			batches: [][]QueryStreamSeriesChunks{
				{{SeriesIndex: 0, Chunks: series0}},
				{{SeriesIndex: 1, Chunks: series1}},
				{{SeriesIndex: 2, Chunks: series2}},
				{{SeriesIndex: 3, Chunks: series3}},
				{{SeriesIndex: 4, Chunks: series4}},
			},
		},
		"multiple series per batch": {
			batches: [][]QueryStreamSeriesChunks{
				{
					{SeriesIndex: 0, Chunks: series0},
					{SeriesIndex: 1, Chunks: series1},
					{SeriesIndex: 2, Chunks: series2},
				},
				{
					{SeriesIndex: 3, Chunks: series3},
					{SeriesIndex: 4, Chunks: series4},
				},
			},
		},
		"empty batches": {
			batches: [][]QueryStreamSeriesChunks{
				{
					{SeriesIndex: 0, Chunks: series0},
					{SeriesIndex: 1, Chunks: series1},
					{SeriesIndex: 2, Chunks: series2},
				},
				{},
				{
					{SeriesIndex: 3, Chunks: series3},
					{SeriesIndex: 4, Chunks: series4},
				},
				{},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			mockClient := &mockQueryStreamClient{ctx: ctx, batches: testCase.batches}
			cleanedUp := atomic.NewBool(false)
			cleanup := func() { cleanedUp.Store(true) }
			reader := NewSeriesChunksStreamReader(ctx, mockClient, "ingester", 5, limiter.NewQueryLimiter(0, 0, 0, 0, nil), cleanup, log.NewNopLogger())
			reader.StartBuffering()

			for i, expected := range [][]Chunk{series0, series1, series2, series3, series4} {
				actual, err := reader.GetChunks(uint64(i))
				require.NoError(t, err)
				require.Equalf(t, expected, actual, "received unexpected chunk for series index %v", i)
			}

			require.Eventually(t, func() bool {
				return mockClient.closed.Load()
			}, time.Second, 10*time.Millisecond)

			require.True(t, cleanedUp.Load(), "expected cleanup function to be called")
		})
	}
}

func TestSeriesChunksStreamReader_AbortsWhenParentContextCancelled(t *testing.T) {
	// Ensure that the buffering goroutine is not leaked after context cancellation.
	test.VerifyNoLeak(t)

	// Create multiple batches to ensure that the buffering goroutine becomes blocked waiting to send further chunks to GetChunks().
	batches := [][]QueryStreamSeriesChunks{
		{
			{SeriesIndex: 0, Chunks: []Chunk{createTestChunk(t, 1000, 1.23)}},
		},
		{
			{SeriesIndex: 1, Chunks: []Chunk{createTestChunk(t, 1000, 4.56)}},
		},
		{
			{SeriesIndex: 2, Chunks: []Chunk{createTestChunk(t, 1000, 7.89)}},
		},
	}

	streamCtx := context.Background()
	mockClient := &mockQueryStreamClient{ctx: streamCtx, batches: batches}
	cleanedUp := atomic.NewBool(false)
	cleanup := func() { cleanedUp.Store(true) }

	parentCtx, cancel := context.WithCancel(context.Background())
	reader := NewSeriesChunksStreamReader(parentCtx, mockClient, "ingester", 3, limiter.NewQueryLimiter(0, 0, 0, 0, nil), cleanup, log.NewNopLogger())
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

	require.True(t, mockClient.closed.Load(), "expected gRPC client to be closed after parent context cancelled")
	require.True(t, cleanedUp.Load(), "expected cleanup function to be called")
}

func TestSeriesChunksStreamReader_DoesNotAbortWhenStreamContextCancelled(t *testing.T) {
	// Ensure that the buffering goroutine is not leaked after context cancellation.
	test.VerifyNoLeak(t)

	// Create multiple batches to ensure that the buffering goroutine becomes blocked waiting to send further chunks to GetChunks().
	batches := [][]QueryStreamSeriesChunks{
		{
			{SeriesIndex: 0, Chunks: []Chunk{createTestChunk(t, 1000, 1.23)}},
		},
		{
			{SeriesIndex: 1, Chunks: []Chunk{createTestChunk(t, 1000, 4.56)}},
		},
		{
			{SeriesIndex: 2, Chunks: []Chunk{createTestChunk(t, 1000, 7.89)}},
		},
	}

	streamCtx, cancel := context.WithCancel(context.Background())
	mockClient := &mockQueryStreamClient{ctx: streamCtx, batches: batches}
	cleanedUp := atomic.NewBool(false)
	cleanup := func() { cleanedUp.Store(true) }

	parentCtx := context.Background()
	reader := NewSeriesChunksStreamReader(parentCtx, mockClient, "ingester", 3, limiter.NewQueryLimiter(0, 0, 0, 0, nil), cleanup, log.NewNopLogger())
	cancel()
	reader.StartBuffering()

	for i := 0; i < 3; i++ {
		_, err := reader.GetChunks(uint64(i))
		require.NoError(t, err, "expected GetChunks to not report context cancellation error from stream context")
	}

	require.True(t, mockClient.closed.Load(), "expected gRPC client to be closed after stream exhausted")
	require.True(t, cleanedUp.Load(), "expected cleanup function to be called")
}

func TestSeriesChunksStreamReader_ReadingSeriesOutOfOrder(t *testing.T) {
	batches := [][]QueryStreamSeriesChunks{
		{
			{SeriesIndex: 0, Chunks: []Chunk{createTestChunk(t, 1000, 1.23)}},
		},
	}

	ctx := context.Background()
	mockClient := &mockQueryStreamClient{ctx: ctx, batches: batches}
	cleanup := func() {}
	reader := NewSeriesChunksStreamReader(ctx, mockClient, "ingester", 1, limiter.NewQueryLimiter(0, 0, 0, 0, nil), cleanup, log.NewNopLogger())
	reader.StartBuffering()

	s, err := reader.GetChunks(1)
	require.Nil(t, s)
	expectedError := "attempted to read series at index 1 from ingester chunks stream, but the stream has series with index 0"
	require.EqualError(t, err, expectedError)

	// Ensure we continue to return the error, even for subsequent calls to GetChunks.
	_, err = reader.GetChunks(2)
	require.EqualError(t, err, "attempted to read series at index 2 from ingester chunks stream, but the stream previously failed and returned an error: "+expectedError)
	_, err = reader.GetChunks(3)
	require.EqualError(t, err, "attempted to read series at index 3 from ingester chunks stream, but the stream previously failed and returned an error: "+expectedError)
}

func TestSeriesChunksStreamReader_ReadingMoreSeriesThanAvailable(t *testing.T) {
	firstSeries := []Chunk{createTestChunk(t, 1000, 1.23)}
	batches := [][]QueryStreamSeriesChunks{
		{
			{SeriesIndex: 0, Chunks: firstSeries},
		},
	}

	ctx := context.Background()
	mockClient := &mockQueryStreamClient{ctx: ctx, batches: batches}
	cleanup := func() {}
	reader := NewSeriesChunksStreamReader(ctx, mockClient, "ingester", 1, limiter.NewQueryLimiter(0, 0, 0, 0, nil), cleanup, log.NewNopLogger())
	reader.StartBuffering()

	s, err := reader.GetChunks(0)
	require.NoError(t, err)
	require.Equal(t, s, firstSeries)

	s, err = reader.GetChunks(1)
	require.Nil(t, s)
	expectedError := "attempted to read series at index 1 from ingester chunks stream, but the stream has already been exhausted (was expecting 1 series)"
	require.EqualError(t, err, expectedError)

	// Ensure we continue to return the error, even for subsequent calls to GetChunks.
	_, err = reader.GetChunks(2)
	require.EqualError(t, err, "attempted to read series at index 2 from ingester chunks stream, but the stream previously failed and returned an error: "+expectedError)
	_, err = reader.GetChunks(3)
	require.EqualError(t, err, "attempted to read series at index 3 from ingester chunks stream, but the stream previously failed and returned an error: "+expectedError)
}

func TestSeriesChunksStreamReader_ReceivedFewerSeriesThanExpected(t *testing.T) {
	firstSeries := []Chunk{createTestChunk(t, 1000, 1.23)}
	batches := [][]QueryStreamSeriesChunks{
		{
			{SeriesIndex: 0, Chunks: firstSeries},
		},
	}

	ctx := context.Background()
	mockClient := &mockQueryStreamClient{ctx: ctx, batches: batches}
	cleanedUp := atomic.NewBool(false)
	cleanup := func() { cleanedUp.Store(true) }

	reader := NewSeriesChunksStreamReader(ctx, mockClient, "ingester", 3, limiter.NewQueryLimiter(0, 0, 0, 0, nil), cleanup, log.NewNopLogger())
	reader.StartBuffering()

	s, err := reader.GetChunks(0)
	require.NoError(t, err)
	require.Equal(t, s, firstSeries)

	s, err = reader.GetChunks(1)
	require.Nil(t, s)
	expectedError := "attempted to read series at index 1 from ingester chunks stream, but the stream has failed: expected to receive 3 series, but got EOF after receiving 1 series"
	require.EqualError(t, err, expectedError)

	require.True(t, mockClient.closed.Load(), "expected gRPC client to be closed after failure")
	require.True(t, cleanedUp.Load(), "expected cleanup function to be called")

	// Ensure we continue to return the error, even for subsequent calls to GetChunks.
	_, err = reader.GetChunks(2)
	require.EqualError(t, err, "attempted to read series at index 2 from ingester chunks stream, but the stream previously failed and returned an error: "+expectedError)
	_, err = reader.GetChunks(3)
	require.EqualError(t, err, "attempted to read series at index 3 from ingester chunks stream, but the stream previously failed and returned an error: "+expectedError)
}

func TestSeriesChunksStreamReader_ReceivedMoreSeriesThanExpected(t *testing.T) {
	testCases := map[string][][]QueryStreamSeriesChunks{
		"extra series received as part of batch for last expected series": {
			{
				{SeriesIndex: 0, Chunks: []Chunk{createTestChunk(t, 1000, 1.23)}},
				{SeriesIndex: 1, Chunks: []Chunk{createTestChunk(t, 1000, 4.56)}},
				{SeriesIndex: 2, Chunks: []Chunk{createTestChunk(t, 1000, 7.89)}},
			},
		},
		"extra series received as part of batch after batch containing last expected series": {
			{
				{SeriesIndex: 0, Chunks: []Chunk{createTestChunk(t, 1000, 1.23)}},
			},
			{
				{SeriesIndex: 1, Chunks: []Chunk{createTestChunk(t, 1000, 4.56)}},
				{SeriesIndex: 2, Chunks: []Chunk{createTestChunk(t, 1000, 7.89)}},
			},
		},
	}

	for name, batches := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			mockClient := &mockQueryStreamClient{ctx: ctx, batches: batches}
			cleanedUp := atomic.NewBool(false)
			cleanup := func() { cleanedUp.Store(true) }

			reader := NewSeriesChunksStreamReader(ctx, mockClient, "ingester", 1, limiter.NewQueryLimiter(0, 0, 0, 0, nil), cleanup, log.NewNopLogger())
			reader.StartBuffering()

			s, err := reader.GetChunks(0)
			require.Nil(t, s)
			expectedError := "attempted to read series at index 0 from ingester chunks stream, but the stream has failed: expected to receive only 1 series, but received at least 3 series"
			require.EqualError(t, err, expectedError)

			require.Eventually(t, mockClient.closed.Load, time.Second, 10*time.Millisecond, "expected gRPC client to be closed after receiving more series than expected")
			require.Eventually(t, cleanedUp.Load, time.Second, 10*time.Millisecond, "expected cleanup function to be called")

			// Ensure we continue to return the error, even for subsequent calls to GetChunks.
			_, err = reader.GetChunks(1)
			require.EqualError(t, err, "attempted to read series at index 1 from ingester chunks stream, but the stream previously failed and returned an error: "+expectedError)
			_, err = reader.GetChunks(2)
			require.EqualError(t, err, "attempted to read series at index 2 from ingester chunks stream, but the stream previously failed and returned an error: "+expectedError)
		})
	}
}

func TestSeriesChunksStreamReader_ChunksLimits(t *testing.T) {
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
			expectedError: "", // The limit on the number of chunks is enforced earlier, in distributor.queryIngesterStream()
		},
		"query selects too many chunk bytes": {
			maxChunks:     4,
			maxChunkBytes: 100,
			expectedError: "the query exceeded the aggregated chunks size limit (limit: 100 bytes) (err-mimir-max-chunks-bytes-per-query). Consider reducing the time range and/or number of series selected by the query. One way to reduce the number of selected series is to add more label matchers to the query. Otherwise, to adjust the related per-tenant limit, configure -querier.max-fetched-chunk-bytes-per-query, or contact your service administrator.",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			batches := [][]QueryStreamSeriesChunks{
				{
					{SeriesIndex: 0, Chunks: []Chunk{createTestChunk(t, 1000, 1.23), createTestChunk(t, 2000, 4.56), createTestChunk(t, 3000, 7.89)}},
				},
			}

			ctx := context.Background()
			mockClient := &mockQueryStreamClient{ctx: ctx, batches: batches}
			cleanedUp := atomic.NewBool(false)
			cleanup := func() { cleanedUp.Store(true) }
			queryMetrics := stats.NewQueryMetrics(prometheus.NewPedanticRegistry())
			reader := NewSeriesChunksStreamReader(ctx, mockClient, "ingester", 1, limiter.NewQueryLimiter(0, testCase.maxChunkBytes, testCase.maxChunks, 0, queryMetrics), cleanup, log.NewNopLogger())
			reader.StartBuffering()

			_, err := reader.GetChunks(0)

			if testCase.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, testCase.expectedError)
			}

			require.Eventually(t, mockClient.closed.Load, time.Second, 10*time.Millisecond, "expected gRPC client to be closed")
			require.Eventually(t, cleanedUp.Load, time.Second, 10*time.Millisecond, "expected cleanup function to be called")

			if testCase.expectedError != "" {
				// Ensure we continue to return the error, even for subsequent calls to GetChunks.
				_, err := reader.GetChunks(1)
				require.EqualError(t, err, "attempted to read series at index 1 from ingester chunks stream, but the stream previously failed and returned an error: "+testCase.expectedError)
				_, err = reader.GetChunks(2)
				require.EqualError(t, err, "attempted to read series at index 2 from ingester chunks stream, but the stream previously failed and returned an error: "+testCase.expectedError)
			}
		})
	}
}

func createTestChunk(t *testing.T, time int64, value float64) Chunk {
	promChunk, err := chunk.NewForEncoding(chunk.PrometheusXorChunk)
	require.NoError(t, err)

	_, err = promChunk.Add(model.SamplePair{Timestamp: model.Time(time), Value: model.SampleValue(value)})
	require.NoError(t, err)

	chunks, err := ToChunks([]chunk.Chunk{chunk.NewChunk(labels.EmptyLabels(), promChunk, model.Earliest, model.Latest)})
	require.NoError(t, err)

	return chunks[0]
}

type mockQueryStreamClient struct {
	ctx     context.Context
	batches [][]QueryStreamSeriesChunks
	closed  atomic.Bool
}

func (m *mockQueryStreamClient) Recv() (*QueryStreamResponse, error) {
	if len(m.batches) == 0 {
		return nil, io.EOF
	}

	batch := m.batches[0]
	m.batches = m.batches[1:]

	return &QueryStreamResponse{
		StreamingSeriesChunks: batch,
	}, nil
}

func (m *mockQueryStreamClient) Header() (metadata.MD, error) {
	panic("not supported on mock")
}

func (m *mockQueryStreamClient) Trailer() metadata.MD {
	panic("not supported on mock")
}

func (m *mockQueryStreamClient) CloseSend() error {
	m.closed.Store(true)
	return nil
}

func (m *mockQueryStreamClient) Context() context.Context {
	return m.ctx
}

func (m *mockQueryStreamClient) SendMsg(interface{}) error {
	panic("not supported on mock")
}

func (m *mockQueryStreamClient) RecvMsg(interface{}) error {
	panic("not supported on mock")
}
