// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/util/chunkcompat"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestStreamingChunkSeries_DeduplicatesIdenticalChunks(t *testing.T) {
	chunkIteratorFunc := func(chunks []chunk.Chunk, from, through model.Time) chunkenc.Iterator {
		return streamingChunkSeriesTestIterator{
			chunks:  chunks,
			from:    from,
			through: through,
		}
	}

	chunkUniqueToFirstSource := createTestChunk(t, 1500, 1.23)
	chunkUniqueToSecondSource := createTestChunk(t, 2000, 4.56)
	chunkPresentInBothSources := createTestChunk(t, 2500, 7.89)

	series := streamingChunkSeries{
		labels:            labels.FromStrings("the-name", "the-value"),
		chunkIteratorFunc: chunkIteratorFunc,
		mint:              1000,
		maxt:              6000,
		sources: []StreamingSeriesSource{
			{SeriesIndex: 0, StreamReader: createTestStreamReader([]client.QueryStreamSeriesChunks{{SeriesIndex: 0, Chunks: []client.Chunk{chunkUniqueToFirstSource, chunkPresentInBothSources}}})},
			{SeriesIndex: 0, StreamReader: createTestStreamReader([]client.QueryStreamSeriesChunks{{SeriesIndex: 0, Chunks: []client.Chunk{chunkUniqueToSecondSource, chunkPresentInBothSources}}})},
		},
	}

	iterator := series.Iterator(nil)
	require.NotNil(t, iterator)
	testIterator, ok := iterator.(streamingChunkSeriesTestIterator)
	require.True(t, ok)
	require.Equal(t, model.Time(1000), testIterator.from)
	require.Equal(t, model.Time(6000), testIterator.through)

	expectedChunks, err := chunkcompat.FromChunks(series.labels, []client.Chunk{chunkUniqueToFirstSource, chunkUniqueToSecondSource, chunkPresentInBothSources})
	require.NoError(t, err)
	require.ElementsMatch(t, testIterator.chunks, expectedChunks)
}

func createTestChunk(t *testing.T, time int64, value float64) client.Chunk {
	promChunk, err := chunk.NewForEncoding(chunk.PrometheusXorChunk)
	require.NoError(t, err)

	_, err = promChunk.Add(model.SamplePair{Timestamp: model.Time(time), Value: model.SampleValue(value)})
	require.NoError(t, err)

	chunks, err := chunkcompat.ToChunks([]chunk.Chunk{chunk.NewChunk(labels.EmptyLabels(), promChunk, model.Earliest, model.Latest)})
	require.NoError(t, err)

	return chunks[0]
}

func createTestStreamReader(batches ...[]client.QueryStreamSeriesChunks) *SeriesChunksStreamReader {
	seriesCount := 0

	for _, batch := range batches {
		seriesCount += len(batch)
	}

	mockClient := &mockQueryStreamClient{
		ctx:     context.Background(),
		batches: batches,
	}

	reader := NewSeriesStreamReader(mockClient, seriesCount)
	reader.StartBuffering()

	return reader
}

func TestSeriesChunksStreamReader_HappyPaths(t *testing.T) {
	series0 := []client.Chunk{createTestChunk(t, 1000, 1)}
	series1 := []client.Chunk{createTestChunk(t, 1000, 2)}
	series2 := []client.Chunk{createTestChunk(t, 1000, 3)}
	series3 := []client.Chunk{createTestChunk(t, 1000, 4)}
	series4 := []client.Chunk{createTestChunk(t, 1000, 5)}

	testCases := map[string]struct {
		batches [][]client.QueryStreamSeriesChunks
	}{
		"single series per batch": {
			batches: [][]client.QueryStreamSeriesChunks{
				{{SeriesIndex: 0, Chunks: series0}},
				{{SeriesIndex: 1, Chunks: series1}},
				{{SeriesIndex: 2, Chunks: series2}},
				{{SeriesIndex: 3, Chunks: series3}},
				{{SeriesIndex: 4, Chunks: series4}},
			},
		},
		"multiple series per batch": {
			batches: [][]client.QueryStreamSeriesChunks{
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
		"empty batch": {
			batches: [][]client.QueryStreamSeriesChunks{
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
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			mockClient := &mockQueryStreamClient{ctx: context.Background(), batches: testCase.batches}
			reader := NewSeriesStreamReader(mockClient, 5)
			reader.StartBuffering()

			for i, expected := range [][]client.Chunk{series0, series1, series2, series3, series4} {
				actual, err := reader.GetChunks(uint64(i))
				require.NoError(t, err)
				require.Equalf(t, expected, actual, "received unexpected chunk for series index %v", i)
			}

			require.Eventually(t, func() bool {
				return mockClient.closed
			}, time.Second, 10*time.Millisecond)
		})
	}
}

func TestSeriesChunksStreamReader_AbortsWhenContextCancelled(t *testing.T) {
	// Ensure that the buffering goroutine is not leaked after context cancellation.
	test.VerifyNoLeak(t)

	// Create multiple batches to ensure that the buffering goroutine becomes blocked waiting to send further chunks to GetChunks().
	batches := [][]client.QueryStreamSeriesChunks{
		{
			{SeriesIndex: 0, Chunks: []client.Chunk{createTestChunk(t, 1000, 1.23)}},
		},
		{
			{SeriesIndex: 1, Chunks: []client.Chunk{createTestChunk(t, 1000, 4.56)}},
		},
		{
			{SeriesIndex: 2, Chunks: []client.Chunk{createTestChunk(t, 1000, 7.89)}},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	mockClient := &mockQueryStreamClient{ctx: ctx, batches: batches}

	reader := NewSeriesStreamReader(mockClient, 3)
	cancel()
	reader.StartBuffering()

	for i := 0; i < 3; i++ {
		_, err := reader.GetChunks(uint64(i))

		if errors.Is(err, context.Canceled) {
			break
		} else {
			require.NoError(t, err)
		}

		if i == 2 {
			require.Fail(t, "expected GetChunks to report context cancellation error before reaching end of stream")
		}
	}

	require.True(t, mockClient.closed, "expected gRPC client to be closed after context cancelled")
}

func TestSeriesChunksStreamReader_ReadingSeriesOutOfOrder(t *testing.T) {
	batches := [][]client.QueryStreamSeriesChunks{
		{
			{SeriesIndex: 0, Chunks: []client.Chunk{createTestChunk(t, 1000, 1.23)}},
		},
	}

	mockClient := &mockQueryStreamClient{ctx: context.Background(), batches: batches}
	reader := NewSeriesStreamReader(mockClient, 1)
	reader.StartBuffering()

	s, err := reader.GetChunks(1)
	require.Nil(t, s)
	require.EqualError(t, err, "attempted to read series at index 1 from stream, but the stream has series with index 0")
}

func TestSeriesChunksStreamReader_ReadingMoreSeriesThanAvailable(t *testing.T) {
	firstSeries := []client.Chunk{createTestChunk(t, 1000, 1.23)}
	batches := [][]client.QueryStreamSeriesChunks{
		{
			{SeriesIndex: 0, Chunks: firstSeries},
		},
	}

	mockClient := &mockQueryStreamClient{ctx: context.Background(), batches: batches}
	reader := NewSeriesStreamReader(mockClient, 1)
	reader.StartBuffering()

	s, err := reader.GetChunks(0)
	require.NoError(t, err)
	require.Equal(t, s, firstSeries)

	s, err = reader.GetChunks(1)
	require.Nil(t, s)
	require.EqualError(t, err, "attempted to read series at index 1 from stream, but the stream has already been exhausted")
}

func TestSeriesChunksStreamReader_ReceivedFewerSeriesThanExpected(t *testing.T) {
	firstSeries := []client.Chunk{createTestChunk(t, 1000, 1.23)}
	batches := [][]client.QueryStreamSeriesChunks{
		{
			{SeriesIndex: 0, Chunks: firstSeries},
		},
	}

	mockClient := &mockQueryStreamClient{ctx: context.Background(), batches: batches}
	reader := NewSeriesStreamReader(mockClient, 3)
	reader.StartBuffering()

	s, err := reader.GetChunks(0)
	require.NoError(t, err)
	require.Equal(t, s, firstSeries)

	s, err = reader.GetChunks(1)
	require.Nil(t, s)
	require.EqualError(t, err, "attempted to read series at index 1 from stream, but the stream has failed: expected to receive 3 series, but got EOF after receiving 1 series")
}

func TestSeriesChunksStreamReader_ReceivedMoreSeriesThanExpected(t *testing.T) {
	batches := [][]client.QueryStreamSeriesChunks{
		{
			{SeriesIndex: 0, Chunks: []client.Chunk{createTestChunk(t, 1000, 1.23)}},
			{SeriesIndex: 1, Chunks: []client.Chunk{createTestChunk(t, 1000, 4.56)}},
			{SeriesIndex: 2, Chunks: []client.Chunk{createTestChunk(t, 1000, 7.89)}},
		},
	}

	mockClient := &mockQueryStreamClient{ctx: context.Background(), batches: batches}
	reader := NewSeriesStreamReader(mockClient, 1)
	reader.StartBuffering()

	s, err := reader.GetChunks(0)
	require.Nil(t, s)
	require.EqualError(t, err, "attempted to read series at index 0 from stream, but the stream has failed: expected to receive only 1 series, but received more than this")

	require.True(t, mockClient.closed, "expected gRPC client to be closed after receiving more series than expected")
}

type streamingChunkSeriesTestIterator struct {
	chunks  []chunk.Chunk
	from    model.Time
	through model.Time
}

func (s streamingChunkSeriesTestIterator) Next() chunkenc.ValueType {
	panic("not implemented")
}

func (s streamingChunkSeriesTestIterator) Seek(t int64) chunkenc.ValueType {
	panic("not implemented")
}

func (s streamingChunkSeriesTestIterator) At() (int64, float64) {
	panic("not implemented")
}

func (s streamingChunkSeriesTestIterator) AtHistogram() (int64, *histogram.Histogram) {
	panic("not implemented")
}

func (s streamingChunkSeriesTestIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	panic("not implemented")
}

func (s streamingChunkSeriesTestIterator) AtT() int64 {
	panic("not implemented")
}

func (s streamingChunkSeriesTestIterator) Err() error {
	panic("not implemented")
}

type mockQueryStreamClient struct {
	ctx     context.Context
	batches [][]client.QueryStreamSeriesChunks
	closed  bool
}

func (m *mockQueryStreamClient) Recv() (*client.QueryStreamResponse, error) {
	if len(m.batches) == 0 {
		return nil, io.EOF
	}

	batch := m.batches[0]
	m.batches = m.batches[1:]

	return &client.QueryStreamResponse{
		SeriesChunks: batch,
	}, nil
}

func (m *mockQueryStreamClient) Header() (metadata.MD, error) {
	panic("not supported on mock")
}

func (m *mockQueryStreamClient) Trailer() metadata.MD {
	panic("not supported on mock")
}

func (m *mockQueryStreamClient) CloseSend() error {
	m.closed = true
	return nil
}

func (m *mockQueryStreamClient) Context() context.Context {
	return m.ctx
}

func (m *mockQueryStreamClient) SendMsg(msg interface{}) error {
	panic("not supported on mock")
}

func (m *mockQueryStreamClient) RecvMsg(msg interface{}) error {
	panic("not supported on mock")
}
