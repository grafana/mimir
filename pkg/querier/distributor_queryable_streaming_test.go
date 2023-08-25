// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"io"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func streamingChunkSeriesTestIteratorFunc(_ chunkenc.Iterator, chunks []chunk.Chunk, from, through model.Time) chunkenc.Iterator {
	return streamingChunkSeriesTestIterator{
		chunks:  chunks,
		from:    from,
		through: through,
	}
}

func TestStreamingChunkSeries_HappyPath(t *testing.T) {
	chunkUniqueToFirstSource := createTestChunk(t, 1500, 1.23)
	chunkUniqueToSecondSource := createTestChunk(t, 2000, 4.56)
	chunkPresentInBothSources := createTestChunk(t, 2500, 7.89)

	reg := prometheus.NewPedanticRegistry()
	queryStats := &stats.Stats{}
	series := streamingChunkSeries{
		labels: labels.FromStrings("the-name", "the-value"),
		sources: []client.StreamingSeriesSource{
			{SeriesIndex: 0, StreamReader: createTestStreamReader([]client.QueryStreamSeriesChunks{{SeriesIndex: 0, Chunks: []client.Chunk{chunkUniqueToFirstSource, chunkPresentInBothSources}}})},
			{SeriesIndex: 0, StreamReader: createTestStreamReader([]client.QueryStreamSeriesChunks{{SeriesIndex: 0, Chunks: []client.Chunk{chunkUniqueToSecondSource, chunkPresentInBothSources}}})},
		},
		context: &streamingChunkSeriesContext{
			chunkIteratorFunc: streamingChunkSeriesTestIteratorFunc,
			mint:              1000,
			maxt:              6000,
			queryMetrics:      stats.NewQueryMetrics(reg),
			queryStats:        queryStats,
		},
	}

	iterator := series.Iterator(nil)
	require.NotNil(t, iterator)
	testIterator, ok := iterator.(streamingChunkSeriesTestIterator)
	require.True(t, ok)
	require.Equal(t, model.Time(1000), testIterator.from)
	require.Equal(t, model.Time(6000), testIterator.through)

	expectedChunks, err := client.FromChunks(series.labels, []client.Chunk{chunkUniqueToFirstSource, chunkUniqueToSecondSource, chunkPresentInBothSources})
	require.NoError(t, err)
	require.ElementsMatch(t, testIterator.chunks, expectedChunks)

	m, err := metrics.NewMetricFamilyMapFromGatherer(reg)
	require.NoError(t, err)
	require.Equal(t, 4.0, m.SumCounters("cortex_distributor_query_ingester_chunks_total"))
	require.Equal(t, 1.0, m.SumCounters("cortex_distributor_query_ingester_chunks_deduped_total"))

	require.Equal(t, uint64(3), queryStats.FetchedChunksCount)
	require.Equal(t, uint64(114), queryStats.FetchedChunkBytes)
}

func TestStreamingChunkSeries_StreamReaderReturnsError(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	queryStats := &stats.Stats{}
	series := streamingChunkSeries{
		labels: labels.FromStrings("the-name", "the-value"),
		// Create a stream reader that will always return an error because we'll try to read a series when it has no series to read.
		sources: []client.StreamingSeriesSource{
			{SeriesIndex: 0, StreamReader: createTestStreamReader([]client.QueryStreamSeriesChunks{})},
		},
		context: &streamingChunkSeriesContext{
			chunkIteratorFunc: nil,
			mint:              1000,
			maxt:              6000,
			queryMetrics:      stats.NewQueryMetrics(reg),
			queryStats:        queryStats,
		},
	}

	iterator := series.Iterator(nil)
	require.NotNil(t, iterator)
	require.EqualError(t, iterator.Err(), "attempted to read series at index 0 from stream, but the stream has already been exhausted")
}

func TestStreamingChunkSeries_CreateIteratorTwice(t *testing.T) {
	series := streamingChunkSeries{
		labels: labels.FromStrings("the-name", "the-value"),
		sources: []client.StreamingSeriesSource{
			{SeriesIndex: 0, StreamReader: createTestStreamReader([]client.QueryStreamSeriesChunks{{SeriesIndex: 0, Chunks: []client.Chunk{createTestChunk(t, 1500, 1.23)}}})},
		},
		context: &streamingChunkSeriesContext{
			chunkIteratorFunc: streamingChunkSeriesTestIteratorFunc,
			mint:              1000,
			maxt:              6000,
			queryMetrics:      stats.NewQueryMetrics(prometheus.NewPedanticRegistry()),
			queryStats:        &stats.Stats{},
		},
	}

	iterator := series.Iterator(nil)
	require.NotNil(t, iterator)
	require.NoError(t, iterator.Err())

	iterator = series.Iterator(iterator)
	require.NotNil(t, iterator)
	require.EqualError(t, iterator.Err(), `can't create iterator multiple times for the one streaming series ({the-name="the-value"})`)
}

func createTestChunk(t *testing.T, time int64, value float64) client.Chunk {
	promChunk, err := chunk.NewForEncoding(chunk.PrometheusXorChunk)
	require.NoError(t, err)

	_, err = promChunk.Add(model.SamplePair{Timestamp: model.Time(time), Value: model.SampleValue(value)})
	require.NoError(t, err)

	chunks, err := client.ToChunks([]chunk.Chunk{chunk.NewChunk(labels.EmptyLabels(), promChunk, model.Earliest, model.Latest)})
	require.NoError(t, err)

	return chunks[0]
}

func createTestStreamReader(batches ...[]client.QueryStreamSeriesChunks) *client.SeriesChunksStreamReader {
	seriesCount := 0

	for _, batch := range batches {
		seriesCount += len(batch)
	}

	mockClient := &mockQueryStreamClient{
		ctx:     context.Background(),
		batches: batches,
	}

	cleanup := func() {}

	reader := client.NewSeriesChunksStreamReader(mockClient, seriesCount, limiter.NewQueryLimiter(0, 0, 0, 0, nil), cleanup, log.NewNopLogger())
	reader.StartBuffering()

	return reader
}

type streamingChunkSeriesTestIterator struct {
	chunks  []chunk.Chunk
	from    model.Time
	through model.Time
}

func (s streamingChunkSeriesTestIterator) Next() chunkenc.ValueType {
	panic("not implemented")
}

func (s streamingChunkSeriesTestIterator) Seek(int64) chunkenc.ValueType {
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
	return nil
}

type mockQueryStreamClient struct {
	ctx     context.Context
	batches [][]client.QueryStreamSeriesChunks
}

func (m *mockQueryStreamClient) Recv() (*client.QueryStreamResponse, error) {
	if len(m.batches) == 0 {
		return nil, io.EOF
	}

	batch := m.batches[0]
	m.batches = m.batches[1:]

	return &client.QueryStreamResponse{
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
