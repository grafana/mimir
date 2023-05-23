// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"io"
	"testing"

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

func TestStreamingChunkSeries(t *testing.T) {
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

	reg := prometheus.NewPedanticRegistry()

	series := streamingChunkSeries{
		labels:            labels.FromStrings("the-name", "the-value"),
		chunkIteratorFunc: chunkIteratorFunc,
		mint:              1000,
		maxt:              6000,
		sources: []client.StreamingSeriesSource{
			{SeriesIndex: 0, StreamReader: createTestStreamReader([]client.QueryStreamSeriesChunks{{SeriesIndex: 0, Chunks: []client.Chunk{chunkUniqueToFirstSource, chunkPresentInBothSources}}})},
			{SeriesIndex: 0, StreamReader: createTestStreamReader([]client.QueryStreamSeriesChunks{{SeriesIndex: 0, Chunks: []client.Chunk{chunkUniqueToSecondSource, chunkPresentInBothSources}}})},
		},
		queryChunkMetrics: stats.NewQueryChunkMetrics(reg),
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

	reader := client.NewSeriesChunksStreamReader(mockClient, seriesCount, limiter.NewQueryLimiter(0, 0, 0))
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
