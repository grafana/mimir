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
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/querier/batch"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/util/limiter"
)

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
			queryMetrics: stats.NewQueryMetrics(reg),
			queryStats:   queryStats,
		},
	}

	iterator := series.Iterator(nil)
	require.NotNil(t, iterator)

	expectedChunks, err := client.FromChunks(series.labels, []client.Chunk{chunkUniqueToFirstSource, chunkUniqueToSecondSource, chunkPresentInBothSources})
	require.NoError(t, err)
	assertChunkIteratorsEqual(t, iterator, batch.NewChunkMergeIterator(nil, series.labels, expectedChunks))

	m, err := metrics.NewMetricFamilyMapFromGatherer(reg)
	require.NoError(t, err)
	require.Equal(t, 4.0, m.SumCounters("cortex_distributor_query_ingester_chunks_total"))
	require.Equal(t, 1.0, m.SumCounters("cortex_distributor_query_ingester_chunks_deduped_total"))

	require.Equal(t, uint64(3), queryStats.FetchedChunksCount)
	require.Equal(t, uint64(114), queryStats.FetchedChunkBytes)
}

func assertChunkIteratorsEqual(t testing.TB, c1, c2 chunkenc.Iterator) {
	for sampleIdx := 0; ; sampleIdx++ {
		c1Next := c1.Next()
		c2Next := c2.Next()

		require.Equal(t, c1Next, c2Next, sampleIdx)
		if c1Next == chunkenc.ValNone {
			break
		}
		var (
			atT1, atT2 int64
			atV1, atV2 any
		)
		switch c1Next {
		case chunkenc.ValHistogram:
			atT1, atV1 = c1.AtHistogram(nil)
			atT2, atV2 = c2.AtHistogram(nil)
		case chunkenc.ValFloatHistogram:
			atT1, atV1 = c1.AtFloatHistogram(nil)
			atT2, atV2 = c2.AtFloatHistogram(nil)
		case chunkenc.ValFloat:
			atT1, atV1 = c1.At()
			atT2, atV2 = c2.At()
		default:
			panic("unhandled chunkenc.ValueType")
		}
		assert.Equal(t, atT1, atT2, sampleIdx)
		assert.Equal(t, atV1, atV2, sampleIdx)
	}
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
			queryMetrics: stats.NewQueryMetrics(reg),
			queryStats:   queryStats,
		},
	}

	iterator := series.Iterator(nil)
	require.NotNil(t, iterator)
	require.EqualError(t, iterator.Err(), "attempted to read series at index 0 from ingester chunks stream, but the stream has already been exhausted (was expecting 0 series)")
}

func TestStreamingChunkSeries_CreateIteratorTwice(t *testing.T) {
	series := streamingChunkSeries{
		labels: labels.FromStrings("the-name", "the-value"),
		sources: []client.StreamingSeriesSource{
			{SeriesIndex: 0, StreamReader: createTestStreamReader([]client.QueryStreamSeriesChunks{{SeriesIndex: 0, Chunks: []client.Chunk{createTestChunk(t, 1500, 1.23)}}})},
		},
		context: &streamingChunkSeriesContext{
			queryMetrics: stats.NewQueryMetrics(prometheus.NewPedanticRegistry()),
			queryStats:   &stats.Stats{},
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

	chunks, err := client.ToChunks([]chunk.Chunk{chunk.NewChunk(labels.EmptyLabels(), promChunk, model.Earliest, model.Latest, 0)})
	require.NoError(t, err)

	return chunks[0]
}

func createTestStreamReader(batches ...[]client.QueryStreamSeriesChunks) *client.SeriesChunksStreamReader {
	seriesCount := 0

	for _, batch := range batches {
		seriesCount += len(batch)
	}

	ctx := context.Background()
	mockClient := &mockQueryStreamClient{
		ctx:     ctx,
		batches: batches,
	}

	cleanup := func() {}

	reader := client.NewSeriesChunksStreamReader(ctx, mockClient, "ingester", seriesCount, limiter.NewQueryLimiter(0, 0, 0, 0, nil), cleanup, log.NewNopLogger())
	reader.StartBuffering()

	return reader
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
