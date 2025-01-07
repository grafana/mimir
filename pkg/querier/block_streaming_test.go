// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestBlockStreamingQuerierSeriesSet(t *testing.T) {
	cases := map[string]struct {
		input              []testSeries
		expResult          []testSeries
		errorChunkStreamer bool
	}{
		"simple case of one series": {
			input: []testSeries{
				{
					lbls:   labels.FromStrings("foo", "bar"),
					values: []testSample{{1, 1}},
				},
			},
			expResult: []testSeries{
				{
					lbls:   labels.FromStrings("foo", "bar"),
					values: []testSample{{1, 1}},
				},
			},
		},
		"multiple unique series": {
			input: []testSeries{
				{
					lbls:   labels.FromStrings("foo", "bar1"),
					values: []testSample{{1, 1}, {2, 1}, {5, 10}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar2"),
					values: []testSample{{2, 2}, {9, 2}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar3"),
					values: []testSample{{3, 3}},
				},
			},
			expResult: []testSeries{
				{
					lbls:   labels.FromStrings("foo", "bar1"),
					values: []testSample{{1, 1}, {2, 1}, {5, 10}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar2"),
					values: []testSample{{2, 2}, {9, 2}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar3"),
					values: []testSample{{3, 3}},
				},
			},
		},
		"multiple entries of the same series": {
			input: []testSeries{
				{
					lbls:   labels.FromStrings("foo", "bar1"),
					values: []testSample{{1, 1}, {2, 1}, {5, 10}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar1"),
					values: []testSample{{6, 2}, {9, 2}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar3"),
					values: []testSample{{3, 3}},
				},
			},
			expResult: []testSeries{
				{
					lbls:   labels.FromStrings("foo", "bar1"),
					values: []testSample{{1, 1}, {2, 1}, {5, 10}, {6, 2}, {9, 2}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar3"),
					values: []testSample{{3, 3}},
				},
			},
		},
		"multiple entries of the same series again": {
			input: []testSeries{
				{
					lbls:   labels.FromStrings("foo", "bar1"),
					values: []testSample{{1, 1}, {2, 1}, {5, 10}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar1"),
					values: []testSample{{6, 2}, {9, 2}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar3"),
					values: []testSample{{3, 3}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar3"),
					values: []testSample{{4, 3}, {5, 3}, {6, 3}},
				},
			},
			expResult: []testSeries{
				{
					lbls:   labels.FromStrings("foo", "bar1"),
					values: []testSample{{1, 1}, {2, 1}, {5, 10}, {6, 2}, {9, 2}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar3"),
					values: []testSample{{3, 3}, {4, 3}, {5, 3}, {6, 3}},
				},
			},
		},
		"multiple unique series but with erroring chunk streamer": {
			errorChunkStreamer: true,
			input: []testSeries{
				{
					lbls:   labels.FromStrings("foo", "bar1"),
					values: []testSample{{1, 1}, {2, 1}, {5, 10}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar2"),
					values: []testSample{{2, 2}, {9, 2}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar3"),
					values: []testSample{{3, 3}},
				},
			},
			expResult: []testSeries{
				{
					lbls: labels.FromStrings("foo", "bar1"),
				},
				{
					lbls: labels.FromStrings("foo", "bar2"),
				},
				{
					lbls: labels.FromStrings("foo", "bar3"),
				},
			},
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			ss := &blockStreamingQuerierSeriesSet{streamReader: &mockChunkStreamer{series: c.input, causeError: c.errorChunkStreamer}}
			for _, s := range c.input {
				ss.series = append(ss.series, s.lbls)
			}
			idx := 0
			var it chunkenc.Iterator
			for ss.Next() {
				s := ss.At()
				require.Equal(t, c.expResult[idx].lbls, s.Labels())
				it = s.Iterator(it)
				if c.errorChunkStreamer {
					require.EqualError(t, it.Err(), "mocked error")
				} else {
					var actSamples []testSample
					for it.Next() != chunkenc.ValNone {
						ts, val := it.At()
						actSamples = append(actSamples, testSample{t: ts, v: val})
					}
					require.Equal(t, c.expResult[idx].values, actSamples)
					require.NoError(t, it.Err())
				}
				idx++
			}
			require.NoError(t, ss.Err())
			require.Equal(t, len(c.expResult), idx)
		})
	}
}

type testSeries struct {
	lbls   labels.Labels
	values []testSample
}

type testSample struct {
	t int64
	v float64
}

type mockChunkStreamer struct {
	series     []testSeries
	next       int
	causeError bool
}

func (m *mockChunkStreamer) GetChunks(seriesIndex uint64) ([]storepb.AggrChunk, error) {
	if m.causeError {
		return nil, fmt.Errorf("mocked error")
	}
	if m.next >= len(m.series) {
		return nil, fmt.Errorf("out of chunks")
	}

	if uint64(m.next) != seriesIndex {
		return nil, fmt.Errorf("asked for the wrong series, exp: %d, got %d", m.next, seriesIndex)
	}

	chk := chunkenc.NewXORChunk()
	app, err := chk.Appender()
	if err != nil {
		return nil, err
	}

	samples := m.series[m.next].values
	mint, maxt := int64(math.MaxInt64), int64(math.MinInt64)
	for _, s := range samples {
		app.Append(s.t, s.v)
		if s.t < mint {
			mint = s.t
		}
		if s.t > maxt {
			maxt = s.t
		}
	}

	m.next++

	return []storepb.AggrChunk{{
		MinTime: mint,
		MaxTime: maxt,
		Raw:     storepb.Chunk{Data: chk.Bytes()},
	}}, nil
}

func TestStoreGatewayStreamReader_HappyPaths(t *testing.T) {
	series0 := []storepb.AggrChunk{createChunk(t, 1000, 1)}
	series1 := []storepb.AggrChunk{createChunk(t, 1000, 2)}
	series2 := []storepb.AggrChunk{createChunk(t, 1000, 3)}
	series3 := []storepb.AggrChunk{createChunk(t, 1000, 4)}
	series4 := []storepb.AggrChunk{createChunk(t, 1000, 5)}

	testCases := map[string]struct {
		messages               []*storepb.SeriesResponse
		expectedChunksEstimate int
	}{
		"single series per batch": {
			messages: batchesToMessages(
				40,
				storepb.StreamingChunksBatch{Series: []*storepb.StreamingChunks{{SeriesIndex: 0, Chunks: series0}}},
				storepb.StreamingChunksBatch{Series: []*storepb.StreamingChunks{{SeriesIndex: 1, Chunks: series1}}},
				storepb.StreamingChunksBatch{Series: []*storepb.StreamingChunks{{SeriesIndex: 2, Chunks: series2}}},
				storepb.StreamingChunksBatch{Series: []*storepb.StreamingChunks{{SeriesIndex: 3, Chunks: series3}}},
				storepb.StreamingChunksBatch{Series: []*storepb.StreamingChunks{{SeriesIndex: 4, Chunks: series4}}},
			),
			expectedChunksEstimate: 40,
		},
		"multiple series per batch": {
			messages: batchesToMessages(
				40,
				storepb.StreamingChunksBatch{
					Series: []*storepb.StreamingChunks{
						{SeriesIndex: 0, Chunks: series0},
						{SeriesIndex: 1, Chunks: series1},
						{SeriesIndex: 2, Chunks: series2},
					},
				},
				storepb.StreamingChunksBatch{
					Series: []*storepb.StreamingChunks{
						{SeriesIndex: 3, Chunks: series3},
						{SeriesIndex: 4, Chunks: series4},
					},
				},
			),
			expectedChunksEstimate: 40,
		},
		"empty batches": {
			messages: batchesToMessages(
				40,
				storepb.StreamingChunksBatch{
					Series: []*storepb.StreamingChunks{
						{SeriesIndex: 0, Chunks: series0},
						{SeriesIndex: 1, Chunks: series1},
						{SeriesIndex: 2, Chunks: series2},
					},
				},
				storepb.StreamingChunksBatch{},
				storepb.StreamingChunksBatch{
					Series: []*storepb.StreamingChunks{
						{SeriesIndex: 3, Chunks: series3},
						{SeriesIndex: 4, Chunks: series4},
					},
				},
				storepb.StreamingChunksBatch{},
			),
			expectedChunksEstimate: 40,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			mockClient := &mockStoreGatewayQueryStreamClient{ctx: ctx, messages: testCase.messages}
			metrics := newBlocksStoreQueryableMetrics(prometheus.NewPedanticRegistry())
			reader := newStoreGatewayStreamReader(ctx, mockClient, 5, limiter.NewQueryLimiter(0, 0, 0, 0, nil), &stats.Stats{}, metrics, log.NewNopLogger())
			reader.StartBuffering()

			actualChunksEstimate := reader.EstimateChunkCount()
			require.Equal(t, testCase.expectedChunksEstimate, actualChunksEstimate)

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

func TestStoreGatewayStreamReader_AbortsWhenParentContextCancelled(t *testing.T) {
	// Ensure that the buffering goroutine is not leaked after context cancellation.
	test.VerifyNoLeak(t)

	testCases := map[string]func(t *testing.T, reader *storeGatewayStreamReader){
		"GetChunks()": func(t *testing.T, reader *storeGatewayStreamReader) {
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
		},
		"EstimateChunkCount()": func(t *testing.T, reader *storeGatewayStreamReader) {
			estimateChunkCountReturned := make(chan struct{})

			go func() {
				reader.EstimateChunkCount()
				close(estimateChunkCountReturned)
			}()

			select {
			case <-estimateChunkCountReturned:
				// Nothing to do, this is what we want.
			case <-time.After(time.Second):
				require.FailNow(t, "expected EstimateChunkCount() to return after context cancelled")
			}
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			// Create multiple batches to ensure that the buffering goroutine becomes blocked waiting to send further chunks to GetChunks().
			batches := []storepb.StreamingChunksBatch{
				{Series: []*storepb.StreamingChunks{{SeriesIndex: 0, Chunks: []storepb.AggrChunk{createChunk(t, 1000, 1.23)}}}},
				{Series: []*storepb.StreamingChunks{{SeriesIndex: 1, Chunks: []storepb.AggrChunk{createChunk(t, 1000, 4.56)}}}},
				{Series: []*storepb.StreamingChunks{{SeriesIndex: 2, Chunks: []storepb.AggrChunk{createChunk(t, 1000, 7.89)}}}},
			}

			streamCtx := context.Background()
			mockClient := &mockStoreGatewayQueryStreamClient{ctx: streamCtx, messages: batchesToMessages(3, batches...)}

			parentCtx, cancel := context.WithCancel(context.Background())
			metrics := newBlocksStoreQueryableMetrics(prometheus.NewPedanticRegistry())
			reader := newStoreGatewayStreamReader(parentCtx, mockClient, 3, limiter.NewQueryLimiter(0, 0, 0, 0, nil), &stats.Stats{}, metrics, log.NewNopLogger())
			cancel()
			reader.StartBuffering()

			testCase(t, reader)

			require.Eventually(t, mockClient.closed.Load, time.Second, 10*time.Millisecond, "expected gRPC client to be closed after context cancelled")
		})
	}
}

func TestStoreGatewayStreamReader_DoesNotAbortWhenStreamContextCancelled(t *testing.T) {
	// Ensure that the buffering goroutine is not leaked.
	test.VerifyNoLeak(t)

	// Create multiple batches to ensure that the buffering goroutine becomes blocked waiting to send further chunks to GetChunks().
	batches := []storepb.StreamingChunksBatch{
		{Series: []*storepb.StreamingChunks{{SeriesIndex: 0, Chunks: []storepb.AggrChunk{createChunk(t, 1000, 1.23)}}}},
		{Series: []*storepb.StreamingChunks{{SeriesIndex: 1, Chunks: []storepb.AggrChunk{createChunk(t, 1000, 4.56)}}}},
		{Series: []*storepb.StreamingChunks{{SeriesIndex: 2, Chunks: []storepb.AggrChunk{createChunk(t, 1000, 7.89)}}}},
	}

	streamCtx, cancel := context.WithCancel(context.Background())
	cancel()
	const expectedChunksEstimate uint64 = 5
	mockClient := &mockStoreGatewayQueryStreamClient{ctx: streamCtx, messages: batchesToMessages(expectedChunksEstimate, batches...)}
	metrics := newBlocksStoreQueryableMetrics(prometheus.NewPedanticRegistry())

	parentCtx := context.Background()
	reader := newStoreGatewayStreamReader(parentCtx, mockClient, 3, limiter.NewQueryLimiter(0, 0, 0, 0, nil), &stats.Stats{}, metrics, log.NewNopLogger())
	reader.StartBuffering()

	actualChunksEstimate := reader.EstimateChunkCount()
	require.Equal(t, expectedChunksEstimate, uint64(actualChunksEstimate), "expected EstimateChunkCount to return actual estimate and ignore cancelled stream context")

	for i := 0; i < 3; i++ {
		_, err := reader.GetChunks(uint64(i))
		require.NoError(t, err, "expected GetChunks to not report context cancellation error from stream context")
	}

	require.Eventually(t, mockClient.closed.Load, time.Second, 10*time.Millisecond, "expected gRPC client to be closed after stream exhausted")
}

func TestStoreGatewayStreamReader_ReadingSeriesOutOfOrder(t *testing.T) {
	batches := []storepb.StreamingChunksBatch{
		{Series: []*storepb.StreamingChunks{{SeriesIndex: 0, Chunks: []storepb.AggrChunk{createChunk(t, 1000, 1.23)}}}},
	}

	ctx := context.Background()
	mockClient := &mockStoreGatewayQueryStreamClient{ctx: ctx, messages: batchesToMessages(3, batches...)}
	metrics := newBlocksStoreQueryableMetrics(prometheus.NewPedanticRegistry())
	reader := newStoreGatewayStreamReader(ctx, mockClient, 1, limiter.NewQueryLimiter(0, 0, 0, 0, nil), &stats.Stats{}, metrics, log.NewNopLogger())
	reader.StartBuffering()

	s, err := reader.GetChunks(1)
	require.Nil(t, s)
	require.EqualError(t, err, "attempted to read series at index 1 from store-gateway chunks stream, but the stream has series with index 0")
}

func TestStoreGatewayStreamReader_ReadingMoreSeriesThanAvailable(t *testing.T) {
	firstSeries := []storepb.AggrChunk{createChunk(t, 1000, 1.23)}
	batches := []storepb.StreamingChunksBatch{
		{Series: []*storepb.StreamingChunks{{SeriesIndex: 0, Chunks: firstSeries}}},
	}

	ctx := context.Background()
	mockClient := &mockStoreGatewayQueryStreamClient{ctx: ctx, messages: batchesToMessages(3, batches...)}
	metrics := newBlocksStoreQueryableMetrics(prometheus.NewPedanticRegistry())
	reader := newStoreGatewayStreamReader(ctx, mockClient, 1, limiter.NewQueryLimiter(0, 0, 0, 0, nil), &stats.Stats{}, metrics, log.NewNopLogger())
	reader.StartBuffering()

	s, err := reader.GetChunks(0)
	require.NoError(t, err)
	require.Equal(t, s, firstSeries)

	s, err = reader.GetChunks(1)
	require.Nil(t, s)
	expectedError := "attempted to read series at index 1 from store-gateway chunks stream, but the stream has already been exhausted (was expecting 1 series)"
	require.EqualError(t, err, expectedError)

	// Ensure we continue to return the error, even for subsequent calls to GetChunks.
	_, err = reader.GetChunks(2)
	require.EqualError(t, err, "attempted to read series at index 2 from store-gateway chunks stream, but the stream previously failed and returned an error: "+expectedError)
	_, err = reader.GetChunks(3)
	require.EqualError(t, err, "attempted to read series at index 3 from store-gateway chunks stream, but the stream previously failed and returned an error: "+expectedError)
}

func TestStoreGatewayStreamReader_ReceivedFewerSeriesThanExpected(t *testing.T) {
	firstSeries := []storepb.AggrChunk{createChunk(t, 1000, 1.23)}
	batches := []storepb.StreamingChunksBatch{
		{Series: []*storepb.StreamingChunks{{SeriesIndex: 0, Chunks: firstSeries}}},
	}

	ctx := context.Background()
	mockClient := &mockStoreGatewayQueryStreamClient{ctx: ctx, messages: batchesToMessages(3, batches...)}
	metrics := newBlocksStoreQueryableMetrics(prometheus.NewPedanticRegistry())
	reader := newStoreGatewayStreamReader(ctx, mockClient, 3, limiter.NewQueryLimiter(0, 0, 0, 0, nil), &stats.Stats{}, metrics, log.NewNopLogger())
	reader.StartBuffering()

	s, err := reader.GetChunks(0)
	require.NoError(t, err)
	require.Equal(t, s, firstSeries)

	s, err = reader.GetChunks(1)
	require.Nil(t, s)
	expectedError := "attempted to read series at index 1 from store-gateway chunks stream, but the stream has failed: expected to receive 3 series, but got EOF after receiving 1 series"
	require.EqualError(t, err, expectedError)

	// Poll for the client to be closed, as the closure happens in a separate goroutine (the same one which buffers).
	require.Eventually(t, mockClient.closed.Load, time.Second, 10*time.Millisecond, "expected gRPC client to be closed after receiving more series than expected")

	// Ensure we continue to return the error, even for subsequent calls to GetChunks.
	_, err = reader.GetChunks(2)
	require.EqualError(t, err, "attempted to read series at index 2 from store-gateway chunks stream, but the stream previously failed and returned an error: "+expectedError)
	_, err = reader.GetChunks(3)
	require.EqualError(t, err, "attempted to read series at index 3 from store-gateway chunks stream, but the stream previously failed and returned an error: "+expectedError)
}

func TestStoreGatewayStreamReader_ReceivedMoreSeriesThanExpected(t *testing.T) {
	testCases := map[string][]storepb.StreamingChunksBatch{
		"extra series received as part of batch for last expected series": {
			{
				Series: []*storepb.StreamingChunks{
					{SeriesIndex: 0, Chunks: []storepb.AggrChunk{createChunk(t, 1000, 1.23)}},
					{SeriesIndex: 1, Chunks: []storepb.AggrChunk{createChunk(t, 1000, 4.56)}},
					{SeriesIndex: 2, Chunks: []storepb.AggrChunk{createChunk(t, 1000, 7.89)}},
				},
			},
		},
		"extra series received as part of batch after batch containing last expected series": {
			{
				Series: []*storepb.StreamingChunks{
					{SeriesIndex: 0, Chunks: []storepb.AggrChunk{createChunk(t, 1000, 1.23)}},
				},
			},
			{
				Series: []*storepb.StreamingChunks{
					{SeriesIndex: 1, Chunks: []storepb.AggrChunk{createChunk(t, 1000, 4.56)}},
					{SeriesIndex: 2, Chunks: []storepb.AggrChunk{createChunk(t, 1000, 7.89)}},
				},
			},
		},
	}

	for name, batches := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			mockClient := &mockStoreGatewayQueryStreamClient{ctx: ctx, messages: batchesToMessages(3, batches...)}
			metrics := newBlocksStoreQueryableMetrics(prometheus.NewPedanticRegistry())
			reader := newStoreGatewayStreamReader(ctx, mockClient, 1, limiter.NewQueryLimiter(0, 0, 0, 0, nil), &stats.Stats{}, metrics, log.NewNopLogger())
			reader.StartBuffering()

			s, err := reader.GetChunks(0)
			require.Nil(t, s)
			expectedError := "attempted to read series at index 0 from store-gateway chunks stream, but the stream has failed: expected to receive only 1 series, but received at least 3 series"
			require.EqualError(t, err, expectedError)

			// Poll for the client to be closed, as the closure happens in a separate goroutine (the same one which buffers).
			require.Eventually(t, mockClient.closed.Load, time.Second, 10*time.Millisecond, "expected gRPC client to be closed after receiving more series than expected")

			// Ensure we continue to return the error, even for subsequent calls to GetChunks.
			_, err = reader.GetChunks(1)
			require.EqualError(t, err, "attempted to read series at index 1 from store-gateway chunks stream, but the stream previously failed and returned an error: "+expectedError)
			_, err = reader.GetChunks(2)
			require.EqualError(t, err, "attempted to read series at index 2 from store-gateway chunks stream, but the stream previously failed and returned an error: "+expectedError)
		})
	}
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
		// Estimated limits are enforced by the consumer of the reader, so that we can check the total estimate is beneath the limit before loading too many batches from too many streams.
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

			ctx := context.Background()
			mockClient := &mockStoreGatewayQueryStreamClient{ctx: ctx, messages: batchesToMessages(3, batches...)}
			registry := prometheus.NewPedanticRegistry()
			metrics := newBlocksStoreQueryableMetrics(registry)
			queryMetrics := stats.NewQueryMetrics(registry)

			reader := newStoreGatewayStreamReader(ctx, mockClient, 1, limiter.NewQueryLimiter(0, testCase.maxChunkBytes, testCase.maxChunks, 0, queryMetrics), &stats.Stats{}, metrics, log.NewNopLogger())
			reader.StartBuffering()

			_, err := reader.GetChunks(0)

			if testCase.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, testCase.expectedError)
			}

			require.Eventually(t, mockClient.closed.Load, time.Second, 10*time.Millisecond, "expected gRPC client to be closed")

			if testCase.expectedError != "" {
				// Ensure we continue to return the error, even for subsequent calls to GetChunks.
				_, err := reader.GetChunks(1)
				require.EqualError(t, err, "attempted to read series at index 1 from store-gateway chunks stream, but the stream previously failed and returned an error: "+testCase.expectedError)
				_, err = reader.GetChunks(2)
				require.EqualError(t, err, "attempted to read series at index 2 from store-gateway chunks stream, but the stream previously failed and returned an error: "+testCase.expectedError)
			}
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
		Raw: storepb.Chunk{
			Type: storepb.Chunk_XOR,
			Data: promChunk.Bytes(),
		},
	}
}

func batchesToMessages(estimatedChunks uint64, batches ...storepb.StreamingChunksBatch) []*storepb.SeriesResponse {
	messages := make([]*storepb.SeriesResponse, len(batches)+1)

	messages[0] = storepb.NewStreamingChunksEstimate(estimatedChunks)

	for i, b := range batches {
		messages[i+1] = storepb.NewStreamingChunksResponse(&b)
	}

	return messages
}

type mockStoreGatewayQueryStreamClient struct {
	ctx      context.Context
	messages []*storepb.SeriesResponse
	closed   atomic.Bool
}

func (m *mockStoreGatewayQueryStreamClient) Recv() (*storepb.SeriesResponse, error) {
	if len(m.messages) == 0 {
		return nil, io.EOF
	}

	msg := m.messages[0]
	m.messages = m.messages[1:]

	return msg, nil
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
