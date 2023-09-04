// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/remote_read_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	prom_remote "github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/util/test"
)

type mockSampleAndChunkQueryable struct {
	queryableFn      func(ctx context.Context, mint, maxt int64) (storage.Querier, error)
	chunkQueryableFn func(ctx context.Context, mint, maxt int64) (storage.ChunkQuerier, error)
}

func (m mockSampleAndChunkQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return m.queryableFn(ctx, mint, maxt)
}

func (m mockSampleAndChunkQueryable) ChunkQuerier(ctx context.Context, mint, maxt int64) (storage.ChunkQuerier, error) {
	return m.chunkQueryableFn(ctx, mint, maxt)
}

type mockQuerier struct {
	storage.Querier
	seriesSet storage.SeriesSet
}

func (m mockQuerier) Select(_ bool, sp *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
	if sp == nil {
		panic("mockQuerier: select params must be set")
	}
	return m.seriesSet
}

type mockChunkQuerier struct {
	storage.ChunkQuerier
	seriesSet storage.SeriesSet
}

func (m mockChunkQuerier) Select(_ bool, sp *storage.SelectHints, _ ...*labels.Matcher) storage.ChunkSeriesSet {
	if sp == nil {
		panic("mockChunkQuerier: select params must be set")
	}
	return storage.NewSeriesSetToChunkSet(m.seriesSet)
}

func TestSampledRemoteRead(t *testing.T) {
	q := &mockSampleAndChunkQueryable{
		queryableFn: func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
			return mockQuerier{
				seriesSet: series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
					series.NewConcreteSeries(
						labels.FromStrings("foo", "bar"),
						[]model.SamplePair{{Timestamp: 0, Value: 0}, {Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}, {Timestamp: 3, Value: 3}},
						[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(4, test.GenerateTestHistogram(4))},
					),
				}),
			}, nil
		},
	}
	handler := RemoteReadHandler(q, log.NewNopLogger())

	requestBody, err := proto.Marshal(&client.ReadRequest{
		Queries: []*client.QueryRequest{
			{StartTimestampMs: 0, EndTimestampMs: 10},
		},
	})
	require.NoError(t, err)
	requestBody = snappy.Encode(nil, requestBody)
	request, err := http.NewRequest(http.MethodPost, "/api/v1/read", bytes.NewReader(requestBody))
	require.NoError(t, err)
	request.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	require.Equal(t, 200, recorder.Result().StatusCode)
	require.Equal(t, []string([]string{"application/x-protobuf"}), recorder.Result().Header["Content-Type"])
	responseBody, err := io.ReadAll(recorder.Result().Body)
	require.NoError(t, err)
	responseBody, err = snappy.Decode(nil, responseBody)
	require.NoError(t, err)
	var response client.ReadResponse
	err = proto.Unmarshal(responseBody, &response)
	require.NoError(t, err)

	expected := client.ReadResponse{
		Results: []*client.QueryResponse{
			{
				Timeseries: []mimirpb.TimeSeries{
					{
						Labels: []mimirpb.LabelAdapter{
							{Name: "foo", Value: "bar"},
						},
						Samples: []mimirpb.Sample{
							{Value: 0, TimestampMs: 0},
							{Value: 1, TimestampMs: 1},
							{Value: 2, TimestampMs: 2},
							{Value: 3, TimestampMs: 3},
						},
						Histograms: []mimirpb.Histogram{
							mimirpb.FromHistogramToHistogramProto(4, test.GenerateTestHistogram(4)),
						},
					},
				},
			},
		},
	}
	require.Equal(t, expected, response)
}

func TestStreamedRemoteRead(t *testing.T) {
	tcs := map[string]struct {
		samples         []model.SamplePair
		histograms      []mimirpb.Histogram
		expectedResults []*client.StreamReadResponse
	}{
		"with 120 samples, we expect 1 frame with 1 chunk": {
			samples: getNSamples(120),
			expectedResults: []*client.StreamReadResponse{
				{
					ChunkedSeries: []*client.StreamChunkedSeries{
						{
							Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}},
							Chunks: []client.StreamChunk{
								{
									MinTimeMs: 0,
									MaxTimeMs: 119,
									Type:      client.XOR,
									Data:      getIndexedChunk(0, 120, chunkenc.EncXOR),
								},
							},
						},
					},
					QueryIndex: 0,
				},
			},
		},
		"with 121 samples, we expect 1 frame with 2 chunks": {
			samples: getNSamples(121),
			expectedResults: []*client.StreamReadResponse{
				{
					ChunkedSeries: []*client.StreamChunkedSeries{
						{
							Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}},
							Chunks: []client.StreamChunk{
								{
									MinTimeMs: 0,
									MaxTimeMs: 119,
									Type:      client.XOR,
									Data:      getIndexedChunk(0, 121, chunkenc.EncXOR),
								},
								{
									MinTimeMs: 120,
									MaxTimeMs: 120,
									Type:      client.XOR,
									Data:      getIndexedChunk(1, 121, chunkenc.EncXOR),
								},
							},
						},
					},
					QueryIndex: 0,
				},
			},
		},
		"with 481 samples, we expect 2 frames with 2 chunks, and 1 frame with 1 chunk due to frame limit": {
			samples: getNSamples(481),
			expectedResults: []*client.StreamReadResponse{
				{
					ChunkedSeries: []*client.StreamChunkedSeries{
						{
							Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}},
							Chunks: []client.StreamChunk{
								{
									MinTimeMs: 0,
									MaxTimeMs: 119,
									Type:      client.XOR,
									Data:      getIndexedChunk(0, 481, chunkenc.EncXOR),
								},
								{
									MinTimeMs: 120,
									MaxTimeMs: 239,
									Type:      client.XOR,
									Data:      getIndexedChunk(1, 481, chunkenc.EncXOR),
								},
							},
						},
					},
				},
				{
					ChunkedSeries: []*client.StreamChunkedSeries{
						{
							Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}},
							Chunks: []client.StreamChunk{
								{
									MinTimeMs: 240,
									MaxTimeMs: 359,
									Type:      client.XOR,
									Data:      getIndexedChunk(2, 481, chunkenc.EncXOR),
								},
								{
									MinTimeMs: 360,
									MaxTimeMs: 479,
									Type:      client.XOR,
									Data:      getIndexedChunk(3, 481, chunkenc.EncXOR),
								},
							},
						},
					},
				},
				{
					ChunkedSeries: []*client.StreamChunkedSeries{
						{
							Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}},
							Chunks: []client.StreamChunk{
								{
									MinTimeMs: 480,
									MaxTimeMs: 480,
									Type:      client.XOR,
									Data:      getIndexedChunk(4, 481, chunkenc.EncXOR),
								},
							},
						},
					},
				},
			},
		},
		"120 native histograms": {
			histograms: getNHistogramSamples(120),
			expectedResults: []*client.StreamReadResponse{
				{
					ChunkedSeries: []*client.StreamChunkedSeries{
						{
							Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}},
							Chunks: []client.StreamChunk{
								{
									MinTimeMs: 0,
									MaxTimeMs: 119,
									Type:      client.HISTOGRAM,
									Data:      getIndexedChunk(0, 120, chunkenc.EncHistogram),
								},
							},
						},
					},
					QueryIndex: 0,
				},
			},
		},
		"120 native float histograms": {
			histograms: getNFloatHistogramSamples(120),
			expectedResults: []*client.StreamReadResponse{
				{
					ChunkedSeries: []*client.StreamChunkedSeries{
						{
							Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}},
							Chunks: []client.StreamChunk{
								{
									MinTimeMs: 0,
									MaxTimeMs: 119,
									Type:      client.FLOAT_HISTOGRAM,
									Data:      getIndexedChunk(0, 120, chunkenc.EncFloatHistogram),
								},
							},
						},
					},
				},
			},
		},
	}
	for tn, tc := range tcs {
		t.Run(tn, func(t *testing.T) {
			q := &mockSampleAndChunkQueryable{
				chunkQueryableFn: func(ctx context.Context, mint, maxt int64) (storage.ChunkQuerier, error) {
					return mockChunkQuerier{
						seriesSet: series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
							series.NewConcreteSeries(
								labels.FromStrings("foo", "bar"),
								tc.samples,
								tc.histograms,
							),
						}),
					}, nil
				},
			}
			// The labelset for this test has 10 bytes and a full chunk is roughly 165 bytes; for this test we want a
			// frame to contain at most 2 chunks.
			maxBytesInFrame := 10 + 165*2

			handler := remoteReadHandler(q, maxBytesInFrame, log.NewNopLogger())

			requestBody, err := proto.Marshal(&client.ReadRequest{
				Queries: []*client.QueryRequest{
					{StartTimestampMs: 0, EndTimestampMs: 10},
				},
				AcceptedResponseTypes: []client.ReadRequest_ResponseType{client.STREAMED_XOR_CHUNKS},
			})
			require.NoError(t, err)
			requestBody = snappy.Encode(nil, requestBody)
			request, err := http.NewRequest(http.MethodPost, "/api/v1/read", bytes.NewReader(requestBody))
			require.NoError(t, err)
			request.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)

			require.Equal(t, 200, recorder.Result().StatusCode)
			require.Equal(t, []string{"application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"}, recorder.Result().Header["Content-Type"])

			stream := prom_remote.NewChunkedReader(recorder.Result().Body, prom_remote.DefaultChunkedReadLimit, nil)

			i := 0
			for {
				var res client.StreamReadResponse
				err := stream.NextProto(&res)
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)

				if len(tc.expectedResults) < i+1 {
					require.Fail(t, "unexpected result message")
				}
				require.Equal(t, tc.expectedResults[i], &res)
				i++
			}
			require.Len(t, tc.expectedResults, i)
		})
	}
}

func getNSamples(n int) []model.SamplePair {
	var ret []model.SamplePair
	for i := 0; i < n; i++ {
		ret = append(ret, model.SamplePair{
			Timestamp: model.Time(i),
			Value:     model.SampleValue(i),
		})
	}
	return ret
}

func getNHistogramSamples(n int) []mimirpb.Histogram {
	var ret []mimirpb.Histogram
	for i := 0; i < n; i++ {
		h := test.GenerateTestHistogram(i)
		ret = append(ret, mimirpb.FromHistogramToHistogramProto(int64(i), h))
	}
	return ret
}

func getNFloatHistogramSamples(n int) []mimirpb.Histogram {
	var ret []mimirpb.Histogram
	for i := 0; i < n; i++ {
		h := test.GenerateTestFloatHistogram(i)
		ret = append(ret, mimirpb.FromFloatHistogramToHistogramProto(int64(i), h))
	}
	return ret
}

func getIndexedChunk(idx, samplesCount int, encoding chunkenc.Encoding) []byte {
	const samplesPerChunk = 120

	var enc chunkenc.Chunk
	switch encoding {
	case chunkenc.EncXOR:
		enc = chunkenc.NewXORChunk()
	case chunkenc.EncHistogram:
		enc = chunkenc.NewHistogramChunk()
	case chunkenc.EncFloatHistogram:
		enc = chunkenc.NewFloatHistogramChunk()
	}
	ap, _ := enc.Appender()

	baseIdx := idx * samplesPerChunk
	for i := 0; i < samplesPerChunk; i++ {
		j := baseIdx + i
		if j >= samplesCount {
			break
		}

		switch encoding {
		case chunkenc.EncXOR:
			ap.Append(int64(j), float64(j))
		case chunkenc.EncHistogram:
			_, _, _, err := ap.AppendHistogram(nil, int64(j), test.GenerateTestHistogram(j), true)
			if err != nil {
				panic(err)
			}
		case chunkenc.EncFloatHistogram:
			_, _, _, err := ap.AppendFloatHistogram(nil, int64(j), test.GenerateTestFloatHistogram(j), true)
			if err != nil {
				panic(err)
			}
		}
	}
	return enc.Bytes()
}
