// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/remote_read_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	prom_remote "github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/series"
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
	matrix model.Matrix
}

func (m mockQuerier) Select(_ bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if sp == nil {
		panic("mockQuerier: select params must be set")
	}
	return series.MatrixToSeriesSet(m.matrix)
}

type mockChunkQuerier struct {
	storage.ChunkQuerier
	matrix model.Matrix
}

func (m mockChunkQuerier) Select(_ bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	if sp == nil {
		panic("mockChunkQuerier: select params must be set")
	}
	return storage.NewSeriesSetToChunkSet(series.MatrixToSeriesSet(m.matrix))
}

func TestSampledRemoteRead(t *testing.T) {
	q := &mockSampleAndChunkQueryable{
		queryableFn: func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
			return mockQuerier{
				matrix: model.Matrix{
					{
						Metric: model.Metric{"foo": "bar"},
						Values: []model.SamplePair{
							{Timestamp: 0, Value: 0},
							{Timestamp: 1, Value: 1},
							{Timestamp: 2, Value: 2},
							{Timestamp: 3, Value: 3},
						},
					},
				},
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
	responseBody, err := ioutil.ReadAll(recorder.Result().Body)
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
									Data:      getIndexedXORChunk(0, 120),
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
									Data:      getIndexedXORChunk(0, 121),
								},
								{
									MinTimeMs: 120,
									MaxTimeMs: 120,
									Type:      client.XOR,
									Data:      getIndexedXORChunk(1, 121),
								},
							},
						},
					},
					QueryIndex: 0,
				},
			},
		},
		"with 241 samples, we expect 1 frame with 2 chunks, and 1 frame with 1 chunk due to frame limit": {
			samples: getNSamples(241),
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
									Data:      getIndexedXORChunk(0, 241),
								},
								{
									MinTimeMs: 120,
									MaxTimeMs: 239,
									Type:      client.XOR,
									Data:      getIndexedXORChunk(1, 241),
								},
							},
						},
					},
					QueryIndex: 0,
				},
				{
					ChunkedSeries: []*client.StreamChunkedSeries{
						{
							Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}},
							Chunks: []client.StreamChunk{
								{
									MinTimeMs: 240,
									MaxTimeMs: 240,
									Type:      client.XOR,
									Data:      getIndexedXORChunk(2, 241),
								},
							},
						},
					},
					QueryIndex: 0,
				},
			},
		},
	}
	for tn, tc := range tcs {
		t.Run(tn, func(t *testing.T) {
			q := &mockSampleAndChunkQueryable{
				chunkQueryableFn: func(ctx context.Context, mint, maxt int64) (storage.ChunkQuerier, error) {
					return mockChunkQuerier{
						matrix: model.Matrix{
							{
								Metric: model.Metric{"foo": "bar"},
								Values: tc.samples,
							},
						},
					}, nil
				},
			}
			// Labelset has 10 bytes. Full frame in test data has roughly 160 bytes. This allows us to have at max 2 frames in this test.
			maxBytesInFrame := 10 + 160*2

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
				if err == io.EOF {
					break
				}
				require.NoError(t, err)

				if len(tc.expectedResults) < i+1 {
					require.Fail(t, "unexpected result message")
				}
				require.Equal(t, tc.expectedResults[i], &res)
				i++
			}
		})
	}
}

func getNSamples(n int) []model.SamplePair {
	var retVal []model.SamplePair
	for i := 0; i < n; i++ {
		retVal = append(retVal, model.SamplePair{
			Timestamp: model.Time(i),
			Value:     model.SampleValue(i),
		})
	}
	return retVal
}

func getIndexedXORChunk(idx, samplesCount int) []byte {
	const samplesPerChunk = 120

	enc := chunkenc.NewXORChunk()
	ap, _ := enc.Appender()

	baseIdx := idx * samplesPerChunk
	for i := 0; i < samplesPerChunk; i++ {
		j := baseIdx + i
		if j >= samplesCount {
			break
		}
		ap.Append(int64(j), float64(j))
	}
	return enc.Bytes()
}
