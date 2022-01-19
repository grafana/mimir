// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/marshaling_test.go
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/query_range_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"bytes"
	"context"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"testing"

	"github.com/go-kit/log"
	jsoniter "github.com/json-iterator/go"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/mimirpb"
)

var (
	matrix = model.ValMatrix.String()
)

func TestRequest(t *testing.T) {
	for i, tc := range []struct {
		url         string
		expected    Request
		expectedErr error
	}{
		{
			url: "/api/v1/query_range?end=1536716880&query=sum%28container_memory_rss%29+by+%28namespace%29&start=1536673680&step=120",
			expected: &PrometheusRangeQueryRequest{
				Path:  "/api/v1/query_range",
				Start: 1536673680 * 1e3,
				End:   1536716880 * 1e3,
				Step:  120 * 1e3,
				Query: "sum(container_memory_rss) by (namespace)",
			},
		},
		{
			url: "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&time=1536716880",
			expected: &PrometheusInstantQueryRequest{
				Path:  "/api/v1/query",
				Time:  1536716880 * 1e3,
				Query: "sum(container_memory_rss) by (namespace)",
			},
		},
		{
			url:         "api/v1/query_range?start=foo",
			expectedErr: apierror.New(apierror.TypeBadData, "invalid parameter \"start\": cannot parse \"foo\" to a valid timestamp"),
		},
		{
			url:         "api/v1/query_range?start=123&end=bar",
			expectedErr: apierror.New(apierror.TypeBadData, "invalid parameter \"end\": cannot parse \"bar\" to a valid timestamp"),
		},
		{
			url:         "api/v1/query_range?start=123&end=0",
			expectedErr: errEndBeforeStart,
		},
		{
			url:         "api/v1/query_range?start=123&end=456&step=baz",
			expectedErr: apierror.New(apierror.TypeBadData, "invalid parameter \"step\": cannot parse \"baz\" to a valid duration"),
		},
		{
			url:         "api/v1/query_range?start=123&end=456&step=-1",
			expectedErr: errNegativeStep,
		},
		{
			url:         "api/v1/query_range?start=0&end=11001&step=1",
			expectedErr: errStepTooSmall,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			r, err := http.NewRequest("GET", tc.url, nil)
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), "1")
			r = r.WithContext(ctx)

			req, err := PrometheusCodec.DecodeRequest(ctx, r)
			if err != nil {
				require.EqualValues(t, tc.expectedErr, err)
				return
			}
			require.EqualValues(t, tc.expected, req)

			rdash, err := PrometheusCodec.EncodeRequest(context.Background(), req)
			require.NoError(t, err)
			require.EqualValues(t, tc.url, rdash.RequestURI)
		})
	}
}

type prometheusAPIResponse struct {
	Status    string       `json:"status"`
	Data      interface{}  `json:"data,omitempty"`
	ErrorType v1.ErrorType `json:"errorType,omitempty"`
	Error     string       `json:"error,omitempty"`
	Warnings  []string     `json:"warnings,omitempty"`
}

type prometeheusResponseData struct {
	Type   model.ValueType `json:"resultType"`
	Result model.Value     `json:"result"`
}

func TestResponseRoundtrip(t *testing.T) {
	headers := http.Header{"Content-Type": []string{"application/json"}}
	expectedRespHeaders := []*PrometheusResponseHeader{
		{
			Name:   "Content-Type",
			Values: []string{"application/json"},
		},
	}

	for _, tc := range []struct {
		name     string
		resp     prometheusAPIResponse
		expected *PrometheusResponse
	}{
		{
			name: "successful string response",
			resp: prometheusAPIResponse{
				Status: statusSuccess,
				Data: prometeheusResponseData{
					Type:   model.ValString,
					Result: &model.String{Value: "foo", Timestamp: 1_500},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValString.String(),
					Result: []SampleStream{
						{
							Labels:  []mimirpb.LabelAdapter{{Name: "value", Value: "foo"}},
							Samples: []mimirpb.Sample{{TimestampMs: 1_500}},
						},
					},
				},
				Headers: expectedRespHeaders,
			},
		},
		{
			name: "successful scalar response",
			resp: prometheusAPIResponse{
				Status: statusSuccess,
				Data: prometeheusResponseData{
					Type: model.ValScalar,
					Result: &model.Scalar{
						Value:     200,
						Timestamp: 1_000,
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValScalar.String(),
					Result: []SampleStream{
						{Samples: []mimirpb.Sample{{TimestampMs: 1_000, Value: 200}}},
					},
				},
				Headers: expectedRespHeaders,
			},
		},
		{
			name: "successful instant response",
			resp: prometheusAPIResponse{
				Status: statusSuccess,
				Data: prometeheusResponseData{
					Type: model.ValVector,
					Result: model.Vector{
						{Metric: model.Metric{"foo": "bar"}, Timestamp: 1_000, Value: 200},
						{Metric: model.Metric{"bar": "baz"}, Timestamp: 1_000, Value: 201},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValVector.String(),
					Result: []SampleStream{
						{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}}, Samples: []mimirpb.Sample{{TimestampMs: 1_000, Value: 200}}},
						{Labels: []mimirpb.LabelAdapter{{Name: "bar", Value: "baz"}}, Samples: []mimirpb.Sample{{TimestampMs: 1_000, Value: 201}}},
					},
				},
				Headers: expectedRespHeaders,
			},
		},
		{
			name: "successful range response",
			resp: prometheusAPIResponse{
				Status: statusSuccess,
				Data: prometeheusResponseData{
					Type: model.ValMatrix,
					Result: model.Matrix{
						{Metric: model.Metric{"foo": "bar"}, Values: []model.SamplePair{{Timestamp: 1_000, Value: 100}, {Timestamp: 2_000, Value: 200}}},
						{Metric: model.Metric{"bar": "baz"}, Values: []model.SamplePair{{Timestamp: 1_000, Value: 101}, {Timestamp: 2_000, Value: 201}}},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValMatrix.String(),
					Result: []SampleStream{
						{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}}, Samples: []mimirpb.Sample{{TimestampMs: 1_000, Value: 100}, {TimestampMs: 2_000, Value: 200}}},
						{Labels: []mimirpb.LabelAdapter{{Name: "bar", Value: "baz"}}, Samples: []mimirpb.Sample{{TimestampMs: 1_000, Value: 101}, {TimestampMs: 2_000, Value: 201}}},
					},
				},
				Headers: expectedRespHeaders,
			},
		},
		{
			name: "successful empty matrix response",
			resp: prometheusAPIResponse{
				Status: statusSuccess,
				Data: prometeheusResponseData{
					Type:   model.ValMatrix,
					Result: model.Matrix{},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValMatrix.String(),
					Result:     []SampleStream{},
				},
				Headers: expectedRespHeaders,
			},
		},
		{
			name: "error response",
			resp: prometheusAPIResponse{
				Status:    statusError,
				ErrorType: "expected",
				Error:     "failed",
			},
			expected: &PrometheusResponse{
				Status:    statusError,
				ErrorType: "expected",
				Error:     "failed",
				Headers:   expectedRespHeaders,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, err := json.Marshal(tc.resp)
			require.NoError(t, err)
			httpResponse := &http.Response{
				StatusCode:    200,
				Header:        headers,
				Body:          ioutil.NopCloser(bytes.NewBuffer(body)),
				ContentLength: int64(len(body)),
			}
			decoded, err := PrometheusCodec.DecodeResponse(context.Background(), httpResponse, nil, log.NewNopLogger())
			require.NoError(t, err)
			assert.Equal(t, tc.expected, decoded)

			// Reset response, as the above call will have consumed the body reader.
			httpResponse = &http.Response{
				StatusCode:    200,
				Header:        headers,
				Body:          ioutil.NopCloser(bytes.NewBuffer(body)),
				ContentLength: int64(len(body)),
			}
			encoded, err := PrometheusCodec.EncodeResponse(context.Background(), decoded)
			require.NoError(t, err)

			expectedJSON, err := bodyBuffer(httpResponse)
			require.NoError(t, err)
			encodedJSON, err := bodyBuffer(encoded)
			require.NoError(t, err)

			require.JSONEq(t, string(expectedJSON), string(encodedJSON))
			assert.Equal(t, httpResponse, encoded)
		})
	}
}

func TestMergeAPIResponses(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    []Response
		expected Response
	}{
		{
			name:  "No responses shouldn't panic and return a non-null result and result type.",
			input: []Response{},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: matrix,
					Result:     []SampleStream{},
				},
			},
		},

		{
			name: "A single empty response shouldn't panic.",
			input: []Response{
				&PrometheusResponse{
					Status: statusSuccess,
					Data: &PrometheusData{
						ResultType: matrix,
						Result:     []SampleStream{},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: matrix,
					Result:     []SampleStream{},
				},
			},
		},

		{
			name: "Multiple empty responses shouldn't panic.",
			input: []Response{
				&PrometheusResponse{
					Status: statusSuccess,
					Data: &PrometheusData{
						ResultType: matrix,
						Result:     []SampleStream{},
					},
				},
				&PrometheusResponse{
					Status: statusSuccess,
					Data: &PrometheusData{
						ResultType: matrix,
						Result:     []SampleStream{},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: matrix,
					Result:     []SampleStream{},
				},
			},
		},

		{
			name: "Basic merging of two responses.",
			input: []Response{
				&PrometheusResponse{
					Status: statusSuccess,
					Data: &PrometheusData{
						ResultType: matrix,
						Result: []SampleStream{
							{
								Labels: []mimirpb.LabelAdapter{},
								Samples: []mimirpb.Sample{
									{Value: 0, TimestampMs: 0},
									{Value: 1, TimestampMs: 1},
								},
							},
						},
					},
				},
				&PrometheusResponse{
					Status: statusSuccess,
					Data: &PrometheusData{
						ResultType: matrix,
						Result: []SampleStream{
							{
								Labels: []mimirpb.LabelAdapter{},
								Samples: []mimirpb.Sample{
									{Value: 2, TimestampMs: 2},
									{Value: 3, TimestampMs: 3},
								},
							},
						},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []mimirpb.LabelAdapter{},
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
		},

		{
			name: "Merging of responses when labels are in different order.",
			input: []Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[0,"0"],[1,"1"]]}]}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[2,"2"],[3,"3"]]}]}}`),
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []mimirpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
							Samples: []mimirpb.Sample{
								{Value: 0, TimestampMs: 0},
								{Value: 1, TimestampMs: 1000},
								{Value: 2, TimestampMs: 2000},
								{Value: 3, TimestampMs: 3000},
							},
						},
					},
				},
			},
		},

		{
			name: "Merging of samples where there is single overlap.",
			input: []Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[1,"1"],[2,"2"]]}]}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[2,"2"],[3,"3"]]}]}}`),
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []mimirpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
							Samples: []mimirpb.Sample{
								{Value: 1, TimestampMs: 1000},
								{Value: 2, TimestampMs: 2000},
								{Value: 3, TimestampMs: 3000},
							},
						},
					},
				},
			},
		},
		{
			name: "Merging of samples where there is multiple partial overlaps.",
			input: []Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[1,"1"],[2,"2"],[3,"3"]]}]}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[2,"2"],[3,"3"],[4,"4"],[5,"5"]]}]}}`),
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []mimirpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
							Samples: []mimirpb.Sample{
								{Value: 1, TimestampMs: 1000},
								{Value: 2, TimestampMs: 2000},
								{Value: 3, TimestampMs: 3000},
								{Value: 4, TimestampMs: 4000},
								{Value: 5, TimestampMs: 5000},
							},
						},
					},
				},
			},
		},
		{
			name: "Merging of samples where there is complete overlap.",
			input: []Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[2,"2"],[3,"3"]]}]}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[2,"2"],[3,"3"],[4,"4"],[5,"5"]]}]}}`),
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []mimirpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
							Samples: []mimirpb.Sample{
								{Value: 2, TimestampMs: 2000},
								{Value: 3, TimestampMs: 3000},
								{Value: 4, TimestampMs: 4000},
								{Value: 5, TimestampMs: 5000},
							},
						},
					},
				},
			},
		}} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := PrometheusCodec.MergeResponse(tc.input...)
			require.NoError(t, err)
			require.Equal(t, tc.expected, output)
		})
	}

	t.Run("shouldn't merge unsuccessful responses", func(t *testing.T) {
		successful := &PrometheusResponse{
			Status: statusSuccess,
			Data:   &PrometheusData{ResultType: matrix},
		}
		unsuccessful := &PrometheusResponse{
			Status: statusError,
			Data:   &PrometheusData{ResultType: matrix},
		}

		_, err := PrometheusCodec.MergeResponse(successful, unsuccessful)
		require.Error(t, err)
	})

	t.Run("shouldn't merge nil data", func(t *testing.T) {
		// nil data has no type, so we can't merge it, it's basically an unsuccessful response,
		// and we should never reach the point where we're merging an unsuccessful response.
		successful := &PrometheusResponse{
			Status: statusSuccess,
			Data:   &PrometheusData{ResultType: matrix},
		}
		nilData := &PrometheusResponse{
			Status: statusSuccess, // shouldn't have nil data with a successful response, but we want to test everything.
			Data:   nil,
		}
		_, err := PrometheusCodec.MergeResponse(successful, nilData)
		require.Error(t, err)
	})

	t.Run("shouldn't merge non-matrix data", func(t *testing.T) {
		matrixResponse := &PrometheusResponse{
			Status: statusSuccess,
			Data:   &PrometheusData{ResultType: matrix},
		}
		vectorResponse := &PrometheusResponse{
			Status: statusSuccess,
			Data:   &PrometheusData{ResultType: model.ValVector.String()},
		}
		_, err := PrometheusCodec.MergeResponse(matrixResponse, vectorResponse)
		require.Error(t, err)
	})
}

func mustParse(t *testing.T, response string) Response {
	var resp PrometheusResponse
	// Needed as goimports automatically add a json import otherwise.
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	require.NoError(t, json.Unmarshal([]byte(response), &resp))
	return &resp
}

func BenchmarkPrometheusCodec_DecodeResponse(b *testing.B) {
	const (
		numSeries           = 1000
		numSamplesPerSeries = 1000
	)

	// Generate a mocked response and marshal it.
	res := mockPrometheusResponse(numSeries, numSamplesPerSeries)
	encodedRes, err := json.Marshal(res)
	require.NoError(b, err)
	b.Log("test prometheus response size:", len(encodedRes))

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		_, err := PrometheusCodec.DecodeResponse(context.Background(), &http.Response{
			StatusCode:    200,
			Body:          ioutil.NopCloser(bytes.NewReader(encodedRes)),
			ContentLength: int64(len(encodedRes)),
		}, nil, log.NewNopLogger())
		require.NoError(b, err)
	}
}

func BenchmarkPrometheusCodec_EncodeResponse(b *testing.B) {
	const (
		numSeries           = 1000
		numSamplesPerSeries = 1000
	)

	// Generate a mocked response and marshal it.
	res := mockPrometheusResponse(numSeries, numSamplesPerSeries)

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		_, err := PrometheusCodec.EncodeResponse(context.Background(), res)
		require.NoError(b, err)
	}
}

func mockPrometheusResponse(numSeries, numSamplesPerSeries int) *PrometheusResponse {
	stream := make([]SampleStream, numSeries)
	for s := 0; s < numSeries; s++ {
		// Generate random samples.
		samples := make([]mimirpb.Sample, numSamplesPerSeries)
		for i := 0; i < numSamplesPerSeries; i++ {
			samples[i] = mimirpb.Sample{
				Value:       rand.Float64(),
				TimestampMs: int64(i),
			}
		}

		// Generate random labels.
		lbls := make([]mimirpb.LabelAdapter, 10)
		for i := range lbls {
			lbls[i].Name = "a_medium_size_label_name"
			lbls[i].Value = "a_medium_size_label_value_that_is_used_to_benchmark_marshalling"
		}

		stream[s] = SampleStream{
			Labels:  lbls,
			Samples: samples,
		}
	}

	return &PrometheusResponse{
		Status: "success",
		Data: &PrometheusData{
			ResultType: "vector",
			Result:     stream,
		},
	}
}

func mockPrometheusResponseSingleSeries(series []mimirpb.LabelAdapter, samples ...mimirpb.Sample) *PrometheusResponse {
	return &PrometheusResponse{
		Status: "success",
		Data: &PrometheusData{
			ResultType: "matrix",
			Result: []SampleStream{
				{
					Labels:  series,
					Samples: samples,
				},
			},
		},
	}
}

func Test_DecodeOptions(t *testing.T) {
	for _, tt := range []struct {
		name     string
		input    *http.Request
		expected *Options
	}{
		{
			name: "default",
			input: &http.Request{
				Header: http.Header{},
			},
			expected: &Options{},
		},
		{
			name: "disable cache",
			input: &http.Request{
				Header: http.Header{
					cacheControlHeader: []string{noStoreValue},
				},
			},
			expected: &Options{
				CacheDisabled: true,
			},
		},
		{
			name: "custom sharding",
			input: &http.Request{
				Header: http.Header{
					totalShardsControlHeader: []string{"64"},
				},
			},
			expected: &Options{
				TotalShards: 64,
			},
		},
		{
			name: "disable sharding",
			input: &http.Request{
				Header: http.Header{
					totalShardsControlHeader: []string{"0"},
				},
			},
			expected: &Options{
				ShardingDisabled: true,
			},
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			actual := &Options{}
			decodeOptions(tt.input, actual)
			require.Equal(t, tt.expected, actual)
		})
	}
}
