// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/marshaling_test.go
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/query_range_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	jsoniter "github.com/json-iterator/go"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/mimirpb"
)

var (
	matrix = model.ValMatrix.String()
)

func TestRequest(t *testing.T) {
	codec := newTestPrometheusCodec()

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
			url:         "/api/v1/query_range?start=foo",
			expectedErr: apierror.New(apierror.TypeBadData, "invalid parameter \"start\": cannot parse \"foo\" to a valid timestamp"),
		},
		{
			url:         "/api/v1/query_range?start=123&end=bar",
			expectedErr: apierror.New(apierror.TypeBadData, "invalid parameter \"end\": cannot parse \"bar\" to a valid timestamp"),
		},
		{
			url:         "/api/v1/query_range?start=123&end=0",
			expectedErr: errEndBeforeStart,
		},
		{
			url:         "/api/v1/query_range?start=123&end=456&step=baz",
			expectedErr: apierror.New(apierror.TypeBadData, "invalid parameter \"step\": cannot parse \"baz\" to a valid duration"),
		},
		{
			url:         "/api/v1/query_range?start=123&end=456&step=-1",
			expectedErr: errNegativeStep,
		},
		{
			url:         "/api/v1/query_range?start=0&end=11001&step=1",
			expectedErr: errStepTooSmall,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			r, err := http.NewRequest("GET", tc.url, nil)
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), "1")
			r = r.WithContext(ctx)

			req, err := codec.DecodeRequest(ctx, r)
			if err != nil || tc.expectedErr != nil {
				require.EqualValues(t, tc.expectedErr, err)
				return
			}
			require.EqualValues(t, tc.expected, req)

			rdash, err := codec.EncodeRequest(context.Background(), req)
			require.NoError(t, err)
			require.EqualValues(t, tc.url, rdash.RequestURI)
		})
	}
}

func TestPrometheusCodec_EncodeRequest_AcceptHeader(t *testing.T) {
	for _, queryResultPayloadFormat := range allFormats {
		t.Run(queryResultPayloadFormat, func(t *testing.T) {
			codec := NewPrometheusCodec(prometheus.NewPedanticRegistry(), queryResultPayloadFormat)
			req := PrometheusInstantQueryRequest{}
			encodedRequest, err := codec.EncodeRequest(context.Background(), &req)
			require.NoError(t, err)

			switch queryResultPayloadFormat {
			case formatJSON:
				require.Equal(t, "application/json", encodedRequest.Header.Get("Accept"))
			case formatProtobuf:
				require.Equal(t, "application/vnd.mimir.queryresponse+protobuf,application/json", encodedRequest.Header.Get("Accept"))
			default:
				t.Fatalf(fmt.Sprintf("unknown query result payload format: %v", queryResultPayloadFormat))
			}
		})
	}
}

func TestPrometheusCodec_EncodeResponse_ContentNegotiation(t *testing.T) {
	testResponse := &PrometheusResponse{
		Status:    statusError,
		ErrorType: string(v1.ErrExec),
		Error:     "something went wrong",
	}

	jsonBody, err := jsonFormatter{}.EncodeResponse(testResponse)
	require.NoError(t, err)

	protobufBody, err := protobufFormatter{}.EncodeResponse(testResponse)
	require.NoError(t, err)

	scenarios := map[string]struct {
		acceptHeader                string
		expectedResponseContentType string
		expectedResponseBody        []byte
		expectedError               error
	}{
		"no content type in Accept header": {
			acceptHeader:                "",
			expectedResponseContentType: jsonMimeType,
			expectedResponseBody:        jsonBody,
		},
		"unsupported content type in Accept header": {
			acceptHeader:  "testing/not-a-supported-content-type",
			expectedError: apierror.New(apierror.TypeNotAcceptable, "none of the content types in the Accept header are supported"),
		},
		"multiple unsupported content types in Accept header": {
			acceptHeader:  "testing/not-a-supported-content-type,testing/also-not-a-supported-content-type",
			expectedError: apierror.New(apierror.TypeNotAcceptable, "none of the content types in the Accept header are supported"),
		},
		"single supported content type in Accept header": {
			acceptHeader:                "application/json",
			expectedResponseContentType: jsonMimeType,
			expectedResponseBody:        jsonBody,
		},
		"wildcard subtype in Accept header": {
			acceptHeader:                "application/*",
			expectedResponseContentType: jsonMimeType,
			expectedResponseBody:        jsonBody,
		},
		"wildcard in Accept header": {
			acceptHeader:                "*/*",
			expectedResponseContentType: jsonMimeType,
			expectedResponseBody:        jsonBody,
		},
		"multiple supported content types in Accept header": {
			acceptHeader:                "application/vnd.mimir.queryresponse+protobuf,application/json",
			expectedResponseContentType: mimirpb.QueryResponseMimeType,
			expectedResponseBody:        protobufBody,
		},
	}

	codec := newTestPrometheusCodec()

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, "/something", nil)
			require.NoError(t, err)
			req.Header.Set("Accept", scenario.acceptHeader)

			encodedResponse, err := codec.EncodeResponse(context.Background(), req, testResponse)
			require.Equal(t, scenario.expectedError, err)

			if scenario.expectedError == nil {
				actualResponseContentType := encodedResponse.Header.Get("Content-Type")
				require.Equal(t, scenario.expectedResponseContentType, actualResponseContentType)

				actualResponseBody, err := io.ReadAll(encodedResponse.Body)
				require.NoError(t, err)
				require.Equal(t, scenario.expectedResponseBody, actualResponseBody)
			}
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

type prometheusResponseData struct {
	Type   model.ValueType `json:"resultType"`
	Result model.Value     `json:"result"`
}

func TestDecodeFailedResponse(t *testing.T) {
	codec := newTestPrometheusCodec()

	t.Run("internal error", func(t *testing.T) {
		_, err := codec.DecodeResponse(context.Background(), &http.Response{
			StatusCode: http.StatusInternalServerError,
			Body:       io.NopCloser(strings.NewReader("something failed")),
		}, nil, log.NewNopLogger())
		require.Error(t, err)

		resp, ok := httpgrpc.HTTPResponseFromError(err)
		require.True(t, ok, "Error should have an HTTPResponse encoded")
		require.Equal(t, int32(http.StatusInternalServerError), resp.Code)
	})

	t.Run("too many requests", func(t *testing.T) {
		_, err := codec.DecodeResponse(context.Background(), &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Body:       io.NopCloser(strings.NewReader("something failed")),
		}, nil, log.NewNopLogger())
		require.Error(t, err)

		require.True(t, apierror.IsAPIError(err))
		resp, ok := apierror.HTTPResponseFromError(err)
		require.True(t, ok, "Error should have an HTTPResponse encoded")
		require.Equal(t, int32(http.StatusTooManyRequests), resp.Code)
	})

	t.Run("too large entry", func(t *testing.T) {
		_, err := codec.DecodeResponse(context.Background(), &http.Response{
			StatusCode: http.StatusRequestEntityTooLarge,
			Body:       io.NopCloser(strings.NewReader("something failed")),
		}, nil, log.NewNopLogger())
		require.Error(t, err)

		require.True(t, apierror.IsAPIError(err))
		resp, ok := apierror.HTTPResponseFromError(err)
		require.True(t, ok, "Error should have an HTTPResponse encoded")
		require.Equal(t, int32(http.StatusRequestEntityTooLarge), resp.Code)
	})
}

func TestPrometheusCodec_DecodeResponse_ContentTypeHandling(t *testing.T) {
	for _, tc := range []struct {
		name            string
		responseHeaders http.Header
		expectedErr     error
	}{
		{
			name:            "unknown content type in response",
			responseHeaders: http.Header{"Content-Type": []string{"something/else"}},
			expectedErr:     apierror.New(apierror.TypeInternal, "unknown response content type 'something/else'"),
		},
		{
			name:            "no content type in response",
			responseHeaders: http.Header{},
			expectedErr:     apierror.New(apierror.TypeInternal, "unknown response content type ''"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			codec := NewPrometheusCodec(reg, formatJSON)

			resp := prometheusAPIResponse{}
			body, err := json.Marshal(resp)
			require.NoError(t, err)
			httpResponse := &http.Response{
				StatusCode:    200,
				Header:        tc.responseHeaders,
				Body:          io.NopCloser(bytes.NewBuffer(body)),
				ContentLength: int64(len(body)),
			}

			_, err = codec.DecodeResponse(context.Background(), httpResponse, nil, log.NewNopLogger())
			require.Equal(t, tc.expectedErr, err)
		})
	}
}

func TestMergeAPIResponses(t *testing.T) {
	codec := newTestPrometheusCodec()

	histogram1 := mimirpb.FloatHistogram{
		CounterResetHint: histogram.GaugeType,
		Schema:           3,
		ZeroThreshold:    1.23,
		ZeroCount:        456,
		Count:            9001,
		Sum:              789.1,
		PositiveSpans: []mimirpb.BucketSpan{
			{Offset: 4, Length: 1},
			{Offset: 3, Length: 2},
		},
		NegativeSpans: []mimirpb.BucketSpan{
			{Offset: 7, Length: 3},
			{Offset: 9, Length: 1},
		},
		PositiveBuckets: []float64{100, 200, 300},
		NegativeBuckets: []float64{400, 500, 600, 700},
	}

	histogram2 := mimirpb.FloatHistogram{
		CounterResetHint: histogram.GaugeType,
		Schema:           3,
		ZeroThreshold:    1.23,
		ZeroCount:        456,
		Count:            9001,
		Sum:              100789.1,
		PositiveSpans: []mimirpb.BucketSpan{
			{Offset: 4, Length: 1},
			{Offset: 3, Length: 2},
		},
		NegativeSpans: []mimirpb.BucketSpan{
			{Offset: 7, Length: 3},
			{Offset: 9, Length: 1},
		},
		PositiveBuckets: []float64{100, 200, 300},
		NegativeBuckets: []float64{400, 500, 600, 700},
	}

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
		},

		{
			name: "Handling single histogram result",
			input: []Response{
				&PrometheusResponse{
					Status: statusSuccess,
					Data: &PrometheusData{
						ResultType: matrix,
						Result: []SampleStream{
							{
								Labels: []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
								Histograms: []mimirpb.FloatHistogramPair{
									{TimestampMs: 1000, Histogram: &histogram1},
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
							Labels: []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
							Histograms: []mimirpb.FloatHistogramPair{
								{
									TimestampMs: 1000,
									Histogram:   &histogram1,
								},
							},
						},
					},
				},
			},
		},

		{
			name: "Handling non overlapping histogram result",
			input: []Response{
				&PrometheusResponse{
					Status: statusSuccess,
					Data: &PrometheusData{
						ResultType: matrix,
						Result: []SampleStream{
							{
								Labels: []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
								Histograms: []mimirpb.FloatHistogramPair{
									{TimestampMs: 1000, Histogram: &histogram1},
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
								Labels: []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
								Histograms: []mimirpb.FloatHistogramPair{
									{TimestampMs: 2000, Histogram: &histogram2},
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
							Labels: []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
							Histograms: []mimirpb.FloatHistogramPair{
								{TimestampMs: 1000, Histogram: &histogram1},
								{TimestampMs: 2000, Histogram: &histogram2},
							},
						},
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := codec.MergeResponse(tc.input...)
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

		_, err := codec.MergeResponse(successful, unsuccessful)
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
		_, err := codec.MergeResponse(successful, nilData)
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
		_, err := codec.MergeResponse(matrixResponse, vectorResponse)
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

	codec := newTestPrometheusCodec()

	// Generate a mocked response and marshal it.
	res := mockPrometheusResponse(numSeries, numSamplesPerSeries)
	encodedRes, err := json.Marshal(res)
	require.NoError(b, err)
	b.Log("test prometheus response size:", len(encodedRes))

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		_, err := codec.DecodeResponse(context.Background(), &http.Response{
			StatusCode:    200,
			Body:          io.NopCloser(bytes.NewReader(encodedRes)),
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

	codec := newTestPrometheusCodec()
	req, err := http.NewRequest(http.MethodGet, "/something", nil)
	require.NoError(b, err)

	// Generate a mocked response and marshal it.
	res := mockPrometheusResponse(numSeries, numSamplesPerSeries)

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		_, err := codec.EncodeResponse(context.Background(), req, res)
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
			ResultType: "matrix",
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

func mockPrometheusResponseWithSamplesAndHistograms(labels []mimirpb.LabelAdapter, samples []mimirpb.Sample, histograms []mimirpb.FloatHistogramPair) *PrometheusResponse {
	return &PrometheusResponse{
		Status: "success",
		Data: &PrometheusData{
			ResultType: "matrix",
			Result: []SampleStream{
				{
					Labels:     labels,
					Samples:    samples,
					Histograms: histograms,
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
		{
			name: "custom instant query splitting",
			input: &http.Request{
				Header: http.Header{
					instantSplitControlHeader: []string{"1h"},
				},
			},
			expected: &Options{
				InstantSplitInterval: time.Hour.Nanoseconds(),
			},
		},
		{
			name: "disable instant query splitting",
			input: &http.Request{
				Header: http.Header{
					instantSplitControlHeader: []string{"0"},
				},
			},
			expected: &Options{
				InstantSplitDisabled: true,
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

func newTestPrometheusCodec() Codec {
	return NewPrometheusCodec(prometheus.NewPedanticRegistry(), formatJSON)
}
