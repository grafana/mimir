// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/status"
	"github.com/golang/snappy"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type mockHTTPGRPCClient func(ctx context.Context, req *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error)

func (c mockHTTPGRPCClient) Handle(ctx context.Context, req *httpgrpc.HTTPRequest, opts ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
	return c(ctx, req, opts...)
}

func TestRemoteQuerier_ReadReq(t *testing.T) {
	var inReq *httpgrpc.HTTPRequest
	var body []byte

	mockClientFn := func(ctx context.Context, req *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
		inReq = req

		b, err := proto.Marshal(&prompb.ReadResponse{
			Results: []*prompb.QueryResult{
				{},
			},
		})
		require.NoError(t, err)

		body = snappy.Encode(nil, b)
		return &httpgrpc.HTTPResponse{
			Code: http.StatusOK,
			Body: snappy.Encode(nil, b),
		}, nil
	}
	q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), time.Minute, formatJSON, "/prometheus", log.NewNopLogger())

	_, err := q.Read(context.Background(), &prompb.Query{})
	require.NoError(t, err)

	require.NotNil(t, inReq)
	require.Equal(t, http.MethodPost, inReq.Method)
	require.Equal(t, body, inReq.Body)
	require.Equal(t, "/prometheus/api/v1/read", inReq.Url)
}

func TestRemoteQuerier_ReadReqTimeout(t *testing.T) {
	mockClientFn := func(ctx context.Context, req *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), time.Second, formatJSON, "/prometheus", log.NewNopLogger())

	_, err := q.Read(context.Background(), &prompb.Query{})
	require.Error(t, err)
}

func TestRemoteQuerier_QueryReq(t *testing.T) {
	for _, format := range allFormats {
		t.Run(format, func(t *testing.T) {
			var inReq *httpgrpc.HTTPRequest
			mockClientFn := func(ctx context.Context, req *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
				inReq = req
				return &httpgrpc.HTTPResponse{
					Code: http.StatusOK,
					Headers: []*httpgrpc.Header{
						{Key: "Content-Type", Values: []string{"application/json"}},
					},
					Body: []byte(`{
						"status": "success","data": {"resultType":"vector","result":[]}
					}`),
				}, nil
			}
			q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), time.Minute, format, "/prometheus", log.NewNopLogger())

			tm := time.Unix(1649092025, 515834)
			_, err := q.Query(context.Background(), "qs", tm)
			require.NoError(t, err)

			require.NotNil(t, inReq)
			require.Equal(t, http.MethodPost, inReq.Method)
			require.Equal(t, "query=qs&time="+url.QueryEscape(tm.Format(time.RFC3339Nano)), string(inReq.Body))
			require.Equal(t, "/prometheus/api/v1/query", inReq.Url)

			acceptHeader := getHeader(inReq.Headers, "Accept")

			switch format {
			case formatJSON:
				require.Equal(t, "application/json", acceptHeader)
			case formatProtobuf:
				require.Equal(t, "application/vnd.mimir.queryresponse+protobuf,application/json", acceptHeader)
			default:
				t.Fatalf("unknown format '%s'", format)
			}
		})
	}

}

func TestRemoteQuerier_QueryJSONDecoding(t *testing.T) {
	scenarios := map[string]struct {
		body          string
		expected      promql.Vector
		expectedError error
	}{
		"vector response with no series": {
			body: `{
					"status": "success",
					"data": {"resultType":"vector","result":[]}
				}`,
			expected: promql.Vector{},
		},
		"vector response with one series": {
			body: `{
					"status": "success",
					"data": {
						"resultType": "vector",
						"result": [
							{
								"metric": {"foo":"bar"},
								"value": [1649092025.515,"1.23"]
							}
						]
					}
				}`,
			expected: promql.Vector{
				{
					Metric: labels.FromStrings("foo", "bar"),
					T:      1649092025515,
					F:      1.23,
				},
			},
		},
		"vector response with many series": {
			body: `{
					"status": "success",
					"data": {
						"resultType": "vector",
						"result": [
							{
								"metric": {"foo":"bar"},
								"value": [1649092025.515,"1.23"]
							},
							{
								"metric": {"bar":"baz"},
								"value": [1649092025.515,"4.56"]
							}
						]
					}
				}`,
			expected: promql.Vector{
				{
					Metric: labels.FromStrings("foo", "bar"),
					T:      1649092025515,
					F:      1.23,
				},
				{
					Metric: labels.FromStrings("bar", "baz"),
					T:      1649092025515,
					F:      4.56,
				},
			},
		},
		"scalar response": {
			body: `{
					"status": "success",
					"data": {"resultType":"scalar","result":[1649092025.515,"1.23"]}
				}`,
			expected: promql.Vector{
				{
					Metric: labels.EmptyLabels(),
					T:      1649092025515,
					F:      1.23,
				},
			},
		},
		"matrix response": {
			body: `{
					"status": "success",
					"data": {"resultType":"matrix","result":[]}
				}`,
			expectedError: errors.New("rule result is not a vector or scalar: \"matrix\""),
		},
		"execution error": {
			body: `{
					"status": "error",
					"errorType": "errorExec",
					"error": "something went wrong"
				}`,
			expectedError: errors.New("query execution failed with error: something went wrong"),
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			mockClientFn := func(ctx context.Context, req *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
				return &httpgrpc.HTTPResponse{
					Code: http.StatusOK,
					Headers: []*httpgrpc.Header{
						{Key: "Content-Type", Values: []string{"application/json"}},
					},
					Body: []byte(scenario.body),
				}, nil
			}
			q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), time.Minute, formatJSON, "/prometheus", log.NewNopLogger())

			tm := time.Unix(1649092025, 515834)
			actual, err := q.Query(context.Background(), "qs", tm)
			require.Equal(t, scenario.expectedError, err)

			if scenario.expectedError == nil {
				require.Equal(t, scenario.expected, actual)
			}
		})
	}
}

func TestRemoteQuerier_QueryProtobufDecoding(t *testing.T) {
	protobufHistogram := mimirpb.FloatHistogram{
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

	promqlHistogram := histogram.FloatHistogram{
		CounterResetHint: histogram.GaugeType,
		Schema:           3,
		ZeroThreshold:    1.23,
		ZeroCount:        456,
		Count:            9001,
		Sum:              789.1,
		PositiveSpans: []histogram.Span{
			{Offset: 4, Length: 1},
			{Offset: 3, Length: 2},
		},
		NegativeSpans: []histogram.Span{
			{Offset: 7, Length: 3},
			{Offset: 9, Length: 1},
		},
		PositiveBuckets: []float64{100, 200, 300},
		NegativeBuckets: []float64{400, 500, 600, 700},
	}

	scenarios := map[string]struct {
		body          mimirpb.QueryResponse
		expected      promql.Vector
		expectedError error
	}{
		"vector response with no series": {
			body: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Vector{
					Vector: &mimirpb.VectorData{},
				},
			},
			expected: promql.Vector{},
		},
		"vector response with one series": {
			body: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Vector{
					Vector: &mimirpb.VectorData{
						Samples: []mimirpb.VectorSample{
							{
								Metric:      []string{"foo", "bar"},
								TimestampMs: 1649092025515,
								Value:       1.23,
							},
						},
					},
				},
			},
			expected: promql.Vector{
				{
					Metric: labels.FromStrings("foo", "bar"),
					T:      1649092025515,
					F:      1.23,
				},
			},
		},
		"vector response with many series": {
			body: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Vector{
					Vector: &mimirpb.VectorData{
						Samples: []mimirpb.VectorSample{
							{
								Metric:      []string{"foo", "bar"},
								TimestampMs: 1649092025515,
								Value:       1.23,
							},
							{
								Metric:      []string{"bar", "baz"},
								TimestampMs: 1649092025515,
								Value:       4.56,
							},
						},
					},
				},
			},
			expected: promql.Vector{
				{
					Metric: labels.FromStrings("foo", "bar"),
					T:      1649092025515,
					F:      1.23,
				},
				{
					Metric: labels.FromStrings("bar", "baz"),
					T:      1649092025515,
					F:      4.56,
				},
			},
		},
		"vector response with many labels": {
			body: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Vector{
					Vector: &mimirpb.VectorData{
						Samples: []mimirpb.VectorSample{
							{
								Metric:      []string{"bar", "baz", "foo", "blah"},
								TimestampMs: 1649092025515,
								Value:       1.23,
							},
						},
					},
				},
			},
			expected: promql.Vector{
				{
					Metric: labels.FromStrings("bar", "baz", "foo", "blah"),
					T:      1649092025515,
					F:      1.23,
				},
			},
		},
		"vector response with histogram value": {
			body: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Vector{
					Vector: &mimirpb.VectorData{
						Histograms: []mimirpb.VectorHistogram{
							{
								Metric:      []string{"foo", "baz"},
								TimestampMs: 1649092025515,
								Histogram:   protobufHistogram,
							},
						},
					},
				},
			},
			expected: promql.Vector{
				{
					Metric: labels.FromStrings("foo", "baz"),
					T:      1649092025515,
					H:      &promqlHistogram,
				},
			},
		},
		"vector response with float and histogram values": {
			body: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Vector{
					Vector: &mimirpb.VectorData{
						Samples: []mimirpb.VectorSample{
							{
								Metric:      []string{"foo", "baz"},
								TimestampMs: 1649092025515,
								Value:       1.23,
							},
						},
						Histograms: []mimirpb.VectorHistogram{
							{
								Metric:      []string{"foo", "bar"},
								TimestampMs: 1649092025515,
								Histogram:   protobufHistogram,
							},
						},
					},
				},
			},
			expected: promql.Vector{
				{
					Metric: labels.FromStrings("foo", "baz"),
					T:      1649092025515,
					F:      1.23,
				},
				{
					Metric: labels.FromStrings("foo", "bar"),
					T:      1649092025515,
					H:      &promqlHistogram,
				},
			},
		},
		"vector response with malformed metric": {
			body: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Vector{
					Vector: &mimirpb.VectorData{
						Samples: []mimirpb.VectorSample{
							{
								Metric:      []string{"foo", "bar", "baz"},
								TimestampMs: 1649092025515,
								Value:       1.23,
							},
						},
					},
				},
			},
			expectedError: errors.New("metric is malformed, it contains an odd number of symbols: 3"),
		},
		"scalar response": {
			body: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Scalar{
					Scalar: &mimirpb.ScalarData{
						TimestampMs: 1649092025515,
						Value:       1.23,
					},
				},
			},
			expected: promql.Vector{
				{
					Metric: labels.EmptyLabels(),
					T:      1649092025515,
					F:      1.23,
				},
			},
		},
		"matrix response": {
			body: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Matrix{
					Matrix: &mimirpb.MatrixData{},
				},
			},
			expectedError: errors.New("rule result is not a vector or scalar: \"matrix\""),
		},
		"execution error": {
			body: mimirpb.QueryResponse{
				Status:    mimirpb.QueryResponse_ERROR,
				ErrorType: mimirpb.QueryResponse_EXECUTION,
				Error:     "something went wrong",
			},
			expectedError: errors.New("query execution failed with error: something went wrong"),
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			mockClientFn := func(ctx context.Context, req *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
				b, err := scenario.body.Marshal()
				if err != nil {
					return nil, err
				}

				return &httpgrpc.HTTPResponse{
					Code: http.StatusOK,
					Headers: []*httpgrpc.Header{
						{Key: "Content-Type", Values: []string{mimirpb.QueryResponseMimeType}},
					},
					Body: b,
				}, nil
			}
			q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), time.Minute, formatProtobuf, "/prometheus", log.NewNopLogger())

			tm := time.Unix(1649092025, 515834)
			actual, err := q.Query(context.Background(), "qs", tm)
			require.Equal(t, scenario.expectedError, err)

			if scenario.expectedError == nil {
				require.Equal(t, scenario.expected, actual)
			}
		})
	}
}

func TestRemoteQuerier_QueryUnknownResponseContentType(t *testing.T) {
	mockClientFn := func(ctx context.Context, req *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
		return &httpgrpc.HTTPResponse{
			Code: http.StatusOK,
			Headers: []*httpgrpc.Header{
				{Key: "Content-Type", Values: []string{"something/unknown"}},
			},
			Body: []byte("some body content"),
		}, nil
	}
	q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), time.Minute, formatJSON, "/prometheus", log.NewNopLogger())

	tm := time.Unix(1649092025, 515834)
	_, err := q.Query(context.Background(), "qs", tm)
	require.EqualError(t, err, "unknown response content type 'something/unknown'")
}

func TestRemoteQuerier_QueryReqTimeout(t *testing.T) {
	mockClientFn := func(ctx context.Context, req *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), time.Second, formatJSON, "/prometheus", log.NewNopLogger())

	tm := time.Unix(1649092025, 515834)
	_, err := q.Query(context.Background(), "qs", tm)
	require.Error(t, err)
}

func TestRemoteQuerier_BackoffRetry(t *testing.T) {
	tcs := map[string]struct {
		failedRequests  int
		expectedError   string
		requestDeadline time.Duration
	}{
		"succeed on failed requests <= max retries": {
			failedRequests: maxRequestRetries,
		},
		"fail on failed requests > max retries": {
			failedRequests: maxRequestRetries + 1,
			expectedError:  "failed request: 4",
		},
		"return last known error on context cancellation": {
			failedRequests:  1,
			requestDeadline: 50 * time.Millisecond, // force context cancellation while waiting for retry
			expectedError:   "context deadline exceeded while retrying request, last err was: failed request: 1",
		},
	}
	for tn, tc := range tcs {
		t.Run(tn, func(t *testing.T) {
			retries := 0
			mockClientFn := func(ctx context.Context, req *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
				retries++
				if retries <= tc.failedRequests {
					return nil, fmt.Errorf("failed request: %d", retries)
				}
				return &httpgrpc.HTTPResponse{
					Code: http.StatusOK,
					Headers: []*httpgrpc.Header{
						{Key: "Content-Type", Values: []string{"application/json"}},
					},
					Body: []byte(`{
						"status": "success","data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"773054.5916666666"]}]}
					}`),
				}, nil
			}
			q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), time.Minute, formatJSON, "/prometheus", log.NewNopLogger())

			ctx := context.Background()
			if tc.requestDeadline > 0 {
				var cancelFn context.CancelFunc
				ctx, cancelFn = context.WithTimeout(ctx, tc.requestDeadline)
				defer cancelFn()
			}

			resp, err := q.Query(ctx, "qs", time.Now())
			if tc.expectedError != "" {
				require.EqualError(t, err, tc.expectedError)
			} else {
				require.NotNil(t, resp)
				require.Len(t, resp, 1)
			}
		})
	}
}

func TestRemoteQuerier_StatusErrorResponses(t *testing.T) {
	mockClientFn := func(ctx context.Context, req *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
		return &httpgrpc.HTTPResponse{
			Code: http.StatusUnprocessableEntity,
			Headers: []*httpgrpc.Header{
				{Key: "Content-Type", Values: []string{"application/json"}},
			},
			Body: []byte(`{
				"status": "error","errorType": "execution"
			}`),
		}, nil
	}
	q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), time.Minute, formatJSON, "/prometheus", log.NewNopLogger())

	tm := time.Unix(1649092025, 515834)

	_, err := q.Query(context.Background(), "qs", tm)

	require.Error(t, err)

	st, ok := status.FromError(err)

	require.True(t, ok)
	require.Equal(t, codes.Code(http.StatusUnprocessableEntity), st.Code())
}
