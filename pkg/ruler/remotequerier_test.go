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
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/api"
)

var retryConfig = backoff.Config{
	MinBackoff: 100 * time.Millisecond,
	MaxBackoff: 1 * time.Second,
	MaxRetries: 3,
}

type mockHTTPGRPCClient func(ctx context.Context, req *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error)

func (c mockHTTPGRPCClient) Handle(ctx context.Context, req *httpgrpc.HTTPRequest, opts ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
	return c(ctx, req, opts...)
}

func TestRemoteQuerier_Read(t *testing.T) {
	// setup returns a mocked HTTPgRPC client and a pointer to the received HTTPRequest
	// that will be valued after the request is received.
	setup := func() (mockHTTPGRPCClient, *httpgrpc.HTTPRequest) {
		var inReq httpgrpc.HTTPRequest

		mockClientFn := func(_ context.Context, req *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
			inReq = *req

			b, err := proto.Marshal(&prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{},
				},
			})
			require.NoError(t, err)

			return &httpgrpc.HTTPResponse{
				Code: http.StatusOK,
				Body: snappy.Encode(nil, b),
			}, nil
		}

		return mockClientFn, &inReq
	}

	t.Run("should issue a remote read request", func(t *testing.T) {
		client, inReq := setup()

		q := NewRemoteQuerier(client, retryConfig, time.Minute, formatJSON, "/prometheus", log.NewNopLogger())
		_, err := q.Read(context.Background(), &prompb.Query{}, false)
		require.NoError(t, err)

		require.NotNil(t, inReq)
		require.Equal(t, http.MethodPost, inReq.Method)
		require.Equal(t, "/prometheus/api/v1/read", inReq.Url)
	})

	t.Run("should not inject the read consistency header if none is defined in the context", func(t *testing.T) {
		client, inReq := setup()

		q := NewRemoteQuerier(client, retryConfig, time.Minute, formatJSON, "/prometheus", log.NewNopLogger())
		_, err := q.Read(context.Background(), &prompb.Query{}, false)
		require.NoError(t, err)

		require.Equal(t, "", getHeader(inReq.Headers, api.ReadConsistencyHeader))
	})

	t.Run("should inject the read consistency header if it is defined in the context", func(t *testing.T) {
		client, inReq := setup()

		q := NewRemoteQuerier(client, retryConfig, time.Minute, formatJSON, "/prometheus", log.NewNopLogger())

		ctx := api.ContextWithReadConsistencyLevel(context.Background(), api.ReadConsistencyStrong)
		_, err := q.Read(ctx, &prompb.Query{}, false)
		require.NoError(t, err)

		require.Equal(t, api.ReadConsistencyStrong, getHeader(inReq.Headers, api.ReadConsistencyHeader))
	})
}

func TestRemoteQuerier_ReadReqTimeout(t *testing.T) {
	mockClientFn := func(ctx context.Context, _ *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), retryConfig, time.Second, formatJSON, "/prometheus", log.NewNopLogger())

	_, err := q.Read(context.Background(), &prompb.Query{}, false)
	require.Error(t, err)
}

func TestRemoteQuerier_Query(t *testing.T) {
	var (
		tm = time.Unix(1649092025, 515834)
	)

	// setup returns a mocked HTTPgRPC client and a pointer to the received HTTPRequest
	// that will be valued after the request is received.
	setup := func() (mockHTTPGRPCClient, *httpgrpc.HTTPRequest) {
		var inReq httpgrpc.HTTPRequest

		mockClientFn := func(_ context.Context, req *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
			inReq = *req

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

		return mockClientFn, &inReq
	}

	t.Run("should issue a instant query request with the configured format", func(t *testing.T) {
		for _, format := range allFormats {
			t.Run(fmt.Sprintf("format = %s", format), func(t *testing.T) {
				client, inReq := setup()

				q := NewRemoteQuerier(client, retryConfig, time.Minute, format, "/prometheus", log.NewNopLogger())
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
	})

	t.Run("should not inject the read consistency header if none is defined in the context", func(t *testing.T) {
		client, inReq := setup()

		q := NewRemoteQuerier(client, retryConfig, time.Minute, formatJSON, "/prometheus", log.NewNopLogger())
		_, err := q.Query(context.Background(), "qs", tm)
		require.NoError(t, err)

		require.Equal(t, "", getHeader(inReq.Headers, api.ReadConsistencyHeader))
	})

	t.Run("should inject the read consistency header if it is defined in the context", func(t *testing.T) {
		client, inReq := setup()

		q := NewRemoteQuerier(client, retryConfig, time.Minute, formatJSON, "/prometheus", log.NewNopLogger())

		ctx := api.ContextWithReadConsistencyLevel(context.Background(), api.ReadConsistencyStrong)
		_, err := q.Query(ctx, "qs", tm)
		require.NoError(t, err)

		require.Equal(t, api.ReadConsistencyStrong, getHeader(inReq.Headers, api.ReadConsistencyHeader))
	})
}

func TestRemoteQuerier_QueryRetryOnFailure(t *testing.T) {
	const errMsg = "this is an error"
	successfulResponse := &httpgrpc.HTTPResponse{
		Code: http.StatusOK,
		Headers: []*httpgrpc.Header{
			{Key: "Content-Type", Values: []string{"application/json"}},
		},
		Body: []byte(`{"status": "success","data": {"resultType":"vector","result":[]}}`),
	}
	erroneousResponse := &httpgrpc.HTTPResponse{
		Code: http.StatusBadRequest,
		Headers: []*httpgrpc.Header{
			{Key: "Content-Type", Values: []string{"application/json"}},
		},
		Body: []byte("this is an error"),
	}

	tests := map[string]struct {
		response        *httpgrpc.HTTPResponse
		err             error
		expectedError   error
		expectedRetries bool
	}{
		"errors with code 5xx are retried": {
			err:             httpgrpc.Errorf(http.StatusInternalServerError, errMsg),
			expectedError:   httpgrpc.Errorf(http.StatusInternalServerError, errMsg),
			expectedRetries: true,
		},
		"context.Canceled error is not retried": {
			err:             context.Canceled,
			expectedError:   context.Canceled,
			expectedRetries: false,
		},
		"gRPC context.Canceled error is not retried": {
			err:             status.Error(codes.Canceled, context.Canceled.Error()),
			expectedError:   status.Error(codes.Canceled, context.Canceled.Error()),
			expectedRetries: false,
		},
		"context.DeadlineExceeded error is retried": {
			err:             context.DeadlineExceeded,
			expectedError:   context.DeadlineExceeded,
			expectedRetries: true,
		},
		"gRPC context.DeadlineExceeded error is retried": {
			err:             status.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error()),
			expectedError:   status.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error()),
			expectedRetries: true,
		},
		"gRPC ResourceExhausted error is retried": {
			err:             status.Error(codes.ResourceExhausted, errMsg),
			expectedError:   status.Error(codes.ResourceExhausted, errMsg),
			expectedRetries: true,
		},
		"errors about execeeding gRPC server's limit are not retried": {
			err:             status.Error(codes.ResourceExhausted, "trying to send message larger than max"),
			expectedError:   status.Error(codes.ResourceExhausted, "trying to send message larger than max"),
			expectedRetries: false,
		},
		"errors with code 4xx are not retried": {
			err:             httpgrpc.Errorf(http.StatusBadRequest, errMsg),
			expectedError:   httpgrpc.Errorf(http.StatusBadRequest, errMsg),
			expectedRetries: false,
		},
		"responses with status code 4xx are not retried and are converted to errors": {
			err:             nil,
			response:        erroneousResponse,
			expectedError:   httpgrpc.ErrorFromHTTPResponse(erroneousResponse),
			expectedRetries: false,
		},
		"responses with status code 2xx are not retried": {
			err:             nil,
			response:        successfulResponse,
			expectedError:   nil,
			expectedRetries: false,
		},
	}
	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			var count atomic.Int64

			ctx, cancel := context.WithCancel(context.Background())
			mockClientFn := func(context.Context, *httpgrpc.HTTPRequest, ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
				count.Add(1)
				if testCase.err != nil {
					if grpcutil.IsCanceled(testCase.err) {
						cancel()
					}
					return nil, testCase.err
				}
				return testCase.response, nil
			}
			q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), retryConfig, time.Minute, formatJSON, "/prometheus", log.NewNopLogger())
			require.Equal(t, int64(0), count.Load())
			_, err := q.Query(ctx, "qs", time.Now())
			if testCase.err == nil {
				if testCase.expectedError == nil {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					require.EqualError(t, err, testCase.expectedError.Error())
				}
				require.Equal(t, int64(1), count.Load())
			} else {
				require.Error(t, err)
				require.EqualError(t, err, testCase.expectedError.Error())
				if testCase.expectedRetries {
					require.Greater(t, count.Load(), int64(1))
				} else {
					require.Equal(t, int64(1), count.Load())
				}
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
			mockClientFn := func(context.Context, *httpgrpc.HTTPRequest, ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
				return &httpgrpc.HTTPResponse{
					Code: http.StatusOK,
					Headers: []*httpgrpc.Header{
						{Key: "Content-Type", Values: []string{"application/json"}},
					},
					Body: []byte(scenario.body),
				}, nil
			}
			q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), retryConfig, time.Minute, formatJSON, "/prometheus", log.NewNopLogger())

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
			mockClientFn := func(context.Context, *httpgrpc.HTTPRequest, ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
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
			q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), retryConfig, time.Minute, formatProtobuf, "/prometheus", log.NewNopLogger())

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
	mockClientFn := func(context.Context, *httpgrpc.HTTPRequest, ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
		return &httpgrpc.HTTPResponse{
			Code: http.StatusOK,
			Headers: []*httpgrpc.Header{
				{Key: "Content-Type", Values: []string{"something/unknown"}},
			},
			Body: []byte("some body content"),
		}, nil
	}
	q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), retryConfig, time.Minute, formatJSON, "/prometheus", log.NewNopLogger())

	tm := time.Unix(1649092025, 515834)
	_, err := q.Query(context.Background(), "qs", tm)
	require.EqualError(t, err, "unknown response content type 'something/unknown'")
}

func TestRemoteQuerier_QueryReqTimeout(t *testing.T) {
	mockClientFn := func(ctx context.Context, _ *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), retryConfig, time.Second, formatJSON, "/prometheus", log.NewNopLogger())

	tm := time.Unix(1649092025, 515834)
	_, err := q.Query(context.Background(), "qs", tm)
	require.Error(t, err)
}

func TestRemoteQuerier_StatusErrorResponses(t *testing.T) {
	var (
		errorResp = &httpgrpc.HTTPResponse{
			Code: http.StatusUnprocessableEntity,
			Headers: []*httpgrpc.Header{
				{Key: "Content-Type", Values: []string{"application/json"}},
			},
			Body: []byte(`{
				"status": "error","errorType": "execution"
			}`),
		}
		error4xx = httpgrpc.Errorf(http.StatusUnprocessableEntity, "this is a 4xx error")
		error5xx = httpgrpc.Errorf(http.StatusInternalServerError, "this is a 5xx error")
	)
	testCases := map[string]struct {
		resp         *httpgrpc.HTTPResponse
		err          error
		expectedCode int32
		expectedLogs bool
	}{
		"HTTPResponse with code 4xx is translated to an error with the same code and not logged": {
			resp:         errorResp,
			err:          nil,
			expectedCode: http.StatusUnprocessableEntity,
			expectedLogs: false,
		},
		"error with code 4xx is returned but not logged": {
			resp:         nil,
			err:          error4xx,
			expectedCode: http.StatusUnprocessableEntity,
			expectedLogs: false,
		},
		"error with code 5xx is returned and logged": {
			resp:         nil,
			err:          error5xx,
			expectedCode: http.StatusInternalServerError,
			expectedLogs: true,
		},
		"an error without status code is returned and logged": {
			resp:         nil,
			err:          errors.New("this is an error"),
			expectedCode: int32(codes.Unknown),
			expectedLogs: true,
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			mockClientFn := func(context.Context, *httpgrpc.HTTPRequest, ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
				return testCase.resp, testCase.err
			}
			logger := newLoggerWithCounter()
			q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), retryConfig, time.Minute, formatJSON, "/prometheus", logger)

			tm := time.Unix(1649092025, 515834)

			require.Equal(t, int64(0), logger.count())
			_, err := q.Query(context.Background(), "qs", tm)

			require.Error(t, err)
			code := grpcutil.ErrorToStatusCode(err)
			require.Equal(t, codes.Code(testCase.expectedCode), code)
			if testCase.expectedLogs {
				require.Greater(t, logger.count(), int64(0))
			} else {
				require.Equal(t, int64(0), logger.count())
			}
		})
	}
}

type loggerWithCounter struct {
	logger  log.Logger
	counter atomic.Int64
}

func newLoggerWithCounter() *loggerWithCounter {
	return &loggerWithCounter{
		logger: log.NewNopLogger(),
	}
}

func (l *loggerWithCounter) Log(keyvals ...interface{}) error {
	l.counter.Inc()
	return l.logger.Log(keyvals...)
}

func (l *loggerWithCounter) count() int64 {
	return l.counter.Load()
}
