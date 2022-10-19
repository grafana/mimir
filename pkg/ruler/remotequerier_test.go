// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/status"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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
	q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), time.Minute, "/prometheus", log.NewNopLogger())

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
	q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), time.Second, "/prometheus", log.NewNopLogger())

	_, err := q.Read(context.Background(), &prompb.Query{})
	require.Error(t, err)
}

func TestRemoteQuerier_QueryReq(t *testing.T) {
	var inReq *httpgrpc.HTTPRequest
	mockClientFn := func(ctx context.Context, req *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
		inReq = req
		return &httpgrpc.HTTPResponse{Code: http.StatusOK, Body: []byte(`{
							"status": "success","data": {"resultType":"vector","result":[]}
						}`)}, nil
	}
	q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), time.Minute, "/prometheus", log.NewNopLogger())

	tm := time.Unix(1649092025, 515834)
	_, err := q.Query(context.Background(), "qs", tm)
	require.NoError(t, err)

	require.NotNil(t, inReq)
	require.Equal(t, http.MethodPost, inReq.Method)
	require.Equal(t, "query=qs&time="+url.QueryEscape(tm.Format(time.RFC3339Nano)), string(inReq.Body))
	require.Equal(t, "/prometheus/api/v1/query", inReq.Url)
}

func TestRemoteQuerier_QueryReqTimeout(t *testing.T) {
	mockClientFn := func(ctx context.Context, req *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), time.Second, "/prometheus", log.NewNopLogger())

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
				return &httpgrpc.HTTPResponse{Code: http.StatusOK, Body: []byte(`{
							"status": "success","data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"773054.5916666666"]}]}
						}`)}, nil
			}
			q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), time.Minute, "/prometheus", log.NewNopLogger())

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
		return &httpgrpc.HTTPResponse{Code: http.StatusUnprocessableEntity, Body: []byte(`{
							"status": "error","errorType": "execution"
						}`)}, nil
	}
	q := NewRemoteQuerier(mockHTTPGRPCClient(mockClientFn), time.Minute, "/prometheus", log.NewNopLogger())

	tm := time.Unix(1649092025, 515834)

	_, err := q.Query(context.Background(), "qs", tm)

	require.Error(t, err)

	st, ok := status.FromError(err)

	require.True(t, ok)
	require.Equal(t, codes.Code(http.StatusUnprocessableEntity), st.Code())
}
