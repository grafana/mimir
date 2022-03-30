// SPDX-License-Identifier: AGPL-3.0-only

package remotequerier

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
)

type mockRoundTripper struct {
	fn func(ctx context.Context, req *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error)
}

func (r *mockRoundTripper) RoundTrip(ctx context.Context, req *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error) {
	return r.fn(ctx, req)
}

func TestQuerier_ReadPrometheusHTTPrefix(t *testing.T) {
	var inReq *httpgrpc.HTTPRequest
	mockTr := &mockRoundTripper{
		fn: func(ctx context.Context, req *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error) {
			inReq = req

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
		},
	}
	q := New(mockTr, "/prometheus", log.NewNopLogger())

	_, err := q.Read(context.Background(), &prompb.Query{})
	require.NoError(t, err)

	require.NotNil(t, inReq)
	require.Equal(t, "/prometheus/api/v1/read", inReq.Url)
}

func TestQuerier_QueryPrometheusHTTPrefix(t *testing.T) {
	var inReq *httpgrpc.HTTPRequest
	mockTr := &mockRoundTripper{
		fn: func(ctx context.Context, req *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error) {
			inReq = req
			return &httpgrpc.HTTPResponse{Code: http.StatusOK, Body: []byte(`{
							"status": "success","data": {"resultType":"vector","result":[]}
						}`)}, nil
		},
	}
	q := New(mockTr, "/prometheus", log.NewNopLogger())

	_, _, err := q.Query(context.Background(), "qs", time.Now())
	require.NoError(t, err)

	require.NotNil(t, inReq)
	require.Equal(t, "/prometheus/api/v1/query", inReq.Url)
}
