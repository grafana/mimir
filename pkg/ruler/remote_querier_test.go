// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"net/http"
	"net/textproto"
	"reflect"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/tenant"
)

type mockRoundTripper struct {
	respContent string
	reqHeaders  []*httpgrpc.Header
}

func (r *mockRoundTripper) RoundTrip(_ context.Context, req *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error) {
	r.reqHeaders = req.Headers
	return &httpgrpc.HTTPResponse{Code: http.StatusOK, Body: []byte(r.respContent)}, nil
}

func TestRemoteQuerier_OrgHeaders(t *testing.T) {
	testCases := map[string]struct {
		contextFn       func() context.Context
		expectedHeaders []*httpgrpc.Header
	}{
		"org ID header is included for non federated rule context": {
			contextFn: func() context.Context {
				return user.InjectOrgID(context.Background(), "tenant-1")
			},
			expectedHeaders: []*httpgrpc.Header{
				{Key: textproto.CanonicalMIMEHeaderKey(user.OrgIDHeaderName), Values: []string{"tenant-1"}},
			},
		},
		"multitenant org ID header is included for federated rule context": {
			contextFn: func() context.Context {
				ctx := context.Background()
				return context.WithValue(ctx, federatedGroupSourceTenants, tenant.NormalizeTenantIDs([]string{"tenant-3", "tenant-9", "tenant-1"}))
			},
			expectedHeaders: []*httpgrpc.Header{
				{Key: textproto.CanonicalMIMEHeaderKey(user.OrgIDHeaderName), Values: []string{"tenant-1|tenant-3|tenant-9"}},
			},
		},
	}
	for tName, tData := range testCases {
		t.Run(tName, func(t *testing.T) {
			rt := &mockRoundTripper{
				respContent: `{"status": "success", "data": {"resultType":"vector","result":[]}}`,
			}

			rq := NewRemoteQuerier(NewOrgRoundTripper(rt), "/prometheus", log.NewNopLogger())

			_, err := rq.Query(tData.contextFn(), "up", time.Now())
			require.NoError(t, err)

			for _, hd := range tData.expectedHeaders {
				var isPresent bool
				for _, reqHeader := range rt.reqHeaders {
					if reqHeader.Key != hd.Key || !reflect.DeepEqual(reqHeader.Values, hd.Values) {
						continue
					}
					isPresent = true
					break
				}
				if !isPresent {
					t.Fatalf("mismatching value or missing %s request header", hd.Key)
				}
			}
		})
	}
}

func TestRemoteQuerier_Response(t *testing.T) {
	testCases := map[string]struct {
		respContent       string
		expectedPromQLVec promql.Vector
		expectedError     string
	}{
		"vector result type": {
			respContent: `{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"1"]}]}
						}`,
			expectedPromQLVec: promql.Vector{
				promql.Sample{
					Point:  promql.Point{T: 1000, V: 1},
					Metric: labels.Labels{labels.Label{Name: "foo", Value: "bar"}},
				},
			},
		},
		"scalar result type": {
			respContent: `{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]}
						}`,
			expectedPromQLVec: promql.Vector{
				promql.Sample{
					Point:  promql.Point{T: 1000, V: 1},
					Metric: labels.Labels{},
				},
			},
		},
		"unrecognized result type": {
			respContent: `{
							  "status": "success", 
							  "data": {"resultType":"foo_type","result":""}
						}`,
			expectedError: `unknown value type "foo_type"`,
		},
		"response status error": {
			respContent: `{
				"status": "error",
				"errorType": "crazy_one",
				"error": "foo error"
			}`,
			expectedError: `query response error: foo error`,
		},
	}
	for tName, tData := range testCases {
		t.Run(tName, func(t *testing.T) {
			rt := &mockRoundTripper{respContent: tData.respContent}

			rq := NewRemoteQuerier(NewOrgRoundTripper(rt), "/prometheus", log.NewNopLogger())

			resp, err := rq.Query(user.InjectOrgID(context.Background(), "tenant-1"), "up", time.Now())

			if len(tData.expectedError) > 0 {
				require.Error(t, err)
				require.Equal(t, tData.expectedError, err.Error())
			} else {
				require.Equal(t, tData.expectedPromQLVec, resp)
			}
		})
	}
}
