// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/status"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/querier/querypb"
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

func BenchmarkRemoteQuerier_Decode(b *testing.B) {
	sourceDir := "/Users/charleskorn/Desktop/queries/original-format"
	groupDirs, err := os.ReadDir(sourceDir)
	require.NoError(b, err)
	require.NotEmpty(b, groupDirs)

	for _, groupDir := range groupDirs {
		if !groupDir.IsDir() {
			continue
		}

		filePattern := filepath.Join(sourceDir, groupDir.Name(), "*.json")
		files, err := filepath.Glob(filePattern)
		require.NoError(b, err)
		require.NotEmpty(b, files)

		bodies := make([][]byte, 0, len(files))

		for _, file := range files {
			body, err := os.ReadFile(file)
			require.NoError(b, err)
			bodies = append(bodies, body)
		}

		b.Run(groupDir.Name(), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				body := bodies[i%len(bodies)]
				_, err := originalDecode(body)

				if err != nil {
					require.NoError(b, err)
				}
			}
		})
	}
}

func TestDecoders(t *testing.T) {
	rootDir := "/Users/charleskorn/Desktop/queries"
	originalFormatDir := path.Join(rootDir, "original-format")
	protoFormatDir := path.Join(rootDir, "interned-protobuf")

	originalFileNames, err := filepath.Glob(path.Join(originalFormatDir, "**", "*.json"))
	require.NoError(t, err)

	for _, originalFileName := range originalFileNames {
		relativeName, err := filepath.Rel(originalFormatDir, originalFileName)
		require.NoError(t, err)

		t.Run(relativeName, func(t *testing.T) {
			originalBytes, err := os.ReadFile(originalFileName)
			require.NoError(t, err)

			expected, err := originalDecode(originalBytes)
			require.NoError(t, err)

			t.Run("interned-protobuf", func(t *testing.T) {
				protoBytes, err := os.ReadFile(path.Join(protoFormatDir, relativeName+".pb"))
				require.NoError(t, err)

				actualProto, err := protoDecode(protoBytes)
				require.NoError(t, err)
				requireEqual(t, expected, actualProto)
			})
		})
	}
}

func requireEqual(t *testing.T, expected promql.Vector, actual promql.Vector) {
	require.Len(t, actual, len(expected))

	for i, actualSample := range actual {
		expectedSample := expected[i]

		if math.IsNaN(expectedSample.Point.V) && math.IsNaN(actualSample.Point.V) {
			// NaN != NaN, so we can't assert that the two points are the same. Instead, check the other elements of the point.
			require.Equal(t, expectedSample.Point.H, actualSample.Point.H)
			require.Equal(t, expectedSample.Point.T, actualSample.Point.T)
		} else {
			require.Equal(t, expectedSample.Point, actualSample.Point)
		}

		require.ElementsMatch(t, expectedSample.Metric, actualSample.Metric)
	}
}

func originalDecode(body []byte) (promql.Vector, error) {
	var resp struct {
		Status    string          `json:"status"`
		Data      json.RawMessage `json:"data"`
		ErrorType string          `json:"errorType"`
		Error     string          `json:"error"`
	}

	if err := json.NewDecoder(bytes.NewReader(body)).Decode(&resp); err != nil {
		return nil, err
	}

	if resp.Status == statusError {
		return nil, fmt.Errorf("query response error: %s", resp.Error)
	}

	v := struct {
		Type   model.ValueType `json:"resultType"`
		Result json.RawMessage `json:"result"`
	}{}

	if err := json.Unmarshal(resp.Data, &v); err != nil {
		return nil, err
	}

	return decodeQueryResponse(v.Type, v.Result)
}

func protoDecode(body []byte) (promql.Vector, error) {
	resp := querypb.QueryResponse{}

	if err := resp.Unmarshal(body); err != nil {
		return nil, err
	}

	if resp.Status == statusError {
		return nil, fmt.Errorf("query response error: %s", resp.Error)
	}

	switch t := resp.Data.(type) {
	case *querypb.QueryResponse_Scalar:
		return protoDecodeScalar(t.Scalar), nil

	case *querypb.QueryResponse_Vector:
		return protoDecodeVector(t.Vector), nil

	default:
		panic(fmt.Sprintf("unknown data type: %v", t))
	}
}

func protoDecodeScalar(s *querypb.ScalarData) promql.Vector {
	return promql.Vector{
		promql.Sample{
			Point: promql.Point{
				V: s.Value,
				T: s.Timestamp,
			},
			Metric: labels.EmptyLabels(),
		},
	}
}

func protoDecodeVector(v *querypb.VectorData) promql.Vector {
	vec := make(promql.Vector, len(v.Samples))
	symbols := v.Symbols

	for i, s := range v.Samples {
		labelCount := len(s.MetricSymbols) / 2
		metric := make(labels.Labels, labelCount)

		for i := 0; i < labelCount; i++ {
			name := symbols[s.MetricSymbols[2*i]]
			value := symbols[s.MetricSymbols[2*i+1]]
			metric[i] = labels.Label{Name: name, Value: value}
		}

		vec[i] = promql.Sample{
			Point: promql.Point{
				V: s.Value,
				T: s.Timestamp,
			},
			Metric: metric,
		}
	}

	return vec
}
