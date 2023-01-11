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
	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/querier/internedquerypb"
	"github.com/grafana/mimir/pkg/querier/uninternedquerypb"
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
				for _, body := range bodies {
					_, err := originalDecode(body)

					if err != nil {
						require.NoError(b, err)
					}
				}
			}
		})
	}
}

func TestDecoders(t *testing.T) {
	rootDir := "/Users/charleskorn/Desktop/queries"
	originalFormatDir := path.Join(rootDir, "original-format")
	uninternedProtoFormatDir := path.Join(rootDir, "uninterned-protobuf")
	internedProtoFormatDir := path.Join(rootDir, "interned-protobuf")

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

			t.Run("uninterned-protobuf", func(t *testing.T) {
				protoBytes, err := os.ReadFile(path.Join(uninternedProtoFormatDir, relativeName+".pb"))
				require.NoError(t, err)

				actualProto, err := uninternedProtoDecode(protoBytes)
				require.NoError(t, err)
				requireEqual(t, expected, actualProto)
			})

			t.Run("interned-protobuf", func(t *testing.T) {
				protoBytes, err := os.ReadFile(path.Join(internedProtoFormatDir, relativeName+".pb"))
				require.NoError(t, err)

				actualProto, err := internedProtoDecode(protoBytes)
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

func uninternedProtoDecode(body []byte) (promql.Vector, error) {
	resp := uninternedquerypb.QueryResponse{}

	if err := resp.Unmarshal(body); err != nil {
		return nil, err
	}

	if resp.Status == statusError {
		return nil, fmt.Errorf("query response error: %s", resp.Error)
	}

	switch t := resp.Data.(type) {
	case *uninternedquerypb.QueryResponse_Scalar:
		return uninternedProtoDecodeScalar(t.Scalar), nil

	case *uninternedquerypb.QueryResponse_Vector:
		return uninternedProtoDecodeVector(t.Vector), nil

	default:
		panic(fmt.Sprintf("unknown data type: %v", t))
	}
}

func uninternedProtoDecodeScalar(s *uninternedquerypb.ScalarData) promql.Vector {
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

func uninternedProtoDecodeVector(v *uninternedquerypb.VectorData) promql.Vector {
	vec := make(promql.Vector, len(v.Samples))

	for i, s := range v.Samples {
		labelCount := len(s.Metric) / 2
		metric := make(labels.Labels, labelCount)

		for i := 0; i < labelCount; i++ {
			name := s.Metric[2*i]
			value := s.Metric[2*i+1]
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

func internedProtoDecode(body []byte) (promql.Vector, error) {
	resp := internedquerypb.QueryResponse{}

	if err := resp.Unmarshal(body); err != nil {
		return nil, err
	}

	if resp.Status == statusError {
		return nil, fmt.Errorf("query response error: %s", resp.Error)
	}

	switch t := resp.Data.(type) {
	case *internedquerypb.QueryResponse_Scalar:
		return internedProtoDecodeScalar(t.Scalar), nil

	case *internedquerypb.QueryResponse_Vector:
		return internedProtoDecodeVector(t.Vector), nil

	default:
		panic(fmt.Sprintf("unknown data type: %v", t))
	}
}

func internedProtoDecodeScalar(s *internedquerypb.ScalarData) promql.Vector {
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

func internedProtoDecodeVector(v *internedquerypb.VectorData) promql.Vector {
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

func BenchmarkRemoteQuerier_Encode(b *testing.B) {
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

		bodies := make([]querymiddleware.PrometheusResponse, 0, len(files))

		for _, file := range files {
			body, err := os.ReadFile(file)
			require.NoError(b, err)

			resp := querymiddleware.PrometheusResponse{}
			err = json.Unmarshal(body, &resp)
			require.NoError(b, err)
			bodies = append(bodies, resp)
		}

		b.Run(groupDir.Name(), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for _, body := range bodies {
					//_, err := originalEncode(body)
					//_, err := uninternedProtobufEncode(body)
					_, err := internedProtobufEncode(body)

					if err != nil {
						require.NoError(b, err)
					}
				}
			}
		})
	}
}

var jsonEncoder = jsoniter.Config{
	EscapeHTML:             false, // No HTML in our responses.
	SortMapKeys:            true,
	ValidateJsonRawMessage: true,
}.Froze()

func originalEncode(body querymiddleware.PrometheusResponse) ([]byte, error) {
	return jsonEncoder.Marshal(body)
}

// FIXME: may be able to make this more efficient by simply using the querymiddleware.PrometheusResponse type,
// which already supports being marshalled to protobuf format
// FIXME: regardless of the above, why do we have slightly different formats in different places?
func uninternedProtobufEncode(body querymiddleware.PrometheusResponse) ([]byte, error) {
	resp := uninternedquerypb.QueryResponse{
		Status:    body.Status,
		ErrorType: body.ErrorType,
		Error:     body.Error,
	}

	switch body.Data.ResultType {
	case "vector":
		resp.Data = uninternedVectorEncode(body.Data)

	case "scalar":
		data, err := uninternedScalarEncode(body.Data)
		if err != nil {
			return nil, err
		}

		resp.Data = data

	default:
		panic(fmt.Sprintf("unknown result type %v", body.Data.ResultType))
	}

	return resp.Marshal()
}

func uninternedVectorEncode(data *querymiddleware.PrometheusData) *uninternedquerypb.QueryResponse_Vector {
	vector := uninternedquerypb.VectorData{}

	for _, stream := range data.Result {
		metric := make([]string, len(stream.Labels)*2)

		for i, l := range stream.Labels {
			metric[2*i] = l.Name
			metric[2*i+1] = l.Value
		}

		for _, sample := range stream.Samples {
			vector.Samples = append(vector.Samples, &uninternedquerypb.Sample{
				Metric:    metric,
				Value:     sample.Value,
				Timestamp: sample.TimestampMs,
			})
		}
	}

	return &uninternedquerypb.QueryResponse_Vector{
		Vector: &vector,
	}
}

func uninternedScalarEncode(data *querymiddleware.PrometheusData) (*uninternedquerypb.QueryResponse_Scalar, error) {
	if len(data.Result) != 1 {
		return nil, fmt.Errorf("unexpected number of streams: %v", len(data.Result))
	}

	stream := data.Result[0]
	if len(stream.Samples) != 1 {
		return nil, fmt.Errorf("unexpected number of samples: %v", len(stream.Samples))
	}

	sample := stream.Samples[0]
	return &uninternedquerypb.QueryResponse_Scalar{
		Scalar: &uninternedquerypb.ScalarData{
			Value:     sample.Value,
			Timestamp: sample.TimestampMs,
		},
	}, nil
}

func internedProtobufEncode(body querymiddleware.PrometheusResponse) ([]byte, error) {
	resp := internedquerypb.QueryResponse{
		Status:    body.Status,
		ErrorType: body.ErrorType,
		Error:     body.Error,
	}

	switch body.Data.ResultType {
	case "vector":
		resp.Data = internedVectorEncode(body.Data)

	case "scalar":
		data, err := internedScalarEncode(body.Data)
		if err != nil {
			return nil, err
		}

		resp.Data = data

	default:
		panic(fmt.Sprintf("unknown result type %v", body.Data.ResultType))
	}

	return resp.Marshal()
}

func internedVectorEncode(data *querymiddleware.PrometheusData) *internedquerypb.QueryResponse_Vector {
	sampleCount := 0
	invertedSymbols := map[string]uint64{}

	for _, stream := range data.Result {
		for _, l := range stream.Labels {
			if _, ok := invertedSymbols[l.Name]; !ok {
				invertedSymbols[l.Name] = uint64(len(invertedSymbols))
			}

			if _, ok := invertedSymbols[l.Value]; !ok {
				invertedSymbols[l.Value] = uint64(len(invertedSymbols))
			}
		}

		sampleCount += len(stream.Samples)
	}

	samples := make([]*internedquerypb.Sample, 0, sampleCount)

	for _, stream := range data.Result {
		metricSymbols := make([]uint64, len(stream.Labels)*2)

		for i, l := range stream.Labels {
			metricSymbols[2*i] = invertedSymbols[l.Name]
			metricSymbols[2*i+1] = invertedSymbols[l.Value]
		}

		for _, sample := range stream.Samples {
			samples = append(samples, &internedquerypb.Sample{
				MetricSymbols: metricSymbols,
				Value:         sample.Value,
				Timestamp:     sample.TimestampMs,
			})
		}
	}

	symbols := make([]string, len(invertedSymbols))

	for s, i := range invertedSymbols {
		symbols[i] = s
	}

	return &internedquerypb.QueryResponse_Vector{
		Vector: &internedquerypb.VectorData{
			Samples: samples,
			Symbols: symbols,
		},
	}
}

func internedScalarEncode(data *querymiddleware.PrometheusData) (*internedquerypb.QueryResponse_Scalar, error) {
	if len(data.Result) != 1 {
		return nil, fmt.Errorf("unexpected number of streams: %v", len(data.Result))
	}

	stream := data.Result[0]
	if len(stream.Samples) != 1 {
		return nil, fmt.Errorf("unexpected number of samples: %v", len(stream.Samples))
	}

	sample := stream.Samples[0]
	return &internedquerypb.QueryResponse_Scalar{
		Scalar: &internedquerypb.ScalarData{
			Value:     sample.Value,
			Timestamp: sample.TimestampMs,
		},
	}, nil
}
