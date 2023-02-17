// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/go-kit/log"
	dskit_metrics "github.com/grafana/dskit/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestProtobufFormat_DecodeResponse(t *testing.T) {
	headers := http.Header{"Content-Type": []string{mimirpb.QueryResponseMimeType}}
	expectedRespHeaders := []*PrometheusResponseHeader{
		{
			Name:   "Content-Type",
			Values: []string{mimirpb.QueryResponseMimeType},
		},
	}

	for _, tc := range []struct {
		name        string
		resp        mimirpb.QueryResponse
		expected    *PrometheusResponse
		expectedErr error
	}{
		{
			name: "successful string response",
			resp: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_String_{
					String_: &mimirpb.StringData{Value: "foo", TimestampMilliseconds: 1500},
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
			resp: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Scalar{
					Scalar: &mimirpb.ScalarData{
						Value:                 200,
						TimestampMilliseconds: 1000,
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
			name: "successful empty vector response",
			resp: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Vector{
					Vector: &mimirpb.VectorData{},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValVector.String(),
					Result:     []SampleStream{},
				},
				Headers: expectedRespHeaders,
			},
		},
		{
			name: "successful vector response with single series with no labels",
			resp: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Vector{
					Vector: &mimirpb.VectorData{
						Samples: []mimirpb.VectorSample{
							{Metric: []string{}, TimestampMilliseconds: 1_000, Value: 200},
						},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValVector.String(),
					Result: []SampleStream{
						{Labels: []mimirpb.LabelAdapter{}, Samples: []mimirpb.Sample{{TimestampMs: 1_000, Value: 200}}},
					},
				},
				Headers: expectedRespHeaders,
			},
		},
		{
			name: "successful vector response with single series with one label",
			resp: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Vector{
					Vector: &mimirpb.VectorData{
						Samples: []mimirpb.VectorSample{
							{Metric: []string{"foo", "bar"}, TimestampMilliseconds: 1_000, Value: 200},
						},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValVector.String(),
					Result: []SampleStream{
						{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}}, Samples: []mimirpb.Sample{{TimestampMs: 1_000, Value: 200}}},
					},
				},
				Headers: expectedRespHeaders,
			},
		},
		{
			name: "successful vector response with single series with many labels",
			resp: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Vector{
					Vector: &mimirpb.VectorData{
						Samples: []mimirpb.VectorSample{
							{Metric: []string{"foo", "bar", "baz", "blah"}, TimestampMilliseconds: 1_000, Value: 200},
						},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValVector.String(),
					Result: []SampleStream{
						{
							Labels: []mimirpb.LabelAdapter{
								{Name: "foo", Value: "bar"},
								{Name: "baz", Value: "blah"},
							},
							Samples: []mimirpb.Sample{{TimestampMs: 1_000, Value: 200}},
						},
					},
				},
				Headers: expectedRespHeaders,
			},
		},
		{
			name: "successful vector response with multiple series",
			resp: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Vector{
					Vector: &mimirpb.VectorData{
						Samples: []mimirpb.VectorSample{
							{Metric: []string{"foo", "bar"}, TimestampMilliseconds: 1_000, Value: 200},
							{Metric: []string{"bar", "baz"}, TimestampMilliseconds: 1_000, Value: 201},
						},
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
			name: "successful vector response with malformed metric symbols",
			resp: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Vector{
					Vector: &mimirpb.VectorData{
						Samples: []mimirpb.VectorSample{
							{Metric: []string{"foo"}, TimestampMilliseconds: 1_000, Value: 200},
						},
					},
				},
			},
			expectedErr: apierror.New(apierror.TypeInternal, "error decoding response: metric is malformed: expected even number of symbols, but got 1"),
		},
		{
			name: "successful matrix response with no series",
			resp: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Matrix{
					Matrix: &mimirpb.MatrixData{},
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
			name: "successful matrix response with single series with no labels and no samples",
			resp: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Matrix{
					Matrix: &mimirpb.MatrixData{
						Series: []mimirpb.MatrixSeries{
							{Metric: []string{}, Samples: []mimirpb.MatrixSample{}},
						},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValMatrix.String(),
					Result: []SampleStream{
						{Labels: []mimirpb.LabelAdapter{}, Samples: []mimirpb.Sample{}},
					},
				},
				Headers: expectedRespHeaders,
			},
		},
		{
			name: "successful matrix response with single series with one label and no samples",
			resp: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Matrix{
					Matrix: &mimirpb.MatrixData{
						Series: []mimirpb.MatrixSeries{
							{Metric: []string{"foo", "bar"}, Samples: []mimirpb.MatrixSample{}},
						},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValMatrix.String(),
					Result: []SampleStream{
						{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}}, Samples: []mimirpb.Sample{}},
					},
				},
				Headers: expectedRespHeaders,
			},
		},
		{
			name: "successful matrix response with single series with many labels and no samples",
			resp: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Matrix{
					Matrix: &mimirpb.MatrixData{
						Series: []mimirpb.MatrixSeries{
							{Metric: []string{"foo", "bar", "baz", "blah"}, Samples: []mimirpb.MatrixSample{}},
						},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValMatrix.String(),
					Result: []SampleStream{
						{
							Labels: []mimirpb.LabelAdapter{
								{Name: "foo", Value: "bar"},
								{Name: "baz", Value: "blah"},
							},
							Samples: []mimirpb.Sample{},
						},
					},
				},
				Headers: expectedRespHeaders,
			},
		},
		{
			name: "successful matrix response with single series with one sample",
			resp: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Matrix{
					Matrix: &mimirpb.MatrixData{
						Series: []mimirpb.MatrixSeries{
							{
								Metric: []string{"foo", "bar", "baz", "blah"},
								Samples: []mimirpb.MatrixSample{
									{TimestampMilliseconds: 1_000, Value: 100},
								},
							},
						},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValMatrix.String(),
					Result: []SampleStream{
						{
							Labels: []mimirpb.LabelAdapter{
								{Name: "foo", Value: "bar"},
								{Name: "baz", Value: "blah"},
							},
							Samples: []mimirpb.Sample{
								{TimestampMs: 1_000, Value: 100},
							},
						},
					},
				},
				Headers: expectedRespHeaders,
			},
		},
		{
			name: "successful matrix response with single series with many samples",
			resp: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Matrix{
					Matrix: &mimirpb.MatrixData{
						Series: []mimirpb.MatrixSeries{
							{
								Metric: []string{"foo", "bar", "baz", "blah"},
								Samples: []mimirpb.MatrixSample{
									{TimestampMilliseconds: 1_000, Value: 100},
									{TimestampMilliseconds: 1_001, Value: 101},
								},
							},
						},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValMatrix.String(),
					Result: []SampleStream{
						{
							Labels: []mimirpb.LabelAdapter{
								{Name: "foo", Value: "bar"},
								{Name: "baz", Value: "blah"},
							},
							Samples: []mimirpb.Sample{
								{TimestampMs: 1_000, Value: 100},
								{TimestampMs: 1_001, Value: 101},
							},
						},
					},
				},
				Headers: expectedRespHeaders,
			},
		},
		{
			name: "successful matrix response with multiple series",
			resp: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Matrix{
					Matrix: &mimirpb.MatrixData{
						Series: []mimirpb.MatrixSeries{
							{Metric: []string{"foo", "bar"}, Samples: []mimirpb.MatrixSample{{TimestampMilliseconds: 1_000, Value: 100}, {TimestampMilliseconds: 2_000, Value: 200}}},
							{Metric: []string{"bar", "baz"}, Samples: []mimirpb.MatrixSample{{TimestampMilliseconds: 1_000, Value: 101}, {TimestampMilliseconds: 2_000, Value: 201}}},
						},
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
			name: "successful matrix response with malformed metric symbols",
			resp: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Matrix{
					Matrix: &mimirpb.MatrixData{
						Series: []mimirpb.MatrixSeries{
							{Metric: []string{"foo"}, Samples: []mimirpb.MatrixSample{{TimestampMilliseconds: 1_000, Value: 100}, {TimestampMilliseconds: 2_000, Value: 200}}},
						},
					},
				},
			},
			expectedErr: apierror.New(apierror.TypeInternal, "error decoding response: metric is malformed: expected even number of symbols, but got 1"),
		},
		{
			name: "successful empty matrix response",
			resp: mimirpb.QueryResponse{
				Status: mimirpb.QueryResponse_SUCCESS,
				Data: &mimirpb.QueryResponse_Matrix{
					Matrix: &mimirpb.MatrixData{},
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
			resp: mimirpb.QueryResponse{
				Status:    mimirpb.QueryResponse_ERROR,
				ErrorType: mimirpb.QueryResponse_UNAVAILABLE,
				Error:     "failed",
			},
			expectedErr: apierror.New(apierror.TypeUnavailable, "failed"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			codec := NewPrometheusCodec(reg, formatProtobuf)

			body, err := tc.resp.Marshal()
			require.NoError(t, err)
			httpResponse := &http.Response{
				StatusCode:    200,
				Header:        headers,
				Body:          io.NopCloser(bytes.NewBuffer(body)),
				ContentLength: int64(len(body)),
			}
			decoded, err := codec.DecodeResponse(context.Background(), httpResponse, nil, log.NewNopLogger())
			if err != nil || tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expected, decoded)

			metrics, err := dskit_metrics.NewMetricFamilyMapFromGatherer(reg)
			require.NoError(t, err)
			durationHistogram, err := findHistogramMatchingLabels(metrics, "cortex_frontend_query_response_codec_duration_seconds", "format", "protobuf", "operation", "decode")
			require.NoError(t, err)
			require.Equal(t, uint64(1), *durationHistogram.SampleCount)
			require.Less(t, *durationHistogram.SampleSum, 0.1)
			payloadSizeHistogram, err := findHistogramMatchingLabels(metrics, "cortex_frontend_query_response_codec_payload_bytes", "format", "protobuf", "operation", "decode")
			require.NoError(t, err)
			require.Equal(t, uint64(1), *payloadSizeHistogram.SampleCount)
			require.Equal(t, float64(len(body)), *payloadSizeHistogram.SampleSum)
		})
	}
}
