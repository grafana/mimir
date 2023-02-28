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
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/mimirpb"
)

var expectedProtobufResponseHeaders = []*PrometheusResponseHeader{
	{
		Name:   "Content-Type",
		Values: []string{mimirpb.QueryResponseMimeType},
	},
}

var protobufResponseHistogram = mimirpb.FloatHistogram{
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

var protobufCodecScenarios = []struct {
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
			Headers: expectedProtobufResponseHeaders,
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
			Headers: expectedProtobufResponseHeaders,
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
			Headers: expectedProtobufResponseHeaders,
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
			Headers: expectedProtobufResponseHeaders,
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
			Headers: expectedProtobufResponseHeaders,
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
			Headers: expectedProtobufResponseHeaders,
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
			Headers: expectedProtobufResponseHeaders,
		},
	},
	{
		name: "successful vector response with histogram value",
		resp: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Vector{
				Vector: &mimirpb.VectorData{
					Histograms: []mimirpb.VectorHistogram{
						{
							Metric:                []string{"name-1", "value-1"},
							TimestampMilliseconds: 1234,
							Histogram:             protobufResponseHistogram,
						},
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
						Labels:     []mimirpb.LabelAdapter{{Name: "name-1", Value: "value-1"}},
						Histograms: []mimirpb.FloatHistogramPair{{TimestampMs: 1234, Histogram: protobufResponseHistogram}},
					},
				},
			},
			Headers: expectedProtobufResponseHeaders,
		},
	},
	{
		name: "successful vector response with float and histogram values",
		resp: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Vector{
				Vector: &mimirpb.VectorData{
					Samples: []mimirpb.VectorSample{
						{Metric: []string{"foo", "bar"}, TimestampMilliseconds: 1000, Value: 200},
					},
					Histograms: []mimirpb.VectorHistogram{
						{
							Metric:                []string{"baz", "blah"},
							TimestampMilliseconds: 1234,
							Histogram:             protobufResponseHistogram,
						},
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
						Labels:  []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}},
						Samples: []mimirpb.Sample{{TimestampMs: 1000, Value: 200}},
					},
					{
						Labels:     []mimirpb.LabelAdapter{{Name: "baz", Value: "blah"}},
						Histograms: []mimirpb.FloatHistogramPair{{TimestampMs: 1234, Histogram: protobufResponseHistogram}},
					},
				},
			},
			Headers: expectedProtobufResponseHeaders,
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
			Headers: expectedProtobufResponseHeaders,
		},
	},
	{
		name: "successful matrix response with single series with no labels and no samples",
		resp: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{
					Series: []mimirpb.MatrixSeries{
						{Metric: []string{}, Samples: []mimirpb.Sample{}},
					},
				},
			},
		},
		expected: &PrometheusResponse{
			Status: statusSuccess,
			Data: &PrometheusData{
				ResultType: model.ValMatrix.String(),
				Result: []SampleStream{
					{Labels: []mimirpb.LabelAdapter{}},
				},
			},
			Headers: expectedProtobufResponseHeaders,
		},
	},
	{
		name: "successful matrix response with single series with one label and no samples",
		resp: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{
					Series: []mimirpb.MatrixSeries{
						{Metric: []string{"foo", "bar"}, Samples: []mimirpb.Sample{}},
					},
				},
			},
		},
		expected: &PrometheusResponse{
			Status: statusSuccess,
			Data: &PrometheusData{
				ResultType: model.ValMatrix.String(),
				Result: []SampleStream{
					{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}}},
				},
			},
			Headers: expectedProtobufResponseHeaders,
		},
	},
	{
		name: "successful matrix response with single series with many labels and no samples",
		resp: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{
					Series: []mimirpb.MatrixSeries{
						{Metric: []string{"foo", "bar", "baz", "blah"}, Samples: []mimirpb.Sample{}},
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
					},
				},
			},
			Headers: expectedProtobufResponseHeaders,
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
							Samples: []mimirpb.Sample{
								{TimestampMs: 1_000, Value: 100},
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
			Headers: expectedProtobufResponseHeaders,
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
							Samples: []mimirpb.Sample{
								{TimestampMs: 1_000, Value: 100},
								{TimestampMs: 1_001, Value: 101},
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
			Headers: expectedProtobufResponseHeaders,
		},
	},
	{
		name: "successful matrix response with multiple series",
		resp: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{
					Series: []mimirpb.MatrixSeries{
						{Metric: []string{"foo", "bar"}, Samples: []mimirpb.Sample{{TimestampMs: 1_000, Value: 100}, {TimestampMs: 2_000, Value: 200}}},
						{Metric: []string{"bar", "baz"}, Samples: []mimirpb.Sample{{TimestampMs: 1_000, Value: 101}, {TimestampMs: 2_000, Value: 201}}},
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
						Labels:  []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}},
						Samples: []mimirpb.Sample{{TimestampMs: 1_000, Value: 100}, {TimestampMs: 2_000, Value: 200}},
					},
					{
						Labels:  []mimirpb.LabelAdapter{{Name: "bar", Value: "baz"}},
						Samples: []mimirpb.Sample{{TimestampMs: 1_000, Value: 101}, {TimestampMs: 2_000, Value: 201}},
					},
				},
			},
			Headers: expectedProtobufResponseHeaders,
		},
	},
	{
		name: "successful matrix response with histogram value",
		resp: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{
					Series: []mimirpb.MatrixSeries{
						{
							Metric:     []string{"name-1", "value-1", "name-2", "value-2"},
							Histograms: []mimirpb.FloatHistogramPair{{TimestampMs: 1234, Histogram: protobufResponseHistogram}},
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
						Labels:     []mimirpb.LabelAdapter{{Name: "name-1", Value: "value-1"}, {Name: "name-2", Value: "value-2"}},
						Histograms: []mimirpb.FloatHistogramPair{{TimestampMs: 1234, Histogram: protobufResponseHistogram}},
					},
				},
			},
			Headers: expectedProtobufResponseHeaders,
		},
	},
	{
		name: "successful matrix response with float and histogram values",
		resp: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{
					Series: []mimirpb.MatrixSeries{
						{
							Metric:     []string{"name-1", "value-1", "name-2", "value-2"},
							Samples:    []mimirpb.Sample{{TimestampMs: 1000, Value: 200}},
							Histograms: []mimirpb.FloatHistogramPair{{TimestampMs: 1234, Histogram: protobufResponseHistogram}},
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
						Labels:     []mimirpb.LabelAdapter{{Name: "name-1", Value: "value-1"}, {Name: "name-2", Value: "value-2"}},
						Samples:    []mimirpb.Sample{{TimestampMs: 1000, Value: 200}},
						Histograms: []mimirpb.FloatHistogramPair{{TimestampMs: 1234, Histogram: protobufResponseHistogram}},
					},
				},
			},
			Headers: expectedProtobufResponseHeaders,
		},
	},
	{
		name: "successful matrix response with malformed metric symbols",
		resp: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{
					Series: []mimirpb.MatrixSeries{
						{Metric: []string{"foo"}, Samples: []mimirpb.Sample{{TimestampMs: 1_000, Value: 100}, {TimestampMs: 2_000, Value: 200}}},
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
			Headers: expectedProtobufResponseHeaders,
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
}

func TestProtobufFormat_DecodeResponse(t *testing.T) {
	headers := http.Header{"Content-Type": []string{mimirpb.QueryResponseMimeType}}

	for _, tc := range protobufCodecScenarios {
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

func BenchmarkProtobufFormat_DecodeResponse(b *testing.B) {
	headers := http.Header{"Content-Type": []string{mimirpb.QueryResponseMimeType}}
	reg := prometheus.NewPedanticRegistry()
	codec := NewPrometheusCodec(reg, formatProtobuf)

	for _, tc := range protobufCodecScenarios {
		body, err := tc.resp.Marshal()
		require.NoError(b, err)
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				httpResponse := &http.Response{
					StatusCode:    200,
					Header:        headers,
					Body:          io.NopCloser(bytes.NewBuffer(body)),
					ContentLength: int64(len(body)),
				}

				_, err = codec.DecodeResponse(context.Background(), httpResponse, nil, log.NewNopLogger())
				if err != nil || tc.expectedErr != nil {
					require.Equal(b, tc.expectedErr, err)
				}
			}
		})
	}
}
