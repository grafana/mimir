// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/go-kit/log"
	dskit_metrics "github.com/grafana/dskit/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/mimirpb"
)

var expectedProtobufResponseHeaders = []*PrometheusHeader{
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

var protobufResponseHistogram2 = mimirpb.FloatHistogram{
	CounterResetHint: histogram.GaugeType,
	Schema:           3,
	ZeroThreshold:    1.23,
	ZeroCount:        556,
	Count:            9101,
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
	name                  string
	payload               mimirpb.QueryResponse
	response              *PrometheusResponse
	expectedDecodingError error
}{
	{
		name: "successful string response",
		payload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_String_{
				String_: &mimirpb.StringData{Value: "foo", TimestampMs: 1500},
			},
		},
		response: &PrometheusResponse{
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
		payload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Scalar{
				Scalar: &mimirpb.ScalarData{
					Value:       200,
					TimestampMs: 1000,
				},
			},
		},
		response: &PrometheusResponse{
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
		payload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Vector{
				Vector: &mimirpb.VectorData{},
			},
		},
		response: &PrometheusResponse{
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
		payload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Vector{
				Vector: &mimirpb.VectorData{
					Samples: []mimirpb.VectorSample{
						{Metric: nil, TimestampMs: 1_000, Value: 200},
					},
				},
			},
		},
		response: &PrometheusResponse{
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
		payload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Vector{
				Vector: &mimirpb.VectorData{
					Samples: []mimirpb.VectorSample{
						{Metric: []string{"foo", "bar"}, TimestampMs: 1_000, Value: 200},
					},
				},
			},
		},
		response: &PrometheusResponse{
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
		payload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Vector{
				Vector: &mimirpb.VectorData{
					Samples: []mimirpb.VectorSample{
						{Metric: []string{"foo", "bar", "baz", "blah"}, TimestampMs: 1_000, Value: 200},
					},
				},
			},
		},
		response: &PrometheusResponse{
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
		payload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Vector{
				Vector: &mimirpb.VectorData{
					Samples: []mimirpb.VectorSample{
						{Metric: []string{"foo", "bar"}, TimestampMs: 1_000, Value: 200},
						{Metric: []string{"bar", "baz"}, TimestampMs: 1_000, Value: 201},
					},
				},
			},
		},
		response: &PrometheusResponse{
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
		payload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Vector{
				Vector: &mimirpb.VectorData{
					Histograms: []mimirpb.VectorHistogram{
						{
							Metric:      []string{"name-1", "value-1"},
							TimestampMs: 1234,
							Histogram:   protobufResponseHistogram,
						},
					},
				},
			},
		},
		response: &PrometheusResponse{
			Status: statusSuccess,
			Data: &PrometheusData{
				ResultType: model.ValVector.String(),
				Result: []SampleStream{
					{
						Labels:     []mimirpb.LabelAdapter{{Name: "name-1", Value: "value-1"}},
						Histograms: []mimirpb.FloatHistogramPair{{TimestampMs: 1234, Histogram: &protobufResponseHistogram}},
					},
				},
			},
			Headers: expectedProtobufResponseHeaders,
		},
	},
	{
		name: "successful vector response with float and histogram values",
		payload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Vector{
				Vector: &mimirpb.VectorData{
					Samples: []mimirpb.VectorSample{
						{Metric: []string{"foo", "bar"}, TimestampMs: 1000, Value: 200},
					},
					Histograms: []mimirpb.VectorHistogram{
						{
							Metric:      []string{"baz", "blah"},
							TimestampMs: 1234,
							Histogram:   protobufResponseHistogram,
						},
						{
							Metric:      []string{"baz2", "blah2"},
							TimestampMs: 1234,
							Histogram:   protobufResponseHistogram2,
						},
					},
				},
			},
		},
		response: &PrometheusResponse{
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
						Histograms: []mimirpb.FloatHistogramPair{{TimestampMs: 1234, Histogram: &protobufResponseHistogram}},
					},
					{
						Labels:     []mimirpb.LabelAdapter{{Name: "baz2", Value: "blah2"}},
						Histograms: []mimirpb.FloatHistogramPair{{TimestampMs: 1234, Histogram: &protobufResponseHistogram2}},
					},
				},
			},
			Headers: expectedProtobufResponseHeaders,
		},
	},
	{
		name: "successful vector response with malformed metric symbols",
		payload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Vector{
				Vector: &mimirpb.VectorData{
					Samples: []mimirpb.VectorSample{
						{Metric: []string{"foo"}, TimestampMs: 1_000, Value: 200},
					},
				},
			},
		},
		expectedDecodingError: apierror.New(apierror.TypeInternal, "error decoding response: metric is malformed: expected even number of symbols, but got 1"),
	},
	{
		name: "successful matrix response with no series",
		payload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{},
			},
		},
		response: &PrometheusResponse{
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
		payload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{
					Series: []mimirpb.MatrixSeries{
						{Metric: nil, Samples: nil},
					},
				},
			},
		},
		response: &PrometheusResponse{
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
		payload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{
					Series: []mimirpb.MatrixSeries{
						{Metric: []string{"foo", "bar"}, Samples: nil},
					},
				},
			},
		},
		response: &PrometheusResponse{
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
		payload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{
					Series: []mimirpb.MatrixSeries{
						{Metric: []string{"foo", "bar", "baz", "blah"}, Samples: nil},
					},
				},
			},
		},
		response: &PrometheusResponse{
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
		payload: mimirpb.QueryResponse{
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
		response: &PrometheusResponse{
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
		payload: mimirpb.QueryResponse{
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
		response: &PrometheusResponse{
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
		payload: mimirpb.QueryResponse{
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
		response: &PrometheusResponse{
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
		payload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{
					Series: []mimirpb.MatrixSeries{
						{
							Metric:     []string{"name-1", "value-1", "name-2", "value-2"},
							Histograms: []mimirpb.FloatHistogramPair{{TimestampMs: 1234, Histogram: &protobufResponseHistogram}},
						},
					},
				},
			},
		},
		response: &PrometheusResponse{
			Status: statusSuccess,
			Data: &PrometheusData{
				ResultType: model.ValMatrix.String(),
				Result: []SampleStream{
					{
						Labels:     []mimirpb.LabelAdapter{{Name: "name-1", Value: "value-1"}, {Name: "name-2", Value: "value-2"}},
						Histograms: []mimirpb.FloatHistogramPair{{TimestampMs: 1234, Histogram: &protobufResponseHistogram}},
					},
				},
			},
			Headers: expectedProtobufResponseHeaders,
		},
	},
	{
		name: "successful matrix response with float and histogram values",
		payload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{
					Series: []mimirpb.MatrixSeries{
						{
							Metric:     []string{"name-1", "value-1", "name-2", "value-2"},
							Samples:    []mimirpb.Sample{{TimestampMs: 1000, Value: 200}},
							Histograms: []mimirpb.FloatHistogramPair{{TimestampMs: 1234, Histogram: &protobufResponseHistogram}},
						},
					},
				},
			},
		},
		response: &PrometheusResponse{
			Status: statusSuccess,
			Data: &PrometheusData{
				ResultType: model.ValMatrix.String(),
				Result: []SampleStream{
					{
						Labels:     []mimirpb.LabelAdapter{{Name: "name-1", Value: "value-1"}, {Name: "name-2", Value: "value-2"}},
						Samples:    []mimirpb.Sample{{TimestampMs: 1000, Value: 200}},
						Histograms: []mimirpb.FloatHistogramPair{{TimestampMs: 1234, Histogram: &protobufResponseHistogram}},
					},
				},
			},
			Headers: expectedProtobufResponseHeaders,
		},
	},
	{
		name: "successful matrix response with malformed metric symbols",
		payload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{
					Series: []mimirpb.MatrixSeries{
						{Metric: []string{"foo"}, Samples: []mimirpb.Sample{{TimestampMs: 1_000, Value: 100}, {TimestampMs: 2_000, Value: 200}}},
					},
				},
			},
		},
		expectedDecodingError: apierror.New(apierror.TypeInternal, "error decoding response: metric is malformed: expected even number of symbols, but got 1"),
	},
	{
		name: "successful empty matrix response",
		payload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{},
			},
		},
		response: &PrometheusResponse{
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
		payload: mimirpb.QueryResponse{
			Status:    mimirpb.QueryResponse_ERROR,
			ErrorType: mimirpb.QueryResponse_UNAVAILABLE,
			Error:     "failed",
		},
		response: &PrometheusResponse{
			Status:    statusError,
			ErrorType: string(apierror.TypeUnavailable),
			Error:     "failed",
			Headers:   expectedProtobufResponseHeaders,
		},
		expectedDecodingError: apierror.New(apierror.TypeUnavailable, "failed"),
	},
}

func TestProtobufFormat_DecodeResponse(t *testing.T) {
	headers := http.Header{"Content-Type": []string{mimirpb.QueryResponseMimeType}}

	for _, tc := range protobufCodecScenarios {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			codec := NewPrometheusCodec(reg, 0*time.Minute, formatProtobuf, nil)

			body, err := tc.payload.Marshal()
			require.NoError(t, err)
			httpResponse := &http.Response{
				StatusCode:    200,
				Header:        headers,
				Body:          io.NopCloser(bytes.NewBuffer(body)),
				ContentLength: int64(len(body)),
			}
			decoded, err := codec.DecodeMetricsQueryResponse(context.Background(), httpResponse, nil, log.NewNopLogger())
			if err != nil || tc.expectedDecodingError != nil {
				require.Equal(t, tc.expectedDecodingError, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.response, decoded)

			metrics, err := dskit_metrics.NewMetricFamilyMapFromGatherer(reg)
			require.NoError(t, err)
			durationHistogram, err := dskit_metrics.FindHistogramWithNameAndLabels(metrics, "cortex_frontend_query_response_codec_duration_seconds", "format", "protobuf", "operation", "decode")
			require.NoError(t, err)
			require.Equal(t, uint64(1), *durationHistogram.SampleCount)
			require.Less(t, *durationHistogram.SampleSum, 0.1)
			payloadSizeHistogram, err := dskit_metrics.FindHistogramWithNameAndLabels(metrics, "cortex_frontend_query_response_codec_payload_bytes", "format", "protobuf", "operation", "decode")
			require.NoError(t, err)
			require.Equal(t, uint64(1), *payloadSizeHistogram.SampleCount)
			require.Equal(t, float64(len(body)), *payloadSizeHistogram.SampleSum)
		})
	}
}

func TestProtobufFormat_EncodeResponse(t *testing.T) {
	for _, tc := range protobufCodecScenarios {
		if tc.response == nil {
			continue
		}

		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			codec := NewPrometheusCodec(reg, 0*time.Minute, formatProtobuf, nil)

			expectedBodyBytes, err := tc.payload.Marshal()
			require.NoError(t, err)

			httpRequest := &http.Request{
				Header: http.Header{"Accept": []string{mimirpb.QueryResponseMimeType}},
			}

			httpResponse, err := codec.EncodeMetricsQueryResponse(context.Background(), httpRequest, tc.response)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, httpResponse.StatusCode)
			require.Equal(t, mimirpb.QueryResponseMimeType, httpResponse.Header.Get("Content-Type"))

			actualBodyBytes, err := io.ReadAll(httpResponse.Body)
			require.NoError(t, err)

			actualBody := mimirpb.QueryResponse{}
			err = actualBody.Unmarshal(actualBodyBytes)
			require.NoError(t, err)
			require.Equal(t, tc.payload, actualBody)
			require.Len(t, actualBodyBytes, len(expectedBodyBytes))

			metrics, err := dskit_metrics.NewMetricFamilyMapFromGatherer(reg)
			require.NoError(t, err)
			durationHistogram, err := dskit_metrics.FindHistogramWithNameAndLabels(metrics, "cortex_frontend_query_response_codec_duration_seconds", "format", "protobuf", "operation", "encode")
			require.NoError(t, err)
			require.Equal(t, uint64(1), *durationHistogram.SampleCount)
			require.Less(t, *durationHistogram.SampleSum, 0.1)
			payloadSizeHistogram, err := dskit_metrics.FindHistogramWithNameAndLabels(metrics, "cortex_frontend_query_response_codec_payload_bytes", "format", "protobuf", "operation", "encode")
			require.NoError(t, err)
			require.Equal(t, uint64(1), *payloadSizeHistogram.SampleCount)
			require.Equal(t, float64(len(expectedBodyBytes)), *payloadSizeHistogram.SampleSum)
		})
	}
}

func BenchmarkProtobufFormat_DecodeResponse(b *testing.B) {
	headers := http.Header{"Content-Type": []string{mimirpb.QueryResponseMimeType}}
	reg := prometheus.NewPedanticRegistry()
	codec := NewPrometheusCodec(reg, 0*time.Minute, formatProtobuf, nil)

	for _, tc := range protobufCodecScenarios {
		body, err := tc.payload.Marshal()
		require.NoError(b, err)
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				httpResponse := &http.Response{
					StatusCode:    200,
					Header:        headers,
					Body:          io.NopCloser(bytes.NewBuffer(body)),
					ContentLength: int64(len(body)),
				}

				_, err = codec.DecodeMetricsQueryResponse(context.Background(), httpResponse, nil, log.NewNopLogger())
				if err != nil || tc.expectedDecodingError != nil {
					require.Equal(b, tc.expectedDecodingError, err)
				}
			}
		})
	}
}

func BenchmarkProtobufFormat_EncodeResponse(b *testing.B) {
	reg := prometheus.NewPedanticRegistry()
	codec := NewPrometheusCodec(reg, 0*time.Minute, formatProtobuf, nil)

	req := &http.Request{
		Header: http.Header{"Accept": []string{mimirpb.QueryResponseMimeType}},
	}

	for _, tc := range protobufCodecScenarios {
		if tc.response == nil {
			continue
		}

		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := codec.EncodeMetricsQueryResponse(context.Background(), req, tc.response)

				if err != nil {
					require.NoError(b, err)
				}
			}
		})
	}
}
