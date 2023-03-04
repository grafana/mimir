// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestProtobufCodec_CanEncode(t *testing.T) {
	scenarios := map[string]struct {
		response          *v1.Response
		expectedCanEncode bool
	}{
		"query results": {
			response: &v1.Response{
				Status: "success",
				Data:   &v1.QueryData{},
			},
			expectedCanEncode: true,
		},
		"error": {
			response: &v1.Response{
				Status: "error",
				Data:   nil,
			},
			expectedCanEncode: true,
		},
		"another type of result": {
			response: &v1.Response{
				Status: "success",
				Data:   "a string",
			},
			expectedCanEncode: false,
		},
	}

	codec := protobufCodec{}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			actualCanEncode := codec.CanEncode(scenario.response)
			require.Equal(t, scenario.expectedCanEncode, actualCanEncode)
		})
	}
}

var protobufCodecScenarios = map[string]struct {
	response        *v1.Response
	expectedPayload mimirpb.QueryResponse
}{
	"error": {
		response: &v1.Response{
			Status:    "error",
			ErrorType: "unavailable",
			Error:     "something went wrong",
		},
		expectedPayload: mimirpb.QueryResponse{
			Status:    mimirpb.QueryResponse_ERROR,
			ErrorType: mimirpb.QueryResponse_UNAVAILABLE,
			Error:     "something went wrong",
		},
	},
	"string": {
		response: &v1.Response{
			Status: "success",
			Data: &v1.QueryData{
				ResultType: parser.ValueTypeString,
				Result: promql.String{
					T: 1234,
					V: "the-string",
				},
			},
		},
		expectedPayload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_String_{
				String_: &mimirpb.StringData{
					TimestampMs: 1234,
					Value:       "the-string",
				},
			},
		},
	},
	"scalar": {
		response: &v1.Response{
			Status: "success",
			Data: &v1.QueryData{
				ResultType: parser.ValueTypeScalar,
				Result: promql.Scalar{
					T: 1234,
					V: 5.67,
				},
			},
		},
		expectedPayload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Scalar{
				Scalar: &mimirpb.ScalarData{
					TimestampMs: 1234,
					Value:       5.67,
				},
			},
		},
	},
	"vector with no series": {
		response: &v1.Response{
			Status: "success",
			Data: &v1.QueryData{
				ResultType: parser.ValueTypeVector,
				Result:     promql.Vector{},
			},
		},
		expectedPayload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Vector{
				Vector: &mimirpb.VectorData{},
			},
		},
	},
	"vector with single series with no labels": {
		response: &v1.Response{
			Status: "success",
			Data: &v1.QueryData{
				ResultType: parser.ValueTypeVector,
				Result: promql.Vector{
					{
						Metric: labels.EmptyLabels(),
						Point: promql.Point{
							T: 1234,
							V: 5.67,
						},
					},
				},
			},
		},
		expectedPayload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Vector{
				Vector: &mimirpb.VectorData{
					Samples: []mimirpb.VectorSample{
						{
							Metric:      nil,
							TimestampMs: 1234,
							Value:       5.67,
						},
					},
				},
			},
		},
	},
	"vector with single series with one label": {
		response: &v1.Response{
			Status: "success",
			Data: &v1.QueryData{
				ResultType: parser.ValueTypeVector,
				Result: promql.Vector{
					{
						Metric: labels.FromStrings("name-1", "value-1"),
						Point: promql.Point{
							T: 1234,
							V: 5.67,
						},
					},
				},
			},
		},
		expectedPayload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Vector{
				Vector: &mimirpb.VectorData{
					Samples: []mimirpb.VectorSample{
						{
							Metric:      []string{"name-1", "value-1"},
							TimestampMs: 1234,
							Value:       5.67,
						},
					},
				},
			},
		},
	},
	"vector with single series with many labels": {
		response: &v1.Response{
			Status: "success",
			Data: &v1.QueryData{
				ResultType: parser.ValueTypeVector,
				Result: promql.Vector{
					{
						Metric: labels.FromStrings("name-1", "value-1", "name-2", "value-2"),
						Point: promql.Point{
							T: 1234,
							V: 5.67,
						},
					},
				},
			},
		},
		expectedPayload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Vector{
				Vector: &mimirpb.VectorData{
					Samples: []mimirpb.VectorSample{
						{
							Metric:      []string{"name-1", "value-1", "name-2", "value-2"},
							TimestampMs: 1234,
							Value:       5.67,
						},
					},
				},
			},
		},
	},
	"vector with multiple series": {
		response: &v1.Response{
			Status: "success",
			Data: &v1.QueryData{
				ResultType: parser.ValueTypeVector,
				Result: promql.Vector{
					{
						Metric: labels.FromStrings("name-1", "value-1", "name-2", "value-2"),
						Point: promql.Point{
							T: 1234,
							V: 5.67,
						},
					},
					{
						Metric: labels.FromStrings("name-3", "value-3", "name-4", "value-4"),
						Point: promql.Point{
							T: 2345,
							V: 6.78,
						},
					},
				},
			},
		},
		expectedPayload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Vector{
				Vector: &mimirpb.VectorData{
					Samples: []mimirpb.VectorSample{
						{
							Metric:      []string{"name-1", "value-1", "name-2", "value-2"},
							TimestampMs: 1234,
							Value:       5.67,
						},
						{
							Metric:      []string{"name-3", "value-3", "name-4", "value-4"},
							TimestampMs: 2345,
							Value:       6.78,
						},
					},
				},
			},
		},
	},
	"vector with series with histogram value": {
		response: &v1.Response{
			Status: "success",
			Data: &v1.QueryData{
				ResultType: parser.ValueTypeVector,
				Result: promql.Vector{
					{
						Metric: labels.FromStrings("name-1", "value-1"),
						Point: promql.Point{
							T: 1234,
							H: &histogram.FloatHistogram{
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
							},
						},
					},
				},
			},
		},
		expectedPayload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Vector{
				Vector: &mimirpb.VectorData{
					Histograms: []mimirpb.VectorHistogram{
						{
							Metric:      []string{"name-1", "value-1"},
							TimestampMs: 1234,
							Histogram: mimirpb.FloatHistogram{
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
							},
						},
					},
				},
			},
		},
	},
	"vector with float and histogram values": {
		response: &v1.Response{
			Status: "success",
			Data: &v1.QueryData{
				ResultType: parser.ValueTypeVector,
				Result: promql.Vector{
					{
						Metric: labels.FromStrings("name-1", "value-1"),
						Point: promql.Point{
							T: 1234,
							H: &histogram.FloatHistogram{
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
							},
						},
					},
					{
						Metric: labels.FromStrings("name-2", "value-2"),
						Point: promql.Point{
							T: 5678,
							V: 9.01,
						},
					},
					{
						Metric: labels.FromStrings("name-3", "value-3"),
						Point: promql.Point{
							T: 12340,
							H: &histogram.FloatHistogram{
								CounterResetHint: histogram.GaugeType,
								Schema:           4,
								ZeroThreshold:    1.203,
								ZeroCount:        4560,
								Count:            90010,
								Sum:              7890.1,
								PositiveSpans: []histogram.Span{
									{Offset: 40, Length: 1},
									{Offset: 30, Length: 2},
								},
								NegativeSpans: []histogram.Span{
									{Offset: 70, Length: 3},
									{Offset: 90, Length: 1},
								},
								PositiveBuckets: []float64{1000, 2000, 3000},
								NegativeBuckets: []float64{4000, 5000, 6000, 7000},
							},
						},
					},
					{
						Metric: labels.FromStrings("name-4", "value-4"),
						Point: promql.Point{
							T: 56780,
							V: 90.01,
						},
					},
				},
			},
		},
		expectedPayload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Vector{
				Vector: &mimirpb.VectorData{
					Histograms: []mimirpb.VectorHistogram{
						{
							Metric:      []string{"name-1", "value-1"},
							TimestampMs: 1234,
							Histogram: mimirpb.FloatHistogram{
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
							},
						},
						{
							Metric:      []string{"name-3", "value-3"},
							TimestampMs: 12340,
							Histogram: mimirpb.FloatHistogram{
								CounterResetHint: histogram.GaugeType,
								Schema:           4,
								ZeroThreshold:    1.203,
								ZeroCount:        4560,
								Count:            90010,
								Sum:              7890.1,
								PositiveSpans: []mimirpb.BucketSpan{
									{Offset: 40, Length: 1},
									{Offset: 30, Length: 2},
								},
								NegativeSpans: []mimirpb.BucketSpan{
									{Offset: 70, Length: 3},
									{Offset: 90, Length: 1},
								},
								PositiveBuckets: []float64{1000, 2000, 3000},
								NegativeBuckets: []float64{4000, 5000, 6000, 7000},
							},
						},
					},
					Samples: []mimirpb.VectorSample{
						{
							Metric:      []string{"name-2", "value-2"},
							TimestampMs: 5678,
							Value:       9.01,
						},
						{
							Metric:      []string{"name-4", "value-4"},
							TimestampMs: 56780,
							Value:       90.01,
						},
					},
				},
			},
		},
	},
	"matrix with no series": {
		response: &v1.Response{
			Status: "success",
			Data: &v1.QueryData{
				ResultType: parser.ValueTypeMatrix,
				Result:     promql.Matrix{},
			},
		},
		expectedPayload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{},
			},
		},
	},
	"matrix with single series with no points and no labels": {
		response: &v1.Response{
			Status: "success",
			Data: &v1.QueryData{
				ResultType: parser.ValueTypeMatrix,
				Result: promql.Matrix{
					{
						Metric: labels.EmptyLabels(),
						Points: nil,
					},
				},
			},
		},
		expectedPayload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{
					Series: []mimirpb.MatrixSeries{
						{
							Metric:  nil,
							Samples: nil,
						},
					},
				},
			},
		},
	},
	"matrix with single series with no points and one label": {
		response: &v1.Response{
			Status: "success",
			Data: &v1.QueryData{
				ResultType: parser.ValueTypeMatrix,
				Result: promql.Matrix{
					{
						Metric: labels.FromStrings("name-1", "value-1"),
						Points: nil,
					},
				},
			},
		},
		expectedPayload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{
					Series: []mimirpb.MatrixSeries{
						{
							Metric:  []string{"name-1", "value-1"},
							Samples: nil,
						},
					},
				},
			},
		},
	},
	"matrix with single series with no points and many labels": {
		response: &v1.Response{
			Status: "success",
			Data: &v1.QueryData{
				ResultType: parser.ValueTypeMatrix,
				Result: promql.Matrix{
					{
						Metric: labels.FromStrings("name-1", "value-1", "name-2", "value-2"),
						Points: nil,
					},
				},
			},
		},
		expectedPayload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{
					Series: []mimirpb.MatrixSeries{
						{
							Metric:  []string{"name-1", "value-1", "name-2", "value-2"},
							Samples: nil,
						},
					},
				},
			},
		},
	},
	"matrix with single series with one point": {
		response: &v1.Response{
			Status: "success",
			Data: &v1.QueryData{
				ResultType: parser.ValueTypeMatrix,
				Result: promql.Matrix{
					{
						Metric: labels.FromStrings("name-1", "value-1", "name-2", "value-2"),
						Points: []promql.Point{
							{
								T: 1234,
								V: 5.67,
							},
						},
					},
				},
			},
		},
		expectedPayload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{
					Series: []mimirpb.MatrixSeries{
						{
							Metric: []string{"name-1", "value-1", "name-2", "value-2"},
							Samples: []mimirpb.Sample{
								{
									TimestampMs: 1234,
									Value:       5.67,
								},
							},
						},
					},
				},
			},
		},
	},
	"matrix with single series with many points": {
		response: &v1.Response{
			Status: "success",
			Data: &v1.QueryData{
				ResultType: parser.ValueTypeMatrix,
				Result: promql.Matrix{
					{
						Metric: labels.FromStrings("name-1", "value-1", "name-2", "value-2"),
						Points: []promql.Point{
							{T: 1234, V: 5.67},
							{T: 5678, V: 9.01},
						},
					},
				},
			},
		},
		expectedPayload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{
					Series: []mimirpb.MatrixSeries{
						{
							Metric: []string{"name-1", "value-1", "name-2", "value-2"},
							Samples: []mimirpb.Sample{
								{
									TimestampMs: 1234,
									Value:       5.67,
								},
								{
									TimestampMs: 5678,
									Value:       9.01,
								},
							},
						},
					},
				},
			},
		},
	},
	"matrix with many series": {
		response: &v1.Response{
			Status: "success",
			Data: &v1.QueryData{
				ResultType: parser.ValueTypeMatrix,
				Result: promql.Matrix{
					{
						Metric: labels.FromStrings("name-1", "value-1"),
						Points: []promql.Point{
							{T: 1234, V: 5.67},
							{T: 5678, V: 9.01},
						},
					},
					{
						Metric: labels.FromStrings("name-2", "value-2"),
						Points: []promql.Point{
							{T: 12340, V: 50.67},
							{T: 56780, V: 90.01},
						},
					},
				},
			},
		},
		expectedPayload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{
					Series: []mimirpb.MatrixSeries{
						{
							Metric: []string{"name-1", "value-1"},
							Samples: []mimirpb.Sample{
								{
									TimestampMs: 1234,
									Value:       5.67,
								},
								{
									TimestampMs: 5678,
									Value:       9.01,
								},
							},
						},
						{
							Metric: []string{"name-2", "value-2"},
							Samples: []mimirpb.Sample{
								{
									TimestampMs: 12340,
									Value:       50.67,
								},
								{
									TimestampMs: 56780,
									Value:       90.01,
								},
							},
						},
					},
				},
			},
		},
	},
	"matrix with series with histogram value": {
		response: &v1.Response{
			Status: "success",
			Data: &v1.QueryData{
				ResultType: parser.ValueTypeMatrix,
				Result: promql.Matrix{
					{
						Metric: labels.FromStrings("name-1", "value-1", "name-2", "value-2"),
						Points: []promql.Point{
							{
								T: 1234,
								H: &histogram.FloatHistogram{
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
								},
							},
						},
					},
				},
			},
		},
		expectedPayload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{
					Series: []mimirpb.MatrixSeries{
						{
							Metric: []string{"name-1", "value-1", "name-2", "value-2"},
							Histograms: []mimirpb.FloatHistogramPair{
								{
									TimestampMs: 1234,
									Histogram: mimirpb.FloatHistogram{
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
									},
								},
							},
						},
					},
				},
			},
		},
	},
	"matrix with series with both float and histogram values": {
		response: &v1.Response{
			Status: "success",
			Data: &v1.QueryData{
				ResultType: parser.ValueTypeMatrix,
				Result: promql.Matrix{
					{
						Metric: labels.FromStrings("name-1", "value-1", "name-2", "value-2"),
						Points: []promql.Point{
							{
								T: 1234,
								H: &histogram.FloatHistogram{
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
								},
							},
							{
								T: 5678,
								V: 9.01,
							},
							{
								T: 12340,
								H: &histogram.FloatHistogram{
									CounterResetHint: histogram.GaugeType,
									Schema:           4,
									ZeroThreshold:    1.203,
									ZeroCount:        4560,
									Count:            90010,
									Sum:              7890.1,
									PositiveSpans: []histogram.Span{
										{Offset: 40, Length: 1},
										{Offset: 30, Length: 2},
									},
									NegativeSpans: []histogram.Span{
										{Offset: 70, Length: 3},
										{Offset: 90, Length: 1},
									},
									PositiveBuckets: []float64{1000, 2000, 3000},
									NegativeBuckets: []float64{4000, 5000, 6000, 7000},
								},
							},
							{
								T: 56780,
								V: 90.01,
							},
						},
					},
				},
			},
		},
		expectedPayload: mimirpb.QueryResponse{
			Status: mimirpb.QueryResponse_SUCCESS,
			Data: &mimirpb.QueryResponse_Matrix{
				Matrix: &mimirpb.MatrixData{
					Series: []mimirpb.MatrixSeries{
						{
							Metric: []string{"name-1", "value-1", "name-2", "value-2"},
							Samples: []mimirpb.Sample{
								{
									TimestampMs: 5678,
									Value:       9.01,
								},
								{
									TimestampMs: 56780,
									Value:       90.01,
								},
							},
							Histograms: []mimirpb.FloatHistogramPair{
								{
									TimestampMs: 1234,
									Histogram: mimirpb.FloatHistogram{
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
									},
								},
								{
									TimestampMs: 12340,
									Histogram: mimirpb.FloatHistogram{
										CounterResetHint: histogram.GaugeType,
										Schema:           4,
										ZeroThreshold:    1.203,
										ZeroCount:        4560,
										Count:            90010,
										Sum:              7890.1,
										PositiveSpans: []mimirpb.BucketSpan{
											{Offset: 40, Length: 1},
											{Offset: 30, Length: 2},
										},
										NegativeSpans: []mimirpb.BucketSpan{
											{Offset: 70, Length: 3},
											{Offset: 90, Length: 1},
										},
										PositiveBuckets: []float64{1000, 2000, 3000},
										NegativeBuckets: []float64{4000, 5000, 6000, 7000},
									},
								},
							},
						},
					},
				},
			},
		},
	},
}

func TestProtobufCodec_Encode(t *testing.T) {
	codec := protobufCodec{}

	for name, scenario := range protobufCodecScenarios {
		t.Run(name, func(t *testing.T) {
			canEncode := codec.CanEncode(scenario.response)
			require.True(t, canEncode, "unrealistic test scenario: protobufCodec.CanEncode(scenario.response) returns false")

			b, err := codec.Encode(scenario.response)
			require.NoError(t, err)

			actual := mimirpb.QueryResponse{}
			err = actual.Unmarshal(b)
			require.NoError(t, err)

			require.Equal(t, scenario.expectedPayload, actual)
		})
	}
}

func BenchmarkProtobufCodec_Encode(b *testing.B) {
	codec := protobufCodec{}

	for name, scenario := range protobufCodecScenarios {
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := codec.Encode(scenario.response)
				if err != nil {
					require.NoError(b, err)
				}
			}
		})
	}
}
