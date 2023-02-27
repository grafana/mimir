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
)

func TestMimirJSONCodec_CanEncode(t *testing.T) {
	sampleHistogram := &histogram.FloatHistogram{
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
	}

	scenarios := map[string]struct {
		response          *v1.Response
		expectedCanEncode bool
	}{
		"error response": {
			response: &v1.Response{
				Status: "error",
				Data:   nil,
			},
			expectedCanEncode: true,
		},
		"string response": {
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
			expectedCanEncode: true,
		},
		"scalar response": {
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
			expectedCanEncode: true,
		},
		"vector response with no series": {
			response: &v1.Response{
				Status: "success",
				Data: &v1.QueryData{
					ResultType: parser.ValueTypeVector,
					Result:     promql.Vector{},
				},
			},
			expectedCanEncode: true,
		},
		"vector response with float values only": {
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
			expectedCanEncode: true,
		},
		"vector response with float and histogram values": {
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
						{
							Metric: labels.EmptyLabels(),
							Point: promql.Point{
								T: 5678,
								H: sampleHistogram,
							},
						},
					},
				},
			},
			expectedCanEncode: false,
		},
		"vector response with histogram values only": {
			response: &v1.Response{
				Status: "success",
				Data: &v1.QueryData{
					ResultType: parser.ValueTypeVector,
					Result: promql.Vector{
						{
							Metric: labels.EmptyLabels(),
							Point: promql.Point{
								T: 5678,
								H: sampleHistogram,
							},
						},
					},
				},
			},
			expectedCanEncode: false,
		},
		"matrix response with no series": {
			response: &v1.Response{
				Status: "success",
				Data: &v1.QueryData{
					ResultType: parser.ValueTypeMatrix,
					Result:     promql.Matrix{},
				},
			},
			expectedCanEncode: true,
		},
		"matrix response with float values only": {
			response: &v1.Response{
				Status: "success",
				Data: &v1.QueryData{
					ResultType: parser.ValueTypeMatrix,
					Result: promql.Matrix{
						{
							Metric: labels.EmptyLabels(),
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
			expectedCanEncode: true,
		},
		"matrix response with float and histogram values": {
			response: &v1.Response{
				Status: "success",
				Data: &v1.QueryData{
					ResultType: parser.ValueTypeMatrix,
					Result: promql.Matrix{
						{
							Metric: labels.EmptyLabels(),
							Points: []promql.Point{
								{
									T: 1234,
									V: 5.67,
								},
								{
									T: 5678,
									H: sampleHistogram,
								},
							},
						},
					},
				},
			},
			expectedCanEncode: false,
		},
		"matrix response with histogram values only": {
			response: &v1.Response{
				Status: "success",
				Data: &v1.QueryData{
					ResultType: parser.ValueTypeMatrix,
					Result: promql.Matrix{
						{
							Metric: labels.EmptyLabels(),
							Points: []promql.Point{
								{
									T: 5678,
									H: sampleHistogram,
								},
							},
						},
					},
				},
			},
			expectedCanEncode: false,
		},
	}

	codec := mimirJSONCodec{}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			actual := codec.CanEncode(scenario.response)
			require.Equal(t, scenario.expectedCanEncode, actual)
		})
	}
}
