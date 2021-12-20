// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/value_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queryrange

import (
	"fmt"
	"math"
	"testing"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestResponseToSamples(t *testing.T) {
	input := &PrometheusResponse{
		Data: &PrometheusData{
			ResultType: string(parser.ValueTypeMatrix),
			Result: []SampleStream{
				{
					Labels: []mimirpb.LabelAdapter{
						{Name: "a", Value: "a1"},
						{Name: "b", Value: "b1"},
					},
					Samples: []mimirpb.Sample{
						{
							Value:       1,
							TimestampMs: 1,
						},
						{
							Value:       2,
							TimestampMs: 2,
						},
					},
				},
				{
					Labels: []mimirpb.LabelAdapter{
						{Name: "a", Value: "a1"},
						{Name: "b", Value: "b1"},
					},
					Samples: []mimirpb.Sample{
						{
							Value:       8,
							TimestampMs: 1,
						},
						{
							Value:       9,
							TimestampMs: 2,
						},
					},
				},
			},
		},
	}

	streams, err := ResponseToSamples(input)
	require.NoError(t, err)
	assertEqualSampleStream(t, input.Data.Result, streams)
}

func TestFromValue(t *testing.T) {
	var testExpr = []struct {
		input    *promql.Result
		err      bool
		expected []SampleStream
	}{
		// string (errors)
		{
			input: &promql.Result{Value: promql.String{T: 1, V: "hi"}},
			err:   true,
		},
		{
			input: &promql.Result{Err: errors.New("foo")},
			err:   true,
		},
		// Scalar
		{
			input: &promql.Result{Value: promql.Scalar{T: 1, V: 1}},
			err:   false,
			expected: []SampleStream{
				{
					Samples: []mimirpb.Sample{
						{
							Value:       1,
							TimestampMs: 1,
						},
					},
				},
			},
		},
		// Vector
		{
			input: &promql.Result{
				Value: promql.Vector{
					promql.Sample{
						Point: promql.Point{T: 1, V: 1},
						Metric: labels.Labels{
							{Name: "a", Value: "a1"},
							{Name: "b", Value: "b1"},
						},
					},
					promql.Sample{
						Point: promql.Point{T: 2, V: 2},
						Metric: labels.Labels{
							{Name: "a", Value: "a2"},
							{Name: "b", Value: "b2"},
						},
					},
				},
			},
			err: false,
			expected: []SampleStream{
				{
					Labels: []mimirpb.LabelAdapter{
						{Name: "a", Value: "a1"},
						{Name: "b", Value: "b1"},
					},
					Samples: []mimirpb.Sample{
						{
							Value:       1,
							TimestampMs: 1,
						},
					},
				},
				{
					Labels: []mimirpb.LabelAdapter{
						{Name: "a", Value: "a2"},
						{Name: "b", Value: "b2"},
					},
					Samples: []mimirpb.Sample{
						{
							Value:       2,
							TimestampMs: 2,
						},
					},
				},
			},
		},
		// Matrix
		{
			input: &promql.Result{
				Value: promql.Matrix{
					{
						Metric: labels.Labels{
							{Name: "a", Value: "a1"},
							{Name: "b", Value: "b1"},
						},
						Points: []promql.Point{
							{T: 1, V: 1},
							{T: 2, V: 2},
						},
					},
					{
						Metric: labels.Labels{
							{Name: "a", Value: "a2"},
							{Name: "b", Value: "b2"},
						},
						Points: []promql.Point{
							{T: 1, V: 8},
							{T: 2, V: 9},
						},
					},
				},
			},
			err: false,
			expected: []SampleStream{
				{
					Labels: []mimirpb.LabelAdapter{
						{Name: "a", Value: "a1"},
						{Name: "b", Value: "b1"},
					},
					Samples: []mimirpb.Sample{
						{
							Value:       1,
							TimestampMs: 1,
						},
						{
							Value:       2,
							TimestampMs: 2,
						},
					},
				},
				{
					Labels: []mimirpb.LabelAdapter{
						{Name: "a", Value: "a2"},
						{Name: "b", Value: "b2"},
					},
					Samples: []mimirpb.Sample{
						{
							Value:       8,
							TimestampMs: 1,
						},
						{
							Value:       9,
							TimestampMs: 2,
						},
					},
				},
			},
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			result, err := FromResult(c.input)
			if c.err {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				require.Equal(t, c.expected, result)
			}
		})
	}
}

func TestNewSeriesSetFromEmbeddedQueriesResults(t *testing.T) {
	tests := map[string]struct {
		input    []SampleStream
		hints    *storage.SelectHints
		expected []SampleStream
	}{
		"should add a stale marker at the end even if if input samples have no gaps": {
			input: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}},
			}},
			hints: &storage.SelectHints{Step: 10},
			expected: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}, {TimestampMs: 40, Value: math.Float64frombits(value.StaleNaN)}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}, {TimestampMs: 40, Value: math.Float64frombits(value.StaleNaN)}},
			}},
		},
		"should add stale markers at the beginning of each gap and one at the end of the series": {
			input: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 40, Value: 4}, {TimestampMs: 90, Value: 9}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}},
			}},
			hints: &storage.SelectHints{Step: 10},
			expected: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 20, Value: math.Float64frombits(value.StaleNaN)}, {TimestampMs: 40, Value: 4}, {TimestampMs: 50, Value: math.Float64frombits(value.StaleNaN)}, {TimestampMs: 90, Value: 9}, {TimestampMs: 100, Value: math.Float64frombits(value.StaleNaN)}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}, {TimestampMs: 40, Value: math.Float64frombits(value.StaleNaN)}},
			}},
		},
		"should not add stale markers even if points have gaps if hints is not passed": {
			input: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 40, Value: 4}, {TimestampMs: 90, Value: 9}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}},
			}},
			hints: nil,
			expected: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 40, Value: 4}, {TimestampMs: 90, Value: 9}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}},
			}},
		},
		"should not add stale markers even if points have gaps if step == 0": {
			input: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 40, Value: 4}, {TimestampMs: 90, Value: 9}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}},
			}},
			hints: &storage.SelectHints{Step: 0},
			expected: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 40, Value: 4}, {TimestampMs: 90, Value: 9}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}},
			}},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			set := newSeriesSetFromEmbeddedQueriesResults(testData.input, testData.hints)
			actual, err := seriesSetToSampleStreams(set)
			require.NoError(t, err)
			assertEqualSampleStream(t, testData.expected, actual)
		})
	}
}

// seriesSetToSampleStreams iterate through the input storage.SeriesSet and returns it as a []SampleStream.
func seriesSetToSampleStreams(set storage.SeriesSet) ([]SampleStream, error) {
	var out []SampleStream

	for set.Next() {
		stream := SampleStream{Labels: mimirpb.FromLabelsToLabelAdapters(set.At().Labels())}

		it := set.At().Iterator()
		for it.Next() {
			t, v := it.At()
			stream.Samples = append(stream.Samples, mimirpb.Sample{
				Value:       v,
				TimestampMs: t,
			})
		}

		if it.Err() != nil {
			return nil, it.Err()
		}

		out = append(out, stream)
	}

	if set.Err() != nil {
		return nil, set.Err()
	}

	return out, nil
}

func assertEqualSampleStream(t *testing.T, expected, actual []SampleStream) {
	// Expect the same length.
	require.Equal(t, len(expected), len(actual))

	for idx, expectedStream := range expected {
		actualStream := actual[idx]

		// Expect the same labels.
		require.Equal(t, expectedStream.Labels, actualStream.Labels)

		// Expect the same samples (in this comparison, NaN == NaN and StaleNaN == StaleNaN).
		require.Equal(t, len(expectedStream.Samples), len(actualStream.Samples))

		for idx, expectedSample := range expectedStream.Samples {
			actualSample := actualStream.Samples[idx]
			require.Equal(t, expectedSample.TimestampMs, actualSample.TimestampMs)

			if value.IsStaleNaN(expectedSample.Value) {
				assert.True(t, value.IsStaleNaN(actualSample.Value))
			} else if math.IsNaN(expectedSample.Value) {
				assert.True(t, math.IsNaN(actualSample.Value))
			} else {
				assert.Equal(t, expectedSample.Value, actualSample.Value)
			}
		}
	}
}
