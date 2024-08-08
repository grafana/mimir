// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// Most of the functionality of the aggregation operator is tested through the test scripts in
// pkg/streamingpromql/testdata.
//
// The output sorting behaviour is impossible to test through these scripts, so we instead test it here.
func TestAggregation_ReturnsGroupsFinishedFirstEarliest(t *testing.T) {
	testCases := map[string]struct {
		inputSeries               []labels.Labels
		grouping                  []string
		expectedOutputSeriesOrder []labels.Labels
	}{
		"empty input": {
			inputSeries:               []labels.Labels{},
			grouping:                  []string{},
			expectedOutputSeriesOrder: []labels.Labels{},
		},
		"all series grouped into single group": {
			inputSeries: []labels.Labels{
				labels.FromStrings("pod", "1"),
				labels.FromStrings("pod", "2"),
			},
			grouping: []string{},
			expectedOutputSeriesOrder: []labels.Labels{
				labels.EmptyLabels(),
			},
		},
		"input series already sorted by group": {
			inputSeries: []labels.Labels{
				labels.FromStrings("pod", "1", "group", "B"),
				labels.FromStrings("pod", "2", "group", "B"),
				labels.FromStrings("pod", "1", "group", "A"),
				labels.FromStrings("pod", "2", "group", "A"),
				labels.FromStrings("pod", "1", "group", "C"),
				labels.FromStrings("pod", "2", "group", "C"),
			},
			grouping: []string{"group"},
			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("group", "B"),
				labels.FromStrings("group", "A"),
				labels.FromStrings("group", "C"),
			},
		},
		"input series not sorted by group": {
			inputSeries: []labels.Labels{
				labels.FromStrings("pod", "1", "group", "A"),
				labels.FromStrings("pod", "1", "group", "B"),
				labels.FromStrings("pod", "2", "group", "B"),
				labels.FromStrings("pod", "1", "group", "C"),
				labels.FromStrings("pod", "2", "group", "A"),
				labels.FromStrings("pod", "2", "group", "C"),
			},
			grouping: []string{"group"},
			expectedOutputSeriesOrder: []labels.Labels{
				// Should sort so that the group that is completed first is returned first.
				labels.FromStrings("group", "B"),
				labels.FromStrings("group", "A"),
				labels.FromStrings("group", "C"),
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			aggregator := &Aggregation{
				Inner:       &testOperator{series: testCase.inputSeries},
				Grouping:    testCase.grouping,
				metricNames: &MetricNames{},
			}

			outputSeries, err := aggregator.SeriesMetadata(context.Background())
			require.NoError(t, err)
			require.Equal(t, labelsToSeriesMetadata(testCase.expectedOutputSeriesOrder), outputSeries)
		})
	}
}

func TestAggregation_GroupLabelling(t *testing.T) {
	testCases := map[string]struct {
		grouping                    []string
		without                     bool
		inputSeries                 labels.Labels
		expectedOutputSeries        labels.Labels
		overrideExpectedOutputBytes []byte
	}{
		"grouping to a single series": {
			grouping:                    []string{},
			inputSeries:                 labels.FromStrings(labels.MetricName, "my_metric", "env", "prod"),
			expectedOutputSeries:        labels.EmptyLabels(),
			overrideExpectedOutputBytes: []byte{}, // Special case for grouping to a single series.
		},

		// Grouping with 'by'
		"grouping with 'by', single grouping label, input has only metric name": {
			grouping:             []string{"env"},
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric"),
			expectedOutputSeries: labels.EmptyLabels(),
		},
		"grouping with 'by', single grouping label, input does not have grouping label": {
			grouping:             []string{"env"},
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric", "foo", "bar"),
			expectedOutputSeries: labels.EmptyLabels(),
		},
		"grouping with 'by', single grouping label, input does have grouping label": {
			grouping:             []string{"env"},
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric", "env", "prod", "foo", "bar"),
			expectedOutputSeries: labels.FromStrings("env", "prod"),
		},
		"grouping with 'by', multiple grouping labels, input has only metric name": {
			grouping:             []string{"cluster", "env"},
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric"),
			expectedOutputSeries: labels.EmptyLabels(),
		},
		"grouping with 'by', multiple grouping labels, input does not have any grouping labels": {
			grouping:             []string{"cluster", "env"},
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric", "foo", "bar"),
			expectedOutputSeries: labels.EmptyLabels(),
		},
		"grouping with 'by', multiple grouping labels, input has some grouping labels": {
			grouping:             []string{"cluster", "env"},
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric", "env", "prod", "foo", "bar"),
			expectedOutputSeries: labels.FromStrings("env", "prod"),
		},
		"grouping with 'by', multiple grouping labels, input has superset of grouping labels": {
			grouping:             []string{"cluster", "env"},
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric", "env", "prod", "cluster", "cluster-1", "foo", "bar"),
			expectedOutputSeries: labels.FromStrings("env", "prod", "cluster", "cluster-1"),
		},
		"grouping with 'by', multiple grouping labels, input has all grouping labels and no others": {
			grouping:             []string{"cluster", "env"},
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric", "env", "prod", "cluster", "cluster-1"),
			expectedOutputSeries: labels.FromStrings("env", "prod", "cluster", "cluster-1"),
		},
		"grouping with 'by', unsorted grouping labels": {
			grouping:             []string{"env", "cluster"},
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric", "env", "prod", "cluster", "cluster-1", "foo", "bar"),
			expectedOutputSeries: labels.FromStrings("env", "prod", "cluster", "cluster-1"),
		},
		"grouping with 'by', grouping labels include __name__": {
			grouping:             []string{"cluster", "env", "__name__"},
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric", "env", "prod", "cluster", "cluster-1", "foo", "bar"),
			expectedOutputSeries: labels.FromStrings(labels.MetricName, "my_metric", "env", "prod", "cluster", "cluster-1"),
		},

		// Grouping with 'without'
		"grouping with 'without', single grouping label, input has only metric name": {
			grouping:             []string{"env"},
			without:              true,
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric"),
			expectedOutputSeries: labels.EmptyLabels(),
		},
		"grouping with 'without', single grouping label, input does not have grouping label": {
			grouping:             []string{"env"},
			without:              true,
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric", "a-label", "a-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "f-label", "f-value"),
		},
		"grouping with 'without', single grouping label, input does have grouping label": {
			grouping:             []string{"env"},
			without:              true,
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric", "env", "prod", "a-label", "a-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "f-label", "f-value"),
		},
		"grouping with 'without', multiple grouping labels, input has only metric name": {
			grouping:             []string{"cluster", "env"},
			without:              true,
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric"),
			expectedOutputSeries: labels.EmptyLabels(),
		},
		"grouping with 'without', multiple grouping labels, input does not have any grouping labels": {
			grouping:             []string{"cluster", "env"},
			without:              true,
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
		},
		"grouping with 'without', multiple grouping labels, input has some grouping labels": {
			grouping:             []string{"cluster", "env"},
			without:              true,
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric", "env", "prod", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
		},
		"grouping with 'without', multiple grouping labels, input has superset of grouping labels": {
			grouping:             []string{"cluster", "env"},
			without:              true,
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric", "env", "prod", "cluster", "cluster-1", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
		},
		"grouping with 'without', multiple grouping labels, input has all grouping labels and no others": {
			grouping:             []string{"cluster", "env"},
			without:              true,
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric", "env", "prod", "cluster", "cluster-1"),
			expectedOutputSeries: labels.EmptyLabels(),
		},
		"grouping with 'without', unsorted grouping labels": {
			grouping:             []string{"env", "cluster"},
			without:              true,
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric", "env", "prod", "cluster", "cluster-1", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
		},
		"grouping with 'without', grouping labels include __name__": {
			grouping:             []string{"cluster", "env", "__name__"},
			without:              true,
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
		},
		"grouping with 'without', grouping labels include duplicates": {
			grouping:             []string{"cluster", "env", "cluster"},
			without:              true,
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
		},
		"grouping with 'without', grouping labels include label that sorts before __name__": {
			grouping:             []string{"cluster", "env", "__aaa__"},
			without:              true,
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric", "__aaa__", "foo", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			aggregator := NewAggregation(nil, time.Time{}, time.Time{}, time.Minute, testCase.grouping, testCase.without, nil, nil, posrange.PositionRange{})
			bytesFunc, labelsFunc := aggregator.seriesToGroupFuncs()

			actualLabels := labelsFunc(testCase.inputSeries)
			require.Equal(t, testCase.expectedOutputSeries, actualLabels)

			expectedBytes := string(testCase.overrideExpectedOutputBytes)
			if testCase.overrideExpectedOutputBytes == nil {
				expectedBytes = string(testCase.expectedOutputSeries.Bytes(nil))
			}

			actualBytes := string(bytesFunc(testCase.inputSeries))
			require.Equal(t, expectedBytes, actualBytes)
		})
	}
}

func labelsToSeriesMetadata(lbls []labels.Labels) []types.SeriesMetadata {
	if len(lbls) == 0 {
		return nil
	}

	m := make([]types.SeriesMetadata, len(lbls))

	for i, l := range lbls {
		m[i].Labels = l
	}

	return m
}

// This test is copied from promql/functions_internal_test.go.
// If kahanSumInc can be exported we can remove this.
func TestKahanSumInc(t *testing.T) {
	testCases := map[string]struct {
		first    float64
		second   float64
		expected float64
	}{
		"+Inf + anything = +Inf": {
			first:    math.Inf(1),
			second:   2.0,
			expected: math.Inf(1),
		},
		"-Inf + anything = -Inf": {
			first:    math.Inf(-1),
			second:   2.0,
			expected: math.Inf(-1),
		},
		"+Inf + -Inf = NaN": {
			first:    math.Inf(1),
			second:   math.Inf(-1),
			expected: math.NaN(),
		},
		"NaN + anything = NaN": {
			first:    math.NaN(),
			second:   2,
			expected: math.NaN(),
		},
		"NaN + Inf = NaN": {
			first:    math.NaN(),
			second:   math.Inf(1),
			expected: math.NaN(),
		},
		"NaN + -Inf = NaN": {
			first:    math.NaN(),
			second:   math.Inf(-1),
			expected: math.NaN(),
		},
	}

	runTest := func(t *testing.T, a, b, expected float64) {
		t.Run(fmt.Sprintf("%v + %v = %v", a, b, expected), func(t *testing.T) {
			sum, c := kahanSumInc(b, a, 0)
			result := sum + c

			if math.IsNaN(expected) {
				require.Truef(t, math.IsNaN(result), "expected result to be NaN, but got %v (from %v + %v)", result, sum, c)
			} else {
				require.Equalf(t, expected, result, "expected result to be %v, but got %v (from %v + %v)", expected, result, sum, c)
			}
		})
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			runTest(t, testCase.first, testCase.second, testCase.expected)
			runTest(t, testCase.second, testCase.first, testCase.expected)
		})
	}
}
