// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"
	"slices"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
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
				Inner:    &testOperator{series: testCase.inputSeries},
				Grouping: testCase.grouping,
			}

			outputSeries, err := aggregator.SeriesMetadata(context.Background())
			require.NoError(t, err)
			require.Equal(t, labelsToSeriesMetadata(testCase.expectedOutputSeriesOrder), outputSeries)
		})
	}
}

func TestAggregation_GroupLabelling(t *testing.T) {
	testCases := map[string]struct {
		grouping             []string
		without              bool
		inputSeries          labels.Labels
		expectedOutputSeries labels.Labels
	}{
		"grouping to a single series": {
			grouping:             []string{},
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric", "env", "prod"),
			expectedOutputSeries: labels.EmptyLabels(),
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
		"grouping with 'by', multiple grouping labels, input has all grouping labels": {
			grouping:             []string{"cluster", "env"},
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric", "env", "prod", "cluster", "cluster-1", "foo", "bar"),
			expectedOutputSeries: labels.FromStrings("env", "prod", "cluster", "cluster-1"),
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
		"grouping with 'without', multiple grouping labels, input has all grouping labels": {
			grouping:             []string{"cluster", "env"},
			without:              true,
			inputSeries:          labels.FromStrings(labels.MetricName, "my_metric", "env", "prod", "cluster", "cluster-1", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			require.True(t, slices.IsSorted(testCase.grouping), "invalid test case: grouping labels must be sorted")

			aggregator := &Aggregation{
				Grouping: testCase.grouping,
				Without:  testCase.without,
			}

			labelsFunc := aggregator.seriesToGroupLabelsFunc()
			stringFunc := aggregator.seriesToGroupLabelsStringFunc()

			actualLabels := labelsFunc(testCase.inputSeries)
			actualString := stringFunc(testCase.inputSeries)

			require.Equal(t, testCase.expectedOutputSeries, actualLabels)
			require.Equal(t, testCase.expectedOutputSeries.String(), "{"+actualString+"}")
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
