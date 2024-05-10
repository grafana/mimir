// SPDX-License-Identifier: AGPL-3.0-only

package operator

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
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

func labelsToSeriesMetadata(lbls []labels.Labels) []SeriesMetadata {
	if len(lbls) == 0 {
		return nil
	}

	m := make([]SeriesMetadata, len(lbls))

	for i, l := range lbls {
		m[i].Labels = l
	}

	return m
}

type testOperator struct {
	series []labels.Labels
}

func (t *testOperator) SeriesMetadata(_ context.Context) ([]SeriesMetadata, error) {
	return labelsToSeriesMetadata(t.series), nil
}

func (t *testOperator) NextSeries(_ context.Context) (InstantVectorSeriesData, error) {
	panic("NextSeries() not supported")
}

func (t *testOperator) Close() {
	panic("Close() not supported")
}
