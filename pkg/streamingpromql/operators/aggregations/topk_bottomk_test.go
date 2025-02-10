// SPDX-License-Identifier: AGPL-3.0-only

package aggregations

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/scalars"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestTopKBottomK_GroupingAndSorting(t *testing.T) {
	testCases := map[string]struct {
		inputSeries []labels.Labels
		grouping    []string
		without     bool

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
				labels.FromStrings("pod", "1"),
				labels.FromStrings("pod", "2"),
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
				labels.FromStrings("pod", "1", "group", "B"),
				labels.FromStrings("pod", "2", "group", "B"),
				labels.FromStrings("pod", "1", "group", "A"),
				labels.FromStrings("pod", "2", "group", "A"),
				labels.FromStrings("pod", "1", "group", "C"),
				labels.FromStrings("pod", "2", "group", "C"),
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
				labels.FromStrings("pod", "1", "group", "B"),
				labels.FromStrings("pod", "2", "group", "B"),
				labels.FromStrings("pod", "1", "group", "A"),
				labels.FromStrings("pod", "2", "group", "A"),
				labels.FromStrings("pod", "1", "group", "C"),
				labels.FromStrings("pod", "2", "group", "C"),
			},
		},

		"grouping with 'by', single grouping label": {
			inputSeries: []labels.Labels{
				labels.FromStrings("pod", "1", "group", "A"),
				labels.FromStrings("pod", "1", "group", "B"),
				labels.FromStrings("pod", "2", "group", "B"),
				labels.FromStrings("pod", "1"), // Test a series that doesn't have the grouping label.
				labels.FromStrings("pod", "2", "group", "A"),
				labels.FromStrings("pod", "2", "group", "C"),
				labels.FromStrings("pod", "2"),
			},
			grouping: []string{"group"},
			expectedOutputSeriesOrder: []labels.Labels{
				// Should sort so that the group that is completed first is returned first.
				labels.FromStrings("pod", "1", "group", "B"),
				labels.FromStrings("pod", "2", "group", "B"),
				labels.FromStrings("pod", "1", "group", "A"),
				labels.FromStrings("pod", "2", "group", "A"),
				labels.FromStrings("pod", "2", "group", "C"),
				labels.FromStrings("pod", "1"),
				labels.FromStrings("pod", "2"),
			},
		},

		"grouping with 'by', multiple grouping labels": {
			inputSeries: []labels.Labels{
				labels.FromStrings("pod", "1", "env", "prod", "group", "A"),
				labels.FromStrings("pod", "1", "env", "prod", "group", "B"),
				labels.FromStrings("pod", "2", "env", "test", "group", "C"),
				labels.FromStrings("pod", "2", "env", "prod", "group", "B"),
				labels.FromStrings("pod", "1"), // Test some series that have none of the grouping labels.
				labels.FromStrings("pod", "2"),
				labels.FromStrings("pod", "1", "env", "test", "group", "A"),
				labels.FromStrings("pod", "1", "env", "prod"), // Test some series that have some of the grouping labels.
				labels.FromStrings("pod", "2", "env", "test", "group", "A"),
				labels.FromStrings("pod", "2", "env", "prod", "group", "A"),
				labels.FromStrings("pod", "1", "env", "test", "group", "B"),
				labels.FromStrings("pod", "1", "group", "B"),
				labels.FromStrings("pod", "2", "env", "prod"),
				labels.FromStrings("pod", "2", "env", "prod", "group", "C"),
				labels.FromStrings("pod", "2", "group", "B"),
				labels.FromStrings("pod", "2", "env", "test", "group", "B"),
			},
			grouping: []string{"env", "group"},
			expectedOutputSeriesOrder: []labels.Labels{
				// Should sort so that the group that is completed first is returned first.
				labels.FromStrings("pod", "2", "env", "test", "group", "C"),
				labels.FromStrings("pod", "1", "env", "prod", "group", "B"),
				labels.FromStrings("pod", "2", "env", "prod", "group", "B"),
				labels.FromStrings("pod", "1"),
				labels.FromStrings("pod", "2"),
				labels.FromStrings("pod", "1", "env", "test", "group", "A"),
				labels.FromStrings("pod", "2", "env", "test", "group", "A"),
				labels.FromStrings("pod", "1", "env", "prod", "group", "A"),
				labels.FromStrings("pod", "2", "env", "prod", "group", "A"),
				labels.FromStrings("pod", "1", "env", "prod"),
				labels.FromStrings("pod", "2", "env", "prod"),
				labels.FromStrings("pod", "2", "env", "prod", "group", "C"),
				labels.FromStrings("pod", "1", "group", "B"),
				labels.FromStrings("pod", "2", "group", "B"),
				labels.FromStrings("pod", "1", "env", "test", "group", "B"),
				labels.FromStrings("pod", "2", "env", "test", "group", "B"),
			},
		},

		"grouping with 'by', multiple grouping labels in alternative order": {
			inputSeries: []labels.Labels{
				labels.FromStrings("pod", "1", "env", "prod", "group", "A"),
				labels.FromStrings("pod", "1", "env", "prod", "group", "B"),
				labels.FromStrings("pod", "2", "env", "test", "group", "C"),
				labels.FromStrings("pod", "2", "env", "prod", "group", "B"),
				labels.FromStrings("pod", "1"), // Test some series that have none of the grouping labels.
				labels.FromStrings("pod", "2"),
				labels.FromStrings("pod", "1", "env", "test", "group", "A"),
				labels.FromStrings("pod", "1", "env", "prod"), // Test some series that have some of the grouping labels.
				labels.FromStrings("pod", "2", "env", "test", "group", "A"),
				labels.FromStrings("pod", "2", "env", "prod", "group", "A"),
				labels.FromStrings("pod", "1", "env", "test", "group", "B"),
				labels.FromStrings("pod", "1", "group", "B"),
				labels.FromStrings("pod", "2", "env", "prod"),
				labels.FromStrings("pod", "2", "env", "prod", "group", "C"),
				labels.FromStrings("pod", "2", "group", "B"),
				labels.FromStrings("pod", "2", "env", "test", "group", "B"),
			},
			grouping: []string{"group", "env"},
			expectedOutputSeriesOrder: []labels.Labels{
				// Should sort so that the group that is completed first is returned first.
				labels.FromStrings("pod", "2", "env", "test", "group", "C"),
				labels.FromStrings("pod", "1", "env", "prod", "group", "B"),
				labels.FromStrings("pod", "2", "env", "prod", "group", "B"),
				labels.FromStrings("pod", "1"),
				labels.FromStrings("pod", "2"),
				labels.FromStrings("pod", "1", "env", "test", "group", "A"),
				labels.FromStrings("pod", "2", "env", "test", "group", "A"),
				labels.FromStrings("pod", "1", "env", "prod", "group", "A"),
				labels.FromStrings("pod", "2", "env", "prod", "group", "A"),
				labels.FromStrings("pod", "1", "env", "prod"),
				labels.FromStrings("pod", "2", "env", "prod"),
				labels.FromStrings("pod", "2", "env", "prod", "group", "C"),
				labels.FromStrings("pod", "1", "group", "B"),
				labels.FromStrings("pod", "2", "group", "B"),
				labels.FromStrings("pod", "1", "env", "test", "group", "B"),
				labels.FromStrings("pod", "2", "env", "test", "group", "B"),
			},
		},

		"grouping with 'without', single grouping label": {
			inputSeries: []labels.Labels{
				labels.FromStrings("pod", "1", "group", "A"),
				labels.FromStrings("pod", "1", "group", "B"),
				labels.FromStrings("pod", "2", "group", "B"),
				labels.FromStrings("pod", "1"), // Test a series that doesn't have the grouping label.
				labels.FromStrings("pod", "2", "group", "A"),
				labels.FromStrings("pod", "2", "group", "C"),
				labels.FromStrings("pod", "2"),
			},
			grouping: []string{"pod"},
			without:  true,
			expectedOutputSeriesOrder: []labels.Labels{
				// Should sort so that the group that is completed first is returned first.
				labels.FromStrings("pod", "1", "group", "B"),
				labels.FromStrings("pod", "2", "group", "B"),
				labels.FromStrings("pod", "1", "group", "A"),
				labels.FromStrings("pod", "2", "group", "A"),
				labels.FromStrings("pod", "2", "group", "C"),
				labels.FromStrings("pod", "1"),
				labels.FromStrings("pod", "2"),
			},
		},

		"grouping with 'without', multiple grouping labels": {
			inputSeries: []labels.Labels{
				labels.FromStrings("pod", "1", "foo", "1", "env", "prod", "group", "A"),
				labels.FromStrings("pod", "1", "foo", "0", "env", "prod", "group", "B"),
				labels.FromStrings("pod", "2", "foo", "1", "env", "test", "group", "C"),
				labels.FromStrings("pod", "2", "foo", "0", "env", "prod", "group", "B"),
				labels.FromStrings("group", "D", "something-else", "yes"), // Test some series that have none of the grouping labels.
				labels.FromStrings("group", "D"),
				labels.FromStrings("pod", "1", "foo", "1", "env", "test", "group", "A"),
				labels.FromStrings("pod", "1", "foo", "0", "env", "prod"), // Test some series that have some of the grouping labels.
				labels.FromStrings("pod", "2", "foo", "1", "env", "test", "group", "A"),
				labels.FromStrings("pod", "2", "foo", "0", "env", "prod", "group", "A"),
				labels.FromStrings("pod", "1", "foo", "1", "env", "test", "group", "B"),
				labels.FromStrings("pod", "1", "foo", "0", "group", "B"),
				labels.FromStrings("pod", "2", "foo", "1", "env", "prod"),
				labels.FromStrings("pod", "2", "foo", "0", "env", "prod", "group", "C"),
				labels.FromStrings("pod", "2", "foo", "1", "group", "B"),
				labels.FromStrings("pod", "2", "foo", "0", "env", "test", "group", "B"),
			},
			grouping: []string{"pod", "foo"},
			without:  true,
			expectedOutputSeriesOrder: []labels.Labels{
				// Should sort so that the group that is completed first is returned first.
				labels.FromStrings("pod", "2", "foo", "1", "env", "test", "group", "C"),
				labels.FromStrings("pod", "1", "foo", "0", "env", "prod", "group", "B"),
				labels.FromStrings("pod", "2", "foo", "0", "env", "prod", "group", "B"),
				labels.FromStrings("group", "D", "something-else", "yes"),
				labels.FromStrings("group", "D"),
				labels.FromStrings("pod", "1", "foo", "1", "env", "test", "group", "A"),
				labels.FromStrings("pod", "2", "foo", "1", "env", "test", "group", "A"),
				labels.FromStrings("pod", "1", "foo", "1", "env", "prod", "group", "A"),
				labels.FromStrings("pod", "2", "foo", "0", "env", "prod", "group", "A"),
				labels.FromStrings("pod", "1", "foo", "0", "env", "prod"),
				labels.FromStrings("pod", "2", "foo", "1", "env", "prod"),
				labels.FromStrings("pod", "2", "foo", "0", "env", "prod", "group", "C"),
				labels.FromStrings("pod", "1", "foo", "0", "group", "B"),
				labels.FromStrings("pod", "2", "foo", "1", "group", "B"),
				labels.FromStrings("pod", "1", "foo", "1", "env", "test", "group", "B"),
				labels.FromStrings("pod", "2", "foo", "0", "env", "test", "group", "B"),
			},
		},

		"grouping with 'without', multiple grouping labels in alternative order": {
			inputSeries: []labels.Labels{
				labels.FromStrings("pod", "1", "foo", "1", "env", "prod", "group", "A"),
				labels.FromStrings("pod", "1", "foo", "0", "env", "prod", "group", "B"),
				labels.FromStrings("pod", "2", "foo", "1", "env", "test", "group", "C"),
				labels.FromStrings("pod", "2", "foo", "0", "env", "prod", "group", "B"),
				labels.FromStrings("group", "D", "something-else", "yes"), // Test some series that have none of the grouping labels.
				labels.FromStrings("group", "D"),
				labels.FromStrings("pod", "1", "foo", "1", "env", "test", "group", "A"),
				labels.FromStrings("pod", "1", "foo", "0", "env", "prod"), // Test some series that have some of the grouping labels.
				labels.FromStrings("pod", "2", "foo", "1", "env", "test", "group", "A"),
				labels.FromStrings("pod", "2", "foo", "0", "env", "prod", "group", "A"),
				labels.FromStrings("pod", "1", "foo", "1", "env", "test", "group", "B"),
				labels.FromStrings("pod", "1", "foo", "0", "group", "B"),
				labels.FromStrings("pod", "2", "foo", "1", "env", "prod"),
				labels.FromStrings("pod", "2", "foo", "0", "env", "prod", "group", "C"),
				labels.FromStrings("pod", "2", "foo", "1", "group", "B"),
				labels.FromStrings("pod", "2", "foo", "0", "env", "test", "group", "B"),
			},
			grouping: []string{"foo", "pod"},
			without:  true,
			expectedOutputSeriesOrder: []labels.Labels{
				// Should sort so that the group that is completed first is returned first.
				labels.FromStrings("pod", "2", "foo", "1", "env", "test", "group", "C"),
				labels.FromStrings("pod", "1", "foo", "0", "env", "prod", "group", "B"),
				labels.FromStrings("pod", "2", "foo", "0", "env", "prod", "group", "B"),
				labels.FromStrings("group", "D", "something-else", "yes"),
				labels.FromStrings("group", "D"),
				labels.FromStrings("pod", "1", "foo", "1", "env", "test", "group", "A"),
				labels.FromStrings("pod", "2", "foo", "1", "env", "test", "group", "A"),
				labels.FromStrings("pod", "1", "foo", "1", "env", "prod", "group", "A"),
				labels.FromStrings("pod", "2", "foo", "0", "env", "prod", "group", "A"),
				labels.FromStrings("pod", "1", "foo", "0", "env", "prod"),
				labels.FromStrings("pod", "2", "foo", "1", "env", "prod"),
				labels.FromStrings("pod", "2", "foo", "0", "env", "prod", "group", "C"),
				labels.FromStrings("pod", "1", "foo", "0", "group", "B"),
				labels.FromStrings("pod", "2", "foo", "1", "group", "B"),
				labels.FromStrings("pod", "1", "foo", "1", "env", "test", "group", "B"),
				labels.FromStrings("pod", "2", "foo", "0", "env", "test", "group", "B"),
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			timeRange := types.NewRangeQueryTimeRange(timestamp.Time(0), timestamp.Time(0).Add(2*time.Minute), time.Minute)
			memoryConsumptionTracker := limiting.NewMemoryConsumptionTracker(0, nil)

			o := NewTopKBottomK(
				&operators.TestOperator{Series: testCase.inputSeries},
				&scalars.ScalarConstant{Value: 2, TimeRange: timeRange, MemoryConsumptionTracker: memoryConsumptionTracker},
				timeRange,
				testCase.grouping,
				testCase.without,
				true,
				memoryConsumptionTracker,
				nil,
				posrange.PositionRange{Start: 0, End: 10},
			)

			outputSeries, err := o.SeriesMetadata(context.Background())
			require.NoError(t, err)
			require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedOutputSeriesOrder), outputSeries)
		})
	}
}
