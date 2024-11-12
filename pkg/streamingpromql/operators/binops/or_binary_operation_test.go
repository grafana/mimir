// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestOrBinaryOperationSorting(t *testing.T) {
	// The majority of the functionality of OrBinaryOperation is exercised by the tests in the testdata directory.
	// This test exists to exercise the output series sorting functionality, which is complex and may be incorrect
	// but still result in correct query results (eg. if we read and return left side series earlier than strictly
	// necessary).

	testCases := map[string]struct {
		leftSeries  []labels.Labels
		rightSeries []labels.Labels

		expectedOutputSeriesOrder []labels.Labels
	}{
		"no series from either side": {
			leftSeries:  []labels.Labels{},
			rightSeries: []labels.Labels{},

			expectedOutputSeriesOrder: []labels.Labels{},
		},
		"only left series": {
			leftSeries: []labels.Labels{
				labels.FromStrings("series", "1"),
				labels.FromStrings("series", "2"),
			},
			rightSeries: []labels.Labels{},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("series", "1"),
				labels.FromStrings("series", "2"),
			},
		},
		"only right series": {
			leftSeries: []labels.Labels{},
			rightSeries: []labels.Labels{
				labels.FromStrings("series", "1"),
				labels.FromStrings("series", "2"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("series", "1"),
				labels.FromStrings("series", "2"),
			},
		},
		"no matching series on either side": {
			leftSeries: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "3"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("right", "2", "group", "2"),
				labels.FromStrings("right", "4", "group", "4"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("right", "2", "group", "2"),
				labels.FromStrings("right", "4", "group", "4"),
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "3"),
			},
		},
		"single left series returned first": {
			leftSeries: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "1"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "1"),
				labels.FromStrings("left", "3", "group", "2"),
			},
		},
		"multiple left series returned first": {
			leftSeries: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "1"),
				labels.FromStrings("left", "5", "group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "1"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "1"),
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "1"),
				labels.FromStrings("left", "5", "group", "2"),
			},
		},
		"single right series returned first": {
			leftSeries: []labels.Labels{
				labels.FromStrings("left", "4", "group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("right", "1", "group", "1"),
				labels.FromStrings("right", "2", "group", "2"),
				labels.FromStrings("right", "3", "group", "2"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("right", "1", "group", "1"),
				labels.FromStrings("left", "4", "group", "2"),
				labels.FromStrings("right", "2", "group", "2"),
				labels.FromStrings("right", "3", "group", "2"),
			},
		},
		"multiple right series returned first": {
			leftSeries: []labels.Labels{
				labels.FromStrings("left", "4", "group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("right", "1", "group", "1"),
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "3", "group", "2"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("right", "1", "group", "1"),
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("left", "4", "group", "2"),
				labels.FromStrings("right", "3", "group", "2"),
			},
		},
		"single left series returned last": {
			leftSeries: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "1"),
				labels.FromStrings("left", "5", "group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "1"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "1"),
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "1"),
				labels.FromStrings("left", "5", "group", "2"),
			},
		},
		"multiple left series returned last": {
			leftSeries: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "1"),
				labels.FromStrings("left", "5", "group", "2"),
				labels.FromStrings("left", "6", "group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "1"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "1"),
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "1"),
				labels.FromStrings("left", "5", "group", "2"),
				labels.FromStrings("left", "6", "group", "2"),
			},
		},
		"single right series returned last": {
			leftSeries: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "1"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("right", "2", "group", "1"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "1"),
				labels.FromStrings("right", "2", "group", "1"),
			},
		},
		"multiple right series returned last": {
			leftSeries: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "1"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "1"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "1"),
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "1"),
			},
		},
		"group order on left side different to right side": {
			leftSeries: []labels.Labels{
				labels.FromStrings("left", "1", "group", "3"),
				labels.FromStrings("left", "3", "group", "1"),
				labels.FromStrings("left", "5", "group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "2"),
				labels.FromStrings("right", "6", "group", "3"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("left", "1", "group", "3"),
				labels.FromStrings("left", "3", "group", "1"),
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("left", "5", "group", "2"),
				labels.FromStrings("right", "4", "group", "2"),
				labels.FromStrings("right", "6", "group", "3"),
			},
		},
		// OrBinaryOperation does not handle the case where both sides contain series with identical labels, and
		// instead relies on DeduplicateAndMerge to handle merging series with identical labels.
		// Given NewOrBinaryOperation wraps the OrBinaryOperation in a DeduplicateAndMerge, we can still test this
		// here.
		"same series on both sides, one series": {
			leftSeries: []labels.Labels{
				labels.FromStrings("series", "1", "group", "1"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("series", "1", "group", "1"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("series", "1", "group", "1"),
			},
		},
		"same series on both sides, multiple series in same order": {
			leftSeries: []labels.Labels{
				labels.FromStrings("series", "1", "group", "1"),
				labels.FromStrings("series", "2", "group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("series", "1", "group", "1"),
				labels.FromStrings("series", "2", "group", "2"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("series", "1", "group", "1"),
				labels.FromStrings("series", "2", "group", "2"),
			},
		},
		"same series on both sides, multiple series in different order": {
			leftSeries: []labels.Labels{
				labels.FromStrings("series", "1", "group", "1"),
				labels.FromStrings("series", "2", "group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("series", "2", "group", "2"),
				labels.FromStrings("series", "1", "group", "1"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("series", "2", "group", "2"),
				labels.FromStrings("series", "1", "group", "1"),
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			left := &operators.TestOperator{Series: testCase.leftSeries}
			right := &operators.TestOperator{Series: testCase.rightSeries}

			op := NewOrBinaryOperation(
				left,
				right,
				parser.VectorMatching{MatchingLabels: []string{"group"}, On: true},
				limiting.NewMemoryConsumptionTracker(0, nil),
				types.NewInstantQueryTimeRange(timestamp.Time(0)),
				posrange.PositionRange{},
			)

			actualSeriesMetadata, err := op.SeriesMetadata(context.Background())
			require.NoError(t, err)

			expectedSeriesMetadata := testutils.LabelsToSeriesMetadata(testCase.expectedOutputSeriesOrder)
			require.Equal(t, expectedSeriesMetadata, actualSeriesMetadata)
		})
	}
}
