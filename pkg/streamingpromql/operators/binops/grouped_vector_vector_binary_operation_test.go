// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestGroupedVectorVectorBinaryOperation_OutputSeriesSorting(t *testing.T) {
	testCases := map[string]struct {
		leftSeries  []labels.Labels
		rightSeries []labels.Labels

		matching   parser.VectorMatching
		op         parser.ItemType
		returnBool bool

		expectedOutputSeries []labels.Labels
	}{
		"no series on either side": {
			leftSeries:  []labels.Labels{},
			rightSeries: []labels.Labels{},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardManyToOne},

			expectedOutputSeries: []labels.Labels{},
		},

		"no series on left side": {
			leftSeries: []labels.Labels{},
			rightSeries: []labels.Labels{
				labels.FromStrings("series", "a"),
			},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardManyToOne},

			expectedOutputSeries: []labels.Labels{},
		},

		"no series on right side": {
			leftSeries: []labels.Labels{
				labels.FromStrings("series", "a"),
			},
			rightSeries: []labels.Labels{},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardManyToOne},

			expectedOutputSeries: []labels.Labels{},
		},

		"single series on each side matched and both sides' series are in the same order": {
			leftSeries: []labels.Labels{
				labels.FromStrings(labels.MetricName, "left", "group", "a"),
				labels.FromStrings(labels.MetricName, "left", "group", "b"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(labels.MetricName, "right", "group", "a"),
				labels.FromStrings(labels.MetricName, "right", "group", "b"),
			},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardManyToOne, MatchingLabels: []string{"group"}, On: true},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "a"),
				labels.FromStrings("group", "b"),
			},
		},

		"single series on each side matched and both sides' series are in different order with group_left": {
			leftSeries: []labels.Labels{
				labels.FromStrings(labels.MetricName, "left", "group", "a"),
				labels.FromStrings(labels.MetricName, "left", "group", "b"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(labels.MetricName, "right", "group", "b"),
				labels.FromStrings(labels.MetricName, "right", "group", "a"),
			},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardManyToOne, MatchingLabels: []string{"group"}, On: true},

			// Should be sorted to avoid buffering "many" side.
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "a"),
				labels.FromStrings("group", "b"),
			},
		},

		"single series on each side matched and both sides' series are in different order with group_right": {
			leftSeries: []labels.Labels{
				labels.FromStrings(labels.MetricName, "left", "group", "a"),
				labels.FromStrings(labels.MetricName, "left", "group", "b"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(labels.MetricName, "right", "group", "b"),
				labels.FromStrings(labels.MetricName, "right", "group", "a"),
			},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardOneToMany, MatchingLabels: []string{"group"}, On: true},

			// Should be sorted to avoid buffering "many" side.
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "b"),
				labels.FromStrings("group", "a"),
			},
		},

		"multiple series on left side match to a single series on right side with group_left": {
			leftSeries: []labels.Labels{
				labels.FromStrings(labels.MetricName, "left", "group", "a", "idx", "1"),
				labels.FromStrings(labels.MetricName, "left", "group", "a", "idx", "2"),
				labels.FromStrings(labels.MetricName, "left", "group", "a", "idx", "3"),
				labels.FromStrings(labels.MetricName, "left", "group", "b", "idx", "3"),
				labels.FromStrings(labels.MetricName, "left", "group", "b", "idx", "1"),
				labels.FromStrings(labels.MetricName, "left", "group", "b", "idx", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(labels.MetricName, "right", "group", "b"),
				labels.FromStrings(labels.MetricName, "right", "group", "a"),
			},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardManyToOne, MatchingLabels: []string{"group"}, On: true},

			// Should be sorted to avoid buffering "many" side.
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "a", "idx", "1"),
				labels.FromStrings("group", "a", "idx", "2"),
				labels.FromStrings("group", "a", "idx", "3"),
				labels.FromStrings("group", "b", "idx", "3"),
				labels.FromStrings("group", "b", "idx", "1"),
				labels.FromStrings("group", "b", "idx", "2"),
			},
		},

		"multiple series on left side match to a single series on right side with group_right": {
			leftSeries: []labels.Labels{
				labels.FromStrings(labels.MetricName, "left", "group", "a", "idx", "1"),
				labels.FromStrings(labels.MetricName, "left", "group", "a", "idx", "2"),
				labels.FromStrings(labels.MetricName, "left", "group", "a", "idx", "3"),
				labels.FromStrings(labels.MetricName, "left", "group", "b", "idx", "3"),
				labels.FromStrings(labels.MetricName, "left", "group", "b", "idx", "1"),
				labels.FromStrings(labels.MetricName, "left", "group", "b", "idx", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(labels.MetricName, "right", "group", "b"),
				labels.FromStrings(labels.MetricName, "right", "group", "a"),
			},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardOneToMany, MatchingLabels: []string{"group"}, On: true},

			// Should be sorted to avoid buffering "many" side.
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "b"),
				labels.FromStrings("group", "a"),
			},
		},

		"single series on left side match to multiple series on right side with group_left": {
			leftSeries: []labels.Labels{
				labels.FromStrings(labels.MetricName, "left", "group", "a"),
				labels.FromStrings(labels.MetricName, "left", "group", "b"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(labels.MetricName, "right", "group", "b", "idx", "1"),
				labels.FromStrings(labels.MetricName, "right", "group", "b", "idx", "2"),
				labels.FromStrings(labels.MetricName, "right", "group", "b", "idx", "3"),
				labels.FromStrings(labels.MetricName, "right", "group", "a", "idx", "3"),
				labels.FromStrings(labels.MetricName, "right", "group", "a", "idx", "1"),
				labels.FromStrings(labels.MetricName, "right", "group", "a", "idx", "2"),
			},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardManyToOne, MatchingLabels: []string{"group"}, On: true},

			// Should be sorted to avoid buffering "many" side.
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "a"),
				labels.FromStrings("group", "b"),
			},
		},

		"single series on left side match to multiple series on right side with group_right": {
			leftSeries: []labels.Labels{
				labels.FromStrings(labels.MetricName, "left", "group", "a"),
				labels.FromStrings(labels.MetricName, "left", "group", "b"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(labels.MetricName, "right", "group", "b", "idx", "1"),
				labels.FromStrings(labels.MetricName, "right", "group", "b", "idx", "2"),
				labels.FromStrings(labels.MetricName, "right", "group", "b", "idx", "3"),
				labels.FromStrings(labels.MetricName, "right", "group", "a", "idx", "3"),
				labels.FromStrings(labels.MetricName, "right", "group", "a", "idx", "1"),
				labels.FromStrings(labels.MetricName, "right", "group", "a", "idx", "2"),
			},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardOneToMany, MatchingLabels: []string{"group"}, On: true},

			// Should be sorted to avoid buffering "many" side.
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "b", "idx", "1"),
				labels.FromStrings("group", "b", "idx", "2"),
				labels.FromStrings("group", "b", "idx", "3"),
				labels.FromStrings("group", "a", "idx", "3"),
				labels.FromStrings("group", "a", "idx", "1"),
				labels.FromStrings("group", "a", "idx", "2"),
			},
		},

		"multiple series on left side match to multiple series on right side with group_left": {
			leftSeries: []labels.Labels{
				labels.FromStrings(labels.MetricName, "left", "group", "a", "idx_left", "1"),
				labels.FromStrings(labels.MetricName, "left", "group", "b", "idx_left", "3"),
				labels.FromStrings(labels.MetricName, "left", "group", "a", "idx_left", "2"),
				labels.FromStrings(labels.MetricName, "left", "group", "a", "idx_left", "3"),
				labels.FromStrings(labels.MetricName, "left", "group", "b", "idx_left", "1"),
				labels.FromStrings(labels.MetricName, "left", "group", "b", "idx_left", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(labels.MetricName, "right", "group", "b", "idx_right", "4"),
				labels.FromStrings(labels.MetricName, "right", "group", "b", "idx_right", "5"),
				labels.FromStrings(labels.MetricName, "right", "group", "b", "idx_right", "6"),
				labels.FromStrings(labels.MetricName, "right", "group", "a", "idx_right", "5"),
				labels.FromStrings(labels.MetricName, "right", "group", "a", "idx_right", "4"),
				labels.FromStrings(labels.MetricName, "right", "group", "a", "idx_right", "6"),
			},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardManyToOne, MatchingLabels: []string{"group"}, On: true},

			// Should be sorted to avoid buffering "many" side.
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "a", "idx_left", "1"),
				labels.FromStrings("group", "b", "idx_left", "3"),
				labels.FromStrings("group", "a", "idx_left", "2"),
				labels.FromStrings("group", "a", "idx_left", "3"),
				labels.FromStrings("group", "b", "idx_left", "1"),
				labels.FromStrings("group", "b", "idx_left", "2"),
			},
		},

		"multiple series on left side match to multiple series on right side with group_right": {
			leftSeries: []labels.Labels{
				labels.FromStrings(labels.MetricName, "left", "group", "a", "idx_left", "1"),
				labels.FromStrings(labels.MetricName, "left", "group", "b", "idx_left", "3"),
				labels.FromStrings(labels.MetricName, "left", "group", "a", "idx_left", "2"),
				labels.FromStrings(labels.MetricName, "left", "group", "a", "idx_left", "3"),
				labels.FromStrings(labels.MetricName, "left", "group", "b", "idx_left", "1"),
				labels.FromStrings(labels.MetricName, "left", "group", "b", "idx_left", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(labels.MetricName, "right", "group", "b", "idx_right", "4"),
				labels.FromStrings(labels.MetricName, "right", "group", "b", "idx_right", "5"),
				labels.FromStrings(labels.MetricName, "right", "group", "b", "idx_right", "6"),
				labels.FromStrings(labels.MetricName, "right", "group", "a", "idx_right", "5"),
				labels.FromStrings(labels.MetricName, "right", "group", "a", "idx_right", "4"),
				labels.FromStrings(labels.MetricName, "right", "group", "a", "idx_right", "6"),
			},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardOneToMany, MatchingLabels: []string{"group"}, On: true},

			// Should be sorted to avoid buffering "many" side.
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "b", "idx_right", "4"),
				labels.FromStrings("group", "b", "idx_right", "5"),
				labels.FromStrings("group", "b", "idx_right", "6"),
				labels.FromStrings("group", "a", "idx_right", "5"),
				labels.FromStrings("group", "a", "idx_right", "4"),
				labels.FromStrings("group", "a", "idx_right", "6"),
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			left := &operators.TestOperator{Series: testCase.leftSeries}
			right := &operators.TestOperator{Series: testCase.rightSeries}

			o, err := NewGroupedVectorVectorBinaryOperation(
				left,
				right,
				testCase.matching,
				testCase.op,
				testCase.returnBool,
				limiting.NewMemoryConsumptionTracker(0, nil),
				nil,
				posrange.PositionRange{},
				types.QueryTimeRange{},
			)

			require.NoError(t, err)

			outputSeries, err := o.SeriesMetadata(context.Background())
			require.NoError(t, err)

			require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedOutputSeries), outputSeries)
		})
	}
}
