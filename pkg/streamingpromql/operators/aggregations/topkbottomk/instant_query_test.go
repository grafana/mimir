// SPDX-License-Identifier: AGPL-3.0-only

package topkbottomk

import (
	"context"
	"math/rand"
	"slices"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/scalars"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestTopKBottomKInstantQuery_GroupingAndSorting(t *testing.T) {
	testCases := map[string]struct {
		inputSeries []labels.Labels
		grouping    []string
		without     bool

		expectedOutputSeriesGroups [][]labels.Labels
	}{
		"empty input": {
			inputSeries:                []labels.Labels{},
			grouping:                   []string{},
			expectedOutputSeriesGroups: [][]labels.Labels{},
		},
		"all series grouped into single group": {
			inputSeries: []labels.Labels{
				labels.FromStrings("pod", "1"),
				labels.FromStrings("pod", "2"),
			},
			grouping: []string{},
			expectedOutputSeriesGroups: [][]labels.Labels{
				{
					labels.FromStrings("pod", "1"),
					labels.FromStrings("pod", "2"),
				},
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
			expectedOutputSeriesGroups: [][]labels.Labels{
				{
					labels.FromStrings("pod", "1", "group", "A"),
					labels.FromStrings("pod", "2", "group", "A"),
				},
				{
					labels.FromStrings("pod", "1", "group", "B"),
					labels.FromStrings("pod", "2", "group", "B"),
				},
				{
					labels.FromStrings("pod", "2", "group", "C"),
				},
				{
					labels.FromStrings("pod", "1"),
					labels.FromStrings("pod", "2"),
				},
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
			expectedOutputSeriesGroups: [][]labels.Labels{
				{
					labels.FromStrings("pod", "1", "env", "test", "group", "A"),
					labels.FromStrings("pod", "2", "env", "test", "group", "A"),
				},
				{
					labels.FromStrings("pod", "1", "env", "prod", "group", "A"),
					labels.FromStrings("pod", "2", "env", "prod", "group", "A"),
				},
				{
					labels.FromStrings("pod", "1", "env", "test", "group", "B"),
					labels.FromStrings("pod", "2", "env", "test", "group", "B"),
				},
				{
					labels.FromStrings("pod", "1", "env", "prod", "group", "B"),
					labels.FromStrings("pod", "2", "env", "prod", "group", "B"),
				},
				{
					labels.FromStrings("pod", "2", "env", "test", "group", "C"),
				},
				{
					labels.FromStrings("pod", "2", "env", "prod", "group", "C"),
				},
				{
					labels.FromStrings("pod", "1"),
					labels.FromStrings("pod", "2"),
				},
				{
					labels.FromStrings("pod", "1", "env", "prod"),
					labels.FromStrings("pod", "2", "env", "prod"),
				},
				{
					labels.FromStrings("pod", "1", "group", "B"),
					labels.FromStrings("pod", "2", "group", "B"),
				},
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
			expectedOutputSeriesGroups: [][]labels.Labels{
				{
					labels.FromStrings("pod", "1", "env", "test", "group", "A"),
					labels.FromStrings("pod", "2", "env", "test", "group", "A"),
				},
				{
					labels.FromStrings("pod", "1", "env", "prod", "group", "A"),
					labels.FromStrings("pod", "2", "env", "prod", "group", "A"),
				},
				{
					labels.FromStrings("pod", "1", "env", "test", "group", "B"),
					labels.FromStrings("pod", "2", "env", "test", "group", "B"),
				},
				{
					labels.FromStrings("pod", "1", "env", "prod", "group", "B"),
					labels.FromStrings("pod", "2", "env", "prod", "group", "B"),
				},
				{
					labels.FromStrings("pod", "2", "env", "test", "group", "C"),
				},
				{
					labels.FromStrings("pod", "2", "env", "prod", "group", "C"),
				},
				{
					labels.FromStrings("pod", "1"),
					labels.FromStrings("pod", "2"),
				},
				{
					labels.FromStrings("pod", "1", "env", "prod"),
					labels.FromStrings("pod", "2", "env", "prod"),
				},
				{
					labels.FromStrings("pod", "1", "group", "B"),
					labels.FromStrings("pod", "2", "group", "B"),
				},
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
			expectedOutputSeriesGroups: [][]labels.Labels{
				{
					labels.FromStrings("pod", "1", "group", "A"),
					labels.FromStrings("pod", "2", "group", "A"),
				},
				{
					labels.FromStrings("pod", "1", "group", "B"),
					labels.FromStrings("pod", "2", "group", "B"),
				},
				{
					labels.FromStrings("pod", "2", "group", "C"),
				},
				{
					labels.FromStrings("pod", "1"),
					labels.FromStrings("pod", "2"),
				},
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
			expectedOutputSeriesGroups: [][]labels.Labels{
				{
					labels.FromStrings("pod", "1", "foo", "1", "env", "test", "group", "A"),
					labels.FromStrings("pod", "2", "foo", "1", "env", "test", "group", "A"),
				},
				{
					labels.FromStrings("pod", "1", "foo", "1", "env", "prod", "group", "A"),
					labels.FromStrings("pod", "2", "foo", "0", "env", "prod", "group", "A"),
				},
				{
					labels.FromStrings("pod", "1", "foo", "1", "env", "test", "group", "B"),
					labels.FromStrings("pod", "2", "foo", "0", "env", "test", "group", "B"),
				},
				{
					labels.FromStrings("pod", "1", "foo", "0", "env", "prod", "group", "B"),
					labels.FromStrings("pod", "2", "foo", "0", "env", "prod", "group", "B"),
				},
				{
					labels.FromStrings("pod", "2", "foo", "1", "env", "test", "group", "C"),
				},
				{
					labels.FromStrings("pod", "2", "foo", "0", "env", "prod", "group", "C"),
				},
				{
					labels.FromStrings("group", "D"),
				},
				{
					labels.FromStrings("group", "D", "something-else", "yes"),
				},
				{
					labels.FromStrings("pod", "1", "foo", "0", "env", "prod"),
					labels.FromStrings("pod", "2", "foo", "1", "env", "prod"),
				},
				{
					labels.FromStrings("pod", "1", "foo", "0", "group", "B"),
					labels.FromStrings("pod", "2", "foo", "1", "group", "B"),
				},
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
			expectedOutputSeriesGroups: [][]labels.Labels{
				{
					labels.FromStrings("pod", "1", "foo", "1", "env", "test", "group", "A"),
					labels.FromStrings("pod", "2", "foo", "1", "env", "test", "group", "A"),
				},
				{
					labels.FromStrings("pod", "1", "foo", "1", "env", "prod", "group", "A"),
					labels.FromStrings("pod", "2", "foo", "0", "env", "prod", "group", "A"),
				},
				{
					labels.FromStrings("pod", "1", "foo", "1", "env", "test", "group", "B"),
					labels.FromStrings("pod", "2", "foo", "0", "env", "test", "group", "B"),
				},
				{
					labels.FromStrings("pod", "1", "foo", "0", "env", "prod", "group", "B"),
					labels.FromStrings("pod", "2", "foo", "0", "env", "prod", "group", "B"),
				},
				{
					labels.FromStrings("pod", "2", "foo", "1", "env", "test", "group", "C"),
				},
				{
					labels.FromStrings("pod", "2", "foo", "0", "env", "prod", "group", "C"),
				},
				{
					labels.FromStrings("group", "D"),
				},
				{
					labels.FromStrings("group", "D", "something-else", "yes"),
				},
				{
					labels.FromStrings("pod", "1", "foo", "0", "env", "prod"),
					labels.FromStrings("pod", "2", "foo", "1", "env", "prod"),
				},
				{
					labels.FromStrings("pod", "1", "foo", "0", "group", "B"),
					labels.FromStrings("pod", "2", "foo", "1", "group", "B"),
				},
			},
		},
	}

	seed := rand.Int63()
	t.Logf("Using seed %v\n", seed)
	rng := rand.New(rand.NewSource(seed))

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			allExpectedOutputSeries := make([]labels.Labels, 0, len(testCase.inputSeries))

			for _, group := range testCase.expectedOutputSeriesGroups {
				allExpectedOutputSeries = append(allExpectedOutputSeries, group...)
			}

			require.ElementsMatch(t, allExpectedOutputSeries, testCase.inputSeries, "invalid test case: list of input series and all output series do not match")

			timeRange := types.NewInstantQueryTimeRange(timestamp.Time(0))
			memoryConsumptionTracker := limiting.NewMemoryConsumptionTracker(0, nil)

			data := make([]types.InstantVectorSeriesData, 0, len(testCase.inputSeries))
			for idx := range testCase.inputSeries {
				floats, err := types.FPointSlicePool.Get(1, memoryConsumptionTracker)
				require.NoError(t, err)

				floats = append(floats, promql.FPoint{T: 0, F: float64(idx)})
				data = append(data, types.InstantVectorSeriesData{
					Floats: floats,
				})
			}

			// Randomly shuffle the input series, to ensure the sorting is done based on the value.
			rng.Shuffle(len(testCase.inputSeries), func(i, j int) {
				testCase.inputSeries[i], testCase.inputSeries[j] = testCase.inputSeries[j], testCase.inputSeries[i]
				data[i], data[j] = data[j], data[i]
			})

			o := New(
				&operators.TestOperator{Series: testCase.inputSeries, Data: data},
				&scalars.ScalarConstant{Value: 2, TimeRange: timeRange, MemoryConsumptionTracker: memoryConsumptionTracker},
				timeRange,
				testCase.grouping,
				testCase.without,
				false,
				memoryConsumptionTracker,
				nil,
				posrange.PositionRange{Start: 0, End: 10},
			)

			outputSeries, err := o.SeriesMetadata(context.Background())
			require.NoError(t, err)

			require.ElementsMatch(t, outputSeries, testutils.LabelsToSeriesMetadata(testCase.inputSeries), "output does not contain same series as input")
			alreadyUsed := make([]bool, len(outputSeries))

			// topk and bottomk only guarantee that series within a group appear together and in value order, but do not guarantee
			// any particular order for the groups. So we have to accommodate that in this test.
			for _, group := range testCase.expectedOutputSeriesGroups {
				// Find the first output series that matches the first expected series for this group.
				firstSeriesIndex := slices.IndexFunc(outputSeries, func(m types.SeriesMetadata) bool {
					return labels.Equal(m.Labels, group[0])
				})

				require.NotEqualf(t, -1, firstSeriesIndex, "could not find first series for group %v in output %v", group, outputSeries)
				require.Falsef(t, alreadyUsed[firstSeriesIndex], "output series at index %v matches multiple groups", firstSeriesIndex)
				alreadyUsed[firstSeriesIndex] = true

				for i, s := range group[1:] {
					expectedSeriesIndex := firstSeriesIndex + i + 1
					require.Equalf(t, s, outputSeries[expectedSeriesIndex].Labels, "series at index %v in group %v was not at index %v expected in output %v", i+1, group, expectedSeriesIndex, outputSeries)

					require.Falsef(t, alreadyUsed[expectedSeriesIndex], "output series at index %v matches multiple groups", expectedSeriesIndex)
					alreadyUsed[expectedSeriesIndex] = true
				}
			}
		})
	}
}
