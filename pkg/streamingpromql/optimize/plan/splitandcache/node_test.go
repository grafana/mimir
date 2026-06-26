// SPDX-License-Identifier: AGPL-3.0-only

package splitandcache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/selectors"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// This test is based on the equivalent test for the middleware logic, TestSplitQueryByInterval.
func TestMaterializeSplit(t *testing.T) {
	timeZero := timestamp.Time(0)

	parseTime := func(s string) time.Time {
		ts, err := time.Parse(time.RFC3339, s)
		require.NoError(t, err)
		return ts
	}

	testCases := []struct {
		timeRange          types.QueryTimeRange
		splitInterval      time.Duration
		expectedTimeRanges []types.QueryTimeRange
	}{
		{
			timeRange: types.NewRangeQueryTimeRange(timeZero, timeZero.Add(time.Hour), 15*time.Second),
			expectedTimeRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(timeZero, timeZero.Add(time.Hour), 15*time.Second),
			},
			splitInterval: 24 * time.Hour,
		},
		{
			timeRange: types.NewRangeQueryTimeRange(timeZero, timeZero.Add(time.Hour), 15*time.Second),
			expectedTimeRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(timeZero, timeZero.Add(time.Hour), 15*time.Second),
			},
			splitInterval: 3 * time.Hour,
		},
		{
			timeRange: types.NewRangeQueryTimeRange(timeZero, timeZero.Add(24*time.Hour), 15*time.Second),
			expectedTimeRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(timeZero, timeZero.Add(24*time.Hour), 15*time.Second),
			},
			splitInterval: 24 * time.Hour,
		},
		{
			timeRange: types.NewRangeQueryTimeRange(timeZero, timeZero.Add(3*time.Hour), 15*time.Second),
			expectedTimeRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(timeZero, timeZero.Add(3*time.Hour), 15*time.Second),
			},
			splitInterval: 3 * time.Hour,
		},
		{
			timeRange: types.NewRangeQueryTimeRange(timeZero, timeZero.Add(2*24*time.Hour), 15*time.Second),
			expectedTimeRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(timeZero, timeZero.Add((24*time.Hour)-(15*time.Second)), 15*time.Second),
				types.NewRangeQueryTimeRange(timeZero.Add(24*time.Hour), timeZero.Add(2*24*time.Hour), 15*time.Second),
			},
			splitInterval: 24 * time.Hour,
		},
		{
			timeRange: types.NewRangeQueryTimeRange(timeZero, timeZero.Add(2*3*time.Hour), 15*time.Second),
			expectedTimeRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(timeZero, timeZero.Add((3*time.Hour)-(15*time.Second)), 15*time.Second),
				types.NewRangeQueryTimeRange(timeZero.Add(3*time.Hour), timeZero.Add(2*3*time.Hour), 15*time.Second),
			},
			splitInterval: 3 * time.Hour,
		},
		{
			timeRange: types.NewRangeQueryTimeRange(timeZero.Add(3*time.Hour), timeZero.Add(3*24*time.Hour), 15*time.Second),
			expectedTimeRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(timeZero.Add(3*time.Hour), timeZero.Add((24*time.Hour)-(15*time.Second)), 15*time.Second),
				types.NewRangeQueryTimeRange(timeZero.Add(24*time.Hour), timeZero.Add((2*24*time.Hour)-(15*time.Second)), 15*time.Second),
				types.NewRangeQueryTimeRange(timeZero.Add(2*24*time.Hour), timeZero.Add(3*24*time.Hour), 15*time.Second),
			},
			splitInterval: 24 * time.Hour,
		},
		{
			timeRange: types.NewRangeQueryTimeRange(timeZero.Add(2*time.Hour), timeZero.Add(3*3*time.Hour), 15*time.Second),
			expectedTimeRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(timeZero.Add(2*time.Hour), timeZero.Add((3*time.Hour)-(15*time.Second)), 15*time.Second),
				types.NewRangeQueryTimeRange(timeZero.Add(3*time.Hour), timeZero.Add((2*3*time.Hour)-(15*time.Second)), 15*time.Second),
				types.NewRangeQueryTimeRange(timeZero.Add(2*3*time.Hour), timeZero.Add(3*3*time.Hour), 15*time.Second),
			},
			splitInterval: 3 * time.Hour,
		},
		{
			timeRange: types.NewRangeQueryTimeRange(parseTime("2021-10-14T23:48:00Z"), parseTime("2021-10-15T00:03:00Z"), 5*time.Minute),
			expectedTimeRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(parseTime("2021-10-14T23:48:00Z"), parseTime("2021-10-15T00:03:00Z"), 5*time.Minute),
			},
			splitInterval: 24 * time.Hour,
		},
		{
			timeRange: types.NewRangeQueryTimeRange(parseTime("2021-10-14T23:48:00Z"), parseTime("2021-10-15T00:00:00Z"), 6*time.Minute),
			expectedTimeRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(parseTime("2021-10-14T23:48:00Z"), parseTime("2021-10-14T23:54:00Z"), 6*time.Minute),
				types.NewRangeQueryTimeRange(parseTime("2021-10-15T00:00:00Z"), parseTime("2021-10-15T00:00:00Z"), 6*time.Minute),
			},
			splitInterval: 24 * time.Hour,
		},
		{
			timeRange: types.NewRangeQueryTimeRange(parseTime("2021-10-14T22:00:00Z"), parseTime("2021-10-17T22:00:00Z"), 24*time.Hour),
			expectedTimeRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(parseTime("2021-10-14T22:00:00Z"), parseTime("2021-10-14T22:00:00Z"), 24*time.Hour),
				types.NewRangeQueryTimeRange(parseTime("2021-10-15T22:00:00Z"), parseTime("2021-10-15T22:00:00Z"), 24*time.Hour),
				types.NewRangeQueryTimeRange(parseTime("2021-10-16T22:00:00Z"), parseTime("2021-10-16T22:00:00Z"), 24*time.Hour),
				types.NewRangeQueryTimeRange(parseTime("2021-10-17T22:00:00Z"), parseTime("2021-10-17T22:00:00Z"), 24*time.Hour),
			},
			splitInterval: 24 * time.Hour,
		},
		{
			timeRange: types.NewRangeQueryTimeRange(parseTime("2021-10-15T00:00:00Z"), parseTime("2021-10-18T00:00:00Z"), 24*time.Hour),
			expectedTimeRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(parseTime("2021-10-15T00:00:00Z"), parseTime("2021-10-15T00:00:00Z"), 24*time.Hour),
				types.NewRangeQueryTimeRange(parseTime("2021-10-16T00:00:00Z"), parseTime("2021-10-16T00:00:00Z"), 24*time.Hour),
				types.NewRangeQueryTimeRange(parseTime("2021-10-17T00:00:00Z"), parseTime("2021-10-17T00:00:00Z"), 24*time.Hour),
				types.NewRangeQueryTimeRange(parseTime("2021-10-18T00:00:00Z"), parseTime("2021-10-18T00:00:00Z"), 24*time.Hour),
			},
			splitInterval: 24 * time.Hour,
		},
		{
			timeRange: types.NewRangeQueryTimeRange(parseTime("2021-10-15T22:00:00Z"), parseTime("2021-10-22T04:00:00Z"), 30*time.Hour),
			expectedTimeRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(parseTime("2021-10-15T22:00:00Z"), parseTime("2021-10-15T22:00:00Z"), 30*time.Hour),
				types.NewRangeQueryTimeRange(parseTime("2021-10-17T04:00:00Z"), parseTime("2021-10-17T04:00:00Z"), 30*time.Hour),
				types.NewRangeQueryTimeRange(parseTime("2021-10-18T10:00:00Z"), parseTime("2021-10-18T10:00:00Z"), 30*time.Hour),
				types.NewRangeQueryTimeRange(parseTime("2021-10-19T16:00:00Z"), parseTime("2021-10-19T16:00:00Z"), 30*time.Hour),
				types.NewRangeQueryTimeRange(parseTime("2021-10-20T22:00:00Z"), parseTime("2021-10-20T22:00:00Z"), 30*time.Hour),
				types.NewRangeQueryTimeRange(parseTime("2021-10-22T04:00:00Z"), parseTime("2021-10-22T04:00:00Z"), 30*time.Hour),
			},
			splitInterval: 24 * time.Hour,
		},
		{
			timeRange: types.NewRangeQueryTimeRange(parseTime("2021-10-15T06:00:00Z"), parseTime("2021-10-17T14:00:00Z"), 12*time.Hour),
			expectedTimeRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(parseTime("2021-10-15T06:00:00Z"), parseTime("2021-10-15T18:00:00Z"), 12*time.Hour),
				types.NewRangeQueryTimeRange(parseTime("2021-10-16T06:00:00Z"), parseTime("2021-10-16T18:00:00Z"), 12*time.Hour),
				types.NewRangeQueryTimeRange(parseTime("2021-10-17T06:00:00Z"), parseTime("2021-10-17T14:00:00Z"), 12*time.Hour),
			},
			splitInterval: 24 * time.Hour,
		},
		{
			timeRange: types.NewRangeQueryTimeRange(parseTime("2021-10-15T06:00:00Z"), parseTime("2021-10-17T18:00:00Z"), 12*time.Hour),
			expectedTimeRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(parseTime("2021-10-15T06:00:00Z"), parseTime("2021-10-15T18:00:00Z"), 12*time.Hour),
				types.NewRangeQueryTimeRange(parseTime("2021-10-16T06:00:00Z"), parseTime("2021-10-16T18:00:00Z"), 12*time.Hour),
				types.NewRangeQueryTimeRange(parseTime("2021-10-17T06:00:00Z"), parseTime("2021-10-17T18:00:00Z"), 12*time.Hour),
			},
			splitInterval: 24 * time.Hour,
		},
		{
			timeRange: types.NewRangeQueryTimeRange(parseTime("2021-10-15T06:00:00Z"), parseTime("2021-10-17T18:00:00Z"), 10*time.Hour),
			expectedTimeRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(parseTime("2021-10-15T06:00:00Z"), parseTime("2021-10-15T16:00:00Z"), 10*time.Hour),
				types.NewRangeQueryTimeRange(parseTime("2021-10-16T02:00:00Z"), parseTime("2021-10-16T22:00:00Z"), 10*time.Hour),
				types.NewRangeQueryTimeRange(parseTime("2021-10-17T08:00:00Z"), parseTime("2021-10-17T18:00:00Z"), 10*time.Hour),
			},
			splitInterval: 24 * time.Hour,
		},
		{
			timeRange: types.NewRangeQueryTimeRange(parseTime("2021-10-15T06:00:00Z"), parseTime("2021-10-17T08:00:00Z"), 10*time.Hour),
			expectedTimeRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(parseTime("2021-10-15T06:00:00Z"), parseTime("2021-10-15T16:00:00Z"), 10*time.Hour),
				types.NewRangeQueryTimeRange(parseTime("2021-10-16T02:00:00Z"), parseTime("2021-10-16T22:00:00Z"), 10*time.Hour),
				types.NewRangeQueryTimeRange(parseTime("2021-10-17T08:00:00Z"), parseTime("2021-10-17T08:00:00Z"), 10*time.Hour),
			},
			splitInterval: 24 * time.Hour,
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%v to %v, step %v, split interval %v", timestamp.Time(testCase.timeRange.StartT).Format(time.RFC3339), timestamp.Time(testCase.timeRange.EndT).Format(time.RFC3339), time.Duration(testCase.timeRange.IntervalMilliseconds)*time.Millisecond, testCase.splitInterval.String()), func(t *testing.T) {
			innerNode := &core.VectorSelector{
				VectorSelectorDetails: &core.VectorSelectorDetails{
					Matchers: []*core.LabelMatcher{{Type: labels.MatchEqual, Name: model.MetricNameLabel, Value: "my_metric"}},
				},
			}

			splitNode := &TimeRangeSplit{
				TimeRangeSplitDetails: &TimeRangeSplitDetails{
					SplitInterval: testCase.splitInterval,
				},
				Inner: innerNode,
			}

			params := &planning.OperatorParameters{
				QueryParameters: &planning.QueryParameters{LookbackDelta: 5 * time.Minute},
			}

			materializer := planning.NewMaterializer(params, map[planning.NodeType]planning.NodeMaterializer{
				planning.NODE_TYPE_VECTOR_SELECTOR:  planning.NodeMaterializerFunc[*core.VectorSelector](core.MaterializeVectorSelector),
				planning.NODE_TYPE_TIME_RANGE_SPLIT: planning.NodeMaterializerFunc[*TimeRangeSplit](MaterializeSplit),
			})

			queryStats, ctx := stats.ContextWithEmptyStats(context.Background())
			resultFactory, err := MaterializeSplit(ctx, splitNode, materializer, testCase.timeRange, params)
			require.NoError(t, err)

			result, err := resultFactory.Produce()
			require.NoError(t, err)

			require.Equal(t, len(testCase.expectedTimeRanges), int(queryStats.LoadSplitQueries()))

			if len(testCase.expectedTimeRanges) == 1 {
				require.IsType(t, &selectors.InstantVectorSelector{}, result, "should return inner operator directly if only one range")
				selector := result.(*selectors.InstantVectorSelector)
				require.Equal(t, testCase.expectedTimeRanges[0], selector.Selector.TimeRange, "time range should match expected")
			} else {
				require.IsType(t, &TimeRangeSplitOperator{}, result, "should return split operator if multiple ranges expected")
				splitOperator := result.(*TimeRangeSplitOperator)

				var actualTimeRanges []types.QueryTimeRange
				for _, r := range splitOperator.ranges {
					require.IsType(t, &selectors.InstantVectorSelector{}, r.operator, "expected inner operator of range to be a selector")
					selector := r.operator.(*selectors.InstantVectorSelector)
					actualTimeRanges = append(actualTimeRanges, selector.Selector.TimeRange)
				}

				require.Equal(t, testCase.expectedTimeRanges, actualTimeRanges, "time ranges of inner operators should match expected")
			}
		})
	}
}
