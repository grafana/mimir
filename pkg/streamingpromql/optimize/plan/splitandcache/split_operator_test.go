// SPDX-License-Identifier: AGPL-3.0-only

package splitandcache

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestSplitOperator(t *testing.T) {
	firstRangeTimeRange := types.NewRangeQueryTimeRange(timestamp.Time(1), timestamp.Time(3), time.Millisecond)
	secondRangeTimeRange := types.NewRangeQueryTimeRange(timestamp.Time(4), timestamp.Time(6), time.Millisecond)
	thirdRangeTimeRange := types.NewRangeQueryTimeRange(timestamp.Time(7), timestamp.Time(9), time.Millisecond)
	threeRangeCaseOverallTimeRange := types.NewRangeQueryTimeRange(timestamp.Time(1), timestamp.Time(9), time.Millisecond)
	twoRangeCaseOverallTimeRange := types.NewRangeQueryTimeRange(timestamp.Time(1), timestamp.Time(6), time.Millisecond)

	singleStepRangeTimeRange := types.NewRangeQueryTimeRange(timestamp.Time(4), timestamp.Time(4), time.Millisecond)
	overallTimeRangeForSingleStepCase := types.NewRangeQueryTimeRange(timestamp.Time(1), timestamp.Time(4), time.Millisecond)

	testCases := map[string]struct {
		ranges              []testRange
		expectedData        testSeriesSet
		expectedStats       types.EncodedOperatorEvaluationStats
		expectedAnnotations annotations.Annotations
	}{
		"all ranges return no series": {
			ranges: []testRange{
				{
					stats: types.EncodedOperatorEvaluationStats{
						AllSeries: types.EncodedSubsetStats{
							SamplesProcessedPerStep:     []int64{0, 0, 0},
							SamplesReadIfSubsequentStep: []int64{0, 0, 0},
							SamplesReadIfFirstStep:      []int64{0, 0, 0},
						},
						TimeRange: firstRangeTimeRange.Encode(),
					},
				},
				{
					stats: types.EncodedOperatorEvaluationStats{
						AllSeries: types.EncodedSubsetStats{
							SamplesProcessedPerStep:     []int64{0, 0, 0},
							SamplesReadIfSubsequentStep: []int64{0, 0, 0},
							SamplesReadIfFirstStep:      []int64{0, 0, 0},
						},
						TimeRange: secondRangeTimeRange.Encode(),
					},
				},
				{
					stats: types.EncodedOperatorEvaluationStats{
						AllSeries: types.EncodedSubsetStats{
							SamplesProcessedPerStep:     []int64{0, 0, 0},
							SamplesReadIfSubsequentStep: []int64{0, 0, 0},
							SamplesReadIfFirstStep:      []int64{0, 0, 0},
						},
						TimeRange: thirdRangeTimeRange.Encode(),
					},
				},
			},
			expectedData: testSeriesSet{},
			expectedStats: types.EncodedOperatorEvaluationStats{
				AllSeries: types.EncodedSubsetStats{
					SamplesProcessedPerStep:     []int64{0, 0, 0, 0, 0, 0, 0, 0, 0},
					SamplesReadIfSubsequentStep: []int64{0, 0, 0, 0, 0, 0, 0, 0, 0},
					SamplesReadIfFirstStep:      []int64{0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
				TimeRange: threeRangeCaseOverallTimeRange.Encode(),
			},
		},
		"all ranges have the same series": {
			ranges: []testRange{
				{
					data: testSeriesSet{
						{
							labels: labels.FromStrings("idx", "0"),
							floats: []promql.FPoint{{T: 1, F: 10}, {T: 2, F: 20}, {T: 3, F: 30}},
						},
						{
							labels: labels.FromStrings("idx", "1"),
							floats: []promql.FPoint{{T: 1, F: 11}, {T: 2, F: 21}, {T: 3, F: 31}},
						},
						{
							labels: labels.FromStrings("idx", "2"),
							floats: []promql.FPoint{{T: 1, F: 12}, {T: 2, F: 22}, {T: 3, F: 32}},
						},
					},
					stats: types.EncodedOperatorEvaluationStats{
						AllSeries: types.EncodedSubsetStats{
							SamplesProcessedPerStep:     []int64{1, 2, 3},
							SamplesReadIfSubsequentStep: []int64{4, 5, 6},
							SamplesReadIfFirstStep:      []int64{7, 8, 9},
						},
						TimeRange: firstRangeTimeRange.Encode(),
					},
				},
				{
					data: testSeriesSet{
						{
							labels: labels.FromStrings("idx", "0"),
							floats: []promql.FPoint{{T: 4, F: 40}, {T: 5, F: 50}, {T: 6, F: 60}},
						},
						{
							labels: labels.FromStrings("idx", "1"),
							floats: []promql.FPoint{{T: 4, F: 41}, {T: 5, F: 51}, {T: 6, F: 61}},
						},
						{
							labels: labels.FromStrings("idx", "2"),
							floats: []promql.FPoint{{T: 4, F: 42}, {T: 5, F: 52}, {T: 6, F: 62}},
						},
					},
					stats: types.EncodedOperatorEvaluationStats{
						AllSeries: types.EncodedSubsetStats{
							SamplesProcessedPerStep:     []int64{11, 12, 13},
							SamplesReadIfSubsequentStep: []int64{14, 15, 16},
							SamplesReadIfFirstStep:      []int64{17, 18, 19},
						},
						TimeRange: secondRangeTimeRange.Encode(),
					},
				},
				{
					data: testSeriesSet{
						{
							labels: labels.FromStrings("idx", "0"),
							floats: []promql.FPoint{{T: 7, F: 70}, {T: 8, F: 80}, {T: 9, F: 90}},
						},
						{
							labels: labels.FromStrings("idx", "1"),
							floats: []promql.FPoint{{T: 7, F: 71}, {T: 8, F: 81}, {T: 9, F: 91}},
						},
						{
							labels: labels.FromStrings("idx", "2"),
							floats: []promql.FPoint{{T: 7, F: 72}, {T: 8, F: 82}, {T: 9, F: 92}},
						},
					},
					stats: types.EncodedOperatorEvaluationStats{
						AllSeries: types.EncodedSubsetStats{
							SamplesProcessedPerStep:     []int64{21, 22, 23},
							SamplesReadIfSubsequentStep: []int64{24, 25, 26},
							SamplesReadIfFirstStep:      []int64{27, 28, 29},
						},
						TimeRange: thirdRangeTimeRange.Encode(),
					},
				},
			},
			expectedData: testSeriesSet{
				{
					labels: labels.FromStrings("idx", "0"),
					floats: []promql.FPoint{{T: 1, F: 10}, {T: 2, F: 20}, {T: 3, F: 30}, {T: 4, F: 40}, {T: 5, F: 50}, {T: 6, F: 60}, {T: 7, F: 70}, {T: 8, F: 80}, {T: 9, F: 90}},
				},
				{
					labels: labels.FromStrings("idx", "1"),
					floats: []promql.FPoint{{T: 1, F: 11}, {T: 2, F: 21}, {T: 3, F: 31}, {T: 4, F: 41}, {T: 5, F: 51}, {T: 6, F: 61}, {T: 7, F: 71}, {T: 8, F: 81}, {T: 9, F: 91}},
				},
				{
					labels: labels.FromStrings("idx", "2"),
					floats: []promql.FPoint{{T: 1, F: 12}, {T: 2, F: 22}, {T: 3, F: 32}, {T: 4, F: 42}, {T: 5, F: 52}, {T: 6, F: 62}, {T: 7, F: 72}, {T: 8, F: 82}, {T: 9, F: 92}},
				},
			},
			expectedStats: types.EncodedOperatorEvaluationStats{
				AllSeries: types.EncodedSubsetStats{
					SamplesProcessedPerStep:     []int64{1, 2, 3, 11, 12, 13, 21, 22, 23},
					SamplesReadIfSubsequentStep: []int64{4, 5, 6, 14, 15, 16, 24, 25, 26},
					SamplesReadIfFirstStep:      []int64{7, 8, 9, 17, 18, 19, 27, 28, 29},
				},
				TimeRange: threeRangeCaseOverallTimeRange.Encode(),
			},
		},
		"each range has different series": {
			ranges: []testRange{
				{
					data: testSeriesSet{
						{
							labels: labels.FromStrings("idx", "0"),
							floats: []promql.FPoint{{T: 1, F: 10}, {T: 2, F: 20}, {T: 3, F: 30}},
						},
						{
							labels: labels.FromStrings("idx", "1"),
							floats: []promql.FPoint{{T: 1, F: 11}, {T: 2, F: 21}, {T: 3, F: 31}},
						},
						{
							labels: labels.FromStrings("idx", "2"),
							floats: []promql.FPoint{{T: 1, F: 12}, {T: 2, F: 22}, {T: 3, F: 32}},
						},
					},
					stats: types.EncodedOperatorEvaluationStats{
						AllSeries: types.EncodedSubsetStats{
							SamplesProcessedPerStep:     []int64{1, 2, 3},
							SamplesReadIfSubsequentStep: []int64{4, 5, 6},
							SamplesReadIfFirstStep:      []int64{7, 8, 9},
						},
						TimeRange: firstRangeTimeRange.Encode(),
					},
				},
				{
					data: testSeriesSet{
						{
							labels: labels.FromStrings("idx", "3"),
							floats: []promql.FPoint{{T: 4, F: 40}, {T: 5, F: 50}, {T: 6, F: 60}},
						},
						{
							labels: labels.FromStrings("idx", "4"),
							floats: []promql.FPoint{{T: 4, F: 41}, {T: 5, F: 51}, {T: 6, F: 61}},
						},
						{
							labels: labels.FromStrings("idx", "5"),
							floats: []promql.FPoint{{T: 4, F: 42}, {T: 5, F: 52}, {T: 6, F: 62}},
						},
					},
					stats: types.EncodedOperatorEvaluationStats{
						AllSeries: types.EncodedSubsetStats{
							SamplesProcessedPerStep:     []int64{11, 12, 13},
							SamplesReadIfSubsequentStep: []int64{14, 15, 16},
							SamplesReadIfFirstStep:      []int64{17, 18, 19},
						},
						TimeRange: secondRangeTimeRange.Encode(),
					},
				},
				{
					data: testSeriesSet{
						{
							labels: labels.FromStrings("idx", "6"),
							floats: []promql.FPoint{{T: 7, F: 70}, {T: 8, F: 80}, {T: 9, F: 90}},
						},
						{
							labels: labels.FromStrings("idx", "7"),
							floats: []promql.FPoint{{T: 7, F: 71}, {T: 8, F: 81}, {T: 9, F: 91}},
						},
						{
							labels: labels.FromStrings("idx", "8"),
							floats: []promql.FPoint{{T: 7, F: 72}, {T: 8, F: 82}, {T: 9, F: 92}},
						},
					},
					stats: types.EncodedOperatorEvaluationStats{
						AllSeries: types.EncodedSubsetStats{
							SamplesProcessedPerStep:     []int64{21, 22, 23},
							SamplesReadIfSubsequentStep: []int64{24, 25, 26},
							SamplesReadIfFirstStep:      []int64{27, 28, 29},
						},
						TimeRange: thirdRangeTimeRange.Encode(),
					},
				},
			},
			expectedData: testSeriesSet{
				{
					labels: labels.FromStrings("idx", "0"),
					floats: []promql.FPoint{{T: 1, F: 10}, {T: 2, F: 20}, {T: 3, F: 30}},
				},
				{
					labels: labels.FromStrings("idx", "1"),
					floats: []promql.FPoint{{T: 1, F: 11}, {T: 2, F: 21}, {T: 3, F: 31}},
				},
				{
					labels: labels.FromStrings("idx", "2"),
					floats: []promql.FPoint{{T: 1, F: 12}, {T: 2, F: 22}, {T: 3, F: 32}},
				},
				{
					labels: labels.FromStrings("idx", "3"),
					floats: []promql.FPoint{{T: 4, F: 40}, {T: 5, F: 50}, {T: 6, F: 60}},
				},
				{
					labels: labels.FromStrings("idx", "4"),
					floats: []promql.FPoint{{T: 4, F: 41}, {T: 5, F: 51}, {T: 6, F: 61}},
				},
				{
					labels: labels.FromStrings("idx", "5"),
					floats: []promql.FPoint{{T: 4, F: 42}, {T: 5, F: 52}, {T: 6, F: 62}},
				},
				{
					labels: labels.FromStrings("idx", "6"),
					floats: []promql.FPoint{{T: 7, F: 70}, {T: 8, F: 80}, {T: 9, F: 90}},
				},
				{
					labels: labels.FromStrings("idx", "7"),
					floats: []promql.FPoint{{T: 7, F: 71}, {T: 8, F: 81}, {T: 9, F: 91}},
				},
				{
					labels: labels.FromStrings("idx", "8"),
					floats: []promql.FPoint{{T: 7, F: 72}, {T: 8, F: 82}, {T: 9, F: 92}},
				},
			},
			expectedStats: types.EncodedOperatorEvaluationStats{
				AllSeries: types.EncodedSubsetStats{
					SamplesProcessedPerStep:     []int64{1, 2, 3, 11, 12, 13, 21, 22, 23},
					SamplesReadIfSubsequentStep: []int64{4, 5, 6, 14, 15, 16, 24, 25, 26},
					SamplesReadIfFirstStep:      []int64{7, 8, 9, 17, 18, 19, 27, 28, 29},
				},
				TimeRange: threeRangeCaseOverallTimeRange.Encode(),
			},
		},
		"ranges have some same, some different series": {
			ranges: []testRange{
				{
					data: testSeriesSet{
						{
							labels: labels.FromStrings("idx", "0"),
							floats: []promql.FPoint{{T: 1, F: 10}, {T: 2, F: 20}, {T: 3, F: 30}},
						},
						{
							labels: labels.FromStrings("idx", "1"),
							floats: []promql.FPoint{{T: 1, F: 11}, {T: 2, F: 21}, {T: 3, F: 31}},
						},
						{
							labels: labels.FromStrings("idx", "2"),
							floats: []promql.FPoint{{T: 1, F: 12}, {T: 2, F: 22}, {T: 3, F: 32}},
						},
					},
					stats: types.EncodedOperatorEvaluationStats{
						AllSeries: types.EncodedSubsetStats{
							SamplesProcessedPerStep:     []int64{1, 2, 3},
							SamplesReadIfSubsequentStep: []int64{4, 5, 6},
							SamplesReadIfFirstStep:      []int64{7, 8, 9},
						},
						TimeRange: firstRangeTimeRange.Encode(),
					},
				},
				{
					data: testSeriesSet{
						{
							labels: labels.FromStrings("idx", "0"),
							floats: []promql.FPoint{{T: 4, F: 40}, {T: 5, F: 50}, {T: 6, F: 60}},
						},
						{
							labels: labels.FromStrings("idx", "3"),
							floats: []promql.FPoint{{T: 4, F: 41}, {T: 5, F: 51}, {T: 6, F: 61}},
						},
					},
					stats: types.EncodedOperatorEvaluationStats{
						AllSeries: types.EncodedSubsetStats{
							SamplesProcessedPerStep:     []int64{11, 12, 13},
							SamplesReadIfSubsequentStep: []int64{14, 15, 16},
							SamplesReadIfFirstStep:      []int64{17, 18, 19},
						},
						TimeRange: secondRangeTimeRange.Encode(),
					},
				},
				{
					data: testSeriesSet{
						{
							labels: labels.FromStrings("idx", "4"),
							floats: []promql.FPoint{{T: 7, F: 72}, {T: 8, F: 82}, {T: 9, F: 92}},
						},
						{
							labels: labels.FromStrings("idx", "0"),
							floats: []promql.FPoint{{T: 7, F: 70}, {T: 8, F: 80}, {T: 9, F: 90}},
						},
						{
							labels: labels.FromStrings("idx", "1"),
							floats: []promql.FPoint{{T: 7, F: 71}, {T: 8, F: 81}, {T: 9, F: 91}},
						},
					},
					stats: types.EncodedOperatorEvaluationStats{
						AllSeries: types.EncodedSubsetStats{
							SamplesProcessedPerStep:     []int64{21, 22, 23},
							SamplesReadIfSubsequentStep: []int64{24, 25, 26},
							SamplesReadIfFirstStep:      []int64{27, 28, 29},
						},
						TimeRange: thirdRangeTimeRange.Encode(),
					},
				},
			},
			expectedData: testSeriesSet{
				{
					labels: labels.FromStrings("idx", "0"),
					floats: []promql.FPoint{{T: 1, F: 10}, {T: 2, F: 20}, {T: 3, F: 30}, {T: 4, F: 40}, {T: 5, F: 50}, {T: 6, F: 60}, {T: 7, F: 70}, {T: 8, F: 80}, {T: 9, F: 90}},
				},
				{
					labels: labels.FromStrings("idx", "1"),
					floats: []promql.FPoint{{T: 1, F: 11}, {T: 2, F: 21}, {T: 3, F: 31}, {T: 7, F: 71}, {T: 8, F: 81}, {T: 9, F: 91}},
				},
				{
					labels: labels.FromStrings("idx", "2"),
					floats: []promql.FPoint{{T: 1, F: 12}, {T: 2, F: 22}, {T: 3, F: 32}},
				},
				{
					labels: labels.FromStrings("idx", "3"),
					floats: []promql.FPoint{{T: 4, F: 41}, {T: 5, F: 51}, {T: 6, F: 61}},
				},
				{
					labels: labels.FromStrings("idx", "4"),
					floats: []promql.FPoint{{T: 7, F: 72}, {T: 8, F: 82}, {T: 9, F: 92}},
				},
			},
			expectedStats: types.EncodedOperatorEvaluationStats{
				AllSeries: types.EncodedSubsetStats{
					SamplesProcessedPerStep:     []int64{1, 2, 3, 11, 12, 13, 21, 22, 23},
					SamplesReadIfSubsequentStep: []int64{4, 5, 6, 14, 15, 16, 24, 25, 26},
					SamplesReadIfFirstStep:      []int64{7, 8, 9, 17, 18, 19, 27, 28, 29},
				},
				TimeRange: threeRangeCaseOverallTimeRange.Encode(),
			},
		},
		"one range has a single step": {
			ranges: []testRange{
				{
					data: testSeriesSet{
						{
							labels: labels.FromStrings("idx", "0"),
							floats: []promql.FPoint{{T: 1, F: 10}, {T: 2, F: 20}, {T: 3, F: 30}},
						},
						{
							labels: labels.FromStrings("idx", "1"),
							floats: []promql.FPoint{{T: 1, F: 11}, {T: 2, F: 21}, {T: 3, F: 31}},
						},
						{
							labels: labels.FromStrings("idx", "2"),
							floats: []promql.FPoint{{T: 1, F: 12}, {T: 2, F: 22}, {T: 3, F: 32}},
						},
					},
					stats: types.EncodedOperatorEvaluationStats{
						AllSeries: types.EncodedSubsetStats{
							SamplesProcessedPerStep:     []int64{1, 2, 3},
							SamplesReadIfSubsequentStep: []int64{4, 5, 6},
							SamplesReadIfFirstStep:      []int64{7, 8, 9},
						},
						TimeRange: firstRangeTimeRange.Encode(),
					},
				},
				{
					data: testSeriesSet{
						{
							labels: labels.FromStrings("idx", "1"),
							floats: []promql.FPoint{{T: 4, F: 40}},
						},
						{
							labels: labels.FromStrings("idx", "4"),
							floats: []promql.FPoint{{T: 4, F: 41}},
						},
						{
							labels: labels.FromStrings("idx", "2"),
							floats: []promql.FPoint{{T: 4, F: 42}},
						},
					},
					stats: types.EncodedOperatorEvaluationStats{
						AllSeries: types.EncodedSubsetStats{
							SamplesProcessedPerStep:     []int64{10},
							SamplesReadIfSubsequentStep: []int64{11},
							SamplesReadIfFirstStep:      []int64{12},
						},
						TimeRange: singleStepRangeTimeRange.Encode(),
					},
				},
			},
			expectedData: testSeriesSet{
				{
					labels: labels.FromStrings("idx", "0"),
					floats: []promql.FPoint{{T: 1, F: 10}, {T: 2, F: 20}, {T: 3, F: 30}},
				},
				{
					labels: labels.FromStrings("idx", "1"),
					floats: []promql.FPoint{{T: 1, F: 11}, {T: 2, F: 21}, {T: 3, F: 31}, {T: 4, F: 40}},
				},
				{
					labels: labels.FromStrings("idx", "2"),
					floats: []promql.FPoint{{T: 1, F: 12}, {T: 2, F: 22}, {T: 3, F: 32}, {T: 4, F: 42}},
				},
				{
					labels: labels.FromStrings("idx", "4"),
					floats: []promql.FPoint{{T: 4, F: 41}},
				},
			},
			expectedStats: types.EncodedOperatorEvaluationStats{
				AllSeries: types.EncodedSubsetStats{
					SamplesProcessedPerStep:     []int64{1, 2, 3, 10},
					SamplesReadIfSubsequentStep: []int64{4, 5, 6, 11},
					SamplesReadIfFirstStep:      []int64{7, 8, 9, 12},
				},
				TimeRange: overallTimeRangeForSingleStepCase.Encode(),
			},
		},
		"series have histograms": {
			ranges: []testRange{
				{
					data: testSeriesSet{
						{
							labels: labels.FromStrings("type", "only-floats"),
							floats: []promql.FPoint{{T: 1, F: 10}, {T: 2, F: 20}, {T: 3, F: 30}},
						},
						{
							labels:     labels.FromStrings("type", "only-histograms"),
							histograms: []promql.HPoint{{T: 1, H: createHistogram(11)}, {T: 2, H: createHistogram(21)}, {T: 3, H: createHistogram(31)}},
						},
						{
							labels:     labels.FromStrings("type", "mixed"),
							floats:     []promql.FPoint{{T: 1, F: 12}, {T: 3, F: 32}},
							histograms: []promql.HPoint{{T: 2, H: createHistogram(22)}},
						},
					},
					stats: types.EncodedOperatorEvaluationStats{
						AllSeries: types.EncodedSubsetStats{
							SamplesProcessedPerStep:     []int64{1, 2, 3},
							SamplesReadIfSubsequentStep: []int64{4, 5, 6},
							SamplesReadIfFirstStep:      []int64{7, 8, 9},
						},
						TimeRange: firstRangeTimeRange.Encode(),
					},
				},
				{
					data: testSeriesSet{
						{
							labels: labels.FromStrings("type", "only-floats"),
							floats: []promql.FPoint{{T: 4, F: 40}, {T: 5, F: 50}, {T: 6, F: 60}},
						},
						{
							labels:     labels.FromStrings("type", "only-histograms"),
							histograms: []promql.HPoint{{T: 4, H: createHistogram(41)}, {T: 5, H: createHistogram(51)}, {T: 6, H: createHistogram(61)}},
						},
						{
							labels:     labels.FromStrings("type", "mixed"),
							floats:     []promql.FPoint{{T: 4, F: 42}, {T: 6, F: 62}},
							histograms: []promql.HPoint{{T: 5, H: createHistogram(52)}},
						},
					},
					stats: types.EncodedOperatorEvaluationStats{
						AllSeries: types.EncodedSubsetStats{
							SamplesProcessedPerStep:     []int64{11, 12, 13},
							SamplesReadIfSubsequentStep: []int64{14, 15, 16},
							SamplesReadIfFirstStep:      []int64{17, 18, 19},
						},
						TimeRange: secondRangeTimeRange.Encode(),
					},
				},
			},
			expectedData: testSeriesSet{
				{
					labels: labels.FromStrings("type", "only-floats"),
					floats: []promql.FPoint{{T: 1, F: 10}, {T: 2, F: 20}, {T: 3, F: 30}, {T: 4, F: 40}, {T: 5, F: 50}, {T: 6, F: 60}},
				},
				{
					labels:     labels.FromStrings("type", "only-histograms"),
					histograms: []promql.HPoint{{T: 1, H: createHistogram(11)}, {T: 2, H: createHistogram(21)}, {T: 3, H: createHistogram(31)}, {T: 4, H: createHistogram(41)}, {T: 5, H: createHistogram(51)}, {T: 6, H: createHistogram(61)}},
				},
				{
					labels:     labels.FromStrings("type", "mixed"),
					floats:     []promql.FPoint{{T: 1, F: 12}, {T: 3, F: 32}, {T: 4, F: 42}, {T: 6, F: 62}},
					histograms: []promql.HPoint{{T: 2, H: createHistogram(22)}, {T: 5, H: createHistogram(52)}},
				},
			},
			expectedStats: types.EncodedOperatorEvaluationStats{
				AllSeries: types.EncodedSubsetStats{
					SamplesProcessedPerStep:     []int64{1, 2, 3, 11, 12, 13},
					SamplesReadIfSubsequentStep: []int64{4, 5, 6, 14, 15, 16},
					SamplesReadIfFirstStep:      []int64{7, 8, 9, 17, 18, 19},
				},
				TimeRange: twoRangeCaseOverallTimeRange.Encode(),
			},
		},
		"ranges emit annotations": {
			ranges: []testRange{
				{
					stats: types.EncodedOperatorEvaluationStats{
						AllSeries: types.EncodedSubsetStats{
							SamplesProcessedPerStep:     []int64{0, 0, 0},
							SamplesReadIfSubsequentStep: []int64{0, 0, 0},
							SamplesReadIfFirstStep:      []int64{0, 0, 0},
						},
						TimeRange: firstRangeTimeRange.Encode(),
					},
					annotations: annotationsFrom(annotations.NewBadBucketLabelWarning("my_metric", "the_label", posrange.PositionRange{Start: 1, End: 3})),
				},
				{
					stats: types.EncodedOperatorEvaluationStats{
						AllSeries: types.EncodedSubsetStats{
							SamplesProcessedPerStep:     []int64{0, 0, 0},
							SamplesReadIfSubsequentStep: []int64{0, 0, 0},
							SamplesReadIfFirstStep:      []int64{0, 0, 0},
						},
						TimeRange: secondRangeTimeRange.Encode(),
					},
					annotations: annotationsFrom(annotations.NewHistogramIgnoredInAggregationInfo("average", posrange.PositionRange{Start: 7, End: 10})),
				},
			},
			expectedData: testSeriesSet{},
			expectedStats: types.EncodedOperatorEvaluationStats{
				AllSeries: types.EncodedSubsetStats{
					SamplesProcessedPerStep:     []int64{0, 0, 0, 0, 0, 0},
					SamplesReadIfSubsequentStep: []int64{0, 0, 0, 0, 0, 0},
					SamplesReadIfFirstStep:      []int64{0, 0, 0, 0, 0, 0},
				},
				TimeRange: twoRangeCaseOverallTimeRange.Encode(),
			},
			expectedAnnotations: annotationsFrom(
				annotations.NewBadBucketLabelWarning("my_metric", "the_label", posrange.PositionRange{Start: 1, End: 3}),
				annotations.NewHistogramIgnoredInAggregationInfo("average", posrange.PositionRange{Start: 7, End: 10}),
			),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			queryStats, ctx := stats.ContextWithEmptyStats(context.Background())
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

			innerOperators := make([]*operators.TestOperator, len(testCase.ranges))
			for i, r := range testCase.ranges {
				s, err := r.stats.Decode(ctx, memoryConsumptionTracker)
				require.NoError(t, err)

				innerOperators[i] = &operators.TestOperator{
					Series:                   r.data.seriesLabels(),
					Data:                     r.data.seriesData(t, memoryConsumptionTracker),
					EvaluationStats:          s,
					Annotations:              r.annotations,
					MemoryConsumptionTracker: memoryConsumptionTracker,
				}
			}

			ranges := make([]*splitRange, len(testCase.ranges))
			for i, o := range innerOperators {
				ranges[i] = newSplitRange(o, memoryConsumptionTracker)
			}

			o := newTimeRangeSplitOperator(ranges, memoryConsumptionTracker, testCase.expectedStats.TimeRange.Decode())

			require.NoError(t, o.Prepare(ctx, &types.PrepareParams{}))
			require.Equal(t, len(testCase.ranges), int(queryStats.LoadSplitQueries()))
			for i, o := range innerOperators {
				require.Truef(t, o.Prepared, "expected inner operator %d to be prepared", i)
			}

			require.NoError(t, o.AfterPrepare(ctx))
			for i, o := range innerOperators {
				require.Truef(t, o.AfterPrepareCalled, "expected AfterPrepare to be called on inner operator %d", i)
			}

			series, err := o.SeriesMetadata(ctx, nil)
			require.NoError(t, err)
			require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedData.seriesLabels()), series, "expected series metadata to match expected")

			for idx, s := range series {
				data, err := o.NextSeries(ctx)
				require.NoErrorf(t, err, "received unexpected error while trying to read series at index %d (%s)", idx, s.Labels.String())
				require.Equalf(t, testCase.expectedData[idx].floats, data.Floats, "expected data to match expected for series at index %d (%s)", idx, s.Labels.String())
				require.Equalf(t, testCase.expectedData[idx].histograms, data.Histograms, "expected data to match expected for series at index %d (%s)", idx, s.Labels.String())

				types.PutInstantVectorSeriesData(data, memoryConsumptionTracker)
			}

			types.SeriesMetadataSlicePool.Put(&series, memoryConsumptionTracker)

			require.NoError(t, o.FinishedReading(ctx))
			for i, o := range innerOperators {
				require.Truef(t, o.FinishedReadingCalled, "expected inner operator %d to have had FinishedReading called", i)
			}

			operatorStats, annos, err := o.Finalize(ctx)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedStats, operatorStats.Encode(), "expected stats to match expected")
			require.Equal(t, testCase.expectedAnnotations, annos, "expected annotations to match expected")

			operatorStats.Close()

			o.Close()
			require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(), "expected all instances to be returned to pool, current memory consumption is:\n%v", memoryConsumptionTracker.DescribeCurrentMemoryConsumption())
		})
	}
}

func TestSplitOperator_ConflictingDropNameValuesForSameSeries(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	range1 := &operators.TestOperator{
		Series:                   []labels.Labels{labels.FromStrings("idx", "0")},
		DropName:                 []bool{true},
		MemoryConsumptionTracker: memoryConsumptionTracker,
	}

	range2 := &operators.TestOperator{
		Series:                   []labels.Labels{labels.FromStrings("idx", "0")},
		DropName:                 []bool{false},
		MemoryConsumptionTracker: memoryConsumptionTracker,
	}

	o := newTimeRangeSplitOperator(
		[]*splitRange{
			newSplitRange(range1, memoryConsumptionTracker),
			newSplitRange(range2, memoryConsumptionTracker),
		},
		memoryConsumptionTracker,
		types.NewInstantQueryTimeRange(time.Now()),
	)

	require.NoError(t, o.Prepare(ctx, &types.PrepareParams{}))
	require.NoError(t, o.AfterPrepare(ctx))

	_, err := o.SeriesMetadata(ctx, nil)
	require.EqualError(t, err, `series with labels {idx="0"} has conflicting drop name values in different ranges / extents`)
}

type testRange struct {
	data        testSeriesSet
	stats       types.EncodedOperatorEvaluationStats
	annotations annotations.Annotations
}

type testSeries struct {
	labels     labels.Labels
	floats     []promql.FPoint
	histograms []promql.HPoint
}

type testSeriesSet []testSeries

func (s testSeriesSet) seriesLabels() []labels.Labels {
	labels := make([]labels.Labels, 0, len(s))
	for _, d := range s {
		labels = append(labels, d.labels)
	}
	return labels
}

func (s testSeriesSet) seriesData(t *testing.T, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) []types.InstantVectorSeriesData {
	data := make([]types.InstantVectorSeriesData, 0, len(s))
	for _, d := range s {
		require.NoError(t, memoryConsumptionTracker.IncreaseMemoryConsumption(uint64(cap(d.floats))*types.FPointSize, limiter.FPointSlices))
		require.NoError(t, memoryConsumptionTracker.IncreaseMemoryConsumption(uint64(cap(d.histograms))*types.HPointSize, limiter.HPointSlices))

		data = append(data, types.InstantVectorSeriesData{
			Floats:     d.floats,
			Histograms: d.histograms,
		})
	}
	return data
}

func createHistogram(v float64) *histogram.FloatHistogram {
	return &histogram.FloatHistogram{Count: v}
}

func annotationsFrom(annos ...error) annotations.Annotations {
	all := annotations.Annotations{}

	for _, a := range annos {
		all.Add(a)
	}

	return all
}
