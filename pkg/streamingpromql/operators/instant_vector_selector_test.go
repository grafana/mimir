// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestInstantVectorSelector_NativeHistogramPointerHandling(t *testing.T) {
	requireNotSame := func(t *testing.T, h1, h2 *histogram.FloatHistogram) {
		require.NotSame(t, h1, h2, "must not point to the same *FloatHistogram")

		requireNotSameSlices(t, h1.PositiveSpans, h2.PositiveSpans, "positive spans")
		requireNotSameSlices(t, h1.NegativeSpans, h2.NegativeSpans, "negative spans")
		requireNotSameSlices(t, h1.PositiveBuckets, h2.PositiveBuckets, "positive buckets")
		requireNotSameSlices(t, h1.NegativeBuckets, h2.NegativeBuckets, "negative buckets")
		requireNotSameSlices(t, h1.CustomValues, h2.CustomValues, "custom values")
	}

	testCases := map[string]struct {
		data      string
		stepCount int // For each test case, the step is always 1m, and the lookback window is always 5m. The points loaded in 'data' may be at different intervals.
		check     func(t *testing.T, points []promql.HPoint, floats []promql.FPoint)
	}{
		"different histograms at each point": {
			data: `
				load 1m
					my_metric {{schema:0 sum:5 count:4 buckets:[1 2 1]}} {{schema:0 sum:20 count:7 buckets:[9 10 1]}} {{schema:0 sum:21 count:8 buckets:[9 10 2]}}
					#         0m                                         1m                                           2m
			`,
			stepCount: 3,
			check: func(t *testing.T, points []promql.HPoint, _ []promql.FPoint) {
				require.Len(t, points, 3)
				require.Equal(t, 5.0, points[0].H.Sum)
				require.Equal(t, 20.0, points[1].H.Sum)
				require.Equal(t, 21.0, points[2].H.Sum)

				requireNotSame(t, points[0].H, points[1].H)
				requireNotSame(t, points[1].H, points[2].H)
			},
		},
		"different histograms at each point, some due to lookback": {
			data: `
				load 30s
					my_metric {{schema:0 sum:3 count:2 buckets:[1 0 1]}} _ {{schema:0 sum:5 count:4 buckets:[1 2 1]}} {{schema:0 sum:20 count:7 buckets:[9 10 1]}} _  _    {{schema:0 sum:21 count:8 buckets:[9 10 2]}}
				#             0m                                       30s 1m                                         1m30s                                        2m 2m30 3m
			`,
			stepCount: 4,
			check: func(t *testing.T, points []promql.HPoint, _ []promql.FPoint) {
				require.Len(t, points, 4)
				require.Equal(t, 3.0, points[0].H.Sum)
				require.Equal(t, 5.0, points[1].H.Sum)
				require.Equal(t, 20.0, points[2].H.Sum)
				require.Equal(t, 21.0, points[3].H.Sum)

				requireNotSame(t, points[0].H, points[1].H)
				requireNotSame(t, points[1].H, points[2].H)
				requireNotSame(t, points[2].H, points[3].H)
			},
		},
		"same histogram at each point due to lookback": {
			data: `
				load 1m
					my_metric {{schema:0 sum:5 count:4 buckets:[1 2 1]}}
			`,
			stepCount: 3,
			check: func(t *testing.T, points []promql.HPoint, _ []promql.FPoint) {
				require.Len(t, points, 3)
				require.Equal(t, 5.0, points[0].H.Sum)
				require.Equal(t, 5.0, points[1].H.Sum)
				require.Equal(t, 5.0, points[2].H.Sum)

				require.Same(t, points[0].H, points[1].H)
				require.Same(t, points[1].H, points[2].H)
			},
		},
		"same histogram at each point not due to lookback": {
			data: `
				load 1m
					my_metric {{schema:0 sum:5 count:4 buckets:[1 2 1]}} {{schema:0 sum:5 count:4 buckets:[1 2 1]}}
			`,
			stepCount: 2,
			check: func(t *testing.T, points []promql.HPoint, _ []promql.FPoint) {
				require.Len(t, points, 2)
				require.Equal(t, 5.0, points[0].H.Sum)
				require.Equal(t, 5.0, points[1].H.Sum)

				requireNotSame(t, points[0].H, points[1].H)
			},
		},
		"last point is from lookback and is the same as the previous point": {
			data: `
				load 30s
					my_metric {{schema:0 sum:3 count:2 buckets:[1 0 1]}} _ {{schema:0 sum:5 count:4 buckets:[1 2 1]}} 
					#         0m                                       30s 1m                                         1m30s (nothing)    2m (nothing)
			`,
			stepCount: 3,
			check: func(t *testing.T, points []promql.HPoint, _ []promql.FPoint) {
				require.Len(t, points, 3)
				require.Equal(t, 3.0, points[0].H.Sum)
				require.Equal(t, 5.0, points[1].H.Sum)
				require.Equal(t, 5.0, points[2].H.Sum)

				requireNotSame(t, points[0].H, points[1].H)
				require.Same(t, points[1].H, points[2].H)
			},
		},
		"last point is from lookback but is not the same as the previous point": {
			data: `
				load 30s
					my_metric {{schema:0 sum:3 count:2 buckets:[1 0 1]}} _ {{schema:0 sum:5 count:4 buckets:[1 2 1]}} {{schema:0 sum:20 count:7 buckets:[9 10 1]}} 
					#         0m                                       30s 1m                                         1m30s                                        2m (nothing)
			`,
			stepCount: 3,
			check: func(t *testing.T, points []promql.HPoint, _ []promql.FPoint) {
				require.Len(t, points, 3)
				require.Equal(t, 3.0, points[0].H.Sum)
				require.Equal(t, 5.0, points[1].H.Sum)
				require.Equal(t, 20.0, points[2].H.Sum)

				requireNotSame(t, points[0].H, points[1].H)
				requireNotSame(t, points[1].H, points[2].H)
			},
		},

		"point has same value as a previous point, but there is a different histogram value in between": {
			data: `
				load 1m
					my_metric {{schema:0 sum:3 count:2 buckets:[1 0 1]}} {{schema:0 sum:5 count:4 buckets:[1 2 1]}} {{schema:0 sum:3 count:2 buckets:[1 0 1]}}
			`,
			stepCount: 3,
			check: func(t *testing.T, points []promql.HPoint, _ []promql.FPoint) {
				require.Len(t, points, 3)
				require.Equal(t, 3.0, points[0].H.Sum)
				require.Equal(t, 5.0, points[1].H.Sum)
				require.Equal(t, 3.0, points[2].H.Sum)

				requireNotSame(t, points[0].H, points[1].H)
				requireNotSame(t, points[1].H, points[2].H)

				requireNotSame(t, points[0].H, points[2].H)
			},
		},
		"different histograms should have different spans": {
			data: `
				load 1m
					my_metric {{schema:0 sum:1 count:1 buckets:[1 0 1]}} {{schema:0 sum:3 count:2 buckets:[1 0 1]}}
			`,
			stepCount: 2,
			check: func(t *testing.T, points []promql.HPoint, _ []promql.FPoint) {
				require.Len(t, points, 2)
				requireNotSame(t, points[0].H, points[1].H)
			},
		},
		"successive histograms returned due to lookback should create different histograms at each point": {
			data: `
				load 30s
					my_metric _   {{schema:5 sum:10 count:7 buckets:[1 2 3 1]}} _   {{schema:5 sum:12 count:8 buckets:[1 2 3 2]}} _
					#         0m  30s                                           1m  1m30s                                         2m (nothing)
			`,
			stepCount: 3,
			check: func(t *testing.T, points []promql.HPoint, _ []promql.FPoint) {
				require.Len(t, points, 2)
				requireNotSame(t, points[0].H, points[1].H)
			},
		},
		"lookback points in middle of series reuse existing histogram": {
			data: `
				load 1m
					my_metric _   {{schema:5 sum:10 count:7 buckets:[1 2 3 1]}} _   {{schema:5 sum:12 count:8 buckets:[1 2 3 2]}} _
			`,
			stepCount: 5,
			check: func(t *testing.T, points []promql.HPoint, _ []promql.FPoint) {
				require.Len(t, points, 4)
				requireNotSame(t, points[0].H, points[2].H)
				require.Same(t, points[0].H, points[1].H)
				require.Same(t, points[2].H, points[3].H)
			},
		},
		// FIXME: this test currently fails due to https://github.com/prometheus/prometheus/issues/14172
		//
		//"point has same value as a previous point, but there is a float value in between": {
		//	data: `
		//        load 1m
		//            my_metric {{schema:0 sum:3 count:2 buckets:[1 0 1]}} 2 {{schema:0 sum:3 count:2 buckets:[1 0 1]}}
		//    `,
		//	stepCount: 3,
		//	check: func(t *testing.T, hPoints []promql.HPoint, fPoints []promql.FPoint) {
		//		require.Len(t, hPoints, 2)
		//		require.Equal(t, 3.0, hPoints[0].H.Sum)
		//		require.Equal(t, 3.0, hPoints[1].H.Sum)
		//
		//		require.Same(t, hPoints[0].H, hPoints[1].H)
		//
		//		require.Equal(t, []promql.FPoint{{T: 60000, F: 2}}, fPoints)
		//	},
		//},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			storage := promqltest.LoadedStorage(t, testCase.data)

			startTime := time.Unix(0, 0)
			endTime := startTime.Add(time.Duration(testCase.stepCount-1) * time.Minute)

			selector := &InstantVectorSelector{
				Selector: &Selector{
					Queryable: storage,
					TimeRange: types.NewRangeQueryTimeRange(startTime, endTime, time.Minute),
					Matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "my_metric"),
					},
					LookbackDelta: 5 * time.Minute,
				},
				MemoryConsumptionTracker: limiting.NewMemoryConsumptionTracker(0, nil),
			}

			ctx := context.Background()
			_, err := selector.SeriesMetadata(ctx)
			require.NoError(t, err)

			series, err := selector.NextSeries(ctx)
			require.NoError(t, err)
			testCase.check(t, series.Histograms, series.Floats)
		})
	}
}

func requireNotSameSlices[T any](t *testing.T, s1, s2 []T, description string) {
	require.NotSamef(t, s1, s2, "must not point to the same %v slice", description)

	// require.NotSame only checks the slice headers are different. It does not check that the slices do not point the same underlying arrays.
	// So specifically check if the first elements are different.
	if len(s1) > 0 && len(s2) > 0 {
		require.NotSamef(t, &s1[0], &s2[0], "must not point to the same underlying %v array", description)
	}
}
