// SPDX-License-Identifier: AGPL-3.0-only

package selectors

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// TestStepRange validates that the step data time ranges match the expected query range windows.
// Regardless of it being a smoothed or anchored query, these time ranges remain consistent.
// Any extended range query is performed internal to the range vector selector and not externally exposed.
func TestStepRange(t *testing.T) {
	timeZero := time.Unix(0, 0)

	storage := promqltest.LoadedStorage(t, `
		load 1m
			metric{instance="1"} _ 10 _ 30 _
			metric{instance="2"} 1 2 3 4 5
	`)

	tests := map[string]struct {
		step          time.Duration
		rangeDuration time.Duration
		anchored      bool
		smoothed      bool
		steps         int
	}{
		"anchored - step < range": {
			step:          time.Minute,
			rangeDuration: time.Minute * 2,
			anchored:      true,
			steps:         4,
		},
		"smoothed - step < range": {
			step:          time.Minute,
			rangeDuration: time.Minute * 2,
			anchored:      false,
			smoothed:      true,
			steps:         4,
		},
		"neither anchored nor smoothed - 2 minute range": {
			step:          time.Minute,
			rangeDuration: time.Minute * 2,
			anchored:      false,
			steps:         4,
		},
		"anchored - step > range": {
			step:          time.Minute,
			rangeDuration: time.Second * 30,
			anchored:      true,
			steps:         4,
		},
		"smoothed - step > range": {
			step:          time.Minute,
			rangeDuration: time.Second * 30,
			anchored:      false,
			smoothed:      true,
			steps:         4,
		},
		"anchored - step = range": {
			step:          time.Second * 30,
			rangeDuration: time.Second * 30,
			anchored:      true,
			steps:         7,
		},
		"smoothed - step = range": {
			step:          time.Second * 30,
			rangeDuration: time.Second * 30,
			anchored:      false,
			smoothed:      true,
			steps:         7,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			mc := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
			tr := types.NewRangeQueryTimeRange(timeZero, timeZero.Add(3*time.Minute), tc.step)

			selector := &Selector{
				Queryable:                storage,
				TimeRange:                tr,
				Matchers:                 []types.Matcher{{Type: labels.MatchEqual, Name: model.MetricNameLabel, Value: "metric"}},
				LookbackDelta:            1 * time.Minute,
				Range:                    tc.rangeDuration,
				Anchored:                 tc.anchored,
				Smoothed:                 tc.smoothed,
				CounterAware:             true,
				MemoryConsumptionTracker: mc,
			}

			rv := NewRangeVectorSelector(selector, mc, types.NewQueryStats())

			metadata, err := rv.SeriesMetadata(ctx, nil)
			require.NoError(t, err)
			require.Len(t, metadata, 2)

			for range metadata {
				require.NoError(t, rv.NextSeries(ctx))

				// This is a range query. The start goes back the given range duration
				start := timestamp.Time(tr.StartT).Add(0 - tc.rangeDuration)
				end := timestamp.Time(tr.StartT)

				// Iterate through all the steps, validating that we increment at the given step value
				for range tc.steps {
					stepData, err := rv.NextStepSamples(ctx)
					require.NoError(t, err)
					require.NotNil(t, stepData)

					require.Equal(t, timestamp.FromTime(start), stepData.RangeStart)
					require.Equal(t, timestamp.FromTime(end), stepData.RangeEnd)

					start = start.Add(tc.step)
					end = end.Add(tc.step)
				}

				_, err := rv.NextStepSamples(ctx)
				require.Equal(t, types.EOS, err)
			}
		})
	}
}

func TestRangeVectorSelectorSyntheticPoints(t *testing.T) {

	timeZero := time.Unix(0, 0)

	tests := map[string]struct {
		data         string
		ts           time.Time
		expected     []promql.FPoint
		smoothed     bool
		anchored     bool
		counterAware bool
	}{
		// no points are within the range boundary
		"anchored - no points": {
			data: `load 1m
					metric _ _ 2`,
			ts:       timeZero.Add(1 * time.Minute),
			expected: []promql.FPoint{},
			anchored: true,
		},
		// no synthetic points are needed since the points fall on the boundaries
		"anchored - no synthetic points": {
			data: `load 1m
					metric 5 1 10 2 30`,
			ts:       timeZero.Add(3 * time.Minute),
			expected: []promql.FPoint{{T: 60 * 1000, F: 1}, {T: 60 * 1000 * 2, F: 10}, {T: 60 * 3 * 1000, F: 2}},
			anchored: true,
		},
		// synthetic points are created from points within the range
		"anchored - synthetic head and tail - first.T > rangeStart, last.T < rangeEnd": {
			data: `load 1m
					metric 5 _ 10 _ 30`,
			ts:       timeZero.Add(3 * time.Minute),
			expected: []promql.FPoint{{T: 60 * 1000, F: 10}, {T: 60 * 1000 * 2, F: 10}, {T: 60 * 3 * 1000, F: 10}},
			anchored: true,
		},
		// synthetic points are created from the extended look-back window
		"anchored - synthetic head and tail first.T < rangeStart, last.T < rangeEnd": {
			data: `load 1m
					metric 1 2 3 4`,
			ts:       timeZero.Add(2 * time.Second * 61),
			expected: []promql.FPoint{{T: 2000, F: 1}, {T: 60000, F: 2}, {T: 120000, F: 3}, {T: 122000, F: 3}},
			anchored: true,
		},
		// note for anchored the end of the range is not modified. So we don't expect any last.T > rangeEnd

		// no points are within the range boundary
		"smoothed - no points": {
			data: `load 1m
					metric _ _ _`,
			ts:           timeZero.Add(1 * time.Minute),
			expected:     []promql.FPoint{},
			smoothed:     true,
			counterAware: true,
		},
		// no synthetic points as they are already on the boundary
		"smoothed - no synthetic points": {
			data: `load 1m
					metric 5 1 10 2 30`,
			ts:           timeZero.Add(3 * time.Minute),
			expected:     []promql.FPoint{{T: 60 * 1000, F: 1}, {T: 60 * 1000 * 2, F: 10}, {T: 60 * 3 * 1000, F: 2}},
			smoothed:     true,
			counterAware: true,
		},
		// smoothed has an extended end range. The synthetic points are taken from within the range and the extended end of the range
		"smoothed - synthetic head and tail - first.T > rangeStart, last.T > rangeEnd": {
			data: `load 1m
					metric 5 _ 10 _ 30`,
			ts:           timeZero.Add(3 * time.Minute),
			expected:     []promql.FPoint{{T: 60 * 1000, F: 10}, {T: 60 * 1000 * 2, F: 10}, {T: 60 * 3 * 1000, F: 20}},
			smoothed:     true,
			counterAware: true,
		},
		// the synthetic points are taken from the extended range
		"smoothed - synthetic head and tail first.T < rangeStart, last.T > rangeEnd - counter aware": {
			data: `load 1m
					metric 1 2 3 4`,
			ts:           timeZero.Add(2 * time.Second * 61),
			expected:     []promql.FPoint{{T: 2000, F: 1.0333333333333334}, {T: 60000, F: 2}, {T: 120000, F: 3}, {T: 122000, F: 3.033333333333333}},
			smoothed:     true,
			counterAware: true,
		},
		"smoothed - synthetic head and tail first.T < rangeStart, last.T > rangeEnd - not counter aware": {
			data: `load 1m
					metric 1 2 3 4`,
			ts:           timeZero.Add(2 * time.Second * 61),
			expected:     []promql.FPoint{{T: 2000, F: 1.0333333333333334}, {T: 60000, F: 2}, {T: 120000, F: 3}, {T: 122000, F: 3.033333333333333}},
			smoothed:     true,
			counterAware: false,
		},
		// the synthetic points are taken from within the original range
		"smoothed - synthetic head and tail first.T > rangeStart, last.T < rangeEnd": {
			data: `load 1m
					metric _ 3 _ _ _`,
			ts:           timeZero.Add(time.Minute * 2),
			expected:     []promql.FPoint{{T: 0, F: 3}, {T: 60000, F: 3}, {T: 120000, F: 3}},
			smoothed:     true,
			counterAware: true,
		},
		// the synthetic points are taken from the extended start range
		"smoothed - synthetic head and tail first.T < rangeStart, last.T < rangeEnd": {
			data: `load 1m
					metric 1 2 3 4 _ _`,
			ts:           timeZero.Add(time.Second * 64 * 4),
			expected:     []promql.FPoint{{T: 136000, F: 3.2666666666666666}, {T: 180000, F: 4}, {T: 256000, F: 4}},
			smoothed:     true,
			counterAware: true,
		},
		// the synthetic points are taken from the extended range and both the boundary spans have a counter reset
		"smoothed with counter resets - synthetic head and tail first.T < rangeStart, last.T > rangeEnd - counter aware": {
			data: `load 1m
					metric 10 5 6 1`,
			ts:           timeZero.Add(2 * time.Second * 61),
			expected:     []promql.FPoint{{T: 2000, F: 0.16666666666666666}, {T: 60000, F: 5}, {T: 120000, F: 6}, {T: 122000, F: 6.033333333333333}},
			smoothed:     true,
			counterAware: true,
		},
		// as above, but since this is not a rate/increase function the synthetic points are not counter adjusted
		"smoothed with counter resets - synthetic head and tail first.T < rangeStart, last.T > rangeEnd - not counter aware": {
			data: `load 1m
					metric 10 5 6 1`,
			ts:           timeZero.Add(2 * time.Second * 61),
			expected:     []promql.FPoint{{T: 2000, F: 9.833333333333334}, {T: 60000, F: 5}, {T: 120000, F: 6}, {T: 122000, F: 5.833333333333333}},
			smoothed:     true,
			counterAware: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			storage := promqltest.LoadedStorage(t, tc.data)
			ctx := context.Background()
			mc := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
			tr := types.NewRangeQueryTimeRange(tc.ts, tc.ts, time.Minute)

			require.NotEqual(t, tc.anchored, tc.smoothed)

			selector := &Selector{
				Queryable:                storage,
				TimeRange:                tr,
				Matchers:                 []types.Matcher{{Type: labels.MatchEqual, Name: model.MetricNameLabel, Value: "metric"}},
				LookbackDelta:            1 * time.Minute,
				Range:                    2 * time.Minute,
				Anchored:                 tc.anchored,
				Smoothed:                 tc.smoothed,
				CounterAware:             tc.counterAware,
				MemoryConsumptionTracker: mc,
			}

			rv := NewRangeVectorSelector(selector, mc, types.NewQueryStats())

			_, err := rv.SeriesMetadata(ctx, nil)
			require.NoError(t, err)

			if len(tc.expected) == 0 {
				require.Equal(t, types.EOS, rv.NextSeries(ctx))
				return
			}

			require.NoError(t, rv.NextSeries(ctx))

			step, err := rv.NextStepSamples(ctx)
			require.NoError(t, err)
			require.NotNil(t, step)

			head, tail := step.Floats.UnsafePoints()
			points := append([]promql.FPoint{}, head...)
			points = append(points, tail...)

			require.Equal(t, tc.expected, points)
		})
	}
}
