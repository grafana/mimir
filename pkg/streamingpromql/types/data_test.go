// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestAppendSeriesMetadataFromPool(t *testing.T) {
	tracker := limiter.NewUnlimitedMemoryConsumptionTracker(context.Background())

	meta := func(name string) SeriesMetadata {
		return SeriesMetadata{Labels: labels.FromStrings(model.MetricNameLabel, name)}
	}

	base, err := SeriesMetadataSlicePool.Get(4, tracker)
	require.NoError(t, err)
	require.Empty(t, base)
	baseCap := cap(base)
	require.GreaterOrEqual(t, baseCap, 4)

	// Appends that fit within the existing capacity reuse the same backing array.
	base, err = AppendSeriesMetadataFromPool(tracker, base, meta("a"), meta("b"))
	require.NoError(t, err)
	require.Equal(t, []SeriesMetadata{meta("a"), meta("b")}, base)
	require.Equal(t, baseCap, cap(base), "appending within capacity must not reallocate")

	// Appending past the capacity grows the slice from the pool.
	grown, err := AppendSeriesMetadataFromPool(tracker, base, meta("c"), meta("d"), meta("e"), meta("f"), meta("g"))
	require.NoError(t, err)
	require.Equal(t, []SeriesMetadata{meta("a"), meta("b"), meta("c"), meta("d"), meta("e"), meta("f"), meta("g")}, grown)
	require.Greater(t, cap(grown), baseCap, "appending past capacity must grow the slice")

	// Returning the slice to the pool releases both the slice capacity and the accounted label
	// memory (via the pool's put hook), so consumption returns to zero.
	SeriesMetadataSlicePool.Put(&grown, tracker)
	require.Equal(t, uint64(0), tracker.CurrentEstimatedMemoryConsumptionBytes())
}

func TestAppendSeriesMetadataFromPool_MemoryLimitReturnsSliceToPool(t *testing.T) {
	ctx := context.Background()

	// Learn the exact memory a base slice of this size consumes, so we can build a tracker whose
	// limit is fully consumed by the base slice and thus rejects any subsequent label accounting.
	warmUp := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
	sizeHint := 2
	tmp, err := SeriesMetadataSlicePool.Get(sizeHint, warmUp)
	require.NoError(t, err)
	baseBytes := warmUp.CurrentEstimatedMemoryConsumptionBytes()
	SeriesMetadataSlicePool.Put(&tmp, warmUp)

	_, metric := createRejectedMetric()
	tracker := limiter.NewMemoryConsumptionTracker(ctx, baseBytes, metric, "")

	base, err := SeriesMetadataSlicePool.Get(sizeHint, tracker)
	require.NoError(t, err)
	require.Equal(t, baseBytes, tracker.CurrentEstimatedMemoryConsumptionBytes())

	// The label accounting can't fit under the limit, so the append fails and base is returned to
	// the pool rather than being stranded.
	result, err := AppendSeriesMetadataFromPool(tracker, base, SeriesMetadata{Labels: labels.FromStrings(model.MetricNameLabel, "a")})
	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, uint64(0), tracker.CurrentEstimatedMemoryConsumptionBytes())
}

// TestAppendSeriesMetadataFromPool_SliceGrowthFailureReleasesLabels covers the case where the new
// item's label accounting fits under the limit but the subsequent slice growth does not: the
// item's labels must not be left accounted once the append fails.
func TestAppendSeriesMetadataFromPool_SliceGrowthFailureReleasesLabels(t *testing.T) {
	ctx := context.Background()

	newItem := SeriesMetadata{Labels: labels.FromStrings(model.MetricNameLabel, "new")}
	fillItem := SeriesMetadata{Labels: labels.FromStrings(model.MetricNameLabel, "fill")}

	// Learn, on an unlimited tracker, the consumption of a base slice filled to its capacity (so
	// the next append must grow), so we can set a limit that admits the new item's labels but not
	// the grown slice.
	warmUp := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
	base, err := SeriesMetadataSlicePool.Get(1, warmUp)
	require.NoError(t, err)
	fillItems := make([]SeriesMetadata, cap(base))
	for i := range fillItems {
		fillItems[i] = fillItem
	}
	base, err = AppendSeriesMetadataFromPool(warmUp, base, fillItems...)
	require.NoError(t, err)
	require.Equal(t, len(base), cap(base), "base must be full so the next append triggers a growth")
	fullBytes := warmUp.CurrentEstimatedMemoryConsumptionBytes()

	// Limit leaves exactly enough headroom for the new item's labels, but not for growing the
	// slice, so AppendToSlice's Get fails after the labels have already been accounted.
	_, metric := createRejectedMetric()
	limit := fullBytes + newItem.Labels.ByteSize()
	tracker := limiter.NewMemoryConsumptionTracker(ctx, limit, metric, "")

	base, err = SeriesMetadataSlicePool.Get(1, tracker)
	require.NoError(t, err)
	base, err = AppendSeriesMetadataFromPool(tracker, base, fillItems...)
	require.NoError(t, err)
	require.Equal(t, fullBytes, tracker.CurrentEstimatedMemoryConsumptionBytes())

	result, err := AppendSeriesMetadataFromPool(tracker, base, newItem)
	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, uint64(0), tracker.CurrentEstimatedMemoryConsumptionBytes(),
		"the new item's labels and the base slice must both be released when growth fails")
}

func TestInstantVectorSeriesData_Clone(t *testing.T) {
	original := InstantVectorSeriesData{
		Floats: []promql.FPoint{
			{T: 0, F: 0},
			{T: 1, F: 1},
		},
		Histograms: []promql.HPoint{
			{T: 2, H: &histogram.FloatHistogram{Count: 2}},
			{T: 3, H: &histogram.FloatHistogram{Count: 3}},
		},
	}

	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(context.Background())
	cloned, err := original.Clone(memoryConsumptionTracker)

	require.NoError(t, err)
	require.Equal(t, original, cloned, "clone should have same data as original")
	require.NotSame(t, &original.Floats[0], &cloned.Floats[0], "clone should not share float slice with original")
	require.NotSame(t, &original.Histograms[0], &cloned.Histograms[0], "clone should not share histogram slice with original")
	require.NotSame(t, original.Histograms[0].H, cloned.Histograms[0].H, "clone should not share first histogram with original")
	require.NotSame(t, original.Histograms[1].H, cloned.Histograms[1].H, "clone should not share second histogram with original")
}

func TestInstantVectorSeriesDataIterator(t *testing.T) {
	type expected struct {
		T       int64
		F       float64
		H       *histogram.FloatHistogram
		HIndex  int
		HasNext bool
	}
	type testCase struct {
		name     string
		data     InstantVectorSeriesData
		expected []expected
	}

	testCases := []testCase{
		{
			name: "floats only",
			data: InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1000, F: 1.1},
					{T: 2000, F: 2.2},
					{T: 3000, F: 3.3},
				},
			},
			expected: []expected{
				{1000, 1.1, nil, -1, true},
				{2000, 2.2, nil, -1, true},
				{3000, 3.3, nil, -1, true},
				{0, 0, nil, -1, false},
			},
		},
		{
			name: "histograms only",
			data: InstantVectorSeriesData{
				Histograms: []promql.HPoint{
					{T: 1500, H: &histogram.FloatHistogram{Sum: 1500}},
					{T: 2500, H: &histogram.FloatHistogram{Sum: 2500}},
				},
			},
			expected: []expected{
				{1500, 0, &histogram.FloatHistogram{Sum: 1500}, 0, true},
				{2500, 0, &histogram.FloatHistogram{Sum: 2500}, 1, true},
				{0, 0, nil, -1, false},
			},
		},
		{
			name: "mixed data ends with float",
			data: InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1000, F: 1.1},
					{T: 2000, F: 2.2},
					{T: 3000, F: 3.3},
					{T: 4000, F: 4.4},
					{T: 5000, F: 5.5},
					{T: 6000, F: 6.5},
				},
				Histograms: []promql.HPoint{
					{T: 1500, H: &histogram.FloatHistogram{Sum: 1500}},
					{T: 2500, H: &histogram.FloatHistogram{Sum: 2500}},
					{T: 5500, H: &histogram.FloatHistogram{Sum: 5500}},
				},
			},
			expected: []expected{
				{1000, 1.1, nil, -1, true},
				{1500, 0, &histogram.FloatHistogram{Sum: 1500}, 0, true},
				{2000, 2.2, nil, -1, true},
				{2500, 0, &histogram.FloatHistogram{Sum: 2500}, 1, true},
				{3000, 3.3, nil, -1, true},
				{4000, 4.4, nil, -1, true},
				{5000, 5.5, nil, -1, true},
				{5500, 0, &histogram.FloatHistogram{Sum: 5500}, 2, true},
				{6000, 6.5, nil, -1, true},
				{0, 0, nil, -1, false},
			},
		},
		{
			name: "mixed data ends with histogram",
			data: InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1000, F: 1.1},
					{T: 2000, F: 2.2},
					{T: 3000, F: 3.3},
					{T: 4000, F: 4.4},
					{T: 5000, F: 5.5},
				},
				Histograms: []promql.HPoint{
					{T: 1500, H: &histogram.FloatHistogram{Sum: 1500}},
					{T: 2500, H: &histogram.FloatHistogram{Sum: 2500}},
					{T: 5500, H: &histogram.FloatHistogram{Sum: 5500}},
				},
			},
			expected: []expected{
				{1000, 1.1, nil, -1, true},
				{1500, 0, &histogram.FloatHistogram{Sum: 1500}, 0, true},
				{2000, 2.2, nil, -1, true},
				{2500, 0, &histogram.FloatHistogram{Sum: 2500}, 1, true},
				{3000, 3.3, nil, -1, true},
				{4000, 4.4, nil, -1, true},
				{5000, 5.5, nil, -1, true},
				{5500, 0, &histogram.FloatHistogram{Sum: 5500}, 2, true},
				{0, 0, nil, -1, false},
			},
		},
		{
			name: "empty data",
			data: InstantVectorSeriesData{},
			expected: []expected{
				{0, 0, nil, -1, false},
			},
		},
		{
			name: "multiple next calls after exhaustion",
			data: InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1000, F: 1.1},
				},
			},
			expected: []expected{
				{1000, 1.1, nil, -1, true},
				{0, 0, nil, -1, false},
				{0, 0, nil, -1, false},
				{0, 0, nil, -1, false},
			},
		},
	}

	iter := InstantVectorSeriesDataIterator{}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			iter.Reset(tc.data)

			for _, exp := range tc.expected {
				ts, f, h, hIndex, hasNext := iter.Next()
				require.Equal(t, exp.T, ts)
				require.Equal(t, exp.F, f)
				require.Equal(t, exp.H, h)
				require.Equal(t, exp.HIndex, hIndex)
				require.Equal(t, exp.HasNext, hasNext)
			}
		})
	}
}

func TestHasDuplicateSeries(t *testing.T) {
	testCases := map[string]struct {
		input        []SeriesMetadata
		hasDuplicate bool
	}{
		"no series": {
			input:        []SeriesMetadata{},
			hasDuplicate: false,
		},
		"one series": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo")},
			},
			hasDuplicate: false,
		},
		"two series, both different": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "bar")},
			},
			hasDuplicate: false,
		},
		"two series, some common labels but not completely the same": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "1")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "2")},
			},
			hasDuplicate: false,
		},
		"two series, both the same": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "1")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "1")},
			},
			hasDuplicate: true,
		},
		"three series, all different": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "bar")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "baz")},
			},
			hasDuplicate: false,
		},
		"three series, some with common labels but none completely the same": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "1")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "2")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "3")},
			},
			hasDuplicate: false,
		},
		"three series, some the same": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "1")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "1")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "3")},
			},
			hasDuplicate: true,
		},
		"three series, all the same": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "1")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "1")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "1")},
			},
			hasDuplicate: true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actualHasDuplicate := HasDuplicateSeries(testCase.input)

			require.Equal(t, testCase.hasDuplicate, actualHasDuplicate)
		})
	}
}

func TestQueryTimeRange(t *testing.T) {
	type testCase struct {
		timeRange QueryTimeRange

		expectedIsInstant            bool
		expectedStartT               int64
		expectedEndT                 int64
		expectedIntervalMilliseconds int64
		expectedStepCount            int
		expectedString               string

		testTimes       []time.Time
		expectedIndices []int64
	}

	startTime := time.Date(2020, 1, 2, 3, 4, 5, 678000000, time.UTC)
	testCases := map[string]testCase{
		"instant query": {
			timeRange:                    NewInstantQueryTimeRange(startTime),
			expectedIsInstant:            true,
			expectedStartT:               timestamp.FromTime(startTime),
			expectedEndT:                 timestamp.FromTime(startTime),
			expectedIntervalMilliseconds: 1,
			expectedStepCount:            1,
			expectedString:               "instant query at 1577934245678 (2020-01-02T03:04:05.678Z)",
			testTimes:                    []time.Time{startTime},
			expectedIndices:              []int64{0},
		},
		"range query with 15-minute interval": {
			timeRange:                    NewRangeQueryTimeRange(startTime, startTime.Add(time.Hour), time.Minute*15),
			expectedIsInstant:            false,
			expectedStartT:               timestamp.FromTime(startTime),
			expectedEndT:                 timestamp.FromTime(startTime.Add(time.Hour)),
			expectedIntervalMilliseconds: (time.Minute * 15).Milliseconds(),
			expectedStepCount:            5,
			expectedString:               "range query from 1577934245678 (2020-01-02T03:04:05.678Z) to 1577937845678 (2020-01-02T04:04:05.678Z), 15m0s step (5 steps)",
			testTimes: []time.Time{
				startTime,
				startTime.Add(time.Minute * 15),
				startTime.Add(time.Minute * 30),
				startTime.Add(time.Minute * 45),
				startTime.Add(time.Hour),
			},
			expectedIndices: []int64{0, 1, 2, 3, 4},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expectedStartT, tc.timeRange.StartT, "StartT matches")
			require.Equal(t, tc.expectedEndT, tc.timeRange.EndT, "EndT matches")
			require.Equal(t, tc.expectedIntervalMilliseconds, tc.timeRange.IntervalMilliseconds, "IntervalMs matches")
			require.Equal(t, tc.expectedStepCount, tc.timeRange.StepCount, "StepCount matches")
			require.Equal(t, tc.expectedString, tc.timeRange.String(), "String matches")

			for i, tt := range tc.testTimes {
				ts := timestamp.FromTime(tt)
				pointIdx := tc.timeRange.PointIndex(ts)
				expectedIdx := tc.expectedIndices[i]
				require.Equal(t, expectedIdx, pointIdx, "PointIdx matches for time %v", tt)
			}
		})
	}
}

func TestQueryTimeRange_StepIndexing_RangeQuery(t *testing.T) {
	// Steps at t = 0, 2, 4, 6, 8, 10 minutes (6 steps, 2-minute interval).
	step := 2 * time.Minute
	start := timestamp.Time(0)
	end := start.Add(5 * step)
	tr := NewRangeQueryTimeRange(start, end, step)

	testCases := map[string]struct {
		t                           int64
		expectedFirstIndexAfter     int
		expectedLastIndexAtOrBefore int
	}{
		"before start": {
			t:                           timestamp.FromTime(start.Add(-1 * time.Minute)),
			expectedFirstIndexAfter:     0,  // first step (t=0) is after any t before start
			expectedLastIndexAtOrBefore: -1, // no step at or before this timestamp
		},
		"exactly at start": {
			t:                           timestamp.FromTime(start),
			expectedFirstIndexAfter:     1, // strictly greater than start, so first step after is idx 1 (t=2min)
			expectedLastIndexAtOrBefore: 0, // step at t=0 is at or before t=0
		},
		"between two steps": {
			t:                           timestamp.FromTime(start.Add(3 * time.Minute)),
			expectedFirstIndexAfter:     2, // t=3min → first step strictly after is t=4min at idx 2
			expectedLastIndexAtOrBefore: 1, // t=3min → last step at or before is t=2min at idx 1
		},
		"exactly on a step": {
			t:                           timestamp.FromTime(start.Add(4 * time.Minute)),
			expectedFirstIndexAfter:     3, // t=4min at idx 2; strictly after → idx 3 (t=6min)
			expectedLastIndexAtOrBefore: 2, // step at t=4min is at idx 2
		},
		"exactly at end": {
			t:                           timestamp.FromTime(end),
			expectedFirstIndexAfter:     6, // no step strictly after end; returns StepCount
			expectedLastIndexAtOrBefore: 5, // last step at idx 5 (t=10min)
		},
		"after end": {
			t:                           timestamp.FromTime(end.Add(time.Minute)),
			expectedFirstIndexAfter:     6, // no step strictly after; returns StepCount
			expectedLastIndexAtOrBefore: 5, // still returns last step (idx 5)
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expectedFirstIndexAfter, tr.FirstPointIndexAfter(tc.t))
			require.Equal(t, tc.expectedLastIndexAtOrBefore, tr.LastPointIndexAtOrBefore(tc.t))
		})
	}
}

func TestQueryTimeRange_StepIndexing_InstantQuery(t *testing.T) {
	queryT := timestamp.Time(0)
	tr := NewInstantQueryTimeRange(queryT)

	testCases := map[string]struct {
		t                           int64
		expectedFirstIndexAfter     int
		expectedLastIndexAtOrBefore int
	}{
		"before query time": {
			t:                           timestamp.FromTime(queryT.Add(-time.Minute)),
			expectedFirstIndexAfter:     0,  // the single step is after t
			expectedLastIndexAtOrBefore: -1, // no step at or before t
		},
		"exactly at query time": {
			t:                           timestamp.FromTime(queryT),
			expectedFirstIndexAfter:     1, // no step strictly after query time; returns StepCount (1)
			expectedLastIndexAtOrBefore: 0, // the single step is at query time
		},
		"after query time": {
			t:                           timestamp.FromTime(queryT.Add(time.Minute)),
			expectedFirstIndexAfter:     1, // no step strictly after query time; returns StepCount (1)
			expectedLastIndexAtOrBefore: 0, // the single step is at or before t
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expectedFirstIndexAfter, tr.FirstPointIndexAfter(tc.t))
			require.Equal(t, tc.expectedLastIndexAtOrBefore, tr.LastPointIndexAtOrBefore(tc.t))
		})
	}
}
