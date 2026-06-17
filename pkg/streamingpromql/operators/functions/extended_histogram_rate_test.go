// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// newHistogramView builds an HPointRingBufferView containing points, which must be in ascending
// timestamp order. It is used to exercise the view-based helpers in this file.
func newHistogramView(t *testing.T, points []promql.HPoint) *types.HPointRingBufferView {
	t.Helper()
	tracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
	buf := types.NewHPointRingBuffer(tracker)
	for _, p := range points {
		_, err := buf.Append(p)
		require.NoError(t, err)
	}
	return buf.ViewUntilSearchingForwards(math.MaxInt64, nil)
}

// recordingEmitter returns an EmitAnnotationFunc that records the rendered message of every
// annotation it is asked to emit, so tests can assert which annotations were produced.
func recordingEmitter() (types.EmitAnnotationFunc, *[]string) {
	var msgs []string
	emit := func(g types.AnnotationGenerator) {
		msgs = append(msgs, g("test_metric", posrange.PositionRange{}).Error())
	}
	return emit, &msgs
}

func expHist(count, sum float64, hint histogram.CounterResetHint) *histogram.FloatHistogram {
	return &histogram.FloatHistogram{Schema: 0, Count: count, Sum: sum, CounterResetHint: hint}
}

func customHist(count, sum float64, hint histogram.CounterResetHint) *histogram.FloatHistogram {
	return &histogram.FloatHistogram{Schema: histogram.CustomBucketsSchema, CustomValues: []float64{1}, Count: count, Sum: sum, CounterResetHint: hint}
}

// mixedExpCustomWarning is the substring of the annotation emitted when exponential and
// custom-bucket histograms are mixed.
const mixedExpCustomWarning = "vector contains a mix of histograms with exponential and custom buckets schemas"

// requireHistogramCountSum asserts that h is non-nil and has the expected Count and Sum.
func requireHistogramCountSum(t *testing.T, h *histogram.FloatHistogram, count, sum float64) {
	t.Helper()
	require.NotNil(t, h)
	require.Equal(t, count, h.Count)
	require.Equal(t, sum, h.Sum)
}

// requireSingleAnnotation asserts that exactly one annotation was emitted and that its message
// contains substr.
func requireSingleAnnotation(t *testing.T, msgs *[]string, substr string) {
	t.Helper()
	require.Len(t, *msgs, 1)
	require.Contains(t, (*msgs)[0], substr)
}

func TestInterpolateHistograms(t *testing.T) {
	testCases := map[string]struct {
		h1, h2        *histogram.FloatHistogram
		t1, t2, t     int64
		isCounter     bool
		expectedCount float64
		expectedSum   float64
		expectedErr   error // non-nil means the call must fail and return no histogram.
	}{
		"returns a copy of the left histogram when t equals t1": {
			h1: expHist(10, 100, histogram.UnknownCounterReset),
			h2: expHist(20, 200, histogram.UnknownCounterReset),
			t1: 0, t2: 10, t: 0,
			expectedCount: 10,
			expectedSum:   100,
		},
		"returns a copy of the right histogram when t equals t2": {
			h1: expHist(10, 100, histogram.UnknownCounterReset),
			h2: expHist(20, 200, histogram.UnknownCounterReset),
			t1: 0, t2: 10, t: 10,
			expectedCount: 20,
			expectedSum:   200,
		},
		"linearly interpolates at the midpoint": {
			// fraction = (5-0)/(10-0) = 0.5, so result = 10 + (20-10)*0.5 = 15.
			h1: expHist(10, 100, histogram.UnknownCounterReset),
			h2: expHist(20, 200, histogram.UnknownCounterReset),
			t1: 0, t2: 10, t: 5,
			expectedCount: 15,
			expectedSum:   150,
		},
		"models a detected counter reset as starting from zero": {
			// CounterReset hint forces h2.DetectReset(h1) to return true, so the result is
			// h2 * fraction = 20 * 0.5 = 10 (not interpolated against h1).
			h1: expHist(10, 100, histogram.UnknownCounterReset),
			h2: expHist(20, 200, histogram.CounterReset),
			t1: 0, t2: 10, t: 5,
			isCounter:     true,
			expectedCount: 10,
			expectedSum:   100,
		},
		"returns an error when mixing exponential and custom-bucket schemas": {
			// isCounter false so the reset short-circuit is skipped and Sub is reached.
			h1: expHist(10, 100, histogram.UnknownCounterReset),
			h2: customHist(20, 200, histogram.UnknownCounterReset),
			t1: 0, t2: 10, t: 5,
			expectedErr: histogram.ErrHistogramsIncompatibleSchema,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			emit, msgs := recordingEmitter()

			got, err := interpolateHistograms(tc.h1, tc.t1, tc.h2, tc.t2, tc.t, tc.isCounter, emit)

			if tc.expectedErr != nil {
				require.ErrorIs(t, err, tc.expectedErr)
				require.Nil(t, got)
				require.Empty(t, *msgs)
				return
			}

			require.NoError(t, err)
			requireHistogramCountSum(t, got, tc.expectedCount, tc.expectedSum)
			require.NotSame(t, tc.h1, got, "must return a copy, not an input histogram")
			require.NotSame(t, tc.h2, got, "must return a copy, not an input histogram")
			require.Empty(t, *msgs)
		})
	}
}

func TestNativeHistogramErrorToAnnotation(t *testing.T) {
	t.Run("nil error returns nil and emits no annotation", func(t *testing.T) {
		emit, msgs := recordingEmitter()
		require.NoError(t, NativeHistogramErrorToAnnotation(nil, emit))
		require.Empty(t, *msgs)
	})

	t.Run("incompatible schema emits MixedExponentialCustomHistogramsWarning", func(t *testing.T) {
		emit, msgs := recordingEmitter()
		require.NoError(t, NativeHistogramErrorToAnnotation(histogram.ErrHistogramsIncompatibleSchema, emit))
		requireSingleAnnotation(t, msgs, mixedExpCustomWarning)
	})

	t.Run("unrecognised error is returned and emits no annotation", func(t *testing.T) {
		emit, msgs := recordingEmitter()
		sentinel := errors.New("some other error")
		require.ErrorIs(t, NativeHistogramErrorToAnnotation(sentinel, emit), sentinel)
		require.Empty(t, *msgs)
	})
}

func TestAddHistogramWithAnnotations(t *testing.T) {
	t.Run("compatible histograms are added in place and report success", func(t *testing.T) {
		emit, msgs := recordingEmitter()
		base := expHist(10, 100, histogram.UnknownCounterReset)
		other := expHist(5, 50, histogram.UnknownCounterReset)

		ok, err := addHistogramWithAnnotations(base, other, emit)
		require.True(t, ok)
		require.NoError(t, err)
		requireHistogramCountSum(t, base, 15, 150)
		require.Empty(t, *msgs)
	})

	t.Run("incompatible schemas report failure, warn, and return no error", func(t *testing.T) {
		emit, msgs := recordingEmitter()
		base := expHist(10, 100, histogram.UnknownCounterReset)
		other := customHist(5, 50, histogram.UnknownCounterReset)

		ok, err := addHistogramWithAnnotations(base, other, emit)
		require.False(t, ok)
		require.NoError(t, err)
		requireSingleAnnotation(t, msgs, mixedExpCustomWarning)
	})
}

func TestSubHistogramWithAnnotations(t *testing.T) {
	t.Run("compatible histograms are subtracted in place and report success", func(t *testing.T) {
		emit, msgs := recordingEmitter()
		base := expHist(15, 150, histogram.UnknownCounterReset)
		other := expHist(5, 50, histogram.UnknownCounterReset)

		ok, err := subHistogramWithAnnotations(base, other, emit)
		require.True(t, ok)
		require.NoError(t, err)
		requireHistogramCountSum(t, base, 10, 100)
		require.Empty(t, *msgs)
	})

	t.Run("incompatible schemas report failure, warn, and return no error", func(t *testing.T) {
		emit, msgs := recordingEmitter()
		base := expHist(15, 150, histogram.UnknownCounterReset)
		other := customHist(5, 50, histogram.UnknownCounterReset)

		ok, err := subHistogramWithAnnotations(base, other, emit)
		require.False(t, ok)
		require.NoError(t, err)
		requireSingleAnnotation(t, msgs, mixedExpCustomWarning)
	})
}

func TestValidateHistogramRange(t *testing.T) {
	t.Run("uniform exponential schemas are valid", func(t *testing.T) {
		emit, msgs := recordingEmitter()
		view := newHistogramView(t, []promql.HPoint{
			{T: 0, H: expHist(10, 100, histogram.UnknownCounterReset)},
			{T: 10, H: expHist(20, 200, histogram.UnknownCounterReset)},
		})
		require.True(t, validateHistogramRange(view, 0, view.Count()-1, true, emit))
		require.Empty(t, *msgs)
	})

	t.Run("mixing exponential and custom buckets is invalid and warns", func(t *testing.T) {
		emit, msgs := recordingEmitter()
		view := newHistogramView(t, []promql.HPoint{
			{T: 0, H: expHist(10, 100, histogram.UnknownCounterReset)},
			{T: 10, H: customHist(20, 200, histogram.UnknownCounterReset)},
		})
		require.False(t, validateHistogramRange(view, 0, view.Count()-1, true, emit))
		requireSingleAnnotation(t, msgs, mixedExpCustomWarning)
	})

	t.Run("a gauge sample under a counter function warns but remains valid", func(t *testing.T) {
		emit, msgs := recordingEmitter()
		view := newHistogramView(t, []promql.HPoint{
			{T: 0, H: expHist(10, 100, histogram.GaugeType)},
		})
		require.True(t, validateHistogramRange(view, 0, view.Count()-1, true, emit))
		requireSingleAnnotation(t, msgs, "this native histogram metric is not a counter")
	})
}

func TestCorrectForCounterResetsHistogram(t *testing.T) {
	// left and right (the range-boundary histograms) are taken to be the first and last in-window
	// samples, which matches every non-interpolated boundary - the scenarios these cases exercise.
	testCases := map[string]struct {
		h                []promql.HPoint
		firstSampleIndex int
		lastSampleIndex  int
		rangeStart       int64
		smoothed         bool
		expectCorrection bool
		expectedCount    float64
		expectedSum      float64
	}{
		"single-sample window has nothing to correct": {
			h:                []promql.HPoint{{T: 0, H: expHist(10, 100, histogram.UnknownCounterReset)}},
			firstSampleIndex: 0,
			lastSampleIndex:  0,
		},
		"monotonically increasing samples need no correction": {
			h: []promql.HPoint{
				{T: 0, H: expHist(10, 100, histogram.NotCounterReset)},
				{T: 10, H: expHist(20, 200, histogram.NotCounterReset)},
				{T: 20, H: expHist(30, 300, histogram.NotCounterReset)},
			},
			firstSampleIndex: 0,
			lastSampleIndex:  2,
		},
		"an interior reset contributes the pre-reset value": {
			// CounterReset on h[1] forces a detected reset against the left (h[0]), so the
			// correction is h[0] = count 10.
			h: []promql.HPoint{
				{T: 0, H: expHist(10, 100, histogram.NotCounterReset)},
				{T: 10, H: expHist(3, 30, histogram.CounterReset)},
				{T: 20, H: expHist(8, 80, histogram.NotCounterReset)},
			},
			firstSampleIndex: 0,
			lastSampleIndex:  2,
			expectCorrection: true,
			expectedCount:    10,
			expectedSum:      100,
		},
		"multiple interior resets accumulate their pre-reset values": {
			// Resets at h[1] (vs h[0]=count 10) and h[3] (vs h[2]=count 5); the correction
			// accumulates both: 10 + 5 = 15. Exercises the second addCorrection branch.
			h: []promql.HPoint{
				{T: 0, H: expHist(10, 100, histogram.NotCounterReset)},
				{T: 10, H: expHist(2, 20, histogram.CounterReset)},
				{T: 20, H: expHist(5, 50, histogram.NotCounterReset)},
				{T: 30, H: expHist(1, 10, histogram.CounterReset)},
				{T: 40, H: expHist(6, 60, histogram.NotCounterReset)},
			},
			firstSampleIndex: 0,
			lastSampleIndex:  4,
			expectCorrection: true,
			expectedCount:    15,
			expectedSum:      150,
		},
		"a reset at the right boundary contributes the pre-reset value": {
			// lastSampleIndex is excluded from the interior loop; right (h[1], CounterReset) resets
			// against the left (h[0]), so the correction is h[0] = count 10.
			h: []promql.HPoint{
				{T: 0, H: expHist(10, 100, histogram.NotCounterReset)},
				{T: 10, H: expHist(4, 40, histogram.CounterReset)},
			},
			firstSampleIndex: 0,
			lastSampleIndex:  1,
			expectCorrection: true,
			expectedCount:    10,
			expectedSum:      100,
		},
		"a smoothed left interpolation that already spanned the reset is not double-counted": {
			// rangeStart=5 so h[0].T(0) < rangeStart and the left interpolation spans the h[0]->h[1]
			// reset; that reset must be skipped from the loop.
			h: []promql.HPoint{
				{T: 0, H: expHist(10, 100, histogram.NotCounterReset)},
				{T: 10, H: expHist(3, 30, histogram.CounterReset)},
				{T: 20, H: expHist(8, 80, histogram.NotCounterReset)},
			},
			firstSampleIndex: 0,
			lastSampleIndex:  2,
			rangeStart:       5,
			smoothed:         true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			emit, msgs := recordingEmitter()
			view := newHistogramView(t, tc.h)
			left := view.PointAt(tc.firstSampleIndex).H
			right := view.PointAt(tc.lastSampleIndex).H

			correction, ok, err := correctForCounterResetsHistogram(view, tc.firstSampleIndex, tc.lastSampleIndex, left, right, tc.rangeStart, tc.smoothed, emit)
			require.True(t, ok)
			require.NoError(t, err)

			if tc.expectCorrection {
				requireHistogramCountSum(t, correction, tc.expectedCount, tc.expectedSum)
			} else {
				require.Nil(t, correction)
			}
			require.Empty(t, *msgs)
		})
	}
}

func TestExtendedHistogramRate(t *testing.T) {
	emit, _ := recordingEmitter()

	t.Run("empty input returns no result", func(t *testing.T) {
		got, err := extendedHistogramRate(newHistogramView(t, nil), 0, 10, 0.01, true, true, false, emit)
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("a window with no sample after rangeStart returns no result", func(t *testing.T) {
		view := newHistogramView(t, []promql.HPoint{{T: 0, H: expHist(10, 100, histogram.UnknownCounterReset)}})
		// originalRangeStart=5 is at/after the only sample's timestamp.
		got, err := extendedHistogramRate(view, 5, 15, 0.01, true, true, false, emit)
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("anchored increase subtracts the boundary values", func(t *testing.T) {
		view := newHistogramView(t, []promql.HPoint{
			{T: 0, H: expHist(10, 100, histogram.UnknownCounterReset)},
			{T: 10, H: expHist(30, 300, histogram.UnknownCounterReset)},
		})
		// increase over [0,10]: right(30) - left(10) = 20, no resets, isRate=false.
		got, err := extendedHistogramRate(view, 0, 10, 0.01, true, false, false, emit)
		require.NoError(t, err)
		requireHistogramCountSum(t, got, 20, 200)
		require.Equal(t, histogram.GaugeType, got.CounterResetHint)
	})
}
