// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"
)

func expHist(count, sum float64, hint histogram.CounterResetHint) *histogram.FloatHistogram {
	return &histogram.FloatHistogram{Schema: 0, Count: count, Sum: sum, CounterResetHint: hint}
}

func customHist(bounds []float64, buckets []float64, count, sum float64, hint histogram.CounterResetHint) *histogram.FloatHistogram {
	return &histogram.FloatHistogram{
		Schema:           histogram.CustomBucketsSchema,
		CustomValues:     bounds,
		PositiveSpans:    []histogram.Span{{Offset: 0, Length: uint32(len(buckets))}},
		PositiveBuckets:  buckets,
		Count:            count,
		Sum:              sum,
		CounterResetHint: hint,
	}
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
		"interpolates gauge data without modelling a reset": {
			// isCounter false, so the reset short-circuit is skipped even though h2 < h1.
			// result = 20 + (10-20)*0.5 = 15.
			h1: expHist(20, 200, histogram.GaugeType),
			h2: expHist(10, 100, histogram.GaugeType),
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
			h1: expHist(10, 100, histogram.UnknownCounterReset),
			h2: customHist([]float64{1}, []float64{20}, 20, 200, histogram.UnknownCounterReset),
			t1: 0, t2: 10, t: 5,
			expectedErr: histogram.ErrHistogramsIncompatibleSchema,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var ops []annotations.HistogramOperation
			emit := func(op annotations.HistogramOperation) { ops = append(ops, op) }

			got, err := InterpolateHistograms(tc.h1, tc.t1, tc.h2, tc.t2, tc.t, tc.isCounter, emit)

			if tc.expectedErr != nil {
				require.ErrorIs(t, err, tc.expectedErr)
				require.Nil(t, got)
				require.Empty(t, ops)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, got)
			require.Equal(t, tc.expectedCount, got.Count)
			require.Equal(t, tc.expectedSum, got.Sum)
			require.NotSame(t, tc.h1, got, "must return a copy, not an input histogram")
			require.NotSame(t, tc.h2, got, "must return a copy, not an input histogram")
			require.Empty(t, ops)
		})
	}
}

func TestInterpolateHistograms_EmitsInfoOnCustomBucketReconciliation(t *testing.T) {
	// Two custom-bucket histograms with mismatched bounds force FloatHistogram.Sub to reconcile
	// the bucket layout, which must surface a HistogramSub info via the emitInfo callback.
	h1 := customHist([]float64{1, 2}, []float64{1, 1}, 2, 3, histogram.GaugeType)
	h2 := customHist([]float64{1, 2, 3}, []float64{2, 2, 2}, 6, 12, histogram.GaugeType)

	var ops []annotations.HistogramOperation
	emit := func(op annotations.HistogramOperation) { ops = append(ops, op) }

	got, err := InterpolateHistograms(h1, 0, h2, 10, 5, false, emit)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Contains(t, ops, annotations.HistogramSub)
}
