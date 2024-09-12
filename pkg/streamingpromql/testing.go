// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"math"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func NewTestEngineOpts() EngineOpts {
	return EngineOpts{
		CommonOpts: promql.EngineOpts{
			Logger:               nil,
			Reg:                  nil,
			MaxSamples:           math.MaxInt,
			Timeout:              100 * time.Second,
			EnableAtModifier:     true,
			EnableNegativeOffset: true,
		},

		FeatureToggles: EnableAllFeatures,
		Pedantic:       true,
	}
}

// Why do we do this rather than require.Equal(t, expected, actual)?
// It's possible that floating point values are slightly different due to imprecision, but require.Equal doesn't allow us to set an allowable difference.
func RequireEqualResults(t testing.TB, expr string, expected, actual *promql.Result) {
	require.Equal(t, expected.Err, actual.Err)
	require.Equal(t, expected.Value.Type(), actual.Value.Type())

	expectedWarnings, expectedInfos := expected.Warnings.AsStrings(expr, 0, 0)
	actualWarnings, actualInfos := actual.Warnings.AsStrings(expr, 0, 0)
	require.ElementsMatch(t, expectedWarnings, actualWarnings)
	require.ElementsMatch(t, expectedInfos, actualInfos)

	switch expected.Value.Type() {
	case parser.ValueTypeVector:
		expectedVector, err := expected.Vector()
		require.NoError(t, err)
		actualVector, err := actual.Vector()
		require.NoError(t, err)

		// Instant queries don't guarantee any particular sort order, so sort results here so that we can easily compare them.
		sortVector(expectedVector)
		sortVector(actualVector)

		require.Len(t, actualVector, len(expectedVector))

		for i, expectedSample := range expectedVector {
			actualSample := actualVector[i]

			require.Equal(t, expectedSample.Metric, actualSample.Metric)
			require.Equal(t, expectedSample.T, actualSample.T)
			require.Equal(t, expectedSample.H, actualSample.H)
			requireInEpsilonIfNotZero(t, expectedSample.F, actualSample.F)
		}
	case parser.ValueTypeMatrix:
		expectedMatrix, err := expected.Matrix()
		require.NoError(t, err)
		actualMatrix, err := actual.Matrix()
		require.NoError(t, err)

		require.Len(t, actualMatrix, len(expectedMatrix))

		for i, expectedSeries := range expectedMatrix {
			actualSeries := actualMatrix[i]

			require.Equal(t, expectedSeries.Metric, actualSeries.Metric)

			for j, expectedPoint := range expectedSeries.Floats {
				actualPoint := actualSeries.Floats[j]

				require.Equal(t, expectedPoint.T, actualPoint.T)
				requireInEpsilonIfNotZero(t, expectedPoint.F, actualPoint.F, "expected series %v to have points %v, but result is %v", expectedSeries.Metric.String(), expectedSeries.Floats, actualSeries.Floats)
			}

			for j, expectedPoint := range actualSeries.Histograms {
				actualPoint := actualSeries.Histograms[j]

				require.Equal(t, expectedPoint.T, actualPoint.T)
				if expectedPoint.H == nil {
					require.Equal(t, expectedPoint.H, actualPoint.H)
				} else {
					h1 := expectedPoint.H
					h2 := actualPoint.H

					require.Equal(t, h1.Schema, h2.Schema, "histogram schemas match")
					requireInEpsilonIfNotZero(t, h1.Count, h2.Count, "histogram counts match")
					requireInEpsilonIfNotZero(t, h1.Sum, h2.Sum, "histogram sums match")

					if h1.UsesCustomBuckets() {
						requireFloatBucketsMatch(t, h1.CustomValues, h2.CustomValues)
					}

					requireInEpsilonIfNotZero(t, h1.ZeroThreshold, h2.ZeroThreshold, "histogram thresholds match")
					requireInEpsilonIfNotZero(t, h1.ZeroCount, h2.ZeroCount, "histogram zero counts match")

					require.True(t, spansMatch(h1.NegativeSpans, h2.NegativeSpans), "spans match")
					requireFloatBucketsMatch(t, h1.NegativeBuckets, h2.NegativeBuckets)

					require.True(t, spansMatch(h1.PositiveSpans, h2.PositiveSpans))
					requireFloatBucketsMatch(t, h1.PositiveBuckets, h2.PositiveBuckets)
				}
			}
		}
	default:
		require.Fail(t, "unexpected value type", "type: %v", expected.Value.Type())
	}
}

func requireInEpsilonIfNotZero(t testing.TB, expected, actual float64, msgAndArgs ...interface{}) {
	if expected == 0 {
		require.Equal(t, expected, actual, msgAndArgs...)
	} else {
		require.InEpsilon(t, expected, actual, 1e-10, msgAndArgs...)
	}
}

func requireFloatBucketsMatch(t testing.TB, b1, b2 []float64) {
	require.Equal(t, len(b1), len(b2), "bucket lengths match")
	for i, b := range b1 {
		require.InEpsilon(t, b, b2[i], 1e-10, "bucket values match")
	}
}

// Copied from prometheus as it is not exported
// spansMatch returns true if both spans represent the same bucket layout
// after combining zero length spans with the next non-zero length span.
func spansMatch(s1, s2 []histogram.Span) bool {
	if len(s1) == 0 && len(s2) == 0 {
		return true
	}

	s1idx, s2idx := 0, 0
	for {
		if s1idx >= len(s1) {
			return allEmptySpans(s2[s2idx:])
		}
		if s2idx >= len(s2) {
			return allEmptySpans(s1[s1idx:])
		}

		currS1, currS2 := s1[s1idx], s2[s2idx]
		s1idx++
		s2idx++
		if currS1.Length == 0 {
			// This span is zero length, so we add consecutive such spans
			// until we find a non-zero span.
			for ; s1idx < len(s1) && s1[s1idx].Length == 0; s1idx++ {
				currS1.Offset += s1[s1idx].Offset
			}
			if s1idx < len(s1) {
				currS1.Offset += s1[s1idx].Offset
				currS1.Length = s1[s1idx].Length
				s1idx++
			}
		}
		if currS2.Length == 0 {
			// This span is zero length, so we add consecutive such spans
			// until we find a non-zero span.
			for ; s2idx < len(s2) && s2[s2idx].Length == 0; s2idx++ {
				currS2.Offset += s2[s2idx].Offset
			}
			if s2idx < len(s2) {
				currS2.Offset += s2[s2idx].Offset
				currS2.Length = s2[s2idx].Length
				s2idx++
			}
		}

		if currS1.Length == 0 && currS2.Length == 0 {
			// The last spans of both set are zero length. Previous spans match.
			return true
		}

		if currS1.Offset != currS2.Offset || currS1.Length != currS2.Length {
			return false
		}
	}
}

func allEmptySpans(s []histogram.Span) bool {
	for _, ss := range s {
		if ss.Length > 0 {
			return false
		}
	}
	return true
}

func sortVector(v promql.Vector) {
	slices.SortFunc(v, func(a, b promql.Sample) int {
		return labels.Compare(a.Metric, b.Metric)
	})
}
