// SPDX-License-Identifier: AGPL-3.0-only

package testutils

import (
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// Why do we do this rather than require.Equal(t, expected, actual)?
// It's possible that floating point values are slightly different due to imprecision, but require.Equal doesn't allow us to set an allowable difference.
func RequireEqualResults(t testing.TB, expr string, expected, actual *promql.Result, skipAnnotationComparison bool) {
	require.Equal(t, expected.Err, actual.Err)

	if expected.Err != nil {
		require.Nil(t, expected.Value)
		require.Nil(t, actual.Value)
		return
	}

	require.Equal(t, expected.Value.Type(), actual.Value.Type())

	if !skipAnnotationComparison {
		expectedWarnings, expectedInfos := expected.Warnings.AsStrings(expr, 0, 0)
		actualWarnings, actualInfos := actual.Warnings.AsStrings(expr, 0, 0)
		require.ElementsMatch(t, expectedWarnings, actualWarnings)
		require.ElementsMatch(t, expectedInfos, actualInfos)
	}

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
			requireInEpsilonIfNotZeroOrInf(t, expectedSample.F, actualSample.F)
		}
	case parser.ValueTypeMatrix:
		expectedMatrix, err := expected.Matrix()
		require.NoError(t, err)
		actualMatrix, err := actual.Matrix()
		require.NoError(t, err)

		require.Lenf(t, actualMatrix, len(expectedMatrix), "expected result %v", expectedMatrix)

		for i, expectedSeries := range expectedMatrix {
			actualSeries := actualMatrix[i]

			require.Equal(t, expectedSeries.Metric, actualSeries.Metric)
			require.Lenf(t, actualSeries.Floats, len(expectedSeries.Floats), "expected result %v for series %v", expectedSeries.Floats, expectedSeries.Metric)
			require.Lenf(t, actualSeries.Histograms, len(expectedSeries.Histograms), "expected result %v for series %v", expectedSeries.Histograms, expectedSeries.Metric)

			for j, expectedPoint := range expectedSeries.Floats {
				actualPoint := actualSeries.Floats[j]

				require.Equal(t, expectedPoint.T, actualPoint.T)
				requireInEpsilonIfNotZeroOrInf(t, expectedPoint.F, actualPoint.F, "expected series %v to have points %v, but result is %v", expectedSeries.Metric.String(), expectedSeries.Floats, actualSeries.Floats)
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
					requireInEpsilonIfNotZeroOrInf(t, h1.Count, h2.Count, "histogram counts match")
					requireInEpsilonIfNotZeroOrInf(t, h1.Sum, h2.Sum, "histogram sums match")

					if h1.UsesCustomBuckets() {
						requireFloatBucketsMatch(t, h1.CustomValues, h2.CustomValues)
					}

					requireInEpsilonIfNotZeroOrInf(t, h1.ZeroThreshold, h2.ZeroThreshold, "histogram thresholds match")
					requireInEpsilonIfNotZeroOrInf(t, h1.ZeroCount, h2.ZeroCount, "histogram zero counts match")

					requireSpansMatch(t, h1.NegativeSpans, h2.NegativeSpans)
					requireFloatBucketsMatch(t, h1.NegativeBuckets, h2.NegativeBuckets)

					requireSpansMatch(t, h1.PositiveSpans, h2.PositiveSpans)
					requireFloatBucketsMatch(t, h1.PositiveBuckets, h2.PositiveBuckets)
				}
			}
		}
	case parser.ValueTypeString:
		require.Equal(t, expected.String(), actual.String())
	default:
		require.Fail(t, "unexpected value type", "type: %v", expected.Value.Type())
	}
}

func requireInEpsilonIfNotZeroOrInf(t testing.TB, expected, actual float64, msgAndArgs ...interface{}) {
	if expected == 0 || math.IsInf(expected, +1) || math.IsInf(expected, -1) {
		require.Equal(t, expected, actual, msgAndArgs...)
	} else {
		require.InEpsilon(t, expected, actual, 1e-10, msgAndArgs...)
	}
}

func requireFloatBucketsMatch(t testing.TB, b1, b2 []float64) {
	require.Equal(t, len(b1), len(b2), "bucket lengths match")
	for i, b := range b1 {
		if b == 0 || math.IsInf(b, +1) || math.IsInf(b, -1) {
			require.Equal(t, b, b2[i], "bucket values match")
		} else {
			require.InEpsilon(t, b, b2[i], 1e-10, "bucket values match")
		}
	}
}

func requireSpansMatch(t testing.TB, s1, s2 []histogram.Span) {
	require.Equal(t, len(s1), len(s2), "number of spans")
	for i := range s1 {
		require.Equal(t, s1[i].Length, s2[i].Length, "Span lengths match")
		require.Equal(t, s1[i].Offset, s2[i].Offset, "Span offsets match")
	}
}

func sortVector(v promql.Vector) {
	slices.SortFunc(v, func(a, b promql.Sample) int {
		return labels.Compare(a.Metric, b.Metric)
	})
}

// Combinations generates all Combinations of a given length from a slice of strings.
func Combinations(arr []string, length int) [][]string {
	if length < 0 || length > len(arr) {
		panic("Invalid length requested")
	}
	return combine(arr, length, 0)
}

func combine(arr []string, length int, start int) [][]string {
	if length == 0 {
		return [][]string{{}}
	}
	result := [][]string{}
	for i := start; i <= len(arr)-length; i++ {
		for _, suffix := range combine(arr, length-1, i+1) {
			combination := append([]string{arr[i]}, suffix...)
			result = append(result, combination)
		}
	}
	return result
}

func LabelsToSeriesMetadata(lbls []labels.Labels) []types.SeriesMetadata {
	if len(lbls) == 0 {
		return nil
	}

	m := make([]types.SeriesMetadata, len(lbls))

	for i, l := range lbls {
		m[i].Labels = l
	}

	return m
}

func TrimIndent(s string) string {
	lines := strings.Split(s, "\n")

	// Remove leading empty lines
	for len(lines) > 0 && isEmpty(lines[0]) {
		lines = lines[1:]
	}

	// Remove trailing empty lines
	for len(lines) > 0 && isEmpty(lines[len(lines)-1]) {
		lines = lines[:len(lines)-1]
	}

	if len(lines) == 0 {
		return ""
	}

	// Identify the indentation applied to the first line, and remove it from all lines.
	indentation := ""
	for _, char := range lines[0] {
		if char != '\t' {
			break
		}
		indentation += string(char)
	}

	for i, line := range lines {
		lines[i] = strings.TrimPrefix(line, indentation)
	}

	return strings.Join(lines, "\n")
}

func isEmpty(s string) bool {
	return strings.TrimSpace(s) == ""
}
