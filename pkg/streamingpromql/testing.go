// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"math"
	"testing"
	"time"

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

func sortVector(v promql.Vector) {
	slices.SortFunc(v, func(a, b promql.Sample) int {
		return labels.Compare(a.Metric, b.Metric)
	})
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
			if expectedSample.F == 0 {
				require.Equal(t, expectedSample.F, actualSample.F)
			} else {
				require.InEpsilon(t, expectedSample.F, actualSample.F, 1e-10)
			}
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
			require.Equal(t, expectedSeries.Histograms, actualSeries.Histograms)

			for j, expectedPoint := range expectedSeries.Floats {
				actualPoint := actualSeries.Floats[j]

				require.Equal(t, expectedPoint.T, actualPoint.T)
				if expectedPoint.F == 0 {
					require.Equal(t, expectedPoint.F, actualPoint.F)
				} else {
					require.InEpsilonf(t, expectedPoint.F, actualPoint.F, 1e-10, "expected series %v to have points %v, but result is %v", expectedSeries.Metric.String(), expectedSeries.Floats, actualSeries.Floats)
				}
			}
		}
	default:
		require.Fail(t, "unexpected value type", "type: %v", expected.Value.Type())
	}
}
