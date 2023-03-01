// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
)

func GenerateTestHistogram(i int) *histogram.Histogram {
	return tsdb.GenerateTestHistogram(i)
}

func GenerateTestFloatHistogram(i int) *histogram.FloatHistogram {
	return tsdb.GenerateTestFloatHistogram(i)
}

// RequireHistogramEqual ignores counter resets of non gauge histograms
func RequireHistogramEqual(t require.TestingT, expected, actual *histogram.Histogram, msgAndArgs ...interface{}) {
	if expected.CounterResetHint != histogram.GaugeType {
		// Ignore counter resets injected by tsdb
		actual.CounterResetHint = histogram.UnknownCounterReset
	}
	require.EqualValues(t, expected, actual, msgAndArgs)
}

// RequireFloatHistogramEqual ignores counter resets of non gauge histograms
func RequireFloatHistogramEqual(t require.TestingT, expected, actual *histogram.FloatHistogram, msgAndArgs ...interface{}) {
	if expected.CounterResetHint != histogram.GaugeType {
		// Ignore counter resets injected by tsdb
		actual.CounterResetHint = histogram.UnknownCounterReset
	}
	require.EqualValues(t, expected, actual, msgAndArgs)
}
