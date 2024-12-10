// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/require"
)

func GenerateTestHistograms(i int) []*histogram.Histogram {
	return tsdbutil.GenerateTestHistograms(i)
}

func GenerateTestFloatHistograms(i int) []*histogram.FloatHistogram {
	return tsdbutil.GenerateTestFloatHistograms(i)
}

func GenerateTestHistogram(i int) *histogram.Histogram {
	return tsdbutil.GenerateTestHistogram(i)
}

func GenerateTestFloatHistogram(i int) *histogram.FloatHistogram {
	return tsdbutil.GenerateTestFloatHistogram(i)
}

func GenerateTestGaugeHistogram(i int) *histogram.Histogram {
	return tsdbutil.GenerateTestGaugeHistogram(i)
}

func GenerateTestGaugeFloatHistogram(i int) *histogram.FloatHistogram {
	return tsdbutil.GenerateTestGaugeFloatHistogram(i)
}

// explicit decoded version of GenerateTestHistogram and GenerateTestFloatHistogram
func GenerateTestSampleHistogram(i int) *model.SampleHistogram {
	return &model.SampleHistogram{
		Count: model.FloatString(12 + i*9),
		Sum:   model.FloatString(18.4 * float64(i+1)),
		Buckets: model.HistogramBuckets{
			&model.HistogramBucket{
				Boundaries: 1,
				Lower:      -4,
				Upper:      -2.82842712474619,
				Count:      model.FloatString(1 + i),
			},
			&model.HistogramBucket{
				Boundaries: 1,
				Lower:      -2.82842712474619,
				Upper:      -2,
				Count:      model.FloatString(1 + i),
			},
			&model.HistogramBucket{
				Boundaries: 1,
				Lower:      -1.414213562373095,
				Upper:      -1,
				Count:      model.FloatString(2 + i),
			},
			&model.HistogramBucket{
				Boundaries: 1,
				Lower:      -1,
				Upper:      -0.7071067811865475,
				Count:      model.FloatString(1 + i),
			},
			&model.HistogramBucket{
				Boundaries: 3,
				Lower:      -0.001,
				Upper:      0.001,
				Count:      model.FloatString(2 + i),
			},
			&model.HistogramBucket{
				Boundaries: 0,
				Lower:      0.7071067811865475,
				Upper:      1,
				Count:      model.FloatString(1 + i),
			},
			&model.HistogramBucket{
				Boundaries: 0,
				Lower:      1,
				Upper:      1.414213562373095,
				Count:      model.FloatString(2 + i),
			},
			&model.HistogramBucket{
				Boundaries: 0,
				Lower:      2,
				Upper:      2.82842712474619,
				Count:      model.FloatString(1 + i),
			},
			&model.HistogramBucket{
				Boundaries: 0,
				Lower:      2.82842712474619,
				Upper:      4,
				Count:      model.FloatString(1 + i),
			},
		},
	}
}

// RequireHistogramEqual requires the two histograms to be equal.
func RequireHistogramEqual(t require.TestingT, expected, actual *histogram.Histogram, msgAndArgs ...interface{}) {
	require.EqualValues(t, expected, actual, msgAndArgs...)
}

// RequireFloatHistogramEqual requires the two float histograms to be equal.
func RequireFloatHistogramEqual(t require.TestingT, expected, actual *histogram.FloatHistogram, msgAndArgs ...interface{}) {
	require.EqualValues(t, expected, actual, msgAndArgs...)
}
