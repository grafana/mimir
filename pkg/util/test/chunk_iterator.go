// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
)

// RequireIteratorFloat checks that the iterator contains the expected
// float value and type at the position.
func RequireIteratorFloat(t *testing.T, expectedTs int64, expectedV float64, iter chunkenc.Iterator, valueType chunkenc.ValueType) {
	require.Equal(t, chunkenc.ValFloat, valueType)
	ts, s := iter.At()
	require.Equal(t, expectedTs, ts)
	require.Equal(t, expectedV, s)
	require.NoError(t, iter.Err())
}

func RequireIteratorIthFloat(t *testing.T, i int64, iter chunkenc.Iterator, valueType chunkenc.ValueType) {
	RequireIteratorFloat(t, i, float64(i), iter, valueType)
}

// RequireIteratorHistogram checks that the iterator contains the expected
// histogram value and type at the position. Also test automatic conversion to float histogram.
func RequireIteratorHistogram(t *testing.T, expectedTs int64, expectedV *histogram.Histogram, iter chunkenc.Iterator, valueType chunkenc.ValueType) {
	require.Equal(t, chunkenc.ValHistogram, valueType)
	ts, h := iter.AtHistogram(nil)
	require.Equal(t, expectedTs, ts)
	RequireHistogramEqual(t, expectedV, h)
	require.NoError(t, iter.Err())
	// check auto type conversion for PromQL
	ts2, fh := iter.AtFloatHistogram(nil)
	require.Equal(t, expectedTs, ts2)
	RequireFloatHistogramEqual(t, expectedV.ToFloat(nil), fh)
	require.NoError(t, iter.Err())
}

func RequireIteratorIthHistogram(t *testing.T, i int64, iter chunkenc.Iterator, valueType chunkenc.ValueType) {
	RequireIteratorHistogram(t, i, GenerateTestHistogram(int(i)), iter, valueType)
}

// RequireIteratorFloatHistogram checks that the iterator contains the expected
// float histogram value and type at the position.
func RequireIteratorFloatHistogram(t *testing.T, expectedTs int64, expectedV *histogram.FloatHistogram, iter chunkenc.Iterator, valueType chunkenc.ValueType) {
	require.Equal(t, chunkenc.ValFloatHistogram, valueType)
	ts, h := iter.AtFloatHistogram(nil)
	require.Equal(t, expectedTs, ts)
	RequireFloatHistogramEqual(t, expectedV, h)
	require.NoError(t, iter.Err())
}

func RequireIteratorIthFloatHistogram(t *testing.T, i int64, iter chunkenc.Iterator, valueType chunkenc.ValueType) {
	RequireIteratorFloatHistogram(t, i, GenerateTestFloatHistogram(int(i)), iter, valueType)
}
