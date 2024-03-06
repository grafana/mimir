// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/series/series_set_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package series

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/test"
)

var (
	generateTestHistogram      = test.GenerateTestHistogram
	generateTestFloatHistogram = test.GenerateTestFloatHistogram
)

func TestConcreteSeriesSet(t *testing.T) {
	series1 := &ConcreteSeries{
		labels:  labels.FromStrings("foo", "bar"),
		samples: []model.SamplePair{{Timestamp: 1, Value: 2}},
	}
	series2 := &ConcreteSeries{
		labels:  labels.FromStrings("foo", "baz"),
		samples: []model.SamplePair{{Timestamp: 3, Value: 4}},
	}
	series3 := &ConcreteSeries{
		labels:     labels.FromStrings("foo", "bay"),
		histograms: []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(5, generateTestHistogram(6))},
	}
	c := NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{series3, series2, series1})
	require.True(t, c.Next())
	require.Equal(t, series1, c.At())
	require.True(t, c.Next())
	require.Equal(t, series3, c.At())
	require.True(t, c.Next())
	require.Equal(t, series2, c.At())
	require.False(t, c.Next())
}

func TestMatrixToSeriesSetSortsMetricLabels(t *testing.T) {
	matrix := model.Matrix{
		{
			Metric: model.Metric{
				model.MetricNameLabel: "testmetric",
				"e":                   "f",
				"a":                   "b",
				"g":                   "h",
				"c":                   "d",
			},
			Values: []model.SamplePair{{Timestamp: 0, Value: 0}},
		},
	}
	ss := MatrixToSeriesSet(matrix)
	require.True(t, ss.Next())
	require.NoError(t, ss.Err())

	l := ss.At().Labels()
	require.Equal(t, labels.FromStrings(model.MetricNameLabel, "testmetric", "a", "b", "c", "d", "e", "f", "g", "h"), l)
}

func TestConcreteSeriesSetIterator(t *testing.T) {
	series := &ConcreteSeries{
		labels:     labels.FromStrings("foo", "bar"),
		samples:    []model.SamplePair{{Timestamp: 1, Value: 2}, {Timestamp: 5, Value: 6}, {Timestamp: 9, Value: 10}},
		histograms: []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(3, generateTestHistogram(4)), mimirpb.FromFloatHistogramToHistogramProto(7, generateTestFloatHistogram(8)), mimirpb.FromHistogramToHistogramProto(11, generateTestHistogram(12))},
	}

	// test next
	it := series.Iterator(nil)
	require.Equal(t, chunkenc.ValFloat, it.Next())
	ts, v := it.At()
	require.Equal(t, int64(1), ts)
	require.Equal(t, float64(2), v)
	require.Equal(t, chunkenc.ValHistogram, it.Next())
	ts, h := it.AtHistogram(nil)
	require.Equal(t, int64(3), ts)
	require.Equal(t, generateTestHistogram(4), h)
	require.Equal(t, chunkenc.ValFloat, it.Next())
	ts, v = it.At()
	require.Equal(t, int64(5), ts)
	require.Equal(t, float64(6), v)
	require.Equal(t, chunkenc.ValFloatHistogram, it.Next())
	ts, fh := it.AtFloatHistogram(nil)
	require.Equal(t, int64(7), ts)
	require.Equal(t, generateTestFloatHistogram(8), fh)
	require.Equal(t, chunkenc.ValFloat, it.Next())
	ts, v = it.At()
	require.Equal(t, int64(9), ts)
	require.Equal(t, float64(10), v)
	require.Equal(t, chunkenc.ValHistogram, it.Next())
	ts, h = it.AtHistogram(nil)
	require.Equal(t, int64(11), ts)
	require.Equal(t, generateTestHistogram(12), h)
	// You can also call AtFloatHistogram() on ValHistogram.
	ts, fh = it.AtFloatHistogram(nil)
	require.Equal(t, int64(11), ts)
	require.Equal(t, generateTestHistogram(12).ToFloat(nil), fh)

	require.Equal(t, chunkenc.ValNone, it.Next())

	// test seek to same and next
	it = series.Iterator(nil)
	require.Equal(t, chunkenc.ValHistogram, it.Seek(3)) // Seek to middle
	ts, h = it.AtHistogram(nil)
	require.Equal(t, int64(3), ts)
	require.Equal(t, generateTestHistogram(4), h)
	require.Equal(t, chunkenc.ValHistogram, it.Seek(3)) // Seek to same place
	ts, h = it.AtHistogram(nil)
	require.Equal(t, int64(3), ts)
	require.Equal(t, generateTestHistogram(4), h)
	require.Equal(t, chunkenc.ValFloat, it.Next())
	ts, v = it.At()
	require.Equal(t, int64(5), ts)
	require.Equal(t, float64(6), v)

	// test seek to earlier and next, then to later and next
	it = series.Iterator(nil)
	require.Equal(t, chunkenc.ValHistogram, it.Seek(3)) // Seek to middle
	ts, h = it.AtHistogram(nil)
	require.Equal(t, int64(3), ts)
	require.Equal(t, generateTestHistogram(4), h)
	require.Equal(t, chunkenc.ValHistogram, it.Seek(1)) // Ensure seek doesn't do anything if already past seek target.
	ts, h = it.AtHistogram(nil)
	require.Equal(t, int64(3), ts)
	require.Equal(t, generateTestHistogram(4), h)
	require.Equal(t, chunkenc.ValFloat, it.Next())
	ts, v = it.At()
	require.Equal(t, int64(5), ts)
	require.Equal(t, float64(6), v)
	require.Equal(t, chunkenc.ValFloatHistogram, it.Seek(7)) // Seek to later
	ts, fh = it.AtFloatHistogram(nil)
	require.Equal(t, int64(7), ts)
	require.Equal(t, generateTestFloatHistogram(8), fh)
	require.Equal(t, chunkenc.ValFloat, it.Next())
	ts, v = it.At()
	require.Equal(t, int64(9), ts)
	require.Equal(t, float64(10), v)
	require.Equal(t, chunkenc.ValHistogram, it.Seek(11)) // Seek to end
	ts, h = it.AtHistogram(nil)
	require.Equal(t, int64(11), ts)
	require.Equal(t, generateTestHistogram(12), h)
	require.Equal(t, chunkenc.ValNone, it.Seek(13)) // Seek to past end
	require.Equal(t, chunkenc.ValNone, it.Seek(13)) // Ensure that seeking to same end still returns ValNone
}
