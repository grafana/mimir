// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/timeseries_series_set_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"testing"

	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

var (
	generateTestHistogram      = tsdb.GenerateTestHistogram
	generateTestFloatHistogram = tsdb.GenerateTestFloatHistogram
)

func TestTimeSeriesSeriesSet(t *testing.T) {

	timeseries := []mimirpb.TimeSeries{
		{
			Labels: []mimirpb.LabelAdapter{
				{
					Name:  "label1",
					Value: "value1",
				},
			},
			Samples: []mimirpb.Sample{
				{
					Value:       3.14,
					TimestampMs: 1234,
				},
			},
		},
	}

	ss := newTimeSeriesSeriesSet(timeseries)

	require.True(t, ss.Next())
	series := ss.At()

	require.Equal(t, ss.ts[0].Labels[0].Name, series.Labels()[0].Name)
	require.Equal(t, ss.ts[0].Labels[0].Value, series.Labels()[0].Value)

	it := series.Iterator(nil)
	require.Equal(t, chunkenc.ValFloat, it.Next())
	ts, v := it.At()
	require.Equal(t, 3.14, v)
	require.Equal(t, int64(1234), ts)
	require.False(t, ss.Next())

	// Append a new sample to seek to
	timeseries[0].Samples = append(timeseries[0].Samples, mimirpb.Sample{
		Value:       1.618,
		TimestampMs: 2345,
	})
	ss = newTimeSeriesSeriesSet(timeseries)

	require.True(t, ss.Next())
	it = ss.At().Iterator(it)
	require.Equal(t, chunkenc.ValFloat, it.Seek(2000))
	ts, v = it.At()
	require.Equal(t, 1.618, v)
	require.Equal(t, int64(2345), ts)
}

func TestTimeSeriesIterator(t *testing.T) {
	ts := timeseries{
		series: mimirpb.TimeSeries{
			Labels: []mimirpb.LabelAdapter{
				{
					Name:  "label1",
					Value: "value1",
				},
			},
			Samples: []mimirpb.Sample{
				{
					Value:       3.14,
					TimestampMs: 1234,
				},
				{
					Value:       3.15,
					TimestampMs: 1235,
				},
				{
					Value:       3.16,
					TimestampMs: 1236,
				},
				{
					Value:       3.17,
					TimestampMs: 1237,
				},
			},
			Histograms: []mimirpb.Histogram{
				mimirpb.FromHistogramToHistogramProto(1232, generateTestHistogram(7)),
				mimirpb.FromFloatHistogramToHistogramProto(1233, generateTestFloatHistogram(8)),
			},
		},
	}

	it := ts.Iterator(nil)
	require.Equal(t, chunkenc.ValFloatHistogram, it.Seek(1233)) // Seek to early part
	i, fh := it.AtFloatHistogram()
	require.EqualValues(t, 1233, i)
	require.Equal(t, generateTestFloatHistogram(8), fh)
	require.Equal(t, chunkenc.ValFloat, it.Next())
	i, v := it.At()
	require.EqualValues(t, 1234, i)
	require.Equal(t, 3.14, v)
	require.Equal(t, chunkenc.ValFloat, it.Seek(1235)) // Seek to middle
	i, v = it.At()
	require.EqualValues(t, 1235, i)
	require.Equal(t, 3.15, v)
	require.Equal(t, chunkenc.ValFloat, it.Seek(1235)) // Seek to same place
	i, v = it.At()
	require.EqualValues(t, 1235, i)
	require.Equal(t, 3.15, v)
	require.Equal(t, chunkenc.ValFloat, it.Next())
	i, v = it.At()
	require.EqualValues(t, 1236, i)
	require.Equal(t, 3.16, v)
	require.Equal(t, chunkenc.ValFloat, it.Seek(1237)) // Seek to end
	i, v = it.At()
	require.EqualValues(t, 1237, i)
	require.Equal(t, 3.17, v)
	require.Equal(t, chunkenc.ValNone, it.Seek(1238)) // Seek to past end
	require.Equal(t, chunkenc.ValNone, it.Seek(1238)) // Ensure that seeking to same end still returns ValNone

	it = ts.Iterator(it)
	require.Equal(t, chunkenc.ValHistogram, it.Next())
	i, h := it.AtHistogram()
	require.EqualValues(t, 1232, i)
	require.Equal(t, generateTestHistogram(7), h)
	require.Equal(t, chunkenc.ValFloatHistogram, it.Next())
	i, fh = it.AtFloatHistogram()
	require.EqualValues(t, 1233, i)
	require.Equal(t, generateTestFloatHistogram(8), fh)
	require.Equal(t, chunkenc.ValFloat, it.Next())
	i, v = it.At()
	require.EqualValues(t, 1234, i)
	require.Equal(t, 3.14, v)
	require.Equal(t, chunkenc.ValFloat, it.Next())
	i, v = it.At()
	require.EqualValues(t, 1235, i)
	require.Equal(t, 3.15, v)
	require.Equal(t, chunkenc.ValFloat, it.Seek(1232)) // Ensure seek doesn't do anything if already past seek target.
	i, v = it.At()
	require.EqualValues(t, 1235, i)
	require.Equal(t, 3.15, v)
	require.Equal(t, chunkenc.ValFloat, it.Next())
	i, v = it.At()
	require.EqualValues(t, 1236, i)
	require.Equal(t, 3.16, v)
	require.Equal(t, chunkenc.ValFloat, it.Seek(1237))
	i, v = it.At()
	require.EqualValues(t, 1237, i)
	require.Equal(t, 3.17, v)
	require.Equal(t, chunkenc.ValNone, it.Next())
	it.At() // Ensure an At after a full iteration doesn't cause a panic
}
