// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/timeseries_series_set_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"math"
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/mimirpb_custom"
	"github.com/grafana/mimir/pkg/util/test"
)

var (
	generateTestHistogram      = test.GenerateTestHistogram
	generateTestFloatHistogram = test.GenerateTestFloatHistogram
)

func TestTimeSeriesSeriesSet(t *testing.T) {

	timeseries := []mimirpb.TimeSeries{
		{
			Labels:  []mimirpb_custom.LabelAdapter{{Name: "label1", Value: "value3"}},
			Samples: []mimirpb.Sample{{Value: 3.14, TimestampMs: 1234}},
		},
		{
			Labels: []mimirpb_custom.LabelAdapter{{Name: "label1", Value: "value2"}},
			Samples: []mimirpb.Sample{
				{Value: 3.14, TimestampMs: 1234},
				{Value: 1.618, TimestampMs: 2345},
			},
		},
		{
			Labels:  []mimirpb_custom.LabelAdapter{{Name: "label1", Value: "value1"}},
			Samples: []mimirpb.Sample{{Value: 3.14, TimestampMs: 1234}},
		},
	}

	ss := newTimeSeriesSeriesSet(timeseries)

	require.True(t, ss.Next())
	series := ss.At()

	// Series should sort into alphabetical order by labels.
	require.Equal(t, labels.FromStrings("label1", "value1"), series.Labels())

	it := series.Iterator(nil)
	require.Equal(t, chunkenc.ValFloat, it.Next())
	ts, v := it.At()
	require.Equal(t, 3.14, v)
	require.Equal(t, int64(1234), ts)

	require.True(t, ss.Next())
	require.Equal(t, labels.FromStrings("label1", "value2"), ss.At().Labels())
	it = ss.At().Iterator(it)
	require.Equal(t, chunkenc.ValFloat, it.Seek(2000))
	ts, v = it.At()
	require.Equal(t, 1.618, v)
	require.Equal(t, int64(2345), ts)

	require.True(t, ss.Next())
	require.Equal(t, labels.FromStrings("label1", "value3"), ss.At().Labels())
	require.False(t, ss.Next())
}

func BenchmarkTimeSeriesSeriesSet(b *testing.B) {
	const (
		numSeries           = 8000
		numSamplesPerSeries = 24 * 240
	)

	// Generate series.
	timeseries := []mimirpb.TimeSeries{}
	for seriesID := 0; seriesID < numSeries; seriesID++ {
		lbls := mkZLabels("__name__", "test", "series_id", strconv.Itoa(seriesID))
		var samples []mimirpb.Sample
		for t := int64(0); t <= numSamplesPerSeries; t++ {
			samples = append(samples, mimirpb.Sample{TimestampMs: t, Value: math.Sin(float64(t))})
		}
		timeseries = append(timeseries, mimirpb.TimeSeries{
			Labels:  lbls,
			Samples: samples,
		})
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		_ = newTimeSeriesSeriesSet(timeseries)
	}
}

func TestTimeSeriesIterator(t *testing.T) {
	ts := timeseries{
		series: mimirpb.TimeSeries{
			Labels: []mimirpb_custom.LabelAdapter{
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

	test.RequireIteratorFloatHistogram(t, 1233, generateTestFloatHistogram(8), it, it.Seek(1233)) // Seek to early part
	test.RequireIteratorFloat(t, 1234, 3.14, it, it.Next())
	test.RequireIteratorFloat(t, 1235, 3.15, it, it.Seek(1235)) // Seek to middle
	test.RequireIteratorFloat(t, 1235, 3.15, it, it.Seek(1235)) // Seek to same place
	test.RequireIteratorFloat(t, 1236, 3.16, it, it.Next())
	test.RequireIteratorFloat(t, 1237, 3.17, it, it.Seek(1237)) // Seek to end
	require.Equal(t, chunkenc.ValNone, it.Seek(1238))           // Seek past end
	require.Equal(t, chunkenc.ValNone, it.Seek(1238))           // Ensure that seeking to same end still returns ValNone

	it = ts.Iterator(it)

	test.RequireIteratorHistogram(t, 1232, generateTestHistogram(7), it, it.Next())
	test.RequireIteratorFloatHistogram(t, 1233, generateTestFloatHistogram(8), it, it.Next())
	test.RequireIteratorFloat(t, 1234, 3.14, it, it.Next())
	test.RequireIteratorFloat(t, 1235, 3.15, it, it.Next())
	test.RequireIteratorFloat(t, 1235, 3.15, it, it.Seek(1232)) // Ensure seek doesn't do anything if already past seek target.
	test.RequireIteratorFloat(t, 1236, 3.16, it, it.Next())
	test.RequireIteratorFloat(t, 1237, 3.17, it, it.Next())
	require.Equal(t, chunkenc.ValNone, it.Next())

	it.At() // Ensure an At after a full iteration doesn't cause a panic
}
