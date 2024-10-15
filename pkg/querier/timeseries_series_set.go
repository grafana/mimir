// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/timeseries_series_set.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"sort"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// timeSeriesSeriesSet is a wrapper around a mimirpb.TimeSeries slice to implement to SeriesSet interface
type timeSeriesSeriesSet struct {
	ts []mimirpb.TimeSeries
	i  int
}

func newTimeSeriesSeriesSet(series []mimirpb.TimeSeries) *timeSeriesSeriesSet {
	sort.Sort(byTimeSeriesLabels(series))
	return &timeSeriesSeriesSet{
		ts: series,
		i:  -1,
	}
}

// Next implements storage.SeriesSet interface.
func (t *timeSeriesSeriesSet) Next() bool { t.i++; return t.i < len(t.ts) }

// At implements storage.SeriesSet interface.
func (t *timeSeriesSeriesSet) At() storage.Series {
	if t.i < 0 {
		return nil
	}
	return &timeseries{series: t.ts[t.i]}
}

// Err implements storage.SeriesSet interface.
func (t *timeSeriesSeriesSet) Err() error { return nil }

// Warnings implements storage.SeriesSet interface.
func (t *timeSeriesSeriesSet) Warnings() annotations.Annotations { return nil }

// timeseries is a type wrapper that implements the storage.Series interface
type timeseries struct {
	series mimirpb.TimeSeries
}

// timeSeriesSeriesIterator is a wrapper around a mimirpb.TimeSeries to implement the chunkenc.Iterator.
type timeSeriesSeriesIterator struct {
	ts  *timeseries
	iF  int
	iH  int
	atH bool
}

type byTimeSeriesLabels []mimirpb.TimeSeries

func (b byTimeSeriesLabels) Len() int      { return len(b) }
func (b byTimeSeriesLabels) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b byTimeSeriesLabels) Less(i, j int) bool {
	return mimirpb.CompareLabelAdapters(b[i].Labels, b[j].Labels) < 0
}

// Labels implements the storage.Series interface.
// Conversion is safe because ingester sets these by calling client.FromLabelsToLabelAdapters which guarantees labels are sorted.
func (t *timeseries) Labels() labels.Labels {
	return mimirpb.FromLabelAdaptersToLabels(t.series.Labels)
}

// Iterator implements the storage.Series interface
func (t *timeseries) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	return &timeSeriesSeriesIterator{
		ts: t,
		iF: -1,
		iH: -1,
	}
}

// atTypeHisto is an internal method to differentiate between histogram and float histogram value types
// Checking that t.iH is a valid index in the t.ts.series.Histograms array and that
// t.atH is true must be done outside of this
func (t *timeSeriesSeriesIterator) atTypeHisto() chunkenc.ValueType {
	if t.ts.series.Histograms[t.iH].IsFloatHistogram() {
		return chunkenc.ValFloatHistogram
	}
	return chunkenc.ValHistogram
}

// atType returns current timestamp and value type
func (t *timeSeriesSeriesIterator) atType() (int64, chunkenc.ValueType) {
	if t.atH {
		if t.iH < 0 || t.iH >= len(t.ts.series.Histograms) {
			return 0, chunkenc.ValNone
		}
		return t.ts.series.Histograms[t.iH].Timestamp, t.atTypeHisto()
	}
	if t.iF < 0 || t.iF >= len(t.ts.series.Samples) {
		return 0, chunkenc.ValNone
	}
	return t.ts.series.Samples[t.iF].TimestampMs, chunkenc.ValFloat
}

// Seek implements implements chunkenc.Iterator.
func (t *timeSeriesSeriesIterator) Seek(s int64) chunkenc.ValueType {
	oldTime, oldType := t.atType()
	if oldTime >= s { // only advance via Seek
		return oldType
	}

	t.iF = sort.Search(len(t.ts.series.Samples), func(i int) bool {
		return t.ts.series.Samples[i].TimestampMs >= s
	})
	t.iH = sort.Search(len(t.ts.series.Histograms), func(i int) bool {
		return t.ts.series.Histograms[i].Timestamp >= s
	})

	if t.iF >= len(t.ts.series.Samples) && t.iH >= len(t.ts.series.Histograms) {
		return chunkenc.ValNone
	}
	if t.iF >= len(t.ts.series.Samples) {
		t.atH = true
		return t.atTypeHisto()
	}
	if t.iH >= len(t.ts.series.Histograms) {
		t.atH = false
		return chunkenc.ValFloat
	}
	if t.ts.series.Samples[t.iF].TimestampMs < t.ts.series.Histograms[t.iH].Timestamp {
		t.iH--
		t.atH = false
		return chunkenc.ValFloat
	}
	t.iF--
	t.atH = true
	return t.atTypeHisto()
}

// At implements the implements chunkenc.Iterator.
func (t *timeSeriesSeriesIterator) At() (int64, float64) {
	if t.atH {
		panic(errors.New("timeSeriesSeriesIterator: Calling At() when cursor is at histogram"))
	}
	if t.iF < 0 || t.iF >= len(t.ts.series.Samples) {
		return 0, 0
	}
	return t.ts.series.Samples[t.iF].TimestampMs, t.ts.series.Samples[t.iF].Value
}

// AtHistogram implements chunkenc.Iterator.
func (t *timeSeriesSeriesIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	if !t.atH {
		panic(errors.New("timeSeriesSeriesIterator: Calling AtHistogram() when cursor is not at histogram"))
	}
	if t.iH < 0 || t.iH >= len(t.ts.series.Histograms) {
		return 0, nil
	}
	h := t.ts.series.Histograms[t.iH]
	return h.Timestamp, mimirpb.FromHistogramProtoToHistogram(&h)
}

// AtFloatHistogram implements chunkenc.Iterator.
func (t *timeSeriesSeriesIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	if !t.atH {
		panic(errors.New("timeSeriesSeriesIterator: Calling AtFloatHistogram() when cursor is not at histogram"))
	}
	if t.iH < 0 || t.iH >= len(t.ts.series.Histograms) {
		return 0, nil
	}
	h := t.ts.series.Histograms[t.iH]
	if h.IsFloatHistogram() {
		return h.Timestamp, mimirpb.FromFloatHistogramProtoToFloatHistogram(&h)
	}
	return h.Timestamp, mimirpb.FromHistogramProtoToFloatHistogram(&h)
}

// AtT implements chunkenc.Iterator.
func (t *timeSeriesSeriesIterator) AtT() int64 {
	if t.atH {
		if t.iH < 0 || t.iH >= len(t.ts.series.Histograms) {
			return 0
		}
		return t.ts.series.Histograms[t.iH].Timestamp
	}
	if t.iF < 0 || t.iF >= len(t.ts.series.Samples) {
		return 0
	}
	return t.ts.series.Samples[t.iF].TimestampMs
}

// Next implements implements chunkenc.Iterator.
func (t *timeSeriesSeriesIterator) Next() chunkenc.ValueType {
	if t.iF+1 >= len(t.ts.series.Samples) && t.iH+1 >= len(t.ts.series.Histograms) {
		t.iF = len(t.ts.series.Samples)
		t.iH = len(t.ts.series.Histograms)
		return chunkenc.ValNone
	}
	if t.iF+1 >= len(t.ts.series.Samples) {
		t.iH++
		t.atH = true
		return t.atTypeHisto()
	}
	if t.iH+1 >= len(t.ts.series.Histograms) {
		t.iF++
		t.atH = false
		return chunkenc.ValFloat
	}
	if t.ts.series.Samples[t.iF+1].TimestampMs < t.ts.series.Histograms[t.iH+1].Timestamp {
		t.iF++
		t.atH = false
		return chunkenc.ValFloat
	}
	t.iH++
	t.atH = true
	return t.atTypeHisto()
}

// Err implements the implements chunkenc.Iterator.
func (t *timeSeriesSeriesIterator) Err() error { return nil }
