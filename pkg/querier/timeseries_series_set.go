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
func (t *timeSeriesSeriesSet) Warnings() storage.Warnings { return nil }

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
	return labels.Compare(mimirpb.FromLabelAdaptersToLabels(b[i].Labels), mimirpb.FromLabelAdaptersToLabels(b[j].Labels)) < 0
}

// Labels implements the storage.Series interface.
// Conversion is safe because ingester sets these by calling client.FromLabelsToLabelAdapters which guarantees labels are sorted.
func (t *timeseries) Labels() labels.Labels {
	return mimirpb.FromLabelAdaptersToLabels(t.series.Labels)
}

// Iterator implements the storage.Series interface
func (t *timeseries) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	return &timeSeriesSeriesIterator{
		ts:  t,
		iF:  -1,
		iH:  -1,
		atH: false,
	}
}

// Seek implements implements chunkenc.Iterator.
func (t *timeSeriesSeriesIterator) Seek(s int64) chunkenc.ValueType {
	offsetF := 0
	if t.iF > 0 {
		offsetF = t.iF // only advance via Seek
	}
	offsetH := 0
	if t.iH > 0 {
		offsetH = t.iH // only advance via Seek
	}

	t.iF = sort.Search(len(t.ts.series.Samples[offsetF:]), func(i int) bool {
		return t.ts.series.Samples[offsetF+i].TimestampMs >= s
	}) + offsetF
	t.iH = sort.Search(len(t.ts.series.Histograms[offsetH:]), func(i int) bool {
		return t.ts.series.Histograms[offsetH+i].Timestamp >= s
	}) + offsetH

	if t.iF >= len(t.ts.series.Samples) && t.iH >= len(t.ts.series.Histograms) {
		return chunkenc.ValNone
	}
	if t.iF >= len(t.ts.series.Samples) {
		t.atH = true
		return chunkenc.ValHistogram
	}
	if t.iH >= len(t.ts.series.Histograms) {
		t.atH = false
		return chunkenc.ValFloat
	}
	if t.ts.series.Samples[t.iF].TimestampMs < t.ts.series.Histograms[t.iH].Timestamp {
		t.atH = false
		return chunkenc.ValFloat
	}
	t.atH = true
	return chunkenc.ValHistogram
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
func (t *timeSeriesSeriesIterator) AtHistogram() (int64, *histogram.Histogram) {
	if !t.atH {
		panic(errors.New("timeSeriesSeriesIterator: Calling AtHistogram() when cursor is not at histogram"))
	}
	if t.iH < 0 || t.iH >= len(t.ts.series.Histograms) {
		return 0, nil
	}
	h := t.ts.series.Histograms[t.iH]
	return h.Timestamp, mimirpb.FromHistogramProtoToHistogram(h)
}

// AtFloatHistogram implements chunkenc.Iterator.
func (t *timeSeriesSeriesIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	if !t.atH {
		panic(errors.New("timeSeriesSeriesIterator: Calling AtFloatHistogram() when cursor is not at histogram"))
	}
	if t.iH < 0 || t.iH >= len(t.ts.series.Histograms) {
		return 0, nil
	}
	h := t.ts.series.Histograms[t.iH]
	return h.Timestamp, mimirpb.FromHistogramProtoToHistogram(h).ToFloat()
}

// AtT implements implements chunkenc.Iterator.
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
		t.iF++
		t.iH++
		return chunkenc.ValNone
	}
	if t.iF+1 >= len(t.ts.series.Samples) {
		t.iH++
		t.atH = true
		return chunkenc.ValHistogram
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
	return chunkenc.ValHistogram
}

// Err implements the implements chunkenc.Iterator.
func (t *timeSeriesSeriesIterator) Err() error { return nil }
