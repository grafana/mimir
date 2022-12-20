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
	ts *timeseries
	i  int
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
func (t *timeseries) Iterator() chunkenc.Iterator {
	return &timeSeriesSeriesIterator{
		ts: t,
		i:  -1,
	}
}

// Seek implements implements chunkenc.Iterator.
func (t *timeSeriesSeriesIterator) Seek(s int64) chunkenc.ValueType {
	offset := 0
	if t.i > 0 {
		offset = t.i // only advance via Seek
	}

	t.i = sort.Search(len(t.ts.series.Samples[offset:]), func(i int) bool {
		return t.ts.series.Samples[offset+i].TimestampMs >= s
	}) + offset

	if t.i < len(t.ts.series.Samples) {
		return chunkenc.ValFloat
	}
	return chunkenc.ValNone
}

// At implements the implements chunkenc.Iterator.
func (t *timeSeriesSeriesIterator) At() (int64, float64) {
	if t.i < 0 || t.i >= len(t.ts.series.Samples) {
		return 0, 0
	}
	return t.ts.series.Samples[t.i].TimestampMs, t.ts.series.Samples[t.i].Value
}

// AtHistogram implements chunkenc.Iterator.
func (t *timeSeriesSeriesIterator) AtHistogram() (int64, *histogram.Histogram) {
	panic(errors.New("timeSeriesSeriesIterator: AtFloatHistogram is not implemented"))
}

// AtFloatHistogram implements chunkenc.Iterator.
func (t *timeSeriesSeriesIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	panic(errors.New("timeSeriesSeriesIterator: AtFloatHistogram is not implemented"))
}

// AtT implements implements chunkenc.Iterator.
func (t *timeSeriesSeriesIterator) AtT() int64 {
	ts, _ := t.At()
	return ts
}

// Next implements implements chunkenc.Iterator.
func (t *timeSeriesSeriesIterator) Next() chunkenc.ValueType {
	t.i++
	if t.i < len(t.ts.series.Samples) {
		return chunkenc.ValFloat
	}
	return chunkenc.ValNone
}

// Err implements the implements chunkenc.Iterator.
func (t *timeSeriesSeriesIterator) Err() error { return nil }
