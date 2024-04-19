// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package series

import (
	"errors"
	"sort"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// ConcreteSeriesSet implements storage.SeriesSet.
type ConcreteSeriesSet struct {
	cur    int
	series []storage.Series
}

// NewConcreteSeriesSetFromUnsortedSeries instantiates an in-memory series set from a slice
// of unsorted series. The series will be sorted in place by their labels.
func NewConcreteSeriesSetFromUnsortedSeries(series []storage.Series) storage.SeriesSet {
	sort.Sort(byLabels(series))
	return NewConcreteSeriesSetFromSortedSeries(series)
}

// NewConcreteSeriesSetFromSortedSeries instantiates an in-memory series set from a slice
// of series already sorted by their labels.
func NewConcreteSeriesSetFromSortedSeries(series []storage.Series) storage.SeriesSet {
	return &ConcreteSeriesSet{
		cur:    -1,
		series: series,
	}
}

// Next iterates through a series set and implements storage.SeriesSet.
func (c *ConcreteSeriesSet) Next() bool {
	c.cur++
	return c.cur < len(c.series)
}

// At returns the current series and implements storage.SeriesSet.
func (c *ConcreteSeriesSet) At() storage.Series {
	return c.series[c.cur]
}

// Err implements storage.SeriesSet.
func (c *ConcreteSeriesSet) Err() error {
	return nil
}

// Warnings implements storage.SeriesSet.
func (c *ConcreteSeriesSet) Warnings() annotations.Annotations {
	return nil
}

// ConcreteSeries implements storage.Series.
type ConcreteSeries struct {
	labels     labels.Labels
	samples    []model.SamplePair
	histograms []mimirpb.Histogram
}

// NewConcreteSeries instantiates an in memory series from a list of samples & histograms & labels
func NewConcreteSeries(ls labels.Labels, samples []model.SamplePair, histograms []mimirpb.Histogram) *ConcreteSeries {
	return &ConcreteSeries{
		labels:     ls,
		samples:    samples,
		histograms: histograms,
	}
}

// Labels implements storage.Series
func (c *ConcreteSeries) Labels() labels.Labels {
	return c.labels
}

// Iterator implements storage.Series
func (c *ConcreteSeries) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	return NewConcreteSeriesIterator(c)
}

// concreteSeriesIterator implements chunkenc.Iterator.
type concreteSeriesIterator struct {
	curFloat int
	curHisto int
	atHisto  bool
	series   *ConcreteSeries
}

// NewConcreteSeriesIterator instantiates an in memory chunkenc.Iterator
func NewConcreteSeriesIterator(series *ConcreteSeries) chunkenc.Iterator {
	return &concreteSeriesIterator{
		curFloat: -1,
		curHisto: -1,
		atHisto:  false,
		series:   series,
	}
}

// atTypeHisto is an internal method to differentiate between histogram and float histogram value types
// Checking that c.curHisto is a valid index in the c.series.histograms array and that
// c.atHisto is true must be done outside of this
func (c *concreteSeriesIterator) atTypeHisto() chunkenc.ValueType {
	if c.series.histograms[c.curHisto].IsFloatHistogram() {
		return chunkenc.ValFloatHistogram
	}
	return chunkenc.ValHistogram
}

// atType returns current timestamp and value type
func (c *concreteSeriesIterator) atType() (int64, chunkenc.ValueType) {
	if c.atHisto {
		if c.curHisto < 0 || c.curHisto >= len(c.series.histograms) {
			return 0, chunkenc.ValNone
		}
		return c.series.histograms[c.curHisto].Timestamp, c.atTypeHisto()
	}
	if c.curFloat < 0 || c.curFloat >= len(c.series.samples) {
		return 0, chunkenc.ValNone
	}
	return int64(c.series.samples[c.curFloat].Timestamp), chunkenc.ValFloat
}

func (c *concreteSeriesIterator) Seek(t int64) chunkenc.ValueType {
	oldTime, oldType := c.atType()
	if oldTime >= t { // only advance via Seek
		return oldType
	}

	c.curFloat = sort.Search(len(c.series.samples), func(n int) bool {
		return c.series.samples[n].Timestamp >= model.Time(t)
	})
	c.curHisto = sort.Search(len(c.series.histograms), func(n int) bool {
		return c.series.histograms[n].Timestamp >= t
	})

	if c.curFloat >= len(c.series.samples) && c.curHisto >= len(c.series.histograms) {
		return chunkenc.ValNone
	}
	if c.curFloat >= len(c.series.samples) {
		c.atHisto = true
		return c.atTypeHisto()
	}
	if c.curHisto >= len(c.series.histograms) {
		c.atHisto = false
		return chunkenc.ValFloat
	}
	if int64(c.series.samples[c.curFloat].Timestamp) < c.series.histograms[c.curHisto].Timestamp {
		c.curHisto--
		c.atHisto = false
		return chunkenc.ValFloat
	}
	c.curFloat--
	c.atHisto = true
	return c.atTypeHisto()
}

func (c *concreteSeriesIterator) At() (t int64, v float64) {
	if c.atHisto {
		panic(errors.New("concreteSeriesIterator: Calling At() when cursor is at histogram"))
	}
	s := c.series.samples[c.curFloat]
	return int64(s.Timestamp), float64(s.Value)
}

func (c *concreteSeriesIterator) Next() chunkenc.ValueType {
	if c.curFloat+1 >= len(c.series.samples) && c.curHisto+1 >= len(c.series.histograms) {
		c.curFloat = len(c.series.samples)
		c.curHisto = len(c.series.histograms)
		return chunkenc.ValNone
	}
	if c.curFloat+1 >= len(c.series.samples) {
		c.curHisto++
		c.atHisto = true
		return c.atTypeHisto()
	}
	if c.curHisto+1 >= len(c.series.histograms) {
		c.curFloat++
		c.atHisto = false
		return chunkenc.ValFloat
	}
	if int64(c.series.samples[c.curFloat+1].Timestamp) < c.series.histograms[c.curHisto+1].Timestamp {
		c.curFloat++
		c.atHisto = false
		return chunkenc.ValFloat
	}
	c.curHisto++
	c.atHisto = true
	return c.atTypeHisto()
}

func (c *concreteSeriesIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	if !c.atHisto {
		panic(errors.New("concreteSeriesIterator: Calling AtHistogram() when cursor is not at histogram"))
	}
	h := c.series.histograms[c.curHisto]
	return h.Timestamp, mimirpb.FromHistogramProtoToHistogram(&h)
}

func (c *concreteSeriesIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	if !c.atHisto {
		panic(errors.New("concreteSeriesIterator: Calling AtFloatHistogram() when cursor is not at histogram"))
	}
	h := c.series.histograms[c.curHisto]
	if h.IsFloatHistogram() {
		return h.Timestamp, mimirpb.FromFloatHistogramProtoToFloatHistogram(&h)
	}
	return h.Timestamp, mimirpb.FromHistogramProtoToFloatHistogram(&h)
}

func (c *concreteSeriesIterator) AtT() int64 {
	if c.atHisto {
		return c.series.histograms[c.curHisto].Timestamp
	}
	return int64(c.series.samples[c.curFloat].Timestamp)
}

func (c *concreteSeriesIterator) Err() error {
	return nil
}

// NewErrIterator instantiates an errIterator
func NewErrIterator(err error) chunkenc.Iterator {
	return errIterator{err}
}

// errIterator implements chunkenc.Iterator, just returning an error.
type errIterator struct {
	err error
}

func (errIterator) Seek(int64) chunkenc.ValueType {
	return chunkenc.ValNone
}

func (errIterator) Next() chunkenc.ValueType {
	return chunkenc.ValNone
}

func (errIterator) At() (t int64, v float64) {
	return 0, 0
}

func (errIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	return 0, nil
}

func (errIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return 0, nil
}

func (errIterator) AtT() int64 {
	return 0
}

func (e errIterator) Err() error {
	return e.err
}

// MatrixToSeriesSet creates a storage.SeriesSet from a model.Matrix
// Series will be sorted by labels.
func MatrixToSeriesSet(m model.Matrix) storage.SeriesSet {
	series := make([]storage.Series, 0, len(m))
	for _, ss := range m {
		series = append(series, &ConcreteSeries{
			labels:  metricToLabels(ss.Metric),
			samples: ss.Values,
			// histograms: ss.Histograms, // cannot convert the decoded matrix form to the expected encoded format. this method is only used in tests so ignoring histogram support for now
		})
	}
	return NewConcreteSeriesSetFromUnsortedSeries(series)
}

// LabelsToSeriesSet creates a storage.SeriesSet from a []labels.Labels
func LabelsToSeriesSet(ls []labels.Labels) storage.SeriesSet {
	series := make([]storage.Series, 0, len(ls))
	for _, l := range ls {
		series = append(series, &ConcreteSeries{
			labels: l,
		})
	}
	return NewConcreteSeriesSetFromUnsortedSeries(series)
}

func metricToLabels(m model.Metric) labels.Labels {
	builder := labels.NewScratchBuilder(len(m))
	for k, v := range m {
		builder.Add(string(k), string(v))
	}
	builder.Sort() // PromQL expects all labels to be sorted.
	return builder.Labels()
}

type byLabels []storage.Series

func (b byLabels) Len() int           { return len(b) }
func (b byLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byLabels) Less(i, j int) bool { return labels.Compare(b[i].Labels(), b[j].Labels()) < 0 }

type seriesSetWithWarnings struct {
	wrapped  storage.SeriesSet
	warnings annotations.Annotations
}

func NewSeriesSetWithWarnings(wrapped storage.SeriesSet, warnings annotations.Annotations) storage.SeriesSet {
	return seriesSetWithWarnings{
		wrapped:  wrapped,
		warnings: warnings,
	}
}

func (s seriesSetWithWarnings) Next() bool {
	return s.wrapped.Next()
}

func (s seriesSetWithWarnings) At() storage.Series {
	return s.wrapped.At()
}

func (s seriesSetWithWarnings) Err() error {
	return s.wrapped.Err()
}

func (s seriesSetWithWarnings) Warnings() annotations.Annotations {
	return s.warnings.Merge(s.wrapped.Warnings())
}
