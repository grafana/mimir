// SPDX-License-Identifier: AGPL-3.0-only

package testdatagen

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/util"
)

// NewTestCounterLabels generates series labels for a counter metric used in tests.
func NewTestCounterLabels(id int) labels.Labels {
	return labels.FromStrings(
		"__name__", "metric_counter",
		"const", "fixed", // A constant label.
		"unique", strconv.Itoa(id), // A unique label.
		"group_1", strconv.Itoa(id%10), // A first grouping label.
		"group_2", strconv.Itoa(id%3), // A second grouping label.
	)
}

// NewTestConventionalHistogramLabels generates series labels for a conventional histogram metric used in tests.
func NewTestConventionalHistogramLabels(id int, bucketLe float64) labels.Labels {
	return labels.FromStrings(
		"__name__", "metric_histogram_bucket",
		"le", fmt.Sprintf("%f", bucketLe),
		"const", "fixed", // A constant label.
		"unique", strconv.Itoa(id), // A unique label.
		"group_1", strconv.Itoa(id%10), // A first grouping label.
		"group_2", strconv.Itoa(id%3), // A second grouping label.
	)
}

// NewTestNativeHistogramLabels generates series labels for a native histogram metric used in tests.
func NewTestNativeHistogramLabels(id int) labels.Labels {
	return labels.FromStrings(
		"__name__", "metric_native_histogram",
		"const", "fixed", // A constant label.
		"unique", strconv.Itoa(id), // A unique label.
		"group_1", strconv.Itoa(id%10), // A first grouping label.
		"group_2", strconv.Itoa(id%3), // A second grouping label.
	)
}

// Generator defines a function used to generate sample values in tests.
type Generator func(ts int64) float64

func Factor(f float64) Generator {
	i := 0.
	return func(int64) float64 {
		i++
		res := i * f
		return res
	}
}

func ArithmeticSequence(f float64) Generator {
	i := 0.
	return func(int64) float64 {
		i++
		res := i + f
		return res
	}
}

// Stale wraps the input generator and injects Stale marker between from and to.
func Stale(from, to time.Time, wrap Generator) Generator {
	return func(ts int64) float64 {
		// Always get the next value from the wrapped Generator.
		v := wrap(ts)

		// Inject the Stale marker if we're at the right time.
		if ts >= util.TimeToMillis(from) && ts <= util.TimeToMillis(to) {
			return math.Float64frombits(value.StaleNaN)
		}

		return v
	}
}

type seriesIteratorMock struct {
	idx    int
	series []storage.Series
}

func newSeriesIteratorMock(series []storage.Series) *seriesIteratorMock {
	return &seriesIteratorMock{
		idx:    -1,
		series: series,
	}
}

func (i *seriesIteratorMock) Next() bool {
	i.idx++
	return i.idx < len(i.series)
}

func (i *seriesIteratorMock) At() storage.Series {
	if i.idx >= len(i.series) {
		return nil
	}

	return i.series[i.idx]
}

func (i *seriesIteratorMock) Err() error {
	return nil
}

func (i *seriesIteratorMock) Warnings() annotations.Annotations {
	return nil
}

func NewSeries(metric labels.Labels, from, to time.Time, step time.Duration, gen Generator) storage.Series {
	return newSeriesInner(metric, from, to, step, gen, false)
}

func NewNativeHistogramSeries(metric labels.Labels, from, to time.Time, step time.Duration, gen Generator) storage.Series {
	return newSeriesInner(metric, from, to, step, gen, true)
}

func newSeriesInner(metric labels.Labels, from, to time.Time, step time.Duration, gen Generator, histogram bool) storage.Series {
	var (
		floats     []promql.FPoint
		histograms []promql.HPoint
		prevValue  *float64
	)

	for ts := from; ts.Unix() <= to.Unix(); ts = ts.Add(step) {
		t := ts.Unix() * 1e3
		v := gen(t)

		// If both the previous and current values are the Stale marker, then we omit the
		// point completely (we just keep the 1st one in a consecutive series of Stale markers).
		shouldSkip := prevValue != nil && value.IsStaleNaN(*prevValue) && value.IsStaleNaN(v)
		prevValue = &v
		if shouldSkip {
			continue
		}

		if histogram {
			histograms = append(histograms, promql.HPoint{
				T: t,
				H: generateTestHistogram(v),
			})
		} else {
			floats = append(floats, promql.FPoint{
				T: t,
				F: v,
			})
		}
	}

	return NewThreadSafeStorageSeries(promql.Series{
		Metric:     metric,
		Floats:     floats,
		Histograms: histograms,
	})
}

func generateTestHistogram(v float64) *histogram.FloatHistogram {
	//based on util_test.GenerateTestFloatHistogram(int(v)) but without converting to int
	h := &histogram.FloatHistogram{
		Count:         10 + (v * 8),
		ZeroCount:     2 + v,
		ZeroThreshold: 0.001,
		Sum:           18.4 * (v + 1),
		Schema:        1,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: 2},
			{Offset: 1, Length: 2},
		},
		PositiveBuckets: []float64{v + 1, v + 2, v + 1, v + 1},
		NegativeSpans: []histogram.Span{
			{Offset: 0, Length: 2},
			{Offset: 1, Length: 2},
		},
		NegativeBuckets: []float64{v + 1, v + 2, v + 1, v + 1},
	}
	if value.IsStaleNaN(v) {
		h.Sum = v
	}
	return h
}

// Usually series are read by a single engine in a single goroutine but in
// sharding tests we have multiple engines in multiple goroutines. Thus we
// need a series iterator that doesn't share pointers between goroutines.
type ThreadSafeStorageSeries struct {
	storageSeries *promql.StorageSeries
}

// NewStorageSeries returns a StorageSeries from a Series.
func NewThreadSafeStorageSeries(series promql.Series) *ThreadSafeStorageSeries {
	return &ThreadSafeStorageSeries{
		storageSeries: promql.NewStorageSeries(series),
	}
}

func (ss *ThreadSafeStorageSeries) Labels() labels.Labels {
	return ss.storageSeries.Labels()
}

// Iterator returns a new iterator of the data of the series. In case of
// multiple samples with the same timestamp, it returns the float samples first.
func (ss *ThreadSafeStorageSeries) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	if ssi, ok := it.(*ThreadSafeStorageSeriesIterator); ok {
		return &ThreadSafeStorageSeriesIterator{underlying: ss.storageSeries.Iterator(ssi.underlying)}
	}
	return &ThreadSafeStorageSeriesIterator{underlying: ss.storageSeries.Iterator(nil)}
}

type ThreadSafeStorageSeriesIterator struct {
	underlying chunkenc.Iterator
}

func (ssi *ThreadSafeStorageSeriesIterator) Seek(t int64) chunkenc.ValueType {
	return ssi.underlying.Seek(t)
}

func (ssi *ThreadSafeStorageSeriesIterator) At() (t int64, v float64) {
	return ssi.underlying.At()
}

func (ssi *ThreadSafeStorageSeriesIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	panic(errors.New("storageSeriesIterator: AtHistogram not supported"))
}

// AtFloatHistogram returns the timestamp and the float histogram at the current position.
// This is different from the underlying iterator in that it does a copy so that the user
// can modify the returned histogram without affecting the underlying series.
func (ssi *ThreadSafeStorageSeriesIterator) AtFloatHistogram(toFH *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	t, fh := ssi.underlying.AtFloatHistogram(nil)
	if toFH == nil {
		return t, fh.Copy()
	}
	fh.CopyTo(toFH)
	return t, toFH
}

func (ssi *ThreadSafeStorageSeriesIterator) AtT() int64 {
	return ssi.underlying.AtT()
}

// TODO(krajorama): test AtST when chunk format with start timestamp
// is available.
func (ssi *ThreadSafeStorageSeriesIterator) AtST() int64 {
	return ssi.underlying.AtST()
}

func (ssi *ThreadSafeStorageSeriesIterator) Next() chunkenc.ValueType {
	return ssi.underlying.Next()
}

func (ssi *ThreadSafeStorageSeriesIterator) Err() error {
	return nil
}

func StorageSeriesQueryable(series []storage.Series) storage.Queryable {
	return storage.QueryableFunc(func(int64, int64) (storage.Querier, error) {
		return &QuerierMock{Series: series}, nil
	})
}

type QuerierMock struct {
	Series []storage.Series
}

func (m *QuerierMock) Select(_ context.Context, sorted bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	shard, matchers, err := sharding.RemoveShardFromMatchers(matchers)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	// Filter series by label matchers.
	var filtered []storage.Series

	for _, series := range m.Series {
		if seriesMatches(series, matchers...) {
			filtered = append(filtered, series)
		}
	}

	// Filter series by shard (if any)
	filtered = filterSeriesByShard(filtered, shard)

	// Honor the sorting.
	if sorted {
		slices.SortFunc(filtered, func(a, b storage.Series) int {
			return labels.Compare(a.Labels(), b.Labels())
		})
	}

	return newSeriesIteratorMock(filtered)
}

func (m *QuerierMock) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (m *QuerierMock) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (m *QuerierMock) Close() error { return nil }

func seriesMatches(series storage.Series, matchers ...*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(series.Labels().Get(m.Name)) {
			return false
		}
	}

	return true
}

func filterSeriesByShard(series []storage.Series, shard *sharding.ShardSelector) []storage.Series {
	if shard == nil {
		return series
	}

	var filtered []storage.Series

	for _, s := range series {
		if labels.StableHash(s.Labels())%shard.ShardCount == shard.ShardIndex {
			filtered = append(filtered, s)
		}
	}

	return filtered
}
