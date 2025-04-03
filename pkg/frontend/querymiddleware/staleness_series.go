// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"container/heap"
	"math"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
)

// stalenessMarkerSeriesSet wraps a raw SeriesSet and adds staleness markers at gaps in the data
type stalenessMarkerSeriesSet struct {
	raw  storage.SeriesSet
	step int64
	cur  storage.Series
}

// newStalenessMarkerSeriesSet creates a storage.SeriesSet with staleness markers at gaps
func newStalenessMarkerSeriesSet(rawSeriesSet storage.SeriesSet, step int64) storage.SeriesSet {
	if step == 0 {
		return rawSeriesSet
	}
	return &stalenessMarkerSeriesSet{
		raw:  rawSeriesSet,
		step: step,
	}
}

// Next implements storage.SeriesSet.
func (s *stalenessMarkerSeriesSet) Next() bool {
	if !s.raw.Next() {
		return false
	}

	// Wrap the raw series with a staleness marker series
	rawSeries := s.raw.At()
	s.cur = newStalenessMarkerSeries(rawSeries, s.step)
	return true
}

// At implements storage.SeriesSet.
func (s *stalenessMarkerSeriesSet) At() storage.Series {
	return s.cur
}

// Err implements storage.SeriesSet.
func (s *stalenessMarkerSeriesSet) Err() error {
	return s.raw.Err()
}

// Warnings implements storage.SeriesSet.
func (s *stalenessMarkerSeriesSet) Warnings() annotations.Annotations {
	return s.raw.Warnings()
}

// stalenessMarkerSeries wraps a Series and adds staleness markers where data gaps exist
type stalenessMarkerSeries struct {
	raw  storage.Series
	step int64
}

// newStalenessMarkerSeries creates a new Series with staleness markers
func newStalenessMarkerSeries(rawSeries storage.Series, step int64) storage.Series {
	return &stalenessMarkerSeries{
		raw:  rawSeries,
		step: step,
	}
}

// Labels implements storage.Series.
func (s *stalenessMarkerSeries) Labels() labels.Labels {
	return s.raw.Labels()
}

// Iterator implements storage.Series.
func (s *stalenessMarkerSeries) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	rawIter := s.raw.Iterator(it)
	return newStalenessMarkerIterator(rawIter, s.step)
}

type seriesSetsHeap struct {
	ss                []storage.SeriesSet
	alreadyCalledNext []bool
}

func concatSeriesSets(sets []storage.SeriesSet) storage.SeriesSet {
	h := seriesSetsHeap{}
	for _, set := range sets {
		if !set.Next() {
			continue
		}
		h.ss = append(h.ss, set)
		h.alreadyCalledNext = append(h.alreadyCalledNext, true)
	}
	heap.Init(&h)
	return &h
}

func (s *seriesSetsHeap) Next() bool {
	for len(s.ss) > 0 && !s.alreadyCalledNext[0] && !s.ss[0].Next() {
		s.alreadyCalledNext[0] = false
		heap.Pop(s)
	}
	if len(s.ss) > 0 {
		s.alreadyCalledNext[0] = false
	}
	heap.Fix(s, 0)
	return len(s.ss) > 0
}

func (s *seriesSetsHeap) At() storage.Series {
	return s.ss[0].At()
}

func (s *seriesSetsHeap) Err() error {
	for _, set := range s.ss {
		if err := set.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (s *seriesSetsHeap) Warnings() annotations.Annotations {
	combined := make(annotations.Annotations)
	for _, set := range s.ss {
		combined = combined.Merge(set.Warnings())
	}
	return combined
}

func (s *seriesSetsHeap) Len() int {
	return len(s.ss)
}

func (s *seriesSetsHeap) Less(i, j int) bool {
	return labels.Compare(s.ss[i].At().Labels(), s.ss[j].At().Labels()) < 0
}

func (s *seriesSetsHeap) Swap(i, j int) {
	s.ss[i], s.ss[j] = s.ss[j], s.ss[i]
	s.alreadyCalledNext[i], s.alreadyCalledNext[j] = s.alreadyCalledNext[j], s.alreadyCalledNext[i]
}

func (s *seriesSetsHeap) Push(x any) {
	if !x.(storage.SeriesSet).Next() {
		return
	}
	s.ss = append(s.ss, x.(storage.SeriesSet))
	s.alreadyCalledNext = append(s.alreadyCalledNext, true)
}

func (s *seriesSetsHeap) Pop() any {
	ss := s.ss[len(s.ss)-1]
	s.ss = s.ss[:len(s.ss)-1]
	s.alreadyCalledNext = s.alreadyCalledNext[:len(s.alreadyCalledNext)-1]
	return ss
}

// stalenessMarkerIterator wraps an iterator and inserts staleness markers
// when the gap between samples exceeds the step size.
type stalenessMarkerIterator struct {
	iter chunkenc.Iterator
	step int64

	returnedOnce   bool
	currT          int64
	currIsStale    bool
	afterStaleType chunkenc.ValueType
	afterStaleT    int64
}

// newStalenessMarkerIterator creates a new stalenessMarkerIterator that wraps the provided iterator
// and inserts staleness markers when the gap between samples exceeds the step size.
func newStalenessMarkerIterator(iter chunkenc.Iterator, step int64) *stalenessMarkerIterator {
	return &stalenessMarkerIterator{
		iter: iter,
		step: step,
	}
}

func (it *stalenessMarkerIterator) Next() chunkenc.ValueType {
	if it.currIsStale {
		it.currIsStale = false
		it.currT = it.afterStaleT
		return it.afterStaleType
	}

	valueType := it.iter.Next()
	nextT := it.iter.AtT()

	// If this is the first sample, just record it and return
	if !it.returnedOnce {
		it.returnedOnce = true
		it.currT = nextT
		return valueType
	}

	// In case the embedded query processed series which all ended before the end of the query time range,
	// we don't want the outer query to apply the lookback at the end of the embedded query results. To keep it
	// simple, it's safe always to add an extra stale marker at the end of the query results.
	//
	// This could result in an extra sample (stale marker) after the end of the query time range, but that's
	// not a problem when running the outer query because it will just be discarded.
	if valueType == chunkenc.ValNone {
		it.currIsStale = true
		it.currT += it.step
		it.afterStaleType = chunkenc.ValNone
		return chunkenc.ValFloat
	}

	gap := nextT - it.currT

	// When an embedded query is executed by PromQL engine, any stale marker in the time-series
	// data is used the engine to stop applying the lookback delta but the stale marker is removed
	// from the query results. The result of embedded queries, which we are processing here,
	// is then used as input to run an outer query in the PromQL engine. This data will not contain
	// the stale marker (because has been removed when running the embedded query) but we still need
	// the PromQL engine to not apply the lookback delta when there are gaps in the embedded queries
	// results. For this reason, here we do inject a stale marker at the beginning of each gap in the
	// embedded queries results.
	if gap > it.step {
		it.currIsStale = true
		it.afterStaleT = nextT
		it.afterStaleType = valueType
		it.currT += it.step
		return chunkenc.ValFloat // Staleness markers are floats
	}

	it.currT = nextT
	return valueType
}

func (it *stalenessMarkerIterator) Seek(t int64) chunkenc.ValueType {
	// Reset state
	it.currIsStale = false
	it.afterStaleT = 0

	var (
		hasSampleBeforeT bool
		valueType        chunkenc.ValueType
	)
	if it.returnedOnce && it.currT+it.step >= t {
		// If we know the gap is small enough, avoid seeking.
		// This also prevents invalid use of Seek to go backward.
		hasSampleBeforeT = true
	} else {
		// We seek to check if there is another sample before this one.
		// If there isn't, then this sample needs a stalness marker preceeding it.
		valueType = it.iter.Seek(t - it.step)

		if it.iter.AtT() >= t {
			// There wasn't anything between t and t-step. This means that there is a period which is at least it.step long without samples.
			// So we need to add a staleness marker.
			hasSampleBeforeT = false
		} else {
			hasSampleBeforeT = true
			// There are samples, but we're also not interested in them, so we need to skip over them.
			it.currT = it.iter.AtT()

			// Now we do the seek for what was asked.
			// It's possible that Seek(t) brings up much further away from the sample we just seeked to.
			valueType = it.iter.Seek(t)

			gap := it.iter.AtT() - it.currT

			if gap > it.step {
				hasSampleBeforeT = false
			}
		}
	}

	if !hasSampleBeforeT {
		it.currIsStale = true
		it.currT = t - it.step
		it.afterStaleType = valueType
		it.afterStaleT = it.iter.AtT()
		return chunkenc.ValFloat
	}
	// If we've reached the end or encountered an error, propagate it
	if valueType == chunkenc.ValNone {
		it.currIsStale = true
		it.currT += it.step
		it.afterStaleType = chunkenc.ValNone
		return chunkenc.ValFloat
	}

	it.currT = it.iter.AtT()

	return valueType
}

func (it *stalenessMarkerIterator) At() (int64, float64) {
	if it.currIsStale {
		// For staleness markers, return NaN
		return it.currT, math.Float64frombits(value.StaleNaN)
	}
	return it.iter.At()
}

func (it *stalenessMarkerIterator) AtHistogram(h *histogram.Histogram) (int64, *histogram.Histogram) {
	if it.currIsStale {
		return 0, nil
	}
	return it.iter.AtHistogram(h)
}

func (it *stalenessMarkerIterator) AtFloatHistogram(h *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	if it.currIsStale {
		return 0, nil
	}
	return it.iter.AtFloatHistogram(h)
}

func (it *stalenessMarkerIterator) AtT() int64 {
	return it.currT
}

func (it *stalenessMarkerIterator) Err() error {
	return it.iter.Err()
}
