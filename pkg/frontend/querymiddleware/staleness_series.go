// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"container/heap"
	"math"

	"github.com/grafana/dskit/multierror"
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
	ss                           []storage.SeriesSet
	calledNextWithoutReadingFrom []bool

	doneErrs     multierror.MultiError
	doneWarnings annotations.Annotations
}

func concatSeriesSets(sets []storage.SeriesSet) *seriesSetsHeap {
	h := seriesSetsHeap{}
	for _, set := range sets {
		if !set.Next() {
			h.doneErrs.Add(set.Err())
			h.doneWarnings.Merge(set.Warnings())
			continue
		}
		h.ss = append(h.ss, set)
		h.calledNextWithoutReadingFrom = append(h.calledNextWithoutReadingFrom, true)
	}
	heap.Init(&h)
	return &h
}

func (s *seriesSetsHeap) Next() bool {
	for s.Len() > 0 && !s.calledNextWithoutReadingFrom[0] {
		s.calledNextWithoutReadingFrom[0] = false
		firstSet := s.ss[0]
		if !firstSet.Next() {
			s.doneErrs.Add(firstSet.Err())
			s.doneWarnings.Merge(firstSet.Warnings())
			heap.Pop(s)
			continue
		}
		// If Fix moves the first item, we won't use its At(), so we need to mark it in calledNextWithoutReadingFrom.
		// But we also won't know where it went to mark it in calledNextWithoutReadingFrom.
		// So we mark it and unmark it if Fix didn't end up moving the item.
		s.calledNextWithoutReadingFrom[0] = true
		heap.Fix(s, 0)
		if firstSet == s.ss[0] {
			s.calledNextWithoutReadingFrom[0] = false
		}
		break
	}
	if s.Len() > 0 {
		s.calledNextWithoutReadingFrom[0] = false
	}
	return s.Len() > 0
}

func (s *seriesSetsHeap) At() storage.Series {
	if len(s.ss) == 0 {
		return nil
	}
	return s.ss[0].At()
}

func (s *seriesSetsHeap) Err() error {
	// Create a fresh instance, so we don't modify doneErrs for when Err() is called multiple times.
	errs := multierror.New(s.doneErrs...)
	for _, set := range s.ss {
		errs.Add(set.Err())
	}
	return errs.Err()
}

func (s *seriesSetsHeap) Warnings() annotations.Annotations {
	// Merge into a fresh instance, so we don't modify doneWarnings for when Warnings is called multiple times.
	combined := annotations.Annotations{}
	combined.Merge(s.doneWarnings)
	for _, set := range s.ss {
		combined.Merge(set.Warnings())
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
	s.calledNextWithoutReadingFrom[i], s.calledNextWithoutReadingFrom[j] = s.calledNextWithoutReadingFrom[j], s.calledNextWithoutReadingFrom[i]
}

func (s *seriesSetsHeap) Push(x any) {
	ss := x.(storage.SeriesSet)
	if !ss.Next() {
		s.doneErrs.Add(ss.Err())
		s.doneWarnings.Merge(ss.Warnings())
		return
	}
	s.ss = append(s.ss, ss)
	s.calledNextWithoutReadingFrom = append(s.calledNextWithoutReadingFrom, true)
}

func (s *seriesSetsHeap) Pop() any {
	ss := s.ss[len(s.ss)-1]
	s.ss = s.ss[:len(s.ss)-1]
	s.calledNextWithoutReadingFrom = s.calledNextWithoutReadingFrom[:len(s.calledNextWithoutReadingFrom)-1]
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
	currType       chunkenc.ValueType
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

func (it *stalenessMarkerIterator) Next() (currentType chunkenc.ValueType) {
	defer func() {
		it.currType = currentType
	}()
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

func (it *stalenessMarkerIterator) Seek(t int64) (currentType chunkenc.ValueType) {
	defer func() {
		it.currType = currentType
	}()
	if t <= it.currT {
		// Seeking backward is not allowed.
		return it.currType
	}
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
		valueType = it.iter.Seek(t)

		gap := it.iter.AtT() - it.currT
		if gap > it.step {
			hasSampleBeforeT = false
		}
	} else {
		// We seek to check if there is another sample before this one.
		// If there isn't, then this sample needs a stalness marker preceeding it.
		valueType = it.iter.Seek(t - it.step)

		if valueType == chunkenc.ValNone {
			// Even if we were to inject a staleness marker, it would have been too far back, so we can now just return ValNone.
			it.currT = t
			return valueType
		}

		if it.iter.AtT() > t {
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
	// If we've reached the end or encountered an error, propagate it
	if valueType == chunkenc.ValNone {
		it.currIsStale = true
		it.currT += it.step
		it.afterStaleType = chunkenc.ValNone
		return chunkenc.ValFloat
	}

	if !hasSampleBeforeT {
		it.currIsStale = true
		it.currT = t
		it.afterStaleType = valueType
		it.afterStaleT = it.iter.AtT()
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
