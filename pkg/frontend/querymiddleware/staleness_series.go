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

// stalenessMarkerIterator assumes that samples are within step of each other.
// If there is a gap, then stalenessMarkerIterator will insert a staleness mark (StaleNaN) a step after the earlier sample.
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

// insertStaleSample sets the iterator to a staleness state and returns the appropriate value type.
// It handles setting all the required fields for a staleness marker.
func (it *stalenessMarkerIterator) insertStaleSample(currT int64, afterStaleType chunkenc.ValueType, afterStaleT int64) chunkenc.ValueType {
	it.currIsStale = true
	it.currT = currT
	it.afterStaleType = afterStaleType
	it.afterStaleT = afterStaleT
	return chunkenc.ValFloat // Staleness markers are floats
}

func (it *stalenessMarkerIterator) Next() (currentType chunkenc.ValueType) {
	defer func() {
		it.currType = currentType
		it.returnedOnce = currentType != chunkenc.ValNone
	}()
	if it.currIsStale {
		it.currIsStale = false
		it.currT = it.afterStaleT
		return it.afterStaleType
	}

	nextSampleType := it.iter.Next()
	nextSampleT := it.iter.AtT()

	// If this is the first sample, just record it and return
	if !it.returnedOnce {
		it.currT = nextSampleT
		return nextSampleType
	}

	// In case the embedded query processed series which all ended before the end of the query time range,
	// we don't want the outer query to apply the lookback at the end of the embedded query results. To keep it
	// simple, it's safe always to add an extra stale marker at the end of the query results.
	//
	// This could result in an extra sample (stale marker) after the end of the query time range, but that's
	// not a problem when running the outer query because it will just be discarded.
	if nextSampleType == chunkenc.ValNone {
		return it.insertStaleSample(it.currT+it.step, chunkenc.ValNone, 0)
	}

	// When an embedded query is executed by PromQL engine, any stale marker in the time-series
	// data is used the engine to stop applying the lookback delta but the stale marker is removed
	// from the query results. The result of embedded queries, which we are processing here,
	// is then used as input to run an outer query in the PromQL engine. This data will not contain
	// the stale marker (because has been removed when running the embedded query) but we still need
	// the PromQL engine to not apply the lookback delta when there are gaps in the embedded queries
	// results. For this reason, here we do inject a stale marker at the beginning of each gap in the
	// embedded queries results.
	gap := nextSampleT - it.currT
	if gap > it.step {
		return it.insertStaleSample(it.currT+it.step, nextSampleType, nextSampleT)
	}

	it.currT = nextSampleT
	return nextSampleType
}

func (it *stalenessMarkerIterator) Seek(t int64) (currentType chunkenc.ValueType) {
	if it.currT >= t {
		// Seeking backward is not allowed.
		return it.currType
	}
	defer func() {
		it.currType = currentType
		it.returnedOnce = currentType != chunkenc.ValNone
	}()

	// Check if there was a sample before t at all, or we'll now be seeking to the first sample of this SeriesSet.
	prevSampleExists := it.returnedOnce || (it.iter.Seek(math.MinInt64) != chunkenc.ValNone && it.iter.AtT() < t)
	// Check if there was a sample whole staleness mark (aka stale sample) would be relevant to this Seek call
	_ = it.iter.Seek(t - it.step)
	prevSampleT := it.iter.AtT()
	// If there is some other sample, but it's so far back that it's stale sample wouldn't even qualify for this Seek call, we can ignore that stale sample.
	prevSampleTooFarBack := prevSampleExists && prevSampleT >= t
	staleSampleT := prevSampleT + it.step

	// Finally seek to the requested time.
	nextSampleType := it.iter.Seek(t)
	nextSampleT := it.iter.AtT()
	// Check if we even need to insert a stale sample - that is if the gap between samples is larger than the step.
	thereIsLargeGapBetweenSamples := prevSampleExists && (prevSampleTooFarBack || (nextSampleT > staleSampleT))
	// Check if the stale sample would be the sample this Seek would choose.
	requestedBetweenPrevSampleAndStaleSample := prevSampleExists && !prevSampleTooFarBack && t > prevSampleT && t <= staleSampleT
	insertStaleBetweenSamples := thereIsLargeGapBetweenSamples && requestedBetweenPrevSampleAndStaleSample

	// We also insert a stale sample at the end of iteration. We only do it when there were some samples already ingested.
	isAtEnd := nextSampleType == chunkenc.ValNone
	insertStaleAtEnd := isAtEnd && requestedBetweenPrevSampleAndStaleSample

	insertStale := insertStaleAtEnd || insertStaleBetweenSamples

	if insertStale {
		return it.insertStaleSample(staleSampleT, nextSampleType, nextSampleT)
	}
	it.currT = nextSampleT
	return nextSampleType
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
