// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/storage/merge.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package querier

import (
	"container/heap"
	"errors"
	"math"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// deterministicSeriesMerge is like storage.ChainedSeriesMerge, but when two or more series have a sample at the
// same timestamp, it always picks the same sample, whatever the order of the input series.
//
// We need this because storage.ChainedSeriesMerge keeps the first sample it sees for a timestamp, and the order
// of the input series depends on which store-gateway (or ingester) responds first. The same series can have
// different values at the same timestamp (for example, an out-of-order sample and an in-order sample in
// different blocks, which the compactor never merges), so the query result could change between requests.
//
// The sample is picked with the same rule as batch.decideTimestampConflict, so that the result does not depend
// on whether the samples were merged inside one store-gateway response or across responses.
func deterministicSeriesMerge(series ...storage.Series) storage.Series {
	if len(series) == 0 {
		return nil
	}
	return &storage.SeriesEntry{
		Lset: series[0].Labels(),
		SampleIteratorFn: func(it chunkenc.Iterator) chunkenc.Iterator {
			return newDeterministicChainSampleIterator(it, series)
		},
	}
}

// deterministicChainSampleIterator is a copy of Prometheus' chainSampleIterator. The only difference is that
// its heap orders iterators at the same timestamp by their current sample (see preferSample), so that the sample
// that is kept for a timestamp does not depend on the order of the iterators.
type deterministicChainSampleIterator struct {
	iterators []chunkenc.Iterator
	h         *samplesIteratorHeap

	curr  chunkenc.Iterator
	lastT int64

	// Whether the previous and the current sample are direct neighbors
	// within the same base iterator.
	consecutive bool
}

func newDeterministicChainSampleIterator(it chunkenc.Iterator, series []storage.Series) chunkenc.Iterator {
	csi, ok := it.(*deterministicChainSampleIterator)
	if !ok {
		csi = &deterministicChainSampleIterator{}
	}
	if cap(csi.iterators) < len(series) {
		csi.iterators = make([]chunkenc.Iterator, len(series))
	} else {
		csi.iterators = csi.iterators[:len(series)]
	}
	csi.h = nil
	csi.curr = nil
	csi.lastT = math.MinInt64
	for i, s := range series {
		csi.iterators[i] = s.Iterator(csi.iterators[i])
	}
	return csi
}

func (c *deterministicChainSampleIterator) Seek(t int64) chunkenc.ValueType {
	// No-op check.
	if c.curr != nil && c.lastT >= t {
		return c.curr.Seek(c.lastT)
	}
	// Don't bother to find out if the next sample is consecutive. Callers
	// of Seek usually aren't interested anyway.
	c.consecutive = false
	c.h = &samplesIteratorHeap{}
	for _, iter := range c.iterators {
		if iter.Seek(t) == chunkenc.ValNone {
			if iter.Err() != nil {
				// If any iterator is reporting an error, abort.
				return chunkenc.ValNone
			}
			continue
		}
		heap.Push(c.h, iter)
	}
	if c.h.Len() > 0 {
		c.curr = heap.Pop(c.h).(chunkenc.Iterator)
		c.lastT = c.curr.AtT()
		return c.curr.Seek(c.lastT)
	}
	c.curr = nil
	return chunkenc.ValNone
}

func (c *deterministicChainSampleIterator) At() (t int64, v float64) {
	if c.curr == nil {
		panic("deterministicChainSampleIterator.At called before first .Next or after .Next returned false.")
	}
	return c.curr.At()
}

func (c *deterministicChainSampleIterator) AtHistogram(h *histogram.Histogram) (int64, *histogram.Histogram) {
	if c.curr == nil {
		panic("deterministicChainSampleIterator.AtHistogram called before first .Next or after .Next returned false.")
	}
	t, h := c.curr.AtHistogram(h)
	// If the current sample is not consecutive with the previous one, we
	// cannot be sure anymore about counter resets for counter histograms.
	if !c.consecutive && h.CounterResetHint != histogram.GaugeType {
		h.CounterResetHint = histogram.UnknownCounterReset
	}
	return t, h
}

func (c *deterministicChainSampleIterator) AtFloatHistogram(fh *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	if c.curr == nil {
		panic("deterministicChainSampleIterator.AtFloatHistogram called before first .Next or after .Next returned false.")
	}
	t, fh := c.curr.AtFloatHistogram(fh)
	// If the current sample is not consecutive with the previous one, we
	// cannot be sure anymore about counter resets for counter histograms.
	if !c.consecutive && fh.CounterResetHint != histogram.GaugeType {
		fh.CounterResetHint = histogram.UnknownCounterReset
	}
	return t, fh
}

func (c *deterministicChainSampleIterator) AtT() int64 {
	if c.curr == nil {
		panic("deterministicChainSampleIterator.AtT called before first .Next or after .Next returned false.")
	}
	return c.curr.AtT()
}

func (c *deterministicChainSampleIterator) AtST() int64 {
	if c.curr == nil {
		panic("deterministicChainSampleIterator.AtST called before first .Next or after .Next returned false.")
	}
	return c.curr.AtST()
}

func (c *deterministicChainSampleIterator) Next() chunkenc.ValueType {
	var (
		currT           int64
		currValueType   chunkenc.ValueType
		iteratorChanged bool
	)
	if c.h == nil {
		iteratorChanged = true
		c.h = &samplesIteratorHeap{}
		// We call c.curr.Next() as the first thing below.
		// So, we don't call Next() on it here.
		c.curr = c.iterators[0]
		for _, iter := range c.iterators[1:] {
			if iter.Next() == chunkenc.ValNone {
				if iter.Err() != nil {
					// If any iterator is reporting an error, abort.
					// If c.iterators[0] is reporting an error, we'll handle that below.
					return chunkenc.ValNone
				}
			} else {
				heap.Push(c.h, iter)
			}
		}
	}

	if c.curr == nil {
		return chunkenc.ValNone
	}

	for {
		currValueType = c.curr.Next()

		if currValueType == chunkenc.ValNone {
			if c.curr.Err() != nil {
				// Abort if we've hit an error.
				return chunkenc.ValNone
			}

			if c.h.Len() == 0 {
				// No iterator left to iterate.
				c.curr = nil
				return chunkenc.ValNone
			}
		} else {
			currT = c.curr.AtT()
			if currT == c.lastT {
				// Ignoring sample for the same timestamp: the heap already gave us the preferred one.
				continue
			}
			if c.h.Len() == 0 {
				// curr is the only iterator remaining,
				// no need to check with the heap.
				break
			}

			// Check current iterator with the top of the heap. When the timestamps are equal, we must not
			// keep curr just because it is curr: we push it to the heap so that the heap picks the preferred
			// sample.
			nextT := c.h.its[0].AtT()
			if currT < nextT {
				// Current iterator has smaller timestamp than the heap.
				break
			}
			// Current iterator does not hold the smallest timestamp, or holds it together with other iterators.
			heap.Push(c.h, c.curr)
		}

		c.curr = heap.Pop(c.h).(chunkenc.Iterator)
		iteratorChanged = true
		currT = c.curr.AtT()
		currValueType = c.curr.Seek(currT)
		if currT != c.lastT {
			break
		}
	}

	c.consecutive = !iteratorChanged
	c.lastT = currT
	return currValueType
}

func (c *deterministicChainSampleIterator) Err() error {
	var errs []error
	for _, iter := range c.iterators {
		errs = append(errs, iter.Err())
	}
	return errors.Join(errs...)
}

// samplesIteratorHeap orders iterators by the timestamp of their current sample, and then by preferSample.
type samplesIteratorHeap struct {
	its []chunkenc.Iterator

	// Reused to read histograms when two iterators are at the same timestamp, to avoid an allocation on each
	// comparison.
	lh, rh   *histogram.Histogram
	lfh, rfh *histogram.FloatHistogram
}

func (h *samplesIteratorHeap) Len() int      { return len(h.its) }
func (h *samplesIteratorHeap) Swap(i, j int) { h.its[i], h.its[j] = h.its[j], h.its[i] }

func (h *samplesIteratorHeap) Less(i, j int) bool {
	ti, tj := h.its[i].AtT(), h.its[j].AtT()
	if ti != tj {
		return ti < tj
	}
	return h.preferSample(h.its[i], h.its[j])
}

func (h *samplesIteratorHeap) Push(x any) {
	h.its = append(h.its, x.(chunkenc.Iterator))
}

func (h *samplesIteratorHeap) Pop() any {
	old := h.its
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	h.its = old[0 : n-1]
	return x
}

// preferSample returns true if the current sample of l must be kept instead of the current sample of r, when both
// are at the same timestamp. It uses the same rule as batch.decideTimestampConflict: a float histogram is preferred
// over a histogram, and a histogram over a float. For two samples of the same type, the higher value is preferred
// (for histograms, the higher count and then the higher sum).
//
// Unlike batch.decideTimestampConflict, it also orders NaN values (a NaN is lower than any other value, and NaNs
// are ordered by their bits), because the heap needs a strict weak ordering: if NaN were neither lower nor higher
// than other values, the sample that is kept would depend on the order of the iterators again.
func (h *samplesIteratorHeap) preferSample(l, r chunkenc.Iterator) bool {
	// Seek to the current timestamp does not move the iterator: we only use it to get the type of the current
	// sample, like chainSampleIterator does.
	lt, rt := l.Seek(l.AtT()), r.Seek(r.AtT())

	switch {
	case lt == rt:
		switch lt {
		case chunkenc.ValFloat:
			_, lv := l.At()
			_, rv := r.At()
			return floatGreater(lv, rv)
		case chunkenc.ValHistogram:
			_, h.lh = l.AtHistogram(h.lh)
			_, h.rh = r.AtHistogram(h.rh)
			if h.lh.Count != h.rh.Count {
				return h.lh.Count > h.rh.Count
			}
			return floatGreater(h.lh.Sum, h.rh.Sum)
		case chunkenc.ValFloatHistogram:
			_, h.lfh = l.AtFloatHistogram(h.lfh)
			_, h.rfh = r.AtFloatHistogram(h.rfh)
			if h.lfh.Count != h.rfh.Count {
				return floatGreater(h.lfh.Count, h.rfh.Count)
			}
			return floatGreater(h.lfh.Sum, h.rfh.Sum)
		default:
			return false
		}

	case lt == chunkenc.ValFloatHistogram || rt == chunkenc.ValFloatHistogram:
		return lt == chunkenc.ValFloatHistogram

	case lt == chunkenc.ValHistogram || rt == chunkenc.ValHistogram:
		return lt == chunkenc.ValHistogram

	default:
		return false
	}
}

// floatGreater returns true if l > r, with NaN lower than any other value and NaNs ordered by their bits, so that
// it is a strict total order on the float64 bit patterns that occur in samples.
func floatGreater(l, r float64) bool {
	lNaN, rNaN := math.IsNaN(l), math.IsNaN(r)
	switch {
	case lNaN && rNaN:
		return math.Float64bits(l) > math.Float64bits(r)
	case lNaN || rNaN:
		return rNaN
	default:
		return l > r
	}
}
