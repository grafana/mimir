// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
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
	return storage.ChainedSeriesMergeWithTieBreak(newSampleTieBreak)(series...)
}

// newSampleTieBreak returns a new tie-break for one iterator. Each iterator needs its own, because sampleTieBreak
// reuses histograms between comparisons, and two iterators of the same series can be in use at the same time.
func newSampleTieBreak() storage.SampleTieBreakFunc {
	return (&sampleTieBreak{}).prefer
}

// sampleTieBreak decides which sample to keep when two iterators are at samples with the same timestamp.
type sampleTieBreak struct {
	// Reused to read histograms, to avoid an allocation on each comparison.
	lh, rh   *histogram.Histogram
	lfh, rfh *histogram.FloatHistogram
}

// prefer returns true if the current sample of l must be kept instead of the current sample of r, when both are
// at the same timestamp. It uses the same rule as batch.decideTimestampConflict: a float histogram is preferred
// over a histogram, and a histogram over a float. For two samples of the same type, the higher value is preferred
// (for histograms, the higher count and then the higher sum).
//
// Unlike batch.decideTimestampConflict, it also orders NaN values (a NaN is lower than any other value, and NaNs
// are ordered by their bits), because storage.SampleTieBreakFunc must be a strict weak ordering: if NaN were neither
// lower nor higher than other values, the sample that is kept would depend on the order of the iterators again.
func (tb *sampleTieBreak) prefer(l, r chunkenc.Iterator) bool {
	// Seek to the current timestamp does not move the iterator: we only use it to get the type of the current
	// sample.
	lt, rt := l.Seek(l.AtT()), r.Seek(r.AtT())

	switch {
	case lt == rt:
		switch lt {
		case chunkenc.ValFloat:
			_, lv := l.At()
			_, rv := r.At()
			return floatGreater(lv, rv)
		case chunkenc.ValHistogram:
			_, tb.lh = l.AtHistogram(tb.lh)
			_, tb.rh = r.AtHistogram(tb.rh)
			if tb.lh.Count != tb.rh.Count {
				return tb.lh.Count > tb.rh.Count
			}
			return floatGreater(tb.lh.Sum, tb.rh.Sum)
		case chunkenc.ValFloatHistogram:
			_, tb.lfh = l.AtFloatHistogram(tb.lfh)
			_, tb.rfh = r.AtFloatHistogram(tb.rfh)
			if tb.lfh.Count != tb.rfh.Count {
				return floatGreater(tb.lfh.Count, tb.rfh.Count)
			}
			return floatGreater(tb.lfh.Sum, tb.rfh.Sum)
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
