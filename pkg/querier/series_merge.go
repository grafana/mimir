// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/querier/batch"
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

// newSampleTieBreak returns the tie-break for one iterator. Each iterator needs its own, because sampleTieBreak
// reuses histograms between comparisons, and two iterators of the same series can be in use at the same time.
// If the iterator is reused, prev is its old tie-break, and we reuse it to keep its histograms.
func newSampleTieBreak(prev storage.SampleTieBreaker) storage.SampleTieBreaker {
	if tb, ok := prev.(*sampleTieBreak); ok {
		return tb
	}
	return &sampleTieBreak{}
}

// sampleTieBreak decides which sample to keep when two iterators are at samples with the same timestamp.
type sampleTieBreak struct {
	// Reused to read histograms, to avoid an allocation on each comparison.
	lh, rh   *histogram.Histogram
	lfh, rfh *histogram.FloatHistogram
}

// Prefer returns true if the current sample of l must be kept instead of the current sample of r, when both are
// at the same timestamp. It uses the rules in the batch package, which are also used by batch.decideTimestampConflict.
func (tb *sampleTieBreak) Prefer(l, r chunkenc.Iterator) bool {
	// Seek to the current timestamp does not move the iterator: we only use it to get the type of the current
	// sample.
	lt, rt := l.Seek(l.AtT()), r.Seek(r.AtT())
	if preferLeft, decided := batch.PreferValueType(lt, rt); decided {
		return preferLeft
	}
	switch lt {
	case chunkenc.ValFloat:
		_, lv := l.At()
		_, rv := r.At()
		return batch.PreferFloat(lv, rv)
	case chunkenc.ValHistogram:
		_, tb.lh = l.AtHistogram(tb.lh)
		_, tb.rh = r.AtHistogram(tb.rh)
		return batch.PreferHistogram(tb.lh, tb.rh)
	case chunkenc.ValFloatHistogram:
		_, tb.lfh = l.AtFloatHistogram(tb.lfh)
		_, tb.rfh = r.AtFloatHistogram(tb.rfh)
		return batch.PreferFloatHistogram(tb.lfh, tb.rfh)
	default:
		return false
	}
}
