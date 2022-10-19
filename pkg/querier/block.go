// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/block.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"math"
	"sort"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/storegateway/labelpb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

func convertMatchersToLabelMatcher(matchers []*labels.Matcher) []storepb.LabelMatcher {
	var converted []storepb.LabelMatcher
	for _, m := range matchers {
		var t storepb.LabelMatcher_Type
		switch m.Type {
		case labels.MatchEqual:
			t = storepb.LabelMatcher_EQ
		case labels.MatchNotEqual:
			t = storepb.LabelMatcher_NEQ
		case labels.MatchRegexp:
			t = storepb.LabelMatcher_RE
		case labels.MatchNotRegexp:
			t = storepb.LabelMatcher_NRE
		}

		converted = append(converted, storepb.LabelMatcher{
			Type:  t,
			Name:  m.Name,
			Value: m.Value,
		})
	}
	return converted
}

// Implementation of storage.SeriesSet, based on individual responses from store client.
type blockQuerierSeriesSet struct {
	series   []*storepb.Series
	warnings storage.Warnings

	// next response to process
	next int

	currSeries storage.Series
}

func (bqss *blockQuerierSeriesSet) Next() bool {
	bqss.currSeries = nil

	if bqss.next >= len(bqss.series) {
		return false
	}

	currLabels := labelpb.ZLabelsToPromLabels(bqss.series[bqss.next].Labels)
	currChunks := bqss.series[bqss.next].Chunks

	bqss.next++

	// Merge chunks for current series. Chunks may come in multiple responses, but as soon
	// as the response has chunks for a new series, we can stop searching. Series are sorted.
	// See documentation for StoreClient.Series call for details.
	for bqss.next < len(bqss.series) && labels.Compare(currLabels, labelpb.ZLabelsToPromLabels(bqss.series[bqss.next].Labels)) == 0 {
		currChunks = append(currChunks, bqss.series[bqss.next].Chunks...)
		bqss.next++
	}

	bqss.currSeries = newBlockQuerierSeries(currLabels, currChunks)
	return true
}

func (bqss *blockQuerierSeriesSet) At() storage.Series {
	return bqss.currSeries
}

func (bqss *blockQuerierSeriesSet) Err() error {
	return nil
}

func (bqss *blockQuerierSeriesSet) Warnings() storage.Warnings {
	return bqss.warnings
}

// newBlockQuerierSeries makes a new blockQuerierSeries. Input labels must be already sorted by name.
func newBlockQuerierSeries(lbls []labels.Label, chunks []storepb.AggrChunk) *blockQuerierSeries {
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].MinTime < chunks[j].MinTime
	})

	return &blockQuerierSeries{labels: lbls, chunks: chunks}
}

type blockQuerierSeries struct {
	labels labels.Labels
	chunks []storepb.AggrChunk
}

func (bqs *blockQuerierSeries) Labels() labels.Labels {
	return bqs.labels
}

func (bqs *blockQuerierSeries) Iterator() chunkenc.Iterator {
	if len(bqs.chunks) == 0 {
		// should not happen in practice, but we have a unit test for it
		return series.NewErrIterator(errors.New("no chunks"))
	}

	its := make([]iteratorWithMaxTime, 0, len(bqs.chunks))

	for _, c := range bqs.chunks {
		ch, err := chunkenc.FromData(chunkenc.EncXOR, c.Raw.Data)
		if err != nil {
			return series.NewErrIterator(errors.Wrapf(err, "failed to initialize chunk from XOR encoded raw data (series: %v min time: %d max time: %d)", bqs.Labels(), c.MinTime, c.MaxTime))
		}

		it := ch.Iterator(nil)
		its = append(its, iteratorWithMaxTime{it, c.MaxTime})
	}

	return newBlockQuerierSeriesIterator(bqs.Labels(), its)
}

func newBlockQuerierSeriesIterator(labels labels.Labels, its []iteratorWithMaxTime) *blockQuerierSeriesIterator {
	return &blockQuerierSeriesIterator{labels: labels, iterators: its, lastT: math.MinInt64}
}

// iteratorWithMaxTime is an iterator which is aware of the maxT of its embedded iterator.
type iteratorWithMaxTime struct {
	chunkenc.Iterator
	maxT int64
}

// blockQuerierSeriesIterator implements a series iterator on top
// of a list of time-sorted, non-overlapping chunks.
type blockQuerierSeriesIterator struct {
	// only used for error reporting
	labels labels.Labels

	iterators []iteratorWithMaxTime
	i         int
	lastT     int64
}

func (it *blockQuerierSeriesIterator) Seek(t int64) bool {
	for ; it.i < len(it.iterators); it.i++ {
		// We check the maxT property of each iterator because if the time range which its data covers ends at a lower
		// time than the seeked <t> then we don't even need to try to seek to it, as this wouldn't succeed.
		if it.iterators[it.i].maxT >= t {
			// Once we found an iterator which covers a time range that reaches beyond the seeked <t>
			// we try to seek to and return the result.
			if it.iterators[it.i].Seek(t) {
				// Calling .At() to update it.lastT
				it.At()
				return true
			}
		}
	}

	return false
}

func (it *blockQuerierSeriesIterator) At() (int64, float64) {
	if it.i >= len(it.iterators) {
		return 0, 0
	}

	t, v := it.iterators[it.i].At()
	it.lastT = t
	return t, v
}

func (it *blockQuerierSeriesIterator) Next() bool {
	if it.i >= len(it.iterators) {
		return false
	}

	if it.iterators[it.i].Next() {
		// Calling .At() to update it.lastT
		it.At()
		return true
	}
	if it.iterators[it.i].Err() != nil {
		return false
	}

	it.i++

	if it.i >= len(it.iterators) {
		return false
	}

	// Chunks are guaranteed to be ordered but not generally guaranteed to not overlap.
	// We must ensure to skip any overlapping range between adjacent chunks.
	// .Seek() will update it.lastT if it succeeds
	return it.Seek(it.lastT + 1)
}

func (it *blockQuerierSeriesIterator) Err() error {
	if it.i >= len(it.iterators) {
		return nil
	}

	err := it.iterators[it.i].Err()
	if err != nil {
		return errors.Wrapf(err, "cannot iterate chunk for series: %v", it.labels)
	}
	return nil
}
