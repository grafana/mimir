// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/block.go
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
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/series"
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
	series []*storepb.Series

	// next response to process
	next int

	currSeries storage.Series
}

func (bqss *blockQuerierSeriesSet) Next() bool {
	bqss.currSeries = nil

	if bqss.next >= len(bqss.series) {
		return false
	}

	currLabels := bqss.series[bqss.next].Labels
	currChunks := bqss.series[bqss.next].Chunks

	bqss.next++

	// Merge chunks for current series. Chunks may come in multiple responses, but as soon
	// as the response has chunks for a new series, we can stop searching. Series are sorted.
	// See documentation for StoreClient.Series call for details.
	for bqss.next < len(bqss.series) && mimirpb.CompareLabelAdapters(currLabels, bqss.series[bqss.next].Labels) == 0 {
		currChunks = append(currChunks, bqss.series[bqss.next].Chunks...)
		bqss.next++
	}

	bqss.currSeries = newBlockQuerierSeries(mimirpb.FromLabelAdaptersToLabels(currLabels), currChunks)
	return true
}

func (bqss *blockQuerierSeriesSet) At() storage.Series {
	return bqss.currSeries
}

func (bqss *blockQuerierSeriesSet) Err() error {
	return nil
}

func (bqss *blockQuerierSeriesSet) Warnings() annotations.Annotations {
	return nil
}

// newBlockQuerierSeries makes a new blockQuerierSeries. Input labels must be already sorted by name.
func newBlockQuerierSeries(lbls labels.Labels, chunks []storepb.AggrChunk) *blockQuerierSeries {
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

func (bqs *blockQuerierSeries) Iterator(reuse chunkenc.Iterator) chunkenc.Iterator {
	if len(bqs.chunks) == 0 {
		// should not happen in practice, but we have a unit test for it
		return series.NewErrIterator(errors.New("no chunks"))
	}

	it, err := newBlockQuerierSeriesIterator(reuse, bqs.Labels(), bqs.chunks)
	if err != nil {
		return series.NewErrIterator(err)
	}

	return it
}

func newBlockQuerierSeriesIterator(reuse chunkenc.Iterator, lbls labels.Labels, chunks []storepb.AggrChunk) (*blockQuerierSeriesIterator, error) {
	it := &blockQuerierSeriesIterator{}
	it.labels = lbls
	iterators := make([]iteratorWithMaxTime, len(chunks)) // TODO: reuse existing slice from tree?

	for i, c := range chunks {
		var (
			ch  chunkenc.Chunk
			err error
		)
		switch c.Raw.Type {
		case storepb.Chunk_XOR:
			ch, err = chunkenc.FromData(chunkenc.EncXOR, c.Raw.Data)
		case storepb.Chunk_Histogram:
			ch, err = chunkenc.FromData(chunkenc.EncHistogram, c.Raw.Data)
		case storepb.Chunk_FloatHistogram:
			ch, err = chunkenc.FromData(chunkenc.EncFloatHistogram, c.Raw.Data)
		default:
			return nil, errors.Wrapf(err, "failed to initialize chunk from unknown type (%v) encoded raw data (series: %v min time: %d max time: %d)", c.Raw.Type, lbls, c.MinTime, c.MaxTime)
		}

		if err != nil {
			return nil, errors.Wrapf(err, "failed to initialize chunk from %v type encoded raw data (series: %v min time: %d max time: %d)", c.Raw.Type, lbls, c.MinTime, c.MaxTime)
		}

		iterators[i].Iterator = ch.Iterator(nil) // TODO: reuse existing iterator
		iterators[i].maxT = c.MaxTime
	}

	it.tree = newSeriesChunksIteratorTree(iterators) // TODO: reuse existing tree

	return it, nil
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

	tree *seriesChunksIteratorTree
}

func (it *blockQuerierSeriesIterator) Seek(t int64) chunkenc.ValueType {
	it.tree.Seek(t)

	return it.tree.WinnerValueType()
}

func (it *blockQuerierSeriesIterator) At() (int64, float64) {
	if i := it.tree.WinnerIterator(); i != nil {
		return i.At()
	}

	return 0, 0
}

func (it *blockQuerierSeriesIterator) AtHistogram(h *histogram.Histogram) (int64, *histogram.Histogram) {
	if i := it.tree.WinnerIterator(); i != nil {
		return i.AtHistogram(h)
	}

	return 0, nil
}

func (it *blockQuerierSeriesIterator) AtFloatHistogram(fh *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	if i := it.tree.WinnerIterator(); i != nil {
		return i.AtFloatHistogram(fh)
	}

	return 0, nil
}

func (it *blockQuerierSeriesIterator) AtT() int64 {
	if i := it.tree.WinnerIterator(); i != nil {
		return i.AtT()
	}

	return 0
}

func (it *blockQuerierSeriesIterator) Next() chunkenc.ValueType {
	if !it.tree.Next() {
		return chunkenc.ValNone
	}

	return it.tree.WinnerValueType()
}

func (it *blockQuerierSeriesIterator) Err() error {
	err := it.tree.err
	if err != nil {
		return errors.Wrapf(err, "cannot iterate chunk for series: %v", it.labels)
	}
	return nil
}
