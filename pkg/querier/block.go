// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/block.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"fmt"
	"github.com/grafana/mimir/pkg/querier/batch"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"sort"

	"github.com/pkg/errors"
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

	return newBlockQuerierSeriesIterator(reuse, bqs.Labels(), bqs.chunks)
}

func newBlockQuerierSeriesIterator(reuse chunkenc.Iterator, lbls labels.Labels, chunks []storepb.AggrChunk) chunkenc.Iterator {
	genericChunks := make([]batch.GenericChunk, 0, len(chunks))

	for _, c := range chunks {
		c := c

		genericChunk := batch.NewGenericChunk(c.MinTime, c.MaxTime, func(reuse chunk.Iterator) chunk.Iterator {
			encoding, err := chunkEncodingForAggrChunk(c, lbls)
			if err != nil {
				return chunk.ErrorIterator(err.Error())
			}

			ch, err := chunk.NewForEncoding(encoding)
			if err != nil {
				return chunk.ErrorIterator(fmt.Sprintf("cannot create new chunk for series %s: %s", lbls.String(), err.Error()))
			}

			if err := ch.UnmarshalFromBuf(c.Raw.Data); err != nil {
				return chunk.ErrorIterator(fmt.Sprintf("cannot unmarshal chunk for series %s: %s", lbls.String(), err.Error()))
			}

			return ch.NewIterator(reuse)
		})

		genericChunks = append(genericChunks, genericChunk)
	}

	return batch.NewGenericChunkMergeIterator(reuse, genericChunks)
}

func chunkEncodingForAggrChunk(c storepb.AggrChunk, lbls labels.Labels) (chunk.Encoding, error) {
	switch c.Raw.Type {
	case storepb.Chunk_XOR:
		return chunk.PrometheusXorChunk, nil
	case storepb.Chunk_Histogram:
		return chunk.PrometheusHistogramChunk, nil
	case storepb.Chunk_FloatHistogram:
		return chunk.PrometheusFloatHistogramChunk, nil
	default:
		return 0, fmt.Errorf("failed to initialize chunk from unknown encoded raw data type %v (series: %v, min time: %d, max time: %d)", c.Raw.Type, lbls, c.MinTime, c.MaxTime)
	}
}
