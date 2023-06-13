// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"sort"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// Implementation of storage.SeriesSet, based on individual responses from store client.
type blockStreamingQuerierSeriesSet struct {
	series       []*storepb.StreamingSeries
	streamReader chunkStreamer

	// next response to process
	next int

	currSeries storage.Series
}

type chunkStreamer interface {
	GetChunks(seriesIndex uint64) ([]storepb.AggrChunk, error)
}

func (bqss *blockStreamingQuerierSeriesSet) Next() bool {
	bqss.currSeries = nil

	if bqss.next >= len(bqss.series) {
		return false
	}

	currLabels := mimirpb.FromLabelAdaptersToLabels(bqss.series[bqss.next].Labels)
	seriesIdxStart := bqss.next // First series in this group. We might merge with more below.
	bqss.next++

	// Chunks may come in multiple responses, but as soon as the response has chunks for a new series,
	// we can stop searching. Series are sorted. See documentation for StoreClient.Series call for details.
	// The actually merging of chunks happens in the Iterator() call where chunks are fetched.
	for bqss.next < len(bqss.series) && labels.Compare(currLabels, mimirpb.FromLabelAdaptersToLabels(bqss.series[bqss.next].Labels)) == 0 {
		bqss.next++
	}

	bqss.currSeries = newBlockStreamingQuerierSeries(currLabels, seriesIdxStart, bqss.next-1, bqss.streamReader)
	return true
}

func (bqss *blockStreamingQuerierSeriesSet) At() storage.Series {
	return bqss.currSeries
}

func (bqss *blockStreamingQuerierSeriesSet) Err() error {
	return nil
}

func (bqss *blockStreamingQuerierSeriesSet) Warnings() storage.Warnings {
	return nil
}

// newBlockStreamingQuerierSeries makes a new blockQuerierSeries. Input labels must be already sorted by name.
func newBlockStreamingQuerierSeries(lbls labels.Labels, seriesIdxStart, seriesIdxEnd int, streamReader chunkStreamer) *blockStreamingQuerierSeries {
	return &blockStreamingQuerierSeries{
		labels:         lbls,
		seriesIdxStart: seriesIdxStart,
		seriesIdxEnd:   seriesIdxEnd,
		streamReader:   streamReader,
	}
}

type blockStreamingQuerierSeries struct {
	labels                       labels.Labels
	seriesIdxStart, seriesIdxEnd int
	streamReader                 chunkStreamer
}

func (bqs *blockStreamingQuerierSeries) Labels() labels.Labels {
	return bqs.labels
}

func (bqs *blockStreamingQuerierSeries) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	// Fetch the chunks from the stream.
	var allChunks []storepb.AggrChunk
	for i := bqs.seriesIdxStart; i <= bqs.seriesIdxEnd; i++ {
		chks, err := bqs.streamReader.GetChunks(uint64(i))
		if err != nil {
			return series.NewErrIterator(err)
		}
		allChunks = append(allChunks, chks...)
	}
	if len(allChunks) == 0 {
		// should not happen in practice, but we have a unit test for it
		return series.NewErrIterator(errors.New("no chunks"))
	}

	sort.Slice(allChunks, func(i, j int) bool {
		return allChunks[i].MinTime < allChunks[j].MinTime
	})

	it, err := newBlockQuerierSeriesIterator(bqs.Labels(), allChunks)
	if err != nil {
		return series.NewErrIterator(err)
	}

	return it
}
