// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"errors"
	"fmt"
	"io"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/util/chunkcompat"
)

type streamingChunkSeries struct {
	labels labels.Labels

	chunkIteratorFunc chunkIteratorFunc
	mint, maxt        int64

	ingesters []struct {
		streamer    ingesterSeriesStreamer
		seriesIndex uint64
	}
}

func (s *streamingChunkSeries) Labels() labels.Labels {
	return s.labels
}

func (s *streamingChunkSeries) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	var chunks []chunk.Chunk // TODO: guess a size for this?

	for _, ingester := range s.ingesters {
		c, err := ingester.streamer.GetChunks(ingester.seriesIndex)

		if err != nil {
			return series.NewErrIterator(err)
		}

		// TODO: deduplicate chunks (use similar function to accumulateChunks)
		chunks = append(chunks, c...)
	}

	return s.chunkIteratorFunc(chunks, model.Time(s.mint), model.Time(s.maxt))
}

type ingesterSeriesStreamer struct {
	client       client.Ingester_QueryStreamClient
	buffer       chan streamedIngesterSeries
	errChan      chan error
	seriesLabels []labels.Labels // TODO: need to make sure when creating these that we use a single instance of labels.Labels for identical series across ingesters
}

type streamedIngesterSeries struct {
	chunks      []chunk.Chunk
	seriesIndex uint64
}

func (s *ingesterSeriesStreamer) StartBuffering() {
	s.buffer = make(chan streamedIngesterSeries, 10) // TODO: what is a sensible buffer size?
	s.errChan = make(chan error, 1)

	go func() {
		defer s.client.CloseSend()
		defer close(s.buffer)
		defer close(s.errChan)

		nextSeriesIndex := uint64(0)

		for {
			msg, err := s.client.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					s.errChan <- err
				}

				return
			}

			for _, series := range msg.Chunks {
				chunks, err := chunkcompat.FromChunks(s.seriesLabels[nextSeriesIndex], series.Chunks)
				if err != nil {
					s.errChan <- err
					return
				}

				s.buffer <- streamedIngesterSeries{
					chunks:      chunks,
					seriesIndex: nextSeriesIndex,
				}

				nextSeriesIndex++
			}
		}
	}()
}

func (s *ingesterSeriesStreamer) GetChunks(seriesIndex uint64) ([]chunk.Chunk, error) {
	select {
	case err := <-s.errChan:
		return nil, fmt.Errorf("attempted to read series at index %v from stream, but the stream has failed: %w", seriesIndex, err)

	case series, open := <-s.buffer:
		if !open {
			return nil, fmt.Errorf("attempted to read series at index %v from stream, but the stream has already been closed", seriesIndex)
		}

		if series.seriesIndex != seriesIndex {
			return nil, fmt.Errorf("attempted to read series at index %v from stream, but the stream has series with index %v", seriesIndex, series.seriesIndex)
		}

		return series.chunks, nil
	}
}
