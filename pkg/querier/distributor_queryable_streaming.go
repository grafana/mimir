// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"fmt"
	"hash/crc32"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/querier/batch"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/series"
)

type streamingChunkSeriesContext struct {
	queryMetrics *stats.QueryMetrics
	queryStats   *stats.Stats
}

// streamingChunkSeries is a storage.Series that reads chunks from sources in a streaming way. The chunks are read from
// each source's client.SeriesChunksStreamReader when the series' iterator is created. The stream reader only reads
// further chunks from its underlying gRPC stream when the current buffer is exhausted, limiting the total number of
// chunks in memory at a time.
type streamingChunkSeries struct {
	labels  labels.Labels
	sources []client.StreamingSeriesSource
	context *streamingChunkSeriesContext

	alreadyCreated bool
	// debug
	traceId   string
	lastOne   bool
	chunkInfo *strings.Builder
}

func (s *streamingChunkSeries) Labels() labels.Labels {
	return s.labels
}

func (s *streamingChunkSeries) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	if s.alreadyCreated {
		return series.NewErrIterator(fmt.Errorf("can't create iterator multiple times for the one streaming series (%v)", s.labels.String()))
	}

	s.alreadyCreated = true

	var uniqueChunks []client.Chunk
	totalChunks := 0

	if s.chunkInfo != nil {
		seriesId := s.labels.Get("series_id")
		if (*(s.chunkInfo)).Len() > 0 {
			(*(s.chunkInfo)).WriteString(",\"") // next series
		} else {
			(*(s.chunkInfo)).WriteString("{\"") // first series
		}
		(*(s.chunkInfo)).WriteString(seriesId)
		(*(s.chunkInfo)).WriteString("\":{") // ingesters map
	}
	for i, source := range s.sources {
		c, err := source.StreamReader.GetChunks(source.SeriesIndex)

		if err != nil {
			return series.NewErrIterator(err)
		}

		if s.chunkInfo != nil {
			if i > 0 {
				(*(s.chunkInfo)).WriteRune(',')
			}
			(*(s.chunkInfo)).WriteRune('"')
			(*(s.chunkInfo)).WriteString(source.StreamReader.GetName()[14:])
			(*(s.chunkInfo)).WriteString("\":[") // list of chunks start
			for j, ci := range chunkSliceInfo(c) {
				if j > 0 {
					(*(s.chunkInfo)).WriteRune(',')
				}
				(*(s.chunkInfo)).WriteRune('"')
				(*(s.chunkInfo)).WriteString(ci)
				(*(s.chunkInfo)).WriteRune('"')
			}
			(*(s.chunkInfo)).WriteString("]") // end list of chunks
		}

		totalChunks += len(c)
		uniqueChunks = client.AccumulateChunks(uniqueChunks, c)
	}

	s.context.queryMetrics.IngesterChunksTotal.Add(float64(totalChunks))
	s.context.queryMetrics.IngesterChunksDeduplicated.Add(float64(totalChunks - len(uniqueChunks)))

	s.context.queryStats.AddFetchedChunks(uint64(len(uniqueChunks)))

	chunkBytes := 0

	for _, c := range uniqueChunks {
		chunkBytes += c.Size()
	}

	if s.chunkInfo != nil {
		(*(s.chunkInfo)).WriteString("}") // close ingester map

		//fmt.Printf("CT: trace_id=%v: got the following from %v ingesters for series_id=%v: %v\n", s.traceId, len(perIngesterInfo), seriesId, strings.Join(perIngesterInfo, ","))
		//fmt.Printf("CT: trace_id=%v: got %v unique chunks for series_id=%v: %v\n", s.traceId, len(uniqueChunks), seriesId, strings.Join(chunkSliceInfo(uniqueChunks), ","))

		// for _, c := range uniqueChunks {
		// 	*(s.chunkInfo) = append(*(s.chunkInfo), fmt.Sprintf("%s:%d:%d", seriesId, c.StartTimestampMs/1000, c.EndTimestampMs/1000))
		// }
		if s.lastOne || (*(s.chunkInfo)).Len() > 64*1024 {
			(*(s.chunkInfo)).WriteString("}") // close series map
			fmt.Printf("CT: chunk stream from ingester: trace_id:%s info:%s\n", s.traceId, (*(s.chunkInfo)).String())
			(*(s.chunkInfo)).Reset()
		}
	}

	s.context.queryStats.AddFetchedChunkBytes(uint64(chunkBytes))

	chunks, err := client.FromChunks(s.labels, uniqueChunks)
	if err != nil {
		return series.NewErrIterator(err)
	}

	return batch.NewChunkMergeIterator(it, chunks)
}

func chunkSliceInfo(chks []client.Chunk) []string {
	info := make([]string, 0, len(chks))
	var endTime int64
	for _, chk := range chks {
		info = append(info, chunkInfo(chk, &endTime))
	}

	return info
}

func chunkInfo(chk client.Chunk, endTime *int64) string {
	startTime := chk.StartTimestampMs/1000 - *endTime
	*endTime = chk.EndTimestampMs / 1000
	return fmt.Sprintf("%v:%v:%v:%x", startTime, chk.EndTimestampMs/1000-chk.StartTimestampMs/1000, len(chk.Data), crc32.ChecksumIEEE(chk.Data))
}
