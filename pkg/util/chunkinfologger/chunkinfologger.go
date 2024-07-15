// SPDX-License-Identifier: AGPL-3.0-only

package chunkinfologger

import (
	"fmt"
	"hash/crc32"
	"strings"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// Allow for 10% overhead from string quotes when logger dumps the JSON.
const maxSize = (64 * 1024 * 90) / 100

type ChunkInfoLogger struct {
	chunkInfo strings.Builder
	msg       string
	traceID   string
	logger    log.Logger
	labels    []string

	// sourceCount is the number of sources in the current series to track if we need to add a comma.
	sourceCount int
}

func NewChunkInfoLogger(msg, traceID string, logger log.Logger, labels []string) *ChunkInfoLogger {
	return &ChunkInfoLogger{
		chunkInfo: strings.Builder{},
		msg:       msg,
		traceID:   traceID,
		logger:    logger,
		labels:    labels,
	}
}

func (c *ChunkInfoLogger) SetMsg(msg string) {
	c.msg = msg
}

func (c *ChunkInfoLogger) StartSeries(ls labels.Labels) {
	c.sourceCount = 0
	if c.chunkInfo.Len() > 0 {
		c.chunkInfo.WriteString(`,"`) // next series
	} else {
		c.chunkInfo.WriteString(`{"`) // first series
	}
	for i, l := range c.labels {
		if i > 0 {
			c.chunkInfo.WriteRune(',')
		}
		// Yes we write empty string if the label is not present in the labels.
		c.chunkInfo.WriteString(ls.Get(l))
	}
	c.chunkInfo.WriteString(`":{`) // ingesters map
}

// Close a series in the chunk info and dump into log if it exceeds the max size.
func (c *ChunkInfoLogger) EndSeries(lastOne bool) {
	c.chunkInfo.WriteRune('}') // close ingester map
	if lastOne || c.chunkInfo.Len() > maxSize {
		c.chunkInfo.WriteRune('}') // close series map
		c.logger.Log("chunkinfo", "true", "msg", c.msg, "traceId", c.traceID, "info", c.chunkInfo.String())
		c.chunkInfo.Reset()
	}
}

// Format the chunk info from ingesters
func (c *ChunkInfoLogger) FormatIngesterChunkInfo(sourceID string, chunks []client.Chunk) {
	c.formatChunkInfo(sourceID, len(chunks), func(i int) string {
		chunk := chunks[i]
		return formatChunk(chunk.StartTimestampMs, chunk.EndTimestampMs, chunk.Data)
	})
}

func (c *ChunkInfoLogger) FormatStoreGatewayChunkInfo(sourceID string, chunks []storepb.AggrChunk) {
	c.formatChunkInfo(sourceID, len(chunks), func(i int) string {
		chunk := chunks[i]
		return formatChunk(chunk.MinTime, chunk.MaxTime, chunk.Raw.Data)
	})
}

// Format the chunk info. The formatting does some naive compression to reduce the size of the log.
// The max time of the chunk is relative to the start time.
// Time resolution is in seconds, not milliseconds as scrape intervals are on the order of seconds.
func formatChunk(minT, maxT int64, data []byte) string {
	return fmt.Sprintf("%v:%v:%v:%x", minT/1000, maxT/1000-minT/1000, len(data), crc32.ChecksumIEEE(data))
}

func (c *ChunkInfoLogger) formatChunkInfo(sourceID string, length int, ith func(i int) string) {
	c.startSourceInfo(sourceID)
	for i := 0; i < length; i++ {
		if i > 0 {
			c.chunkInfo.WriteRune(',')
		}
		c.chunkInfo.WriteRune('"')
		c.chunkInfo.WriteString(ith(i))
		c.chunkInfo.WriteRune('"')
	}
	c.endSourceInfo()
}

func (c *ChunkInfoLogger) startSourceInfo(sourceID string) {
	if c.sourceCount > 0 {
		c.chunkInfo.WriteRune(',')
	}
	c.sourceCount++
	c.chunkInfo.WriteRune('"')
	c.chunkInfo.WriteString(sourceID)
	c.chunkInfo.WriteString(`":[`) // list of chunks start
}

func (c *ChunkInfoLogger) endSourceInfo() {
	c.chunkInfo.WriteRune(']') // end list of chunks
}
