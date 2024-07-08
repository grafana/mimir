package chunkreplyformatter

import (
	"fmt"
	"hash/crc32"
	"strings"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

const maxSize = 64 * 1024

type ChunkReplyFormatter struct {
	chunkInfo   strings.Builder
	sourceCount int
}

func NewChunkReplyFormatter() *ChunkReplyFormatter {
	return &ChunkReplyFormatter{
		chunkInfo: strings.Builder{},
	}
}

func (c *ChunkReplyFormatter) StartSeries(seriesId string) {
	c.sourceCount = 0
	if c.chunkInfo.Len() > 0 {
		c.chunkInfo.WriteString(",\"") // next series
	} else {
		c.chunkInfo.WriteString("{\"") // first series
	}
	c.chunkInfo.WriteString(seriesId)
	c.chunkInfo.WriteString("\":{") // ingesters map
}

func (c *ChunkReplyFormatter) EndSeries() bool {
	c.chunkInfo.WriteString("}") // close series map
	return c.chunkInfo.Len() > maxSize
}

func (c *ChunkReplyFormatter) GetChunkInfo() string {
	c.chunkInfo.WriteString("}") // close series map
	result := c.chunkInfo.String()
	c.chunkInfo.Reset()
	return result
}

// Format the chunk info from ingesters
func (c *ChunkReplyFormatter) FormatIngesterChunkInfo(sourceId string, chunks []client.Chunk) {
	c.formatChunkInfo(sourceId, len(chunks), func(i int, prevTime *int64) string {
		chunk := chunks[i]
		startTime := chunk.StartTimestampMs/1000 - *prevTime
		*prevTime = chunk.EndTimestampMs / 1000
		return fmt.Sprintf("%v:%v:%v:%x", startTime, chunk.EndTimestampMs/1000-chunk.StartTimestampMs/1000, len(chunk.Data), crc32.ChecksumIEEE(chunk.Data))
	})
}

func (c *ChunkReplyFormatter) FormatStoreGatewayChunkInfo(sourceId string, chunks []storepb.AggrChunk) {
	c.formatChunkInfo(sourceId, len(chunks), func(i int, prevTime *int64) string {
		chunk := chunks[i]
		startTime := chunk.MinTime/1000 - *prevTime
		*prevTime = chunk.MaxTime / 1000
		return fmt.Sprintf("%v:%v:%v:%x", startTime, chunk.MaxTime/1000-chunk.MinTime/1000, len(chunk.Raw.Data), crc32.ChecksumIEEE(chunk.Raw.Data))
	})
}

func (c *ChunkReplyFormatter) formatChunkInfo(sourceId string, length int, ith func(i int, prevTime *int64) string) {
	c.startSourceInfo(sourceId)
	prevTime := int64(0)
	for i := 0; i < length; i++ {
		if i > 0 {
			c.chunkInfo.WriteRune(',')
		}
		c.chunkInfo.WriteRune('"')
		c.chunkInfo.WriteString(ith(i, &prevTime))
		c.chunkInfo.WriteRune('"')
	}
	c.endSourceInfo()
}

func (c *ChunkReplyFormatter) startSourceInfo(sourceId string) {
	if c.sourceCount > 0 {
		c.chunkInfo.WriteRune(',')
	}
	c.sourceCount++
	c.chunkInfo.WriteRune('"')
	c.chunkInfo.WriteString(sourceId)
	c.chunkInfo.WriteString("\":[") // list of chunks start
}

func (c *ChunkReplyFormatter) endSourceInfo() {
	c.chunkInfo.WriteString("]") // end list of chunks
}
