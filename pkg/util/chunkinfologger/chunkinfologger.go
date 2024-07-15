// SPDX-License-Identifier: AGPL-3.0-only

package chunkinfologger

import (
	"context"
	"fmt"
	"hash/crc32"
	"strings"

	"github.com/go-kit/log"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// Allow for 10% overhead from string quotes when logger dumps the JSON.
const maxSize = (64 * 1024 * 90) / 100

const ChunkInfoLoggingHeader = "X-Mimir-Chunk-Info-Logger"
const ChunkInfoLoggingEnabled = "true"

func ChunkInfoLoggingHeaderValidator(v string) error {
	if v == ChunkInfoLoggingEnabled {
		return nil
	}
	return fmt.Errorf("must be exactly 'true' or not set")
}

func EnableChunkInfoLoggingFromContext(ctx context.Context) bool {
	v, ok := api.HeaderOptionFromContext(ctx, ChunkInfoLoggingHeader)
	return ok && v == ChunkInfoLoggingEnabled
}

type ChunkInfoLogger struct {
	chunkInfo strings.Builder
	msg       string
	traceID   string
	logger    log.Logger

	// sourceCount is the number of sources in the current series to track if we need to add a comma.
	sourceCount int
}

func NewChunkInfoLogger(msg, traceID string, logger log.Logger) *ChunkInfoLogger {
	return &ChunkInfoLogger{
		chunkInfo: strings.Builder{},
		msg:       msg,
		traceID:   traceID,
		logger:    logger,
	}
}

func (c *ChunkInfoLogger) SetMsg(msg string) {
	c.msg = msg
}

func (c *ChunkInfoLogger) StartSeries(seriesID string) {
	c.sourceCount = 0
	if c.chunkInfo.Len() > 0 {
		c.chunkInfo.WriteString(",\"") // next series
	} else {
		c.chunkInfo.WriteString("{\"") // first series
	}
	c.chunkInfo.WriteString(seriesID)
	c.chunkInfo.WriteString("\":{") // ingesters map
}

// Close a series in the chunk info and dump into log if it exceeds the max size.
func (c *ChunkInfoLogger) EndSeries(lastOne bool) {
	c.chunkInfo.WriteString("}") // close ingester map
	if lastOne || c.chunkInfo.Len() > maxSize {
		c.chunkInfo.WriteString("}") // close series map
		c.logger.Log("msg", c.msg, "traceId", c.traceID, "info", c.chunkInfo.String())
		c.chunkInfo.Reset()
	}
}

// Format the chunk info from ingesters
func (c *ChunkInfoLogger) FormatIngesterChunkInfo(sourceID string, chunks []client.Chunk) {
	c.formatChunkInfo(sourceID, len(chunks), func(i int, prevTime *int64) string {
		chunk := chunks[i]
		startTime := chunk.StartTimestampMs/1000 - *prevTime
		*prevTime = chunk.EndTimestampMs / 1000
		return fmt.Sprintf("%v:%v:%v:%x", startTime, chunk.EndTimestampMs/1000-chunk.StartTimestampMs/1000, len(chunk.Data), crc32.ChecksumIEEE(chunk.Data))
	})
}

func (c *ChunkInfoLogger) FormatStoreGatewayChunkInfo(sourceID string, chunks []storepb.AggrChunk) {
	c.formatChunkInfo(sourceID, len(chunks), func(i int, prevTime *int64) string {
		chunk := chunks[i]
		startTime := chunk.MinTime/1000 - *prevTime
		*prevTime = chunk.MaxTime / 1000
		return fmt.Sprintf("%v:%v:%v:%x", startTime, chunk.MaxTime/1000-chunk.MinTime/1000, len(chunk.Raw.Data), crc32.ChecksumIEEE(chunk.Raw.Data))
	})
}

func (c *ChunkInfoLogger) formatChunkInfo(sourceID string, length int, ith func(i int, prevTime *int64) string) {
	c.startSourceInfo(sourceID)
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

func (c *ChunkInfoLogger) startSourceInfo(sourceID string) {
	if c.sourceCount > 0 {
		c.chunkInfo.WriteRune(',')
	}
	c.sourceCount++
	c.chunkInfo.WriteRune('"')
	c.chunkInfo.WriteString(sourceID)
	c.chunkInfo.WriteString("\":[") // list of chunks start
}

func (c *ChunkInfoLogger) endSourceInfo() {
	c.chunkInfo.WriteString("]") // end list of chunks
}
