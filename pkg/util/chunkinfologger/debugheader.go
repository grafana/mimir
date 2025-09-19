// SPDX-License-Identifier: AGPL-3.0-only

package chunkinfologger

import (
	"context"
	"strings"

	"github.com/grafana/mimir/pkg/util/propagation"
)

type chunkInfoLoggingContextType int

const chunkInfoLoggingContextKey = chunkInfoLoggingContextType(1)

const ChunkInfoLoggingHeader = "X-Mimir-Chunk-Info-Logger"

type Extractor struct{}

func (p *Extractor) ExtractFromCarrier(ctx context.Context, carrier propagation.Carrier) (context.Context, error) {
	if value := carrier.Get(ChunkInfoLoggingHeader); value != "" {
		// Split along commas
		labels := strings.Split(value, ",")
		ctx = ContextWithChunkInfoLogging(ctx, labels)
	}

	return ctx, nil
}

func ContextWithChunkInfoLogging(ctx context.Context, labels []string) context.Context {
	return context.WithValue(ctx, chunkInfoLoggingContextKey, labels)
}

func IsChunkInfoLoggingEnabled(ctx context.Context) bool {
	return ctx.Value(chunkInfoLoggingContextKey) != nil
}

func ChunkInfoLoggingFromContext(ctx context.Context) []string {
	stored := ctx.Value(chunkInfoLoggingContextKey)
	if stored == nil {
		return nil
	}
	return stored.([]string)
}
