// SPDX-License-Identifier: AGPL-3.0-only

package chunkinfologger

import (
	"context"
	"net/http"
	"strings"

	"github.com/grafana/dskit/middleware"
)

type chunkInfoLoggingContextType int

const chunkInfoLoggingContextKey = chunkInfoLoggingContextType(1)

const ChunkInfoLoggingHeader = "X-Mimir-Chunk-Info-Logger"

func Middleware() middleware.Interface {
	return middleware.Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			if value := r.Header.Get(ChunkInfoLoggingHeader); value != "" {
				// Split along commas
				labels := strings.Split(value, ",")
				ctx = ContextWithChunkInfoLogging(ctx, labels)
			}
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	})
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
