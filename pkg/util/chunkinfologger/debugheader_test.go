// SPDX-License-Identifier: AGPL-3.0-only

package chunkinfologger

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/propagation"
)

func TestChunkInfoLoggerExtractor(t *testing.T) {
	testCases := map[string]struct {
		headers         http.Header
		expectedEnabled bool
		expectedLabels  []string
	}{
		"no headers": {
			headers:         http.Header{},
			expectedEnabled: false,
			expectedLabels:  nil,
		},
		"single value": {
			headers: http.Header{
				"X-Mimir-Chunk-Info-Logger": []string{"foo"},
			},
			expectedEnabled: true,
			expectedLabels:  []string{"foo"},
		},
		"multiple values": {
			headers: http.Header{
				"X-Mimir-Chunk-Info-Logger": []string{"foo,bar"},
			},
			expectedEnabled: true,
			expectedLabels:  []string{"foo", "bar"},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			extractor := &Extractor{}
			ctx, err := extractor.ExtractFromCarrier(context.Background(), propagation.HttpHeaderCarrier(tc.headers))
			require.NoError(t, err)
			require.Equal(t, tc.expectedEnabled, IsChunkInfoLoggingEnabled(ctx))
			require.Equal(t, tc.expectedLabels, ChunkInfoLoggingFromContext(ctx))
		})
	}
}
