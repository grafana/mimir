// SPDX-License-Identifier: AGPL-3.0-only

package chunkinfologger

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/grafana/dskit/middleware"
	"github.com/stretchr/testify/require"
)

func TestChunkInfoLoggerMiddleware(t *testing.T) {
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
			mw := Middleware()
			var (
				actualEnabled bool
				actualLabels  []string
			)
			recorderHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := r.Context()
				actualEnabled = IsChunkInfoLoggingEnabled(ctx)
				if actualEnabled {
					actualLabels = ChunkInfoLoggingFromContext(ctx)
				}
				w.WriteHeader(http.StatusOK)
			})
			handler := middleware.Merge(mw).Wrap(recorderHandler)
			req := httptest.NewRequest("GET", "/", nil)
			req.Header = tc.headers

			resp := httptest.NewRecorder()

			handler.ServeHTTP(resp, req)
			_, err := io.ReadAll(resp.Body)

			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.Code)
			require.Equal(t, tc.expectedEnabled, actualEnabled)
			require.Equal(t, tc.expectedLabels, actualLabels)
		})
	}
}
