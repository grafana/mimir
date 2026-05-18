// SPDX-License-Identifier: AGPL-3.0-only

package storepb

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

func TestAggrChunk_GetChunkEncoding(t *testing.T) {
	tests := []struct {
		name        string
		rawType     Chunk_Encoding
		expectedEnc chunk.Encoding
		expectedOK  bool
	}{
		{
			name:        "XOR",
			rawType:     Chunk_XOR,
			expectedEnc: chunk.PrometheusXorChunk,
			expectedOK:  true,
		},
		{
			name:        "Histogram",
			rawType:     Chunk_Histogram,
			expectedEnc: chunk.PrometheusHistogramChunk,
			expectedOK:  true,
		},
		{
			name:        "FloatHistogram",
			rawType:     Chunk_FloatHistogram,
			expectedEnc: chunk.PrometheusFloatHistogramChunk,
			expectedOK:  true,
		},
		{
			name:        "XOR2",
			rawType:     Chunk_XOR2,
			expectedEnc: chunk.PrometheusXOR2Chunk,
			expectedOK:  true,
		},
		{
			name:       "unknown encoding returns false",
			rawType:    Chunk_Encoding(99),
			expectedOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := AggrChunk{Raw: Chunk{Type: tc.rawType}}
			enc, ok := c.GetChunkEncoding()
			assert.Equal(t, tc.expectedOK, ok)
			if tc.expectedOK {
				assert.Equal(t, tc.expectedEnc, enc)
			} else {
				assert.Equal(t, chunk.Encoding(0), enc)
			}
		})
	}
}
