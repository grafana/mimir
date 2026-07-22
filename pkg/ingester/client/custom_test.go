// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"testing"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

func TestChunkFromMeta(t *testing.T) {
	tests := []struct {
		name             string
		encoding         chunkenc.Encoding
		expectedMimirEnc chunk.Encoding
	}{
		{
			name:             "XOR",
			encoding:         chunkenc.EncXOR,
			expectedMimirEnc: chunk.PrometheusXorChunk,
		},
		{
			name:             "XOR2",
			encoding:         chunkenc.EncXOR2,
			expectedMimirEnc: chunk.PrometheusXor2Chunk,
		},
		{
			name:             "Histogram",
			encoding:         chunkenc.EncHistogram,
			expectedMimirEnc: chunk.PrometheusHistogramChunk,
		},
		{
			name:             "FloatHistogram",
			encoding:         chunkenc.EncFloatHistogram,
			expectedMimirEnc: chunk.PrometheusFloatHistogramChunk,
		},
		{
			name:             "HistogramST",
			encoding:         chunkenc.EncHistogramST,
			expectedMimirEnc: chunk.PrometheusHistogramSTChunk,
		},
		{
			name:             "FloatHistogramST",
			encoding:         chunkenc.EncFloatHistogramST,
			expectedMimirEnc: chunk.PrometheusFloatHistogramSTChunk,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := chunkenc.NewEmptyChunk(tc.encoding)
			require.NoError(t, err)

			meta := chunks.Meta{
				MinTime: 1000,
				MaxTime: 2000,
				Chunk:   c,
			}

			ch, err := ChunkFromMeta(meta)
			require.NoError(t, err)
			assert.Equal(t, int32(tc.expectedMimirEnc), ch.Encoding)
			assert.Equal(t, int64(1000), ch.StartTimestampMs)
			assert.Equal(t, int64(2000), ch.EndTimestampMs)
			assert.Equal(t, []byte(c.Bytes()), []byte(ch.Data))
		})
	}

	t.Run("unknown encoding returns error", func(t *testing.T) {
		meta := chunks.Meta{
			Chunk: &fakeChunk{enc: chunkenc.Encoding(99)},
		}
		_, err := ChunkFromMeta(meta)
		require.ErrorContains(t, err, "unknown chunk encoding from TSDB chunk querier")
	})
}

type fakeChunk struct {
	enc chunkenc.Encoding
}

func (f *fakeChunk) Bytes() []byte                                  { return nil }
func (f *fakeChunk) Encoding() chunkenc.Encoding                    { return f.enc }
func (f *fakeChunk) Appender() (chunkenc.Appender, error)           { return nil, nil }
func (f *fakeChunk) Iterator(_ chunkenc.Iterator) chunkenc.Iterator { return nil }
func (f *fakeChunk) NumSamples() int                                { return 0 }
func (f *fakeChunk) Compact()                                       {}
func (f *fakeChunk) Reset(_ []byte)                                 {}
