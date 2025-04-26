// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus-community/parquet-common/blob/main/schema/encoder_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package schema

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecode(t *testing.T) {
	mint, maxt := int64(0), int64(120)
	sb := NewBuilder(mint, maxt, 60)
	s, err := sb.Build()
	require.NoError(t, err)

	samples := chunks.GenerateSamples(int(mint), int(maxt)+1)
	it := storage.NewListChunkSeriesFromSamples(labels.FromStrings("foo", "bar"), samples)
	enc := NewPrometheusParquetChunksEncoder(s)
	dec := NewPrometheusParquetChunksDecoder(chunkenc.NewPool())

	chunks, err := enc.Encode(it.Iterator(nil))
	require.NoError(t, err)
	remainingSamplesToCheck := samples
	for _, chunk := range chunks {
		chunkMeta, err := dec.Decode(chunk, 0, 120)
		require.NoError(t, err)
		require.Len(t, chunkMeta, 1)
		sIt := chunkMeta[0].Chunk.Iterator(nil)
		for sIt.Next() != chunkenc.ValNone {
			ts, v := sIt.At()
			require.Equal(t, remainingSamplesToCheck[0].T(), ts)
			require.Equal(t, remainingSamplesToCheck[0].F(), v)
			remainingSamplesToCheck = remainingSamplesToCheck[1:]
		}
	}
	require.Empty(t, remainingSamplesToCheck)
}
