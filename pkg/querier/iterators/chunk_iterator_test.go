// SPDX-License-Identifier: AGPL-3.0-only

package iterators

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

func TestChunkIterator_ScanShortcut(t *testing.T) {
	encChk, err := chunk.NewForEncoding(chunk.PrometheusXorChunk)
	require.NoError(t, err)

	for i := 0; i < 120; i++ {
		overflow, err := encChk.Add(model.SamplePair{
			Timestamp: model.Time(i),
			Value:     model.SampleValue(i),
		})
		require.NoError(t, err)
		require.Nil(t, overflow)
	}

	chk := chunk.NewChunk(labels.FromStrings(labels.MetricName, "foobar"), encChk, 0, 119)

	it := chunkIterator{Chunk: chk, it: chk.Data.NewIterator(nil)}

	// Seek past what's in the chunk; triggers the shortcut in seek and returns chunkenc.ValNone.
	valType := it.Seek(120)
	require.Equal(t, chunkenc.ValNone, valType)

	// The iterator is exhausted so it returns chunkenc.ValNone.
	valType = it.Next()
	require.Equal(t, chunkenc.ValNone, valType)

	// Likewise for seeking.
	valType = it.Seek(100)
	require.Equal(t, chunkenc.ValNone, valType)
}
