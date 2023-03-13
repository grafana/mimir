// SPDX-License-Identifier: AGPL-3.0-only

package iterators

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

func TestChunkIteratorAtFloatHistogramAfterAtHistogram(t *testing.T) {
	c := mkChunk(t, 0, 50, 1*time.Millisecond, chunk.PrometheusHistogramChunk)
	it := chunkIterator{Chunk: c, it: c.Data.NewIterator(nil)}
	require.Equal(t, chunkenc.ValHistogram, it.Next())
	// populate cache with histogram
	_, h := it.AtHistogram()
	require.NotNil(t, h)
	// read float histogram
	_, fh := it.AtFloatHistogram()
	require.NotNil(t, fh)
}
