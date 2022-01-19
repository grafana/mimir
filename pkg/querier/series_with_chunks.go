// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/series_with_chunks.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

// SeriesWithChunks extends storage.Series interface with direct access to Mimir chunks.
type SeriesWithChunks interface {
	storage.Series

	// Returns all chunks with series data.
	Chunks() []chunk.Chunk
}
