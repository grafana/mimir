// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/util.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdb

import (
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/grafana/mimir/pkg/ingester/client"
)

// HashBlockID returns a 32-bit hash of the block ID useful for
// ring-based sharding.
func HashBlockID(id ulid.ULID) uint32 {
	h := client.HashNew32()
	for _, b := range id {
		h = client.HashAddByte32(h, b)
	}
	return h
}

// ChunkRefEncode encodes a segment file number and byte offset into a ChunkRef.
func ChunkRefEncode(segment uint32, offset uint32) chunks.ChunkRef {
	return chunks.ChunkRef(uint64(segment)<<32 | uint64(offset))
}

// ChunkRefDecode decodes a ChunkRef into a segment file number and byte offset.
func ChunkRefDecode(ref chunks.ChunkRef) (uint32, uint32) {
	return uint32(ref >> 32), uint32(ref)
}
