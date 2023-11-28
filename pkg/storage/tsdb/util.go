// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/util.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdb

import (
	"github.com/oklog/ulid"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// HashBlockID returns a 32-bit hash of the block ID useful for
// ring-based sharding.
func HashBlockID(id ulid.ULID) uint32 {
	h := mimirpb.HashNew32()
	for _, b := range id {
		h = mimirpb.HashAddByte32(h, b)
	}
	return h
}
