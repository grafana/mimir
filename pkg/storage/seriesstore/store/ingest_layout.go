// SPDX-License-Identifier: AGPL-3.0-only

package store

import "math/bits"

// ingestLayoutCache needs a repeated batch before allocating positions: changing batches should not create garbage.
// Cached positions remain hints; the ingest path checks the full hash and labels under the shard lock.
type ingestLayoutCache struct {
	slots [256]ingestLayout
	used  int
}

type ingestLayout struct {
	hash      uint64
	positions []refLocation
}

func (c *ingestLayoutCache) get(prepared []floatSeries, positions []int32, series int) []refLocation {
	budget := min(32768, series/2)
	if len(positions) > budget {
		return nil
	}
	hash := uint64(len(positions))
	for _, position := range positions {
		hash = bits.RotateLeft64(hash, 13)*0x9e3779b97f4a7c15 + prepared[position].hash
	}
	slot := &c.slots[hash%uint64(len(c.slots))]
	if slot.hash != hash {
		slot.hash = hash
		// Reuse capacity after a collision, but never use the previous layout as a hit.
		slot.positions = slot.positions[:0]
		return nil
	}
	if len(slot.positions) != len(positions) {
		if cap(slot.positions) < len(positions) {
			if c.used-cap(slot.positions)+len(positions) > budget {
				return nil
			}
			c.used += len(positions) - cap(slot.positions)
			slot.positions = make([]refLocation, len(positions))
		} else {
			slot.positions = slot.positions[:len(positions)]
		}
		for i := range slot.positions {
			slot.positions[i].index = -1
		}
	}
	return slot.positions
}
