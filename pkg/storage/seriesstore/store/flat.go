// SPDX-License-Identifier: AGPL-3.0-only

package store

import "math/bits"

// Open-addressing tables for the indexes every in-memory series is in: a Go map entry costs a
// series 35 to 50 bytes with its control bytes and growth slack, these about half.

// Tables grow past 3/4 full: linear probing stays short below that.
func tableFull(used, slots int) bool {
	return used*4 >= slots*3
}

func tableSlotsFor(count int) int {
	slots := 8
	for tableFull(count, slots) {
		slots *= 2
	}
	return slots
}

// hashIndex finds a name group's first entry with a label hash, by 32 bits of the hash beside
// the entry's index: a probe of another hash reads no entry. The full hash is the entry's.
type hashIndex struct {
	// The entry's index plus one, so zero is an empty slot.
	slots []hashSlot
	used  int
}

type hashSlot struct {
	tag   uint32
	entry uint32
}

func hashTag(hash uint64) uint32 {
	return uint32(hash >> 32)
}

func (h *hashIndex) get(hash uint64, entries []seriesEntry) (int32, bool) {
	if len(h.slots) == 0 {
		return 0, false
	}
	mask := uint64(len(h.slots) - 1)
	tag := hashTag(hash)
	for at := hash & mask; ; at = (at + 1) & mask {
		slot := h.slots[at]
		if slot.entry == 0 {
			return 0, false
		}
		if slot.tag == tag && entries[slot.entry-1].hash == hash {
			return int32(slot.entry - 1), true
		}
	}
}

// set makes index the first entry with hash, which entries holds.
func (h *hashIndex) set(hash uint64, index int32, entries []seriesEntry) {
	if tableFull(h.used+1, len(h.slots)) {
		h.rebuild(tableSlotsFor(h.used+1), entries)
	}
	h.put(hash, uint32(index)+1, entries)
}

func (h *hashIndex) put(hash uint64, entry uint32, entries []seriesEntry) {
	mask := uint64(len(h.slots) - 1)
	tag := hashTag(hash)
	for at := hash & mask; ; at = (at + 1) & mask {
		slot := &h.slots[at]
		if slot.entry == 0 {
			*slot = hashSlot{tag, entry}
			h.used++
			return
		}
		if slot.tag == tag && entries[slot.entry-1].hash == hash {
			slot.entry = entry
			return
		}
	}
}

// rebuild moves the slots to a table of the given size.
func (h *hashIndex) rebuild(slots int, entries []seriesEntry) {
	old := h.slots
	h.slots, h.used = make([]hashSlot, slots), 0
	for _, slot := range old {
		if slot.entry != 0 {
			h.put(entries[slot.entry-1].hash, slot.entry, entries)
		}
	}
}

// reset empties the table, sized for count hashes.
func (h *hashIndex) reset(count int) {
	h.slots, h.used = make([]hashSlot, tableSlotsFor(count)), 0
}

// refIndex finds where each series ref of an engine's shard is. Refs are never zero.
type refIndex struct {
	slots []refSlot
	used  int
}

type refSlot struct {
	ref      uint64
	location refLocation
}

// refHome spreads the refs, which count up, over the table.
func refHome(ref uint64, slots int) uint64 {
	return (ref * 0x9e3779b97f4a7c15) >> (64 - bits.TrailingZeros(uint(slots)))
}

func (r *refIndex) len() int {
	return r.used
}

func (r *refIndex) get(ref uint64) (refLocation, bool) {
	if len(r.slots) == 0 {
		return refLocation{}, false
	}
	mask := uint64(len(r.slots) - 1)
	for at := refHome(ref, len(r.slots)); ; at = (at + 1) & mask {
		slot := &r.slots[at]
		if slot.ref == ref {
			return slot.location, true
		}
		if slot.ref == 0 {
			return refLocation{}, false
		}
	}
}

func (r *refIndex) set(ref uint64, location refLocation) {
	if tableFull(r.used+1, len(r.slots)) {
		r.rebuild(tableSlotsFor(r.used + 1))
	}
	r.put(ref, location)
}

func (r *refIndex) put(ref uint64, location refLocation) {
	mask := uint64(len(r.slots) - 1)
	for at := refHome(ref, len(r.slots)); ; at = (at + 1) & mask {
		slot := &r.slots[at]
		if slot.ref == ref {
			slot.location = location
			return
		}
		if slot.ref == 0 {
			*slot = refSlot{ref, location}
			r.used++
			return
		}
	}
}

// delete removes ref, moving back the refs after it whose probe passed its slot, so lookups need
// no tombstones.
func (r *refIndex) delete(ref uint64) {
	if len(r.slots) == 0 {
		return
	}
	mask := uint64(len(r.slots) - 1)
	at := refHome(ref, len(r.slots))
	for r.slots[at].ref != ref {
		if r.slots[at].ref == 0 {
			return
		}
		at = (at + 1) & mask
	}
	hole := at
	for next := (hole + 1) & mask; r.slots[next].ref != 0; next = (next + 1) & mask {
		// A ref can fill the hole unless its home is strictly after the hole, up to its slot.
		home := refHome(r.slots[next].ref, len(r.slots))
		if (next-home)&mask >= (next-hole)&mask {
			r.slots[hole] = r.slots[next]
			hole = next
		}
	}
	r.slots[hole] = refSlot{}
	r.used--
	// A table that lost most of its refs to head compaction gives the slots back.
	if len(r.slots) > 64 && r.used*8 < len(r.slots) {
		r.rebuild(tableSlotsFor(r.used))
	}
}

func (r *refIndex) rebuild(slots int) {
	old := r.slots
	r.slots, r.used = make([]refSlot, slots), 0
	for _, slot := range old {
		if slot.ref != 0 {
			r.put(slot.ref, slot.location)
		}
	}
}

// reset empties the table, sized for count refs.
func (r *refIndex) reset(count int) {
	r.slots, r.used = make([]refSlot, tableSlotsFor(count)), 0
}
