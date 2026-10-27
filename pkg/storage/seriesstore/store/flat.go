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

// locationCache remembers where a series with a hash was last found, by group and position: a lookup that hits it reads
// one slot, where finding the series' name group and its place in the group's table reads more, each a cache miss for a
// tenant of millions of series. It has no say in what exists: a hit is checked against the entry it points to, so what
// removals move or drop only makes lookups miss, and a slot a series shares with another is the last one's.
type locationCache struct {
	// Buckets of a cache line of slots, for the hashes that share one.
	slots []locationSlot
	mask  uint64
	used  int
}

type locationSlot struct {
	// The position in the group plus one, zero for a slot that is empty.
	index uint32
	// The group, and in the low byte bits of the hash the bucket isn't picked by: a slot of another series of the
	// bucket is told apart without reading its entry, which is a cache miss.
	groupTag uint32
}

const (
	locationBucket  = 8
	locationTagBits = 8
	// A group the slot can't name isn't remembered, and its lookups take the longer way.
	maxLocationGroup = 1<<(32-locationTagBits) - 1
)

func locationTag(hash uint64) uint32 {
	return uint32(hash >> (64 - locationTagBits))
}

// findAll calls visit with each position the hash may be at until it returns true.
func (c *locationCache) findAll(hash uint64, visit func(group uint32, index int) bool) bool {
	if len(c.slots) == 0 {
		return false
	}
	at := (hash & c.mask) &^ (locationBucket - 1)
	tag := locationTag(hash)
	for _, slot := range c.slots[at : at+locationBucket] {
		if slot.index == 0 {
			// A bucket fills from its first slot.
			return false
		}
		if slot.groupTag&(1<<locationTagBits-1) == tag && visit(slot.groupTag>>locationTagBits, int(slot.index)-1) {
			return true
		}
	}
	return false
}

// locationCandidate is the entry a slot of the location cache points to, with the series' hash.
type locationCandidate struct {
	entry *seriesEntry
	group uint32
	index int
	// What was read of the entry's memory, kept so the reads are.
	touched int64
}

// candidate returns the entry of the first slot of the hash's bucket that has its tag and points to an entry of the
// hash. It also reads the start of the entry's labels and of its float head, which checking the entry and appending
// to it read next: for a batch of series looked up first, those cache misses overlap rather than wait on each other.
func (c *locationCache) candidate(hash uint64, groups []group) locationCandidate {
	if len(c.slots) == 0 {
		return locationCandidate{}
	}
	at := (hash & c.mask) &^ (locationBucket - 1)
	tag := locationTag(hash)
	for _, slot := range c.slots[at : at+locationBucket] {
		if slot.index == 0 {
			break
		}
		if slot.groupTag&(1<<locationTagBits-1) != tag {
			continue
		}
		group, index := slot.groupTag>>locationTagBits, int(slot.index)-1
		if entries := groups[group].entries; index < len(entries) && entries[index].hash == hash {
			entry := &entries[index]
			var touched int64
			if size := len(entry.labels); size > 0 {
				// Both ends: the labels of a series are about two cache lines.
				touched = int64(entry.labels[0]) + int64(entry.labels[size-1])
			}
			if head := entry.series.floatHead; head != nil {
				touched += head.minTime
			}
			return locationCandidate{entry: entry, group: group, index: index, touched: touched}
		}
	}
	return locationCandidate{}
}

// remember records where the series with hash is, in the first slot of its bucket: the oldest slot goes when it is
// full.
func (c *locationCache) remember(hash uint64, group uint32, index int, series int) {
	if group > maxLocationGroup {
		return
	}
	if 2*series > len(c.slots) {
		// Grown for four times the series it holds, which empties it: it fills again from the lookups that miss. A
		// bucket then holds two or three series on average, so few overflow.
		c.slots = make([]locationSlot, max(1024, 1<<bits.Len(uint(2*series))))
		c.mask = uint64(len(c.slots) - 1)
	}
	at := (hash & c.mask) &^ (locationBucket - 1)
	bucket := c.slots[at : at+locationBucket]
	slot := locationSlot{uint32(index) + 1, group<<locationTagBits | locationTag(hash)}
	position := 0
	for ; position < locationBucket-1 && bucket[position].index != 0 && bucket[position] != slot; position++ {
	}
	copy(bucket[1:position+1], bucket[:position])
	bucket[0] = slot
}
