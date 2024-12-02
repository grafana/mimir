// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/dolthub/swiss/blob/master/map.go
// Provenance-includes-location: https://github.com/dolthub/swiss/blob/master/bits.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Dolthub, Inc.

package atomicswissmap

import (
	"math/bits"
	"math/rand/v2"
	"slices"
	"sync"
	"unsafe"

	"go.uber.org/atomic"
)

// Map is an open-addressing hash map based on Abseil's flat_hash_map.
// This specific implementation is adapted to persist atomic.Int32 values.
// It's optimized for often updates but less key modifications (churn): multiple _readers_ can update the atomic int32 values,
// and the exclusive lock is only needed when adding or removing keys.
type Map struct {
	mtx sync.RWMutex

	ctrl     []metadata
	groups   []group
	resident uint32
	dead     uint32
	limit    uint32
}

// metadata is the h2 metadata array for a group.
// find operations first probe the controls bytes
// to filter candidates before matching keys
type metadata [groupSize]uint8

// group is a group of 16 key-value pairs
type group struct {
	keys   [groupSize]uint64
	values [groupSize]atomic.Int32
}

const (
	h1Mask    uint64 = 0xffff_ffff_ffff_ff80
	h2Mask    uint64 = 0x0000_0000_0000_007f
	h2Offset         = 2
	empty     uint8  = 0b0000_0000
	tombstone uint8  = 0b0000_0001
)

// h1 is a 57 bit hash prefix
type h1 uint64

// h2 is a 7 bit hash suffix
type h2 uint8

// New constructs a Map.
func New(sz uint32) (m *Map) {
	groups := numGroups(sz)
	m = &Map{
		ctrl:   make([]metadata, groups),
		groups: make([]group, groups),
		limit:  groups * maxAvgGroupLoad,
	}
	return
}

// Lock must be held when performing operations on the keys: GetOrCreate, Delete and Clear.
func (m *Map) Lock() { m.mtx.Lock() }

// Unlock unlocks Lock.
func (m *Map) Unlock() { m.mtx.Unlock() }

// LockKeys must be held while updating the atomic values of the map.
// It is invalid to update the atomic values when this lock is not held.
func (m *Map) LockKeys() { m.mtx.RLock() }

// UnlockKeys unlocks the LockKeys.
func (m *Map) UnlockKeys() { m.mtx.RUnlock() }

// Has returns true if |key| is present in |m|.
// LockKeys or Lock must be held when performing this operation.
func (m *Map) Has(key uint64) (ok bool) {
	hi, lo := splitHash(key)
	g := probeStart(hi, len(m.groups))
	for { // inlined find loop
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			if key == m.groups[g].keys[s] {
				ok = true
				return
			}
		}
		// |key| is not in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			ok = false
			return
		}
		g += 1 // linear probing
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

// Get returns the |value| mapped by |key| if one exists.
// LockKeys or Lock must be held when performing this operation.
// Any key-write operation (GetOrCreate, Delete, Clear) invalidates the returned atomic.Int32 reference (even if the lock is held).
func (m *Map) Get(key uint64) (value *atomic.Int32, ok bool) {
	hi, lo := splitHash(key)
	g := probeStart(hi, len(m.groups))
	for { // inlined find loop
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			if key == m.groups[g].keys[s] {
				value, ok = &m.groups[g].values[s], true
				return
			}
		}
		// |key| is not in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			ok = false
			return
		}
		g += 1 // linear probing
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

// GetOrCreate attempts to insert |key| and |value| unless it already exists, in which case the existing value is returned.
// Lock must be held when performing this operation.
// GetOrCreate invalidates the references of the atomic.Int32 values previously returned by Get.
func (m *Map) GetOrCreate(key uint64, value int32) (_ *atomic.Int32, found bool) {
	if m.resident >= m.limit {
		m.rehash(m.nextSize())
	}
	hi, lo := splitHash(key)
	g := probeStart(hi, len(m.groups))
	for { // inlined find loop
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			if key == m.groups[g].keys[s] { // found
				return &m.groups[g].values[s], true
			}
		}
		// |key| is not in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 { // insert
			s := nextMatch(&matches)
			m.groups[g].keys[s] = key
			m.groups[g].values[s].Store(value)
			m.ctrl[g][s] = uint8(lo)
			m.resident++
			return nil, false
		}
		g += 1 // linear probing
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

// Delete attempts to remove |key|, returns true successful.
// Lock must be held when performing this operation.
func (m *Map) Delete(key uint64) (ok bool) {
	hi, lo := splitHash(key)
	g := probeStart(hi, len(m.groups))
	for {
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			if key == m.groups[g].keys[s] {
				ok = true
				// optimization: if |m.ctrl[g]| contains any empty
				// metadata bytes, we can physically delete |key|
				// rather than placing a tombstone.
				// The observation is that any probes into group |g|
				// would already be terminated by the existing empty
				// slot, and therefore reclaiming slot |s| will not
				// cause premature termination of probes into |g|.
				if metaMatchEmpty(&m.ctrl[g]) != 0 {
					m.ctrl[g][s] = empty
					m.resident--
				} else {
					m.ctrl[g][s] = tombstone
					m.dead++
				}
				m.groups[g].keys[s] = 0
				m.groups[g].values[s].Store(0) // It's okay to do this, nobody is reading this key.
				return
			}
		}
		// |key| is not in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 { // |key| absent
			ok = false
			return
		}
		g += 1 // linear probing
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

// Iter iterates the elements of the Map, passing them to the callback.
// It guarantees that any key in the Map will be visited only once, and
// for un-mutated Maps, every key will be visited once. If the Map is
// Mutated during iteration, mutations will be reflected on return from
// Iter, but the set of keys visited by Iter is non-deterministic.
// LockKeys or Lock must be held when performing this operation.
func (m *Map) Iter(cb func(k uint64, v *atomic.Int32) (stop bool)) {
	// take a consistent view of the table in case
	// we rehash during iteration
	ctrl, groups := m.ctrl, m.groups
	// pick a random starting group
	g := rand.Uint32N(uint32(len(groups)))
	for n := 0; n < len(groups); n++ {
		for s, c := range ctrl[g] {
			if c == empty || c == tombstone {
				continue
			}
			k, v := groups[g].keys[s], &groups[g].values[s]
			if stop := cb(k, v); stop {
				return
			}
		}
		g++
		if g >= uint32(len(groups)) {
			g = 0
		}
	}
}

// Clone clones the contents of the map.
// The atomic values of the cloned map don't reference the values of the orignal map.
func (m *Map) Clone() *Map {
	// using unkeyed definition to make sure we don't miss any future key.
	clone := &Map{
		sync.RWMutex{},
		slices.Clone(m.ctrl),
		slices.Clone(m.groups),
		m.resident,
		m.dead,
		m.limit,
	}
	return clone
}

// Clear removes all elements from the Map.
// Lock must be held when performing this operation.
func (m *Map) Clear() {
	for i := range m.ctrl {
		m.ctrl[i] = metadata{}
	}
	for i := range m.groups {
		g := &m.groups[i]
		for i := range g.keys {
			g.keys[i] = 0
			g.values[i].Store(0)
		}
	}
	m.resident, m.dead = 0, 0
}

// Count returns the number of elements in the Map.
// LockKeys or Lock must be held when performing this operation.
func (m *Map) Count() int {
	return int(m.resident - m.dead)
}

// find returns the location of |key| if present, or its insertion location if absent.
// for performance, find is manually inlined into public methods.
func (m *Map) find(key uint64, hi h1, lo h2) (g, s uint32, ok bool) {
	g = probeStart(hi, len(m.groups))
	for {
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s = nextMatch(&matches)
			if key == m.groups[g].keys[s] {
				return g, s, true
			}
		}
		// |key| is not in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			s = nextMatch(&matches)
			return g, s, false
		}
		g += 1 // linear probing
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

func (m *Map) nextSize() (n uint32) {
	n = uint32(len(m.groups)) * 2
	if m.dead >= (m.resident / 2) {
		n = uint32(len(m.groups))
	}
	return
}

func (m *Map) rehash(n uint32) {
	groups, ctrl := m.groups, m.ctrl
	m.groups = make([]group, n)
	m.ctrl = make([]metadata, n)
	m.limit = n * maxAvgGroupLoad
	m.resident, m.dead = 0, 0
	for g := range ctrl {
		for s := range ctrl[g] {
			c := ctrl[g][s]
			if c == empty || c == tombstone {
				continue
			}
			if _, found := m.GetOrCreate(groups[g].keys[s], groups[g].values[s].Load()); found {
				// This can't happen by design, the new map is empty.
				panic("found duplicate key in new map while rehashing")
			}
		}
	}
}

func (m *Map) loadFactor() float32 {
	slots := float32(len(m.groups) * groupSize)
	return float32(m.resident-m.dead) / slots
}

// numGroups returns the minimum number of groups needed to store |n| elems.
func numGroups(n uint32) (groups uint32) {
	groups = (n + maxAvgGroupLoad - 1) / maxAvgGroupLoad
	if groups == 0 {
		groups = 1
	}
	return
}

// splitHash extracts the h1 and h2 components from a 64 bit hash.
// h1 is the upper 57 bits, h2 is the lower 7 bits plus two.
// By adding 2, it ensures that h2 is never uint8(0) or uint8(1).
func splitHash(h uint64) (h1, h2) {
	return h1((h & h1Mask) >> 7), h2(h&h2Mask) + h2Offset
}

func probeStart(hi h1, groups int) uint32 {
	return fastModN(uint32(hi), uint32(groups))
}

// lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
func fastModN(x, n uint32) uint32 {
	return uint32((uint64(x) * uint64(n)) >> 32)
}

const (
	groupSize       = 8
	maxAvgGroupLoad = 7

	loBits uint64 = 0x0101010101010101
	hiBits uint64 = 0x8080808080808080
)

type bitset uint64

func metaMatchH2(m *metadata, h h2) bitset {
	// https://graphics.stanford.edu/~seander/bithacks.html##ValueInWord
	return hasZeroByte(castUint64(m) ^ (loBits * uint64(h)))
}

func metaMatchEmpty(m *metadata) bitset {
	return hasZeroByte(castUint64(m))
}

func nextMatch(b *bitset) uint32 {
	s := uint32(bits.TrailingZeros64(uint64(*b)))
	*b &= ^(1 << s) // clear bit |s|
	return s >> 3   // div by 8
}

func hasZeroByte(x uint64) bitset {
	return bitset(((x - loBits) & ^(x)) & hiBits)
}

func castUint64(m *metadata) uint64 {
	return *(*uint64)((unsafe.Pointer)(m))
}
