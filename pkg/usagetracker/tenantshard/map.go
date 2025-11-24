// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/dolthub/swiss/blob/master/map.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Dolthub, Inc.

package tenantshard

import (
	"iter"
	"sync"

	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
)

const (
	indexEntryBits = 7
)

// Map is an open-addressing hash map based on Abseil's flat_hash_map.
// This holds uint64 keys and clock.Minutes values that should always be smaller than 255.
// See https://www.dolthub.com/blog/2023-03-28-swiss-map/ to understand the design of github.com/dolthub/swiss, which is the base for this implementation.
//
// The data is stored in the data field, which are groups of up to groupSize data entries.
//
// The value is negated to be able to distinguish the value 0 from an absent value.
// When a data value is not inialized, or it's deleted, the value is 0, which is the same as the value 255, which is not a valid value to store.
//
// One of the advantages of this approach is that we're able to perform the cleanup by iterating only data.
type Map struct {
	sync.Mutex

	index []index
	keys  []keys
	data  []data

	resident uint32
	dead     uint32
	limit    uint32
}

// index is the prefix index array for data.
// find operations first probe the index bytes
// to filter candidates before matching data entries.
type index [groupSize]prefix

type keys [groupSize]uint64

// data is a group of groupSize xorData entries.
type data [groupSize]xorData

// xorData is what we store in data, which is xor-ed clock.Minutes value.
// By storing xor-ed values, our empty data represents an empty dataset.
type xorData uint8

func xor(v clock.Minutes) xorData { return xorData(^v) }

func (x xorData) clockMinutes() clock.Minutes { return clock.Minutes(^x) }

const (
	prefixOffset        = 2
	empty        prefix = 0b0000_0000
	tombstone    prefix = 0b0000_0001
)

type prefix uint8
type suffix uint64

// New constructs a Map.
func New(sz uint32) (m *Map) {
	groups := numGroups(sz)
	return &Map{
		index: make([]index, groups),
		keys:  make([]keys, groups),
		data:  make([]data, groups),

		limit: groups * maxAvgGroupLoad,
	}
}

// Put inserts |key| and |value| into the map.
// Series is incremented if it's not nil and it's below limit, unless track is false.
// If track is false, then the value is only updated if it's greater than the current value.
func (m *Map) Put(key uint64, value clock.Minutes, series, limit *atomic.Uint64, track bool) (created, rejected bool) {
	if m.resident >= m.limit {
		m.rehash(m.nextSize())
	}

	if value == 0xff {
		// We can't store 0xff because it's stored as 0 which has a special meaning.
		panic("value is too large")
	}

	pfx, sfx := splitHash(key)
	i := probeStart(sfx, len(m.index))
	for { // inlined find loop
		matches := m.index[i].match(pfx)
		for matches != 0 {
			j := nextMatch(&matches)
			if key == m.keys[i][j] { // found
				// Always update the value if we're tracking series, but only increment it when processing Load events.
				if track || value.GreaterThan(m.data[i][j].clockMinutes()) {
					m.data[i][j] = xor(value)
				}
				return false, false
			}
		}
		// |key| is not in group |i|,
		// stop probing if we see an empty slot
		matches = m.index[i].matchEmpty()
		if matches != 0 { // insert
			// Only check limit if we're tracking series.
			// We don't check limit for Load events.
			if series != nil {
				if track && series.Load() >= limit.Load() {
					return false, true // rejected
				}
				series.Inc()
			}
			m.insert(key, pfx, xor(value), i, matches)
			return true, false
		}
		i++ // linear probing
		if i >= uint32(len(m.index)) {
			i = 0
		}
	}
}

func (m *Map) insert(key uint64, pfx prefix, entry xorData, i uint32, matches bitset) {
	s := nextMatch(&matches)
	m.index[i][s] = pfx
	m.keys[i][s] = key
	m.data[i][s] = entry
	m.resident++
}

// Load inserts |key| and |value| into the map without checking if it already exists.
// No limits are checked, and series count should be incremented by the caller.
func (m *Map) Load(key uint64, value clock.Minutes) {
	if m.resident >= m.limit {
		m.rehash(m.nextSize())
	}

	if value == 0xff {
		// We can't store 0xff because it's stored as 0 which has a special meaning.
		panic("value is too large")
	}

	m.load(key, xor(value))
}

// load inserts |key| and |entry| into the map without checking if it already exists.
// No limits are checked, and series count should be incremented by the caller.
// This also assumes that map has enough capacity to hold the new element, and that the element is valid.
// This is only expected to be called from rehash().
func (m *Map) load(key uint64, entry xorData) {
	pfx, sfx := splitHash(key)
	i := probeStart(sfx, len(m.index))
	looped := false
	for {
		// Find an empty slot and insert without checking if it already exists.
		matches := m.index[i].matchEmpty()
		if matches != 0 { // insert
			m.insert(key, pfx, entry, i, matches)
			return
		}
		i++ // linear probing
		if i >= uint32(len(m.index)) {
			if looped {
				panic("infinite loop in Load(), this should not happen")
			}
			looped = true
			i = 0
		}
	}
}

// Count returns the number of alive elements in the Map.
func (m *Map) Count() int {
	return int(m.resident - m.dead)
}

func (m *Map) Cleanup(watermark clock.Minutes) int {
	removed := 0
	for i := range m.data {
		for j, entry := range m.data[i] {
			if entry == 0 {
				// There's nothing here.
				continue
			}
			if watermark.GreaterOrEqualThan(entry.clockMinutes()) {
				m.index[i][j] = tombstone
				m.keys[i][j] = 0
				m.data[i][j] = 0
				m.dead++
				removed++
			}
		}
	}
	if m.dead > m.limit/2 {
		m.rehash(m.nextSize())
	}
	return removed
}

// EnsureCapacity ensure that the map has enough capacity to store |n| elements.
// This does not mean that the map will have n empty slots, there might be already n elements in the map and 0 spare capacity.
// If there's no enough capacity, the map is rehashed to accommodate at least |n| elements.
func (m *Map) EnsureCapacity(n uint32) {
	if groups := numGroups(n); len(m.index) < int(groups) {
		m.rehash(groups)
	}
}

func (m *Map) nextSize() (n uint32) {
	n = uint32(len(m.index)) * 2
	if m.dead >= (m.resident / 2) {
		n = uint32(len(m.index))
	}
	return
}

func (m *Map) rehash(n uint32) {
	indices, ks, datas := m.index, m.keys, m.data
	m.index = make([]index, n)
	m.keys = make([]keys, n)
	m.data = make([]data, n)
	m.limit = n * maxAvgGroupLoad
	m.resident, m.dead = 0, 0
	for g := range indices {
		for s := range indices[g] {
			c := indices[g][s]
			if c != empty && c != tombstone {
				m.load(ks[g][s], datas[g][s])
			}
		}
	}
}

// numGroups returns the minimum number of groups needed to store |n| elems.
func numGroups(n uint32) (groups uint32) {
	groups = (n + maxAvgGroupLoad - 1) / maxAvgGroupLoad
	if groups == 0 {
		groups = 1
	}
	return
}

// splitHash extracts the prefix and suffix components from a 64 bit hash.
// prefix is the upper 7 bits plus two, suffix is the lower 57 bits.
// By adding 2 to the prefix, it ensures that prefix is never uint8(0) or uint8(1).
func splitHash(h uint64) (prefix, suffix) {
	return prefix(h>>(64-indexEntryBits)) + prefixOffset, suffix(h << indexEntryBits >> indexEntryBits)
}

func probeStart(s suffix, groups int) uint32 {
	// ignore the lower bits for probing as they're always the same in this shard.
	// We're going to convert it to uint32 anyway, so we don't really care.
	return fastModN(uint32(s>>8), uint32(groups))
}

// lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
func fastModN(x, n uint32) uint32 {
	return uint32((uint64(x) * uint64(n)) >> 32)
}

var (
	keysPool = &sync.Pool{New: func() any { return new([]keys) }}
	dataPool = &sync.Pool{New: func() any { return new([]data) }}
)

func pooledClone[T any](input []T, pool *sync.Pool) *[]T {
	pooled := pool.Get().(*[]T)
	if cap(*pooled) > len(input) {
		*pooled = (*pooled)[:len(input)]
	} else {
		*pooled = make([]T, len(input))
	}
	copy(*pooled, input)
	return pooled
}

func (m *Map) Items() (length int, iterator iter.Seq2[uint64, clock.Minutes]) {
	keysClone := pooledClone(m.keys, keysPool)
	dataClone := pooledClone(m.data, dataPool)
	count := m.Count()

	return count, func(yield func(uint64, clock.Minutes) bool) {
		if count == 0 {
			return
		}

		for i, g := range *dataClone {
			for j, entry := range g {
				if entry == 0 {
					// There's nothing here.
					continue
				}
				if !yield((*keysClone)[i][j], entry.clockMinutes()) {
					return // stop iteration
				}
			}
		}

		keysPool.Put(keysClone)
		dataPool.Put(dataClone)
	}
}
