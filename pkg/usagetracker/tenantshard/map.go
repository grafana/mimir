// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/dolthub/swiss/blob/master/map.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Dolthub, Inc.

package tenantshard

import (
	"iter"
	"math"
	"sync"

	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
)

const (
	indexEntryBits = 7

	// NumShards is the number of shards used by the tracker store per tenant.
	NumShards = 16

	// maxAvgGroupLoad was 7 in dolthub/swiss, but we trade in some memory for less CPU by having to check less entries.
	maxAvgGroupLoad = groupSize / 2
	// last is the last element in the group, just to make the code more readable
	last = groupSize - 1
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
	limit    uint32

	// spilled is the number of groups that (may) have spilled to the next one.
	// i.e. their last slot is empty (either by data or by a spillmark)
	spilled uint32

	// rehashes is only counted for testing purposes.
	rehashes uint32
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
	prefixOffset = 2

	// empty is an index or data mark for an empty slot.
	empty = 0b0000_0000
	// spillmark is an index or data mark for a deleted last group slot
	// that indicates that group may have spilled to the next one.
	spillmark = 0b0000_0001
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
	if value >= 0xfe {
		// We can't store 0xff or 0xfe because it's stored as 0/1 which have a special meaning (empty & spillmark).
		panic("value is too large")
	}

	pfx, sfx := splitHash(key)
	ps := probeStart(sfx, len(m.index))
	i := ps
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
		// |key| is not in group |i|
		// stop probing if last element is empty (not busy and not a spillmark)
		if m.index[i][last] == empty {
			break
		}
		i++ // linear probing
		if i >= uint32(len(m.index)) {
			i = 0
		}
		if i == ps {
			// We're back where we started: every group has its last slot taken, so |key| is not
			// in the map. Stop probing, the check below is going to make room for it.
			break
		}
	}

	// Check whether a rehash is needed
	switch {
	case m.resident >= m.limit:
		// We ran out of capacity, so grow.
		var lim uint64
		if limit != nil {
			lim = limit.Load()
		}
		m.rehash(m.nextSize(lim))
		// probe start may have changed if the number of groups has changed
		ps = probeStart(sfx, len(m.index))
	case m.spilled > m.maxSpilledGroups():
		// Too many groups spill into the next one, so probing has to walk too far.
		// We have capacity, we just need to re-pack at the same size: that drops the spillmarks
		// that Cleanup left behind. The number of groups does not change, so probe start still holds.
		m.rehash(uint32(len(m.index)))
	}

	i = ps
	for {
		// stop probing if we see an empty slot
		matches := m.index[i].matchEmptyOrSpillmark()
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

	wasSpillmark := m.index[i][s] == spillmark
	m.index[i][s] = pfx
	m.keys[i][s] = key
	m.data[i][s] = entry
	m.resident++

	if s == last && !wasSpillmark {
		m.spilled++
	}
}

// Load inserts |key| and |value| into the map without checking if it already exists.
// No limits are checked, and series count should be incremented by the caller.
func (m *Map) Load(key uint64, value clock.Minutes) {
	switch {
	case m.resident >= m.limit:
		m.rehash(m.nextSize(0))
	case m.spilled > m.maxSpilledGroups():
		// See the note in Put: this re-packs at the same size to drop spillmarks.
		m.rehash(uint32(len(m.index)))
	}

	if value >= 0xfe {
		// We can't store 0xff or 0xfe because it's stored as 0/1 which have a special meaning (empty & spillmark).
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
		matches := m.index[i].matchEmptyOrSpillmark()
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
	return int(m.resident)
}

// Stats is a point-in-time snapshot of a Map's internal counters, used for debugging.
type Stats struct {
	// Resident is the number of resident elements, including dead ones (spillmarks).
	Resident uint32 `json:"resident"`
	// Spilled is the number of groups that spilled to the next one(s).
	Spilled uint32 `json:"spilled"`
	// Limit is the resident count that triggers a rehash when reached.
	Limit uint32 `json:"limit"`
	// Length is the number of groups, i.e. the (identical) length of the index, keys and data arrays.
	Length int `json:"length"`
	// Rehashes is the number of rehashes since the Map was created.
	Rehashes uint32 `json:"rehashes"`
}

// Stats returns a snapshot of the Map's internal counters.
func (m *Map) Stats() Stats {
	m.Lock()
	defer m.Unlock()
	return Stats{
		Resident: m.resident,
		Spilled:  m.spilled,
		Limit:    m.limit,
		Length:   len(m.index),
		Rehashes: m.rehashes,
	}
}

func (m *Map) Cleanup(watermark clock.Minutes, limit *atomic.Uint64) int {
	removed := 0
	for i := range m.data {
		occupied := m.index[i].matchOccupied()
		for occupied != 0 {
			j := nextMatch(&occupied)
			if watermark.GreaterOrEqualThan(m.data[i][j].clockMinutes()) {
				removed++
				m.resident--

				if j == last {
					// This is the last element, if it was previously set,
					// then group may have spilled to the next one.
					// We need to keep that signal, so we leave a spillmark here.
					m.data[i][j] = spillmark
					// We need to leave spillmark in the data because that's what iterator uses.
					m.index[i][j] = spillmark
					// We don't need to touch the keys, because nobody will read them if index/data is a spillmark.
					// Keys are groups of uint64 that utilize an entire cache line, better to avoid touching them.
				} else {
					// This is not the last element, so just mark it as empty.
					m.data[i][j] = empty
					m.index[i][j] = empty
				}
			}
		}
	}
	if limit == nil {
		return removed
	}

	// Shrink only when the map is more than twice the size we need: limits fluctuate, and rehashing
	// a whole shard under its lock on every dip costs more than holding on to some extra memory.
	// TODO: use an active-series hint instead of the limit, which usually sits far above actual usage.
	if target := m.nextSize(limit.Load()); uint32(len(m.index))/2 > target {
		m.rehash(target)
	}

	return removed
}

// maxSpilledGroups returns the maximum number of groups we allow to be spilled.
// We allow groups to be half full, so the worst case is half of the groups full and half of them completely empty
// In order to prevent a rehash cycle in that case, we allow one more group to spill before requiring a rehash.
// Note that allowing more spilled groups impacts the Put/Load performance as more buckets need to be checked.
func (m *Map) maxSpilledGroups() uint32 {
	return uint32(len(m.index))*maxAvgGroupLoad/groupSize + 1
}

// EnsureCapacity ensure that the map has enough capacity to store |n| elements.
// This does not mean that the map will have n empty slots, there might be already n elements in the map and 0 spare capacity.
// If there's no enough capacity, the map is rehashed to accommodate at least |n| elements.
func (m *Map) EnsureCapacity(n uint32) {
	if groups := numGroups(n); len(m.index) < int(groups) {
		m.rehash(groups)
	}
}

// nextSize computes the number of groups for the next rehash.
// limit is the total tenant series limit across all shards; it is divided by NumShards internally.
// limit=0 means no limit (used by Load): grows by resident*1.25.
func (m *Map) nextSize(limit uint64) uint32 {
	perShard := limit / NumShards
	alive := uint64(m.resident)
	target := alive * 5 / 4
	// Only let the limit influence growth when it represents a real constraint.
	if perShard > target && perShard <= math.MaxUint32 {
		target = perShard
	}
	if target < alive {
		target = alive
	}
	if target > math.MaxUint32 {
		target = math.MaxUint32
	}
	return numGroups(uint32(target))
}

func (m *Map) rehash(groups uint32) {
	m.rehashes++

	indices, ks, datas := m.index, m.keys, m.data
	m.index = make([]index, groups)
	m.keys = make([]keys, groups)
	m.data = make([]data, groups)
	m.limit = groups * maxAvgGroupLoad
	m.resident, m.spilled = 0, 0
	for g := range indices {
		occupied := indices[g].matchOccupied()
		for occupied != 0 {
			s := nextMatch(&occupied)
			m.load(ks[g][s], datas[g][s])
		}
	}
}

// numGroups returns the minimum number of groups needed to store |n| elems.
func numGroups(n uint32) uint32 {
	if n == 0 {
		return 1
	}
	// Use (n-1)/d+1 instead of (n+d-1)/d to avoid uint32 overflow when n is large.
	return (n-1)/maxAvgGroupLoad + 1
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
				if entry == empty || entry == spillmark {
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
