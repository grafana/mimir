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
		var lim uint64
		if limit != nil {
			lim = limit.Load()
		}
		m.rehash(m.nextSize(lim))
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
		m.rehash(m.nextSize(0))
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

func (m *Map) Cleanup(watermark clock.Minutes, limit *atomic.Uint64) int {
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
		var lim uint64
		if limit != nil {
			lim = limit.Load()
		}
		m.rehash(m.nextSize(lim))
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

// nextSize computes the number of groups for the next rehash.
// limit is the total tenant series limit across all shards; it is divided by NumShards internally.
// limit=0 means no limit (used by Load): grows by resident*1.25.
func (m *Map) nextSize(limit uint64) uint32 {
	perShard := limit / NumShards
	alive := uint64(m.resident - m.dead)
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

// rehashEntry holds a key-value pair extracted during compaction rehash.
type rehashEntry struct {
	key  uint64
	data xorData
}

var rehashBufPool = &sync.Pool{New: func() any { return &[]rehashEntry{} }}

func (m *Map) rehash(n uint32) {
	// Use the compaction path (reuses existing backing arrays) when the new size fits
	// within the current allocation AND has enough capacity for all alive entries.
	alive := m.resident - m.dead
	if n <= uint32(len(m.index)) && n >= numGroups(alive) {
		m.rehashCompact(n)
	} else {
		m.rehashGrow(n)
	}
}

// rehashCompact handles the case where the new size is <= the old size (compaction after cleanup).
// It collects alive entries into a temporary buffer, clears and reslices the existing backing arrays,
// then re-inserts. This avoids allocating new arrays entirely, eliminating the 2x peak memory.
func (m *Map) rehashCompact(n uint32) {
	alive := int(m.resident - m.dead)

	// Collect alive entries into a pooled temporary buffer.
	// At 9 bytes/entry this is much smaller than the group arrays (80 bytes/group at load factor 4 = 20 bytes/entry).
	bufPtr := rehashBufPool.Get().(*[]rehashEntry)
	buf := *bufPtr
	if cap(buf) < alive {
		buf = make([]rehashEntry, 0, alive)
	}
	buf = buf[:0]
	for g := range m.index {
		for s := range m.index[g] {
			if c := m.index[g][s]; c != empty && c != tombstone {
				buf = append(buf, rehashEntry{key: m.keys[g][s], data: m.data[g][s]})
			}
		}
	}

	// Clear and reslice existing arrays to new size.
	clear(m.index[:n])
	clear(m.keys[:n])
	clear(m.data[:n])
	m.index = m.index[:n]
	m.keys = m.keys[:n]
	m.data = m.data[:n]
	m.limit = n * maxAvgGroupLoad
	m.resident, m.dead = 0, 0

	// Re-insert from the temporary buffer.
	for _, e := range buf {
		m.load(e.key, e.data)
	}

	// Return buffer to pool.
	*bufPtr = buf
	rehashBufPool.Put(bufPtr)
}

// rehashGrow handles the case where the new size is larger than the old size.
// It allocates new arrays (trying the pool first), copies entries, then returns old arrays to the pool.
func (m *Map) rehashGrow(n uint32) {
	oldIndex, oldKeys, oldData := m.index, m.keys, m.data

	var indexPtr *[]index
	var keysPtr *[]keys
	var dataPtr *[]data
	indexPtr, m.index = getRehashSlice[index](&rehashIndexPool, n)
	keysPtr, m.keys = getRehashSlice[keys](&rehashKeysPool, n)
	dataPtr, m.data = getRehashSlice[data](&rehashDataPool, n)
	m.limit = n * maxAvgGroupLoad
	m.resident, m.dead = 0, 0
	for g := range oldIndex {
		for s := range oldIndex[g] {
			c := oldIndex[g][s]
			if c != empty && c != tombstone {
				m.load(oldKeys[g][s], oldData[g][s])
			}
		}
	}

	// Swap new slices into the pool handles so the old arrays can be reused by other shards.
	// The new arrays stay live via m.index/m.keys/m.data.
	putRehashSlice(&rehashIndexPool, indexPtr, oldIndex)
	putRehashSlice(&rehashKeysPool, keysPtr, oldKeys)
	putRehashSlice(&rehashDataPool, dataPtr, oldData)
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

	// Pools for rehash array reuse across shards.
	rehashIndexPool sync.Pool
	rehashKeysPool  sync.Pool
	rehashDataPool  sync.Pool
)

// getRehashSlice gets a *[]T handle from the pool (or allocates one) and ensures the slice has capacity for n elements.
// The caller must pass the returned *[]T back to putRehashSlice.
func getRehashSlice[T index | keys | data](pool *sync.Pool, n uint32) (*[]T, []T) {
	var sp *[]T
	if v := pool.Get(); v != nil {
		sp = v.(*[]T)
		if s := *sp; uint32(cap(s)) >= n {
			s = s[:n]
			clear(s)
			return sp, s
		}
	} else {
		sp = new([]T)
	}
	s := make([]T, n)
	return sp, s
}

// putRehashSlice writes the slice back into the pool handle and returns it to the pool.
func putRehashSlice[T index | keys | data](pool *sync.Pool, sp *[]T, s []T) {
	*sp = s
	pool.Put(sp)
}

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
