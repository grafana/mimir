// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/dolthub/swiss/blob/master/map.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Dolthub, Inc.

package tenantshard

import (
	"fmt"
	"slices"
	"sync"

	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
)

const (
	valueBits = 7
	valueMask = Shards - 1

	Shards = 1 << valueBits
)

// Map is an open-addressing hash map based on Abseil's flat_hash_map.
// This holds uint64 keys and clock.Minutes values that should always be smaller than 127 (1<<valueBits - 1).
// This map is expected to hold series refs for a single `shard`, i.e., series refs that have same value modulo 128 (1<<valueBits).
// See https://www.dolthub.com/blog/2023-03-28-swiss-map/ to understand the design of github.com/dolthub/swiss, which is the base for this implementation.
//
// The data is stored in the data field, which are groups of up to groupSize data entries.
// The main modification of this implementation is that each data[i] entry stores both key and value.
// Each data value is a combination of:
// - The upper 64-valueBits are the key
// - The lower valueBits are the negated value.
//
// The value is negated to be able to distinguish the value 0 from an absent value:
// When a data value is not inialized, or it's deleted, the value is 0, which is the same as the value 127, which is not a valid value to store.
//
// One of the advantages of this approach is that we're able to perform the cleanup by iterating only data.
type Map struct {
	sync.Mutex

	shard uint8

	index []index
	data  []data

	resident uint32
	dead     uint32
	limit    uint32
}

// index is the prefix index array for data.
// find operations first probe the index bytes
// to filter candidates before matching data entries.
type index [groupSize]prefix

// data is a group of groupSize data/value entries.
// Each entry is the keyMasked key and valueMasked value.
type data [groupSize]kv

type kv struct {
	// key is the series ref stored as is
	key uint64
	// val is stored negated, so zero value indicates an empty slot.
	val clock.Minutes
}

const (
	prefixOffset        = 2
	empty        prefix = 0b0000_0000
	tombstone    prefix = 0b0000_0001
)

type prefix uint8
type suffix uint64

// New constructs a Map.
func New(sz uint32, shard uint8, totalShards uint8) (m *Map) {
	if totalShards > Shards {
		panic(fmt.Errorf("this implementation is not prepared for more than %d shards, as that would result in skewed group selection (see probeStart)", Shards))
	}
	groups := numGroups(sz)
	return &Map{
		shard: shard,

		index: make([]index, groups),
		data:  make([]data, groups),

		limit: groups * maxAvgGroupLoad,
	}
}

// Put inserts |key| and |value| into the map.
// Series is incremented if it's not nil and it's below limit, unless track is false.
// If track is false, then the value is only updated if it's greater than the current value.
func (m *Map) Put(key uint64, value clock.Minutes, series *atomic.Uint64, limit uint64, track bool) (created, rejected bool) {
	if m.resident >= m.limit {
		m.rehash(m.nextSize())
	}
	if value == 0xff {
		// We use 0xff to represent emptiness (^0xff == 0).
		panic("value is too large")
	}

	pfx, sfx := splitHash(key)
	i := probeStart(sfx, len(m.index))
	for { // inlined find loop
		matches := metaMatchH2(&m.index[i], pfx)
		for matches != 0 {
			j := nextMatch(&matches)
			if key == m.data[i][j].key { // found
				// Always update the value if we're tracking series, but only increment it when processing Load events.
				if track || value.GreaterThan(^m.data[i][j].val) {
					m.data[i][j].val = ^value
				}
				return false, false
			}
		}
		// |key| is not in group |i|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(&m.index[i])
		if matches != 0 { // insert
			// Only check limit if we're tracking series.
			// We don't check limit for Load events.
			if series != nil {
				if track && series.Load() >= limit {
					return false, true // rejected
				}
				series.Inc()
			}
			s := nextMatch(&matches)
			m.index[i][s] = pfx
			m.data[i][s] = kv{key, ^value}
			m.resident++
			return true, false
		}
		i++ // linear probing
		if i >= uint32(len(m.index)) {
			i = 0
		}
	}
}

// count returns the number of alive elements in the Map.
func (m *Map) count() int {
	return int(m.resident - m.dead)
}

func (m *Map) Cleanup(watermark clock.Minutes, series *atomic.Uint64) {
	for i := range m.data {
		for j, e := range m.data[i] {
			if e.val == 0 {
				// There's nothing here.
				continue
			}
			if watermark.GreaterOrEqualThan(^e.val) {
				m.index[i][j] = tombstone
				m.data[i][j] = kv{}
				m.dead++
				series.Dec()
			}
		}
	}
	if m.dead > m.limit/2 {
		m.rehash(m.nextSize())
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
	datas := m.data
	m.data = make([]data, n)
	if int(n) == len(m.index) {
		// same len, we're just removing dead elements
		for i := range m.index {
			m.index[i] = index{}
		}
	} else {
		m.index = make([]index, n)
	}
	m.limit = n * maxAvgGroupLoad
	m.resident, m.dead = 0, 0
	for g := range datas {
		for _, e := range datas[g] {
			if e.val == 0 {
				continue // empty or deleted
			}
			// We don't need to mask the key here, data suffix of the key is always masked out.
			m.Put(e.key, ^e.val, nil, 0, false)
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
	return prefix(h>>(64-valueBits)) + prefixOffset, suffix(h << valueBits >> valueBits)
}

func probeStart(s suffix, groups int) uint32 {
	// ignore the lower |valueBits| bits for probing as they're always the same in this shard.
	// We're going to convert it to uint32 anyway, so we don't really care.
	return fastModN(uint32(s>>valueBits), uint32(groups))
}

// lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
func fastModN(x, n uint32) uint32 {
	return uint32((uint64(x) * uint64(n)) >> 32)
}

type LengthCallback func(int)

type IteratorCallback func(k uint64, v clock.Minutes)

func (m *Map) Iterator() func(LengthCallback, IteratorCallback) {
	clone := slices.Clone(m.data)
	count := m.count()

	return func(length LengthCallback, iterSeries IteratorCallback) {
		length(count)

		for _, g := range clone {
			for _, e := range g {
				if e.val == 0 {
					// There's nothing here.
					continue
				}
				iterSeries(e.key, ^e.val)
			}
		}
	}
}
