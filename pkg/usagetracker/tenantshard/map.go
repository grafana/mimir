// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/dolthub/swiss/blob/master/map.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Dolthub, Inc.

package tenantshard

import (
	"fmt"
	"math"
	"slices"
	"sync"

	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
)

const (
	valueBits        = 7
	valueMask        = Shards - 1
	keyMask   uint64 = uint64(math.MaxUint64) &^ valueMask

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
type data [groupSize]uint64

const (
	prefixOffset        = 2
	empty        prefix = 0b0000_0000
	tombstone    prefix = 0b0000_0001
)

type prefix uint8
type suffix uint64

// New constructs a Map.
func New(sz uint32, shard uint8, totalShards uint8) (m *Map) {
	if totalShards != Shards {
		panic(fmt.Errorf("this implementation doesn't support less than %d total shards: not enough meaningful bits, it's also not prepared for more than %d shards, as that would result in skewed group selection (see probeStart)", Shards, Shards))
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
	if value >= clock.Minutes(valueMask) {
		// we can't store more than valueMask because we don't have enough bits.
		// we also can't store valueMask: if we negate that, we get 0, which we use to represent an empty value.
		panic("value is too large")
	}

	masked := key & keyMask
	pfx, sfx := splitHash(key)
	i := probeStart(sfx, len(m.index))
	for { // inlined find loop
		matches := metaMatchH2(&m.index[i], pfx)
		for matches != 0 {
			j := nextMatch(&matches)
			if masked == m.data[i][j]&keyMask { // found
				// Always update the value if we're tracking series, but only increment it when processing Load events.
				if track || value.GreaterThan(clock.Minutes(m.data[i][j]&valueMask^valueMask)) {
					m.data[i][j] = masked | uint64(value&valueMask^valueMask)
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
			m.data[i][s] = masked | uint64(value&valueMask^valueMask)
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
		for j, v := range m.data[i] {
			raw := v & valueMask
			if raw == 0 {
				// There's nothing here.
				continue
			}
			val := clock.Minutes(raw ^ valueMask)
			if watermark.GreaterOrEqualThan(val) {
				m.index[i][j] = tombstone
				m.data[i][j] = 0
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
	// TODO if we're just removing dead elements, we can reuse same m.index.
	datas, indices := m.data, m.index
	m.data = make([]data, n)
	m.index = make([]index, n)
	m.limit = n * maxAvgGroupLoad
	m.resident, m.dead = 0, 0
	for g := range indices {
		for s := range indices[g] {
			c := indices[g][s]
			if c == empty || c == tombstone {
				continue
			}
			// We don't need to mask the key here, data suffix of the key is always masked out.
			m.Put(datas[g][s], clock.Minutes(datas[g][s]&valueMask^valueMask), nil, 0, false)
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
	shard := m.shard
	clone := slices.Clone(m.data)
	count := m.count()

	return func(length LengthCallback, iterSeries IteratorCallback) {
		length(count)

		for _, g := range clone {
			for _, v := range g {
				raw := v & valueMask
				if raw == 0 {
					// TODO: document somewhwere that we can't store 127 as a value.
					// There's nothing here.
					continue
				}
				key := (v &^ valueMask) | uint64(shard)
				val := clock.Minutes(raw ^ valueMask)
				iterSeries(key, val)
			}
		}
	}
}
