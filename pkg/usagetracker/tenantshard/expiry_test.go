// SPDX-License-Identifier: AGPL-3.0-only

package tenantshard

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
)

// TestMatchExpiredExhaustive checks the branchless range test against the clock comparison
// it replaces, for every watermark and every value clock.ToMinutes can produce.
func TestMatchExpiredExhaustive(t *testing.T) {
	for w := 0; w < 120; w++ {
		watermark := clock.Minutes(w)
		lo, length := expiredRange(watermark)

		for v := 0; v < 120; v++ {
			value := clock.Minutes(v)
			x := uint64(xor(value)) * loBits // same value in all 8 lanes
			want := watermark.GreaterOrEqualThan(value)
			got := matchExpiredWord(x, lo, length)

			if want {
				require.Equal(t, bitset(hiBits), got, "watermark=%d value=%d: every lane should be expired", w, v)
			} else {
				require.Zero(t, got, "watermark=%d value=%d: no lane should be expired", w, v)
			}
		}

		// Empty and tombstone markers must never be reported, whatever the watermark.
		require.Zero(t, matchExpiredWord(uint64(empty), lo, length), "watermark=%d: empty slot reported as expired", w)
		require.Zero(t, matchExpiredWord(uint64(tombstone)*loBits, lo, length), "watermark=%d: tombstone reported as expired", w)
	}
}

// TestMatchExpiredMixedLanes checks that lanes are independent: each slot of a group is
// evaluated on its own value, not on its neighbours.
func TestMatchExpiredMixedLanes(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 20000; i++ {
		watermark := clock.Minutes(r.Intn(120))
		lo, length := expiredRange(watermark)

		var d data
		var want bitset
		for j := range d {
			switch r.Intn(4) {
			case 0:
				d[j] = empty
			case 1:
				d[j] = tombstone
			default:
				value := clock.Minutes(r.Intn(120))
				d[j] = xor(value)
				if watermark.GreaterOrEqualThan(value) {
					want |= bitset(0x80) << (8 * j)
				}
			}
		}

		require.Equal(t, want, d.matchExpired(lo, length), "watermark=%d data=%v", watermark, d)
	}
}

func TestLastMatch(t *testing.T) {
	b := bitset(0)
	for _, lane := range []uint32{0, 3, 7} {
		b |= bitset(0x80) << (8 * lane)
	}
	require.Equal(t, uint32(7), lastMatch(&b))
	require.Equal(t, uint32(3), lastMatch(&b))
	require.Equal(t, uint32(0), lastMatch(&b))
	require.Zero(t, b)
}

// TestCleanupPreservesLookups is the property that matters after Cleanup rearranges slots:
// every entry that should survive must still be found by a probe, and every entry that
// should be gone must be re-created on the next Put. It exercises maps that are dense
// enough to spill probes across groups and to force tombstones.
func TestCleanupPreservesLookups(t *testing.T) {
	r := rand.New(rand.NewSource(1))

	for round := 0; round < 300; round++ {
		size := 1 + r.Intn(400)
		m := New(uint32(size))

		// Insert more than the map was sized for, so groups fill up and probes spill over.
		entries := map[uint64]clock.Minutes{}
		for i := 0; i < size*2; i++ {
			key := r.Uint64()
			value := clock.Minutes(r.Intn(120))
			m.Put(key, value, nil, nil, false)
			if _, ok := entries[key]; !ok {
				entries[key] = value
			} else {
				// Put keeps the newest value when tracking, which is what we asked for above.
				entries[key] = value
			}
		}

		watermark := clock.Minutes(r.Intn(120))
		survivors := map[uint64]clock.Minutes{}
		for key, value := range entries {
			if !watermark.GreaterOrEqualThan(value) {
				survivors[key] = value
			}
		}

		removed := m.Cleanup(watermark, nil)
		require.Equal(t, len(entries)-len(survivors), removed, "round %d: unexpected number of removals", round)
		require.Equal(t, len(survivors), m.Count(), "round %d: unexpected count", round)

		// Items() must report exactly the survivors, with their values intact.
		got := map[uint64]clock.Minutes{}
		_, items := m.Items()
		for key, value := range items {
			got[key] = value
		}
		require.Equal(t, survivors, got, "round %d: unexpected contents", round)

		// A probe for a survivor must find it, so Put reports it as already existing.
		// A probe for a removed key must not find it, so Put reports it as created.
		for key, value := range entries {
			created, _ := m.Put(key, value, nil, nil, false)
			_, survived := survivors[key]
			require.Equal(t, !survived, created, "round %d: key %d lookup after cleanup", round, key)
		}
	}
}

// scanExpiredReference is the obvious implementation that scanExpired must agree with on
// every architecture.
func scanExpiredReference(d []data, lo, length uint8) int {
	for i := range d {
		if d[i].matchExpired(lo, length) != 0 {
			return i
		}
	}
	return len(d)
}

// TestScanExpiredMatchesReference covers the assembly implementations against the portable
// one, including the odd-length tail and the empty input.
func TestScanExpiredMatchesReference(t *testing.T) {
	t.Logf("scanExpired implementation: %s", simdImpl)
	r := rand.New(rand.NewSource(1))

	for round := 0; round < 5000; round++ {
		n := r.Intn(9) // covers 0, the 8 byte tail, and several 16 byte iterations
		d := make([]data, n+1)
		for i := 0; i < n; i++ {
			for j := range d[i] {
				switch r.Intn(6) {
				case 0:
					d[i][j] = empty
				case 1:
					d[i][j] = tombstone
				default:
					d[i][j] = xor(clock.Minutes(r.Intn(120)))
				}
			}
		}
		// Poison the group past the end so an over-read would be caught.
		for j := range d[n] {
			d[n][j] = xor(0)
		}

		watermark := clock.Minutes(r.Intn(120))
		lo, length := expiredRange(watermark)

		want := scanExpiredReference(d[:n], lo, length)
		got := scanExpired(&d[0], n, lo, length)
		require.Equal(t, want, got, "round %d: n=%d watermark=%d data=%v", round, n, watermark, d[:n])
	}
}

// TestScanExpiredAllExpired and its counterpart pin down the two extremes, where the branch
// in the scan loop always goes the same way.
func TestScanExpiredExtremes(t *testing.T) {
	lo, length := expiredRange(60)

	for n := 1; n <= 9; n++ {
		none := make([]data, n)
		for i := range none {
			for j := range none[i] {
				none[i][j] = empty
			}
		}
		require.Equal(t, n, scanExpired(&none[0], n, lo, length), "n=%d: empty groups must report no hit", n)

		all := make([]data, n)
		for i := range all {
			for j := range all[i] {
				all[i][j] = xor(60)
			}
		}
		require.Equal(t, 0, scanExpired(&all[0], n, lo, length), "n=%d: first group must hit", n)

		// A hit in the last group only, which is the tail group when n is odd.
		last := make([]data, n)
		for i := range last {
			for j := range last[i] {
				last[i][j] = empty
			}
		}
		last[n-1][3] = xor(60)
		require.Equal(t, n-1, scanExpired(&last[0], n, lo, length), "n=%d: last group must hit", n)
	}
}

// cleanupLegacy is the per-slot Cleanup loop that scanExpired replaced. It is kept here to
// compare behaviour and speed against the current implementation. The trailing rehash is
// left out so that both can be measured on the same map.
func (m *Map) cleanupLegacy(watermark clock.Minutes) int {
	removed := 0
groups:
	for i := range m.data {
		for j := uint32(0); j < groupSize; {
			if m.data[i][j] == empty {
				continue groups
			}
			if m.data[i][j] == tombstone {
				j++
				continue
			}
			if watermark.GreaterOrEqualThan(m.data[i][j].clockMinutes()) {
				removed++

				if emptySlots := m.index[i].matchEmpty(); emptySlots != 0 {
					m.resident--
					e := nextMatch(&emptySlots)
					if e == j+1 {
						m.index[i][j] = empty
						m.keys[i][j] = 0
						m.data[i][j] = empty
						continue groups
					}

					m.index[i][j], m.index[i][e-1] = m.index[i][e-1], empty
					m.keys[i][j], m.keys[i][e-1] = m.keys[i][e-1], 0
					m.data[i][j], m.data[i][e-1] = m.data[i][e-1], empty
					continue
				}

				m.index[i][j] = tombstone
				m.keys[i][j] = 0
				m.data[i][j] = tombstone
				m.dead++
			}
			j++
		}
	}
	return removed
}

// TestCleanupMatchesLegacy runs both implementations over identical maps and requires them
// to agree on what survives and on the bookkeeping counters. The new one visits slots in a
// different order, so surviving entries may sit in different slots of the same group, which
// is why the comparison is on contents rather than on the raw arrays.
func TestCleanupMatchesLegacy(t *testing.T) {
	r := rand.New(rand.NewSource(1))

	for round := 0; round < 500; round++ {
		size := 1 + r.Intn(300)
		seed := r.Int63()

		build := func() *Map {
			m := New(uint32(size))
			br := rand.New(rand.NewSource(seed))
			for i := 0; i < size*2; i++ {
				m.Put(br.Uint64(), clock.Minutes(br.Intn(120)), nil, nil, false)
			}
			return m
		}

		watermark := clock.Minutes(r.Intn(120))
		legacy, current := build(), build()

		wantRemoved := legacy.cleanupLegacy(watermark)
		gotRemoved := current.Cleanup(watermark, nil)

		require.Equal(t, wantRemoved, gotRemoved, "round %d: removed count", round)
		require.Equal(t, legacy.resident, current.resident, "round %d: resident", round)
		require.Equal(t, legacy.dead, current.dead, "round %d: dead", round)
		require.Equal(t, legacy.Count(), current.Count(), "round %d: count", round)

		collect := func(m *Map) map[uint64]clock.Minutes {
			got := map[uint64]clock.Minutes{}
			_, items := m.Items()
			for key, value := range items {
				got[key] = value
			}
			return got
		}
		require.Equal(t, collect(legacy), collect(current), "round %d: contents", round)
	}
}

// BenchmarkMapCleanupImpl compares the per-slot loop against the scanExpired one on the
// same data, at the two expired fractions that bracket real usage.
func BenchmarkMapCleanupImpl(b *testing.B) {
	b.Logf("scanExpired implementation: %s", simdImpl)
	for _, size := range []int{16e6} {
		for _, fraction := range []float64{0, 0.01, 0.05, 0.25, 1} {
			m, keys, values, watermark := buildMapForCleanup(size, fraction, 1)

			run := func(b *testing.B, cleanup func()) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					cleanup()
					b.StopTimer()
					refill(m, keys, values)
					b.StartTimer()
				}
				b.StopTimer()
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(size), "ns/entry")
			}

			b.Run(fmt.Sprintf("size=%d/expired=%.0f%%/impl=legacy", size, fraction*100), func(b *testing.B) {
				run(b, func() { m.cleanupLegacy(watermark) })
			})
			b.Run(fmt.Sprintf("size=%d/expired=%.0f%%/impl=scan", size, fraction*100), func(b *testing.B) {
				run(b, func() { m.Cleanup(watermark, nil) })
			})
		}
	}
}
