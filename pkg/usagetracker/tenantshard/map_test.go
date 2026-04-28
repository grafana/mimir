// SPDX-License-Identifier: AGPL-3.0-only

package tenantshard

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
)

func TestMap(t *testing.T) {
	series := atomic.NewUint64(0)
	const events = 5
	const seriesPerEvent = 5
	limit := atomic.NewUint64(uint64(events * seriesPerEvent))

	// Start small, let rehashing happen.
	m := New(seriesPerEvent)

	storedValues := map[uint64]clock.Minutes{}
	for i := 1; i <= events; i++ {
		refs := make([]uint64, seriesPerEvent)
		for j := range refs {
			refs[j] = uint64(i*100 + j)
			storedValues[refs[j]] = clock.Minutes(i)
			created, rejected := m.Put(refs[j], clock.Minutes(i), series, limit, false)
			require.True(t, created)
			require.False(t, rejected)
		}
	}

	require.Equal(t, events*seriesPerEvent, m.Count())
	require.Equal(t, uint64(events*seriesPerEvent), series.Load())

	{
		// No more series will fit.
		created, rejected := m.Put(uint64(65535), 1, series, limit, true)
		require.False(t, created)
		require.True(t, rejected)
	}

	{
		gotValues := map[uint64]clock.Minutes{}
		length, items := m.Items()
		require.Equal(t, len(storedValues), length)
		for key, value := range items {
			gotValues[key] = value
		}
		require.Equal(t, storedValues, gotValues)
	}

	{
		// Cleanup first wave of series
		removed := m.Cleanup(clock.Minutes(1), nil)
		series.Add(-uint64(removed))
		expectedSeries := (events - 1) * seriesPerEvent

		// It's unsafe to check m.count() after Cleanup event.
		require.Equal(t, expectedSeries, int(series.Load()))
	}
}

func TestMapValues(t *testing.T) {
	const count = 10e3
	stored := map[uint64]clock.Minutes{}
	m := New(100)
	total := atomic.NewUint64(0)
	for i := 0; i < count; i++ {
		key := rand.Uint64()
		val := clock.Minutes(i)
		if val >= 0xfe {
			continue
		}
		stored[key] = val
		m.Put(key, val, total, nil, false)
	}
	require.Equal(t, len(stored), m.Count())
	require.Equal(t, len(stored), int(total.Load()))

	got := map[uint64]clock.Minutes{}
	l, items := m.Items()
	require.Equal(t, len(stored), l)
	for key, value := range items {
		got[key] = value
	}
	require.Equal(t, stored, got)
}

func TestNextSize(t *testing.T) {
	t.Run("no limit grows by 1.25x resident", func(t *testing.T) {
		m := New(100)
		for i := uint64(0); i < 100; i++ {
			m.Load(i, 1)
		}
		resident := m.resident
		require.True(t, resident > 0)
		require.Zero(t, m.dead)

		got := m.nextSize(0)
		expected := numGroups(resident * 5 / 4)
		require.Equal(t, expected, got)
	})

	t.Run("limit larger than 1.25x resident uses per-shard limit", func(t *testing.T) {
		m := New(100)
		for i := uint64(0); i < 100; i++ {
			m.Load(i, 1)
		}
		// Total limit across all shards, nextSize divides by NumShards.
		const totalLimit = NumShards * 1000
		got := m.nextSize(totalLimit)
		expected := numGroups(1000)
		require.Equal(t, expected, got)
	})

	t.Run("limit smaller than 1.25x resident uses 1.25x", func(t *testing.T) {
		m := New(100)
		for i := uint64(0); i < 100; i++ {
			m.Load(i, 1)
		}
		got := m.nextSize(10)
		expected := numGroups(m.resident * 5 / 4)
		require.Equal(t, expected, got)
	})

	t.Run("compaction with many dead entries does not grow", func(t *testing.T) {
		m := New(200)
		for i := uint64(0); i < 200; i++ {
			m.Load(i, 1)
		}
		m.Cleanup(1, nil)
		require.True(t, m.dead >= m.resident/2)
		alive := m.resident - m.dead
		got := m.nextSize(0)
		require.True(t, got >= numGroups(uint32(alive)))
	})
}

func TestLimitAwareGrowth(t *testing.T) {
	const perShard uint64 = 1000
	m := New(uint32(perShard))
	total := atomic.NewUint64(0)

	for i := uint64(0); i < perShard; i++ {
		m.Put(i, 1, total, nil, false)
	}
	groupsBefore := len(m.index)

	// Trigger a rehash by adding one more element (no series limit check since limit arg is nil).
	m.Put(perShard, 1, total, nil, false)
	groupsAfter := len(m.index)

	// Without limit-aware growth the map would double.
	// With limit-aware growth it should grow to ~1.25x, not 2x.
	require.Less(t, groupsAfter, groupsBefore*2,
		"expected limit-aware growth to be less than 2x: before=%d after=%d", groupsBefore, groupsAfter)

	// When per-shard limit > 1.25x resident, the limit is used.
	m2 := New(uint32(perShard))
	for i := uint64(0); i < perShard; i++ {
		m2.Load(i, 1)
	}
	bigLimit := uint64(m2.resident) * 2 * NumShards
	got := m2.nextSize(bigLimit)
	expected := numGroups(uint32(bigLimit / NumShards))
	require.Equal(t, expected, got)
}

func TestMapCleanup(t *testing.T) {
	// itemsMap is a helper that collects Items() into a map.
	itemsMap := func(t *testing.T, m *Map) map[uint64]clock.Minutes {
		t.Helper()
		got := map[uint64]clock.Minutes{}
		_, items := m.Items()
		for key, value := range items {
			got[key] = value
		}
		return got
	}

	t.Run("empty map", func(t *testing.T) {
		m := New(8)
		removed := m.Cleanup(100, nil)
		require.Equal(t, 0, removed)
		require.Equal(t, 0, m.Count())
		require.Zero(t, m.dead)
		require.Zero(t, m.resident)
	})

	t.Run("no entries expired", func(t *testing.T) {
		m := New(8)
		m.Load(1, 50)
		m.Load(2, 60)
		m.Load(3, 70)

		removed := m.Cleanup(10, nil) // watermark=10 is before all entries
		require.Equal(t, 0, removed)
		require.Equal(t, 3, m.Count())
		require.Zero(t, m.dead)
		require.Equal(t, uint32(3), m.resident)
	})

	t.Run("all entries expired", func(t *testing.T) {
		m := New(8)
		m.Load(1, 10)
		m.Load(2, 20)
		m.Load(3, 30)

		removed := m.Cleanup(30, nil) // watermark=30 expires all
		require.Equal(t, 3, removed)
		require.Equal(t, 0, m.Count())
	})

	t.Run("some entries expired some not", func(t *testing.T) {
		m := New(16)
		// Insert entries with different timestamps.
		m.Load(100, 10)
		m.Load(200, 20)
		m.Load(300, 30)
		m.Load(400, 40)
		m.Load(500, 50)

		removed := m.Cleanup(30, nil) // expire entries with value <= 30
		require.Equal(t, 3, removed)
		require.Equal(t, 2, m.Count())

		// Survivors should be findable.
		got := itemsMap(t, m)
		require.Contains(t, got, uint64(400))
		require.Contains(t, got, uint64(500))
		require.Equal(t, clock.Minutes(40), got[400])
		require.Equal(t, clock.Minutes(50), got[500])
	})

	t.Run("survivors findable via put update path", func(t *testing.T) {
		m := New(16)
		m.Load(100, 10) // will expire
		m.Load(200, 50) // will survive
		m.Load(300, 50) // will survive
		m.Load(400, 10) // will expire

		removed := m.Cleanup(20, nil)
		require.Equal(t, 2, removed)
		require.Equal(t, 2, m.Count())

		// Update survivors via Put — should find them (not create new).
		created, _ := m.Put(200, 60, nil, nil, false)
		require.False(t, created, "should update existing entry, not create new")
		created, _ = m.Put(300, 60, nil, nil, false)
		require.False(t, created, "should update existing entry, not create new")

		require.Equal(t, 2, m.Count())
	})

	t.Run("tombstone avoidance with empty slots in group", func(t *testing.T) {
		// With maxAvgGroupLoad=4 and groupSize=8, a small map will have groups
		// that are partially full, so cleanup should avoid tombstones.
		m := New(4) // 1 group, limit=4
		m.Load(1, 10)
		m.Load(2, 50)

		removed := m.Cleanup(10, nil) // expire key=1
		require.Equal(t, 1, removed)
		require.Equal(t, 1, m.Count())
		require.Zero(t, m.dead, "should not create tombstones when group has empty slots")
		require.Equal(t, uint32(1), m.resident)
	})

	t.Run("tombstone created when group is full", func(t *testing.T) {
		// Force a full group by directly populating all 8 slots.
		m := New(1) // 1 group
		// Fill all groupSize slots directly.
		for j := uint32(0); j < groupSize; j++ {
			m.index[0][j] = prefix(j + prefixOffset)
			m.keys[0][j] = uint64(j + 1)
			if j == 0 {
				m.data[0][j] = xor(10) // will expire
			} else {
				m.data[0][j] = xor(50) // won't expire
			}
			m.resident++
		}

		removed := m.Cleanup(10, nil) // expire entry at slot 0
		require.Equal(t, 1, removed)
		require.Equal(t, uint32(1), m.dead, "should create tombstone when group is full")
	})

	t.Run("expire last element clears to empty", func(t *testing.T) {
		// Set up a group with 2 elements: slots [0] and [1] occupied, rest empty.
		// Expire element at slot [1] (the last). This should hit the e == j+1 path.
		m := New(1)
		m.index[0][0] = prefix(prefixOffset + 10)
		m.keys[0][0] = 100
		m.data[0][0] = xor(50) // won't expire
		m.index[0][1] = prefix(prefixOffset + 20)
		m.keys[0][1] = 200
		m.data[0][1] = xor(10) // will expire
		m.resident = 2

		removed := m.Cleanup(10, nil)
		require.Equal(t, 1, removed)
		require.Equal(t, uint32(1), m.resident)
		require.Zero(t, m.dead)
		// Slot 1 should be empty now.
		require.Equal(t, prefix(empty), m.index[0][1])
		require.Equal(t, xorData(empty), m.data[0][1])
		// Slot 0 should still have its data.
		require.Equal(t, prefix(prefixOffset+10), m.index[0][0])
		require.Equal(t, uint64(100), m.keys[0][0])
	})

	t.Run("expire non-last element swaps with last", func(t *testing.T) {
		// Set up a group with 3 elements: slots [0], [1], [2] occupied.
		// Expire element at slot [0]. The last element ([2]) should be swapped into [0].
		m := New(1)
		m.index[0][0] = prefix(prefixOffset + 10)
		m.keys[0][0] = 100
		m.data[0][0] = xor(10) // will expire

		m.index[0][1] = prefix(prefixOffset + 20)
		m.keys[0][1] = 200
		m.data[0][1] = xor(50) // won't expire

		m.index[0][2] = prefix(prefixOffset + 30)
		m.keys[0][2] = 300
		m.data[0][2] = xor(50) // won't expire
		m.resident = 3

		removed := m.Cleanup(10, nil)
		require.Equal(t, 1, removed)
		require.Equal(t, uint32(2), m.resident)
		require.Zero(t, m.dead)

		// Element from slot [2] should now be in slot [0].
		require.Equal(t, prefix(prefixOffset+30), m.index[0][0])
		require.Equal(t, uint64(300), m.keys[0][0])
		require.Equal(t, xor(50), m.data[0][0])
		// Slot [1] unchanged.
		require.Equal(t, prefix(prefixOffset+20), m.index[0][1])
		require.Equal(t, uint64(200), m.keys[0][1])
		// Slot [2] should be empty.
		require.Equal(t, prefix(empty), m.index[0][2])
		require.Equal(t, xorData(empty), m.data[0][2])
	})

	t.Run("multiple expirations in same group with shifts", func(t *testing.T) {
		// 4 elements: [0]=expire, [1]=survive, [2]=expire, [3]=survive
		// After expiring [0]: [3] swapped to [0], group is [3-data, 1-data, 2-data]
		// Then [1] is checked (survive), then [2] is checked (expire):
		//   [2] is last element, so e==j+1 path clears it.
		// Result: 2 elements at [0] and [1].
		m := New(1)
		m.index[0][0] = prefix(prefixOffset + 10)
		m.keys[0][0] = 100
		m.data[0][0] = xor(10) // expire

		m.index[0][1] = prefix(prefixOffset + 20)
		m.keys[0][1] = 200
		m.data[0][1] = xor(50) // survive

		m.index[0][2] = prefix(prefixOffset + 30)
		m.keys[0][2] = 300
		m.data[0][2] = xor(10) // expire

		m.index[0][3] = prefix(prefixOffset + 40)
		m.keys[0][3] = 400
		m.data[0][3] = xor(50) // survive
		m.resident = 4

		removed := m.Cleanup(10, nil)
		require.Equal(t, 2, removed)
		require.Equal(t, uint32(2), m.resident)
		require.Zero(t, m.dead)
		require.Equal(t, 2, m.Count())
	})

	t.Run("expire all elements in partially full group", func(t *testing.T) {
		m := New(1)
		m.index[0][0] = prefix(prefixOffset + 10)
		m.keys[0][0] = 100
		m.data[0][0] = xor(10)

		m.index[0][1] = prefix(prefixOffset + 20)
		m.keys[0][1] = 200
		m.data[0][1] = xor(10)

		m.index[0][2] = prefix(prefixOffset + 30)
		m.keys[0][2] = 300
		m.data[0][2] = xor(10)
		m.resident = 3

		removed := m.Cleanup(10, nil)
		require.Equal(t, 3, removed)
		require.Zero(t, m.resident)
		require.Zero(t, m.dead)
		require.Equal(t, 0, m.Count())
	})

	t.Run("cleanup skips existing tombstones", func(t *testing.T) {
		// Set up a group with a tombstone followed by a live entry.
		m := New(1)
		// Slot [0]: tombstone (pre-existing)
		m.index[0][0] = tombstone
		m.keys[0][0] = 0
		m.data[0][0] = tombstone
		m.dead = 1
		m.resident = 2 // 1 dead + 1 alive

		// Slot [1]: live entry
		m.index[0][1] = prefix(prefixOffset + 20)
		m.keys[0][1] = 200
		m.data[0][1] = xor(50)

		removed := m.Cleanup(10, nil) // watermark=10, entry at slot[1] has value 50 (won't expire)
		require.Equal(t, 0, removed)
		require.Equal(t, uint32(1), m.dead, "pre-existing tombstone should remain")
		require.Equal(t, uint32(200), uint32(m.keys[0][1]), "live entry should be untouched")
	})

	t.Run("rehash triggered when too many tombstones", func(t *testing.T) {
		// Create a map with many full groups, then expire entries to create tombstones.
		m := New(groupSize * 4) // 4 groups minimum
		// Fill all slots in multiple groups to force tombstone creation.
		for g := 0; g < len(m.index); g++ {
			for j := uint32(0); j < groupSize; j++ {
				m.index[g][j] = prefix(j + prefixOffset)
				m.keys[g][j] = uint64(g*int(groupSize) + int(j) + 1)
				m.data[g][j] = xor(10) // all will expire
				m.resident++
			}
		}

		rehashBefore := m.rehashes
		m.Cleanup(10, nil) // all entries expire, full groups → tombstones
		// dead should exceed limit/2, triggering a rehash.
		require.Greater(t, m.rehashes, rehashBefore, "should trigger rehash when too many tombstones")
	})

	t.Run("tombstone avoidance reduces rehashes", func(t *testing.T) {
		// When groups are not full, cleanup avoids tombstones and thus avoids rehashing.
		// Use randomized keys and a very large capacity to ensure no group is full.
		r := rand.New(rand.NewSource(42))
		m := New(1000)
		for range 20 {
			m.Load(r.Uint64(), 10) // all will expire
		}
		rehashBefore := m.rehashes
		m.Cleanup(10, nil)
		require.Equal(t, rehashBefore, m.rehashes, "should not rehash when tombstones are avoided")
		require.Zero(t, m.dead)
	})

	t.Run("count and items consistent after cleanup", func(t *testing.T) {
		m := New(32)
		expected := map[uint64]clock.Minutes{}
		for i := uint64(0); i < 20; i++ {
			val := clock.Minutes(10)
			if i%2 == 0 {
				val = 50 // survivors
				expected[i] = val
			}
			m.Load(i, val)
		}

		removed := m.Cleanup(20, nil) // expire val<=20
		require.Equal(t, 10, removed)
		require.Equal(t, len(expected), m.Count())

		got := itemsMap(t, m)
		require.Equal(t, expected, got)
	})

	t.Run("sequential cleanups with interleaved puts", func(t *testing.T) {
		m := New(32)

		// Round 1: insert keys with value 10.
		for i := uint64(0); i < 10; i++ {
			m.Load(i, 10)
		}
		removed := m.Cleanup(10, nil) // expire all
		require.Equal(t, 10, removed)
		require.Equal(t, 0, m.Count())

		// Round 2: insert new keys with value 20.
		for i := uint64(100); i < 110; i++ {
			m.Load(i, 20)
		}
		require.Equal(t, 10, m.Count())

		// Round 3: expire half, add more.
		for i := uint64(110); i < 120; i++ {
			m.Load(i, 30)
		}
		removed = m.Cleanup(20, nil) // expire value<=20
		require.Equal(t, 10, removed)
		require.Equal(t, 10, m.Count())

		// All remaining should be keys 110-119.
		got := itemsMap(t, m)
		for i := uint64(110); i < 120; i++ {
			require.Contains(t, got, i)
			require.Equal(t, clock.Minutes(30), got[i])
		}
	})

	t.Run("cleanup with limit triggers limit-aware rehash", func(t *testing.T) {
		// Fill groups completely, expire everything, pass a limit.
		m := New(groupSize * 2)
		for g := 0; g < len(m.index); g++ {
			for j := uint32(0); j < groupSize; j++ {
				m.index[g][j] = prefix(j + prefixOffset)
				m.keys[g][j] = uint64(g*int(groupSize) + int(j) + 1)
				m.data[g][j] = xor(10)
				m.resident++
			}
		}
		limit := atomic.NewUint64(1000)
		rehashBefore := m.rehashes
		m.Cleanup(10, limit)
		if m.rehashes > rehashBefore {
			// After rehash, map should be clean.
			require.Zero(t, m.dead)
		}
	})

	t.Run("large scale correctness", func(t *testing.T) {
		// Insert many elements with mixed timestamps, cleanup, verify survivors.
		const n = 10000
		m := New(uint32(n))
		expected := map[uint64]clock.Minutes{}
		for i := uint64(0); i < n; i++ {
			val := clock.Minutes(i % 100)
			if val >= 0xfe {
				continue
			}
			m.Load(i, val)
			if val > 50 {
				expected[i] = val
			}
		}

		removed := m.Cleanup(50, nil) // expire val <= 50
		require.Equal(t, n-len(expected), removed)
		require.Equal(t, len(expected), m.Count())

		got := itemsMap(t, m)
		require.Equal(t, expected, got)
	})

	t.Run("put after cleanup finds correct entries", func(t *testing.T) {
		// Regression test: after cleanup with shifts, Put must still find
		// existing keys and not create duplicates.
		m := New(32)
		series := atomic.NewUint64(0)
		for i := uint64(0); i < 20; i++ {
			m.Put(i, clock.Minutes(i%50+1), series, nil, false)
		}
		require.Equal(t, uint64(20), series.Load())

		m.Cleanup(10, nil) // expire val <= 10

		// Try to Put all 20 keys again. Expired ones should be created, survivors updated.
		for i := uint64(0); i < 20; i++ {
			created, _ := m.Put(i, 40, series, nil, false)
			if i%50+1 <= 10 {
				require.True(t, created, "key %d was expired, should be created", i)
			} else {
				require.False(t, created, "key %d survived cleanup, should be updated", i)
			}
		}
	})
}

func BenchmarkMapRehash(b *testing.B) {
	for _, size := range []uint32{1e6, 10e6} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			m := New(size)
			r := rand.New(rand.NewSource(1))
			for i := 0; i < int(size); i++ {
				m.Put(r.Uint64(), clock.Minutes(i%128), nil, nil, false)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m.rehash(size)
			}
		})
	}
}

func BenchmarkMapCleanup(b *testing.B) {
	now := time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC)
	const idleTimeout = 20 * time.Minute

	const size = 1e6

	maps := make([]*Map, b.N)
	for i := range maps {
		maps[i] = New(size)
	}
	r := rand.New(rand.NewSource(1))
	for _, m := range maps {
		for i := 0; i < size; i++ {
			ts := now.Add(time.Duration(-r.Float64() * float64(idleTimeout) / 3 * 4))
			m.Put(r.Uint64(), clock.ToMinutes(ts), nil, nil, false)
		}
	}
	b.ResetTimer()
	watermark := now.Add(-idleTimeout)
	for i := 0; i < b.N; i++ {
		maps[i].Cleanup(clock.ToMinutes(watermark), nil)
	}
}

func BenchmarkMapTrackCleanupGarbage(b *testing.B) {
	const series = 100e3
	r := rand.New(rand.NewSource(1))
	hashes := make([]uint64, 3*series)
	for i := 0; i < 3*series; i++ {
		hashes[i] = r.Uint64()
	}

	m := New(series)
	now := time.Now()
	for i := 0; i < 3; i++ {
		t := clock.ToMinutes(now)
		for j := 0; j < series; j++ {
			m.Put(hashes[i*series+j], t, nil, nil, false)
		}
		now = now.Add(time.Minute)
	}
	m.Cleanup(clock.ToMinutes(now.Add(-3*time.Minute)), nil)
	for i := 0; i < b.N; i++ {
		t := clock.ToMinutes(now)
		offset := i % 3
		for j := 0; j < series; j++ {
			m.Put(hashes[offset*series+j], t, nil, nil, false)
		}
		now = now.Add(time.Minute)
		m.Cleanup(clock.ToMinutes(now.Add(-3*time.Minute)), nil)
	}
	b.Logf("Rehashes: %d, rehashes per iteration %.2f", m.rehashes, float64(m.rehashes)/float64(b.N))
}
