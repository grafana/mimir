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

func TestMapStats(t *testing.T) {
	series := atomic.NewUint64(0)
	limit := atomic.NewUint64(1000)

	// Start small so that inserts force a rehash.
	m := New(8)

	s := m.Stats()
	require.Equal(t, uint32(0), s.Resident)
	require.Equal(t, uint32(0), s.Spilled)
	require.Equal(t, uint32(0), s.Rehashes)
	require.Greater(t, s.Length, 0)
	// The limit is always the number of groups times the target average group load.
	require.Equal(t, uint32(s.Length)*maxAvgGroupLoad, s.Limit)

	const inserted = 50
	for i := range inserted {
		created, rejected := m.Put(uint64(i+1), clock.Minutes(1), series, limit, false)
		require.True(t, created)
		require.False(t, rejected)
	}

	s = m.Stats()
	require.Equal(t, uint32(inserted), s.Resident)
	require.Equal(t, inserted, m.Count())
	// Spilled counts groups whose last slot is occupied, which depends on the layout, so only the bound is stable.
	require.LessOrEqual(t, s.Spilled, uint32(s.Length))
	// Inserting 50 elements into a map that started with capacity for 8 must have triggered rehashes.
	require.Greater(t, s.Rehashes, uint32(0))
	require.Equal(t, uint32(s.Length)*maxAvgGroupLoad, s.Limit)
}

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
}

func TestMaxSpilledGroups(t *testing.T) {
	for _, size := range []uint32{1, 8, 100, 1000, 10000} {
		t.Run(fmt.Sprintf("size %d", size), func(t *testing.T) {
			m := New(size)
			groups := uint32(len(m.index))

			// A rehash packs entries at maxAvgGroupLoad per group, so at most limit/groupSize groups
			// can be full right afterwards. The threshold has to sit above that, otherwise a
			// compaction would immediately trigger another one.
			require.Greater(t, m.maxSpilledGroups(), m.limit/groupSize)

			// It also has to stay within reach: only a group can spill, so a threshold above the
			// number of groups would never be crossed.
			require.LessOrEqual(t, m.maxSpilledGroups(), groups)
		})
	}
}

func TestSpillTriggeredRehashCompacts(t *testing.T) {
	// A spill rehash has to re-pack at the same size, not resize: the groups that spill are dead
	// slots left behind by Cleanup, so growing would waste memory and shrinking would fight the
	// growth path. Build the state directly, because spilled only crosses the threshold after
	// accumulating over several track and cleanup rounds.
	m := New(groupSize * 2)
	groups := uint32(len(m.index))
	for g := range m.index {
		m.index[g][last] = spillmark
		m.data[g][last] = spillmark
	}
	m.spilled = groups
	require.Greater(t, m.spilled, m.maxSpilledGroups(), "the test setup must cross the spill threshold")
	require.Less(t, m.resident, m.limit, "the growth trigger must not fire instead")

	before := m.Stats()
	created, rejected := m.Put(1, 10, nil, nil, false)
	require.True(t, created)
	require.False(t, rejected)

	after := m.Stats()
	require.Equal(t, before.Length, after.Length, "a spill rehash must keep the same number of groups")
	require.Equal(t, before.Rehashes+1, after.Rehashes)
	require.Zero(t, after.Spilled, "re-packing must drop the spillmarks")
	require.Equal(t, 1, m.Count())
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
		require.Zero(t, m.spilled)
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
		require.Zero(t, m.spilled)
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

	t.Run("spillmark avoidance with empty slots in group", func(t *testing.T) {
		// With maxAvgGroupLoad=4 and groupSize=8, a small map will have groups
		// that are partially full, so cleanup should avoid spillmarks.
		m := New(4) // 1 group, limit=4
		m.Load(1, 10)
		m.Load(2, 50)

		removed := m.Cleanup(10, nil) // expire key=1
		require.Equal(t, 1, removed)
		require.Equal(t, 1, m.Count())
		require.Zero(t, m.spilled, "should not create spillmarks when group has empty slots")
		require.Equal(t, uint32(1), m.resident)
	})

	t.Run("element expiring at the beginning of a full group becomes empty again", func(t *testing.T) {
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
		// We have spilled one group.
		m.spilled++

		removed := m.Cleanup(10, nil) // expire entry at slot 0
		require.Equal(t, 1, removed)
		require.Zero(t, m.rehashes, "Should not have been rehashed")
		require.Equal(t, prefix(empty), m.index[0][0])
		require.Equal(t, xorData(empty), m.data[0][0])
	})

	t.Run("spillmark created when last group element is expired", func(t *testing.T) {
		// Force a full group by directly populating all 8 slots.
		m := New(1) // 1 group
		// Fill all groupSize slots directly.
		for j := uint32(0); j < groupSize; j++ {
			m.index[0][j] = prefix(j + prefixOffset)
			m.keys[0][j] = uint64(j + 1)
			if j == last {
				m.data[0][j] = xor(10) // will expire
			} else {
				m.data[0][j] = xor(50) // won't expire
			}
			m.resident++
		}
		// We have spilled one group.
		m.spilled++

		removed := m.Cleanup(10, nil) // expire entry at slot 0
		require.Equal(t, 1, removed)
		require.Zero(t, m.rehashes, "Should not have been rehashed")
		require.Equal(t, prefix(spillmark), m.index[0][last], "Last element in the group should be a spillmark after cleanup of a group with full last element")
		require.Equal(t, xorData(spillmark), m.data[0][last], "Last element in the group should be a spillmark after cleanup of a group with full last element")
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
		require.Zero(t, m.spilled)
		// Slot 1 should be empty now.
		require.Equal(t, prefix(empty), m.index[0][1])
		require.Equal(t, xorData(empty), m.data[0][1])
		// Slot 0 should still have its data.
		require.Equal(t, prefix(prefixOffset+10), m.index[0][0])
		require.Equal(t, uint64(100), m.keys[0][0])
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
		require.Zero(t, m.spilled)
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
		require.Zero(t, m.spilled)
		require.Equal(t, 0, m.Count())
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

	t.Run("cleanup shrinks the map when it is far above the limit", func(t *testing.T) {
		m := New(200) // 50 groups
		for i := uint64(0); i < 200; i++ {
			m.Load(i, 10)
		}
		before := m.Stats()

		require.Equal(t, 200, m.Cleanup(10, atomic.NewUint64(1000)))
		require.Zero(t, m.Count())

		after := m.Stats()
		require.Less(t, after.Length, before.Length)
		require.Equal(t, int(numGroups(1000/NumShards)), after.Length)
		require.Equal(t, before.Rehashes+1, after.Rehashes)
	})

	t.Run("cleanup keeps the size when it is within 2x of the limit", func(t *testing.T) {
		m := New(200) // 50 groups
		for i := uint64(0); i < 200; i++ {
			m.Load(i, 10)
		}
		before := m.Stats()

		// 2400/NumShards = 150 series per shard, i.e. 38 groups, and 50 groups is less than 2x that.
		require.Equal(t, 200, m.Cleanup(10, atomic.NewUint64(2400)))
		require.Zero(t, m.Count())

		after := m.Stats()
		require.Equal(t, before.Length, after.Length)
		require.Equal(t, before.Rehashes, after.Rehashes)
	})

	t.Run("cleanup never shrinks below the live entries", func(t *testing.T) {
		// Load() ignores limits, so a shard can hold more than its share of the series: a limit that
		// drops must not shrink the map below what is still resident.
		m := New(2000) // 500 groups
		for i := uint64(0); i < 200; i++ {
			m.Load(i, 50) // survives the watermark below
		}
		before := m.Stats()

		require.Zero(t, m.Cleanup(10, atomic.NewUint64(NumShards))) // 1 series per shard
		require.Equal(t, 200, m.Count())

		after := m.Stats()
		require.Less(t, after.Length, before.Length)
		require.GreaterOrEqual(t, after.Limit, uint32(200))

		got := itemsMap(t, m)
		require.Len(t, got, 200)
		for i := uint64(0); i < 200; i++ {
			require.Equal(t, clock.Minutes(50), got[i])
		}
	})

	// fullGroupWithExpiredLastSlot fills the first group completely with sequential keys, all of
	// which probe from group 0, and then expires only the entry in the last slot. The spillmark it
	// leaves behind then sits next to an occupied slot, which is the layout that actually
	// distinguishes a correct scan from a broken one: a spillmark that follows an empty slot is
	// reported as empty by the byte tricks anyway, so it hides the difference.
	fullGroupWithExpiredLastSlot := func(t *testing.T) (*Map, map[uint64]clock.Minutes) {
		t.Helper()
		m := New(groupSize * 2)
		survivors := map[uint64]clock.Minutes{}
		for i := uint64(0); i < groupSize; i++ {
			val := clock.Minutes(50)
			if i == groupSize-1 {
				val = 10 // expires below, and lands in the last slot of the group
			} else {
				survivors[i] = val
			}
			m.Load(i, val)
		}
		require.Equal(t, uint32(1), m.spilled, "the first group must be full")

		require.Equal(t, 1, m.Cleanup(10, nil))
		require.Equal(t, prefix(spillmark), m.index[0][last])
		require.NotEqual(t, prefix(empty), m.index[0][last-1], "the slot before the spillmark must stay occupied")
		return m, survivors
	}

	t.Run("repeated cleanup at the same watermark is a no-op", func(t *testing.T) {
		// A spillmark decodes to a clock value that reads as long expired, so a scan that mistakes
		// one for a live entry removes it again on every pass, underflowing the resident counter.
		m, survivors := fullGroupWithExpiredLastSlot(t)
		count, resident, spilled := m.Count(), m.resident, m.spilled

		for range 3 {
			require.Zero(t, m.Cleanup(10, nil))
			require.Equal(t, count, m.Count())
			require.Equal(t, resident, m.resident)
			require.Equal(t, spilled, m.spilled)
			require.Equal(t, survivors, itemsMap(t, m))
		}
	})

	t.Run("rehash after cleanup does not resurrect expired entries", func(t *testing.T) {
		// Cleanup leaves the keys of removed entries in place on purpose, because writing them
		// costs a cache line per removal. A rehash must skip those slots instead of reloading them.
		m, survivors := fullGroupWithExpiredLastSlot(t)

		m.rehash(uint32(len(m.index)))

		require.Equal(t, len(survivors), m.Count())
		require.Equal(t, survivors, itemsMap(t, m))
	})

	t.Run("spilled accounting", func(t *testing.T) {
		m := New(groupSize * 2)
		groups := uint32(len(m.index))

		// Nothing is full yet, so nothing has spilled.
		require.Zero(t, m.Stats().Spilled)

		// Sequential keys all probe from group 0, so groups fill up one after another.
		for g := uint32(0); g < groups; g++ {
			for j := uint32(0); j < groupSize; j++ {
				m.Load(uint64(g*groupSize+j), 10)
			}
			require.Equal(t, g+1, m.Stats().Spilled, "every full group spills into the next one")
		}

		// Cleanup replaces the last slot of a full group with a spillmark, which keeps the group
		// spilling: probing must still walk past it.
		require.Equal(t, int(groups*groupSize), m.Cleanup(10, nil))
		require.Equal(t, groups, m.Stats().Spilled)

		// Writing over a spillmark does not add a new spill either.
		for g := uint32(0); g < groups; g++ {
			for j := uint32(0); j < groupSize; j++ {
				m.Load(uint64(g*groupSize+j), 20)
			}
		}
		require.Equal(t, groups, m.Stats().Spilled)
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

func TestIndexMatchEmptyOrSpillmark(t *testing.T) {
	t.Run("all empty", func(t *testing.T) {
		var idx index
		set := idx.matchEmptyOrSpillmark()
		for i := 0; i < groupSize; i++ {
			require.Equal(t, uint32(i), nextMatch(&set))
		}
	})
	t.Run("first busy", func(t *testing.T) {
		var idx index
		idx[0] = prefix(42)
		busy := map[uint32]bool{0: true}
		set := idx.matchEmptyOrSpillmark()
		for i := uint32(0); i < groupSize; i++ {
			if busy[i] {
				continue
			}
			require.Equal(t, i, nextMatch(&set))
		}
	})
	t.Run("alternative", func(t *testing.T) {
		var idx index
		idx[0] = prefix(42)
		idx[2] = prefix(43)
		idx[4] = prefix(44)
		idx[5] = prefix(44)
		busy := map[uint32]bool{0: true, 2: true, 4: true, 5: true}
		set := idx.matchEmptyOrSpillmark()
		for i := uint32(0); i < groupSize; i++ {
			if busy[i] {
				continue
			}
			require.Equal(t, i, nextMatch(&set))
		}
	})
	t.Run("full", func(t *testing.T) {
		var idx index
		for i := 0; i < groupSize; i++ {
			idx[i] = prefix(2 + i)
		}
		set := idx.matchEmptyOrSpillmark()
		require.Zero(t, set)
	})
	t.Run("last is a spillmark", func(t *testing.T) {
		var idx index
		for i := 0; i < groupSize; i++ {
			idx[i] = prefix(2 + i)
		}
		idx[last] = spillmark
		set := idx.matchEmptyOrSpillmark()
		require.NotZero(t, set)
		require.Equal(t, uint32(last), nextMatch(&set))
	})
}

func TestMatchOccupied(t *testing.T) {
	t.Run("all empty", func(t *testing.T) {
		var idx index
		set := idx.matchOccupied()
		require.Zero(t, set)
	})
	t.Run("first busy", func(t *testing.T) {
		var idx index
		idx[0] = prefix(42)
		set := idx.matchOccupied()
		require.NotZero(t, set)
		require.Equal(t, uint32(0), nextMatch(&set))
		require.Zero(t, set)
	})
	t.Run("alternative", func(t *testing.T) {
		var idx index
		idx[0] = prefix(42)
		idx[2] = prefix(43)
		idx[4] = prefix(44)
		idx[5] = prefix(44)

		set := idx.matchOccupied()
		require.NotZero(t, set)
		require.Equal(t, uint32(0), nextMatch(&set))
		require.Equal(t, uint32(2), nextMatch(&set))
		require.Equal(t, uint32(4), nextMatch(&set))
		require.Equal(t, uint32(5), nextMatch(&set))
		require.Zero(t, set)
	})
	t.Run("full", func(t *testing.T) {
		var idx index
		for i := 0; i < groupSize; i++ {
			idx[i] = prefix(2 + i)
		}
		set := idx.matchOccupied()
		for i := 0; i < groupSize; i++ {
			require.NotZero(t, set)
			require.Equal(t, uint32(i), nextMatch(&set))
		}
		require.Zero(t, set)
	})
	t.Run("last is a spillmark", func(t *testing.T) {
		var idx index
		for i := 0; i < groupSize; i++ {
			idx[i] = prefix(2 + i)
		}
		idx[last] = spillmark
		set := idx.matchOccupied()
		for i := 0; i < last; i++ {
			require.NotZero(t, set)
			require.Equal(t, uint32(i), nextMatch(&set))
		}
		require.Zero(t, set)
	})
}
