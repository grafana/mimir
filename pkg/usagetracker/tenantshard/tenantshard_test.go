// SPDX-License-Identifier: AGPL-3.0-only

package tenantshard

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
)

// forEachImplementation runs test against every Map implementation. Tests here cover the behaviour
// that all implementations share, so they only use the Map interface: anything that depends on how
// an implementation stores its elements, counts its rehashes or sizes itself belongs in its own
// package.
func forEachImplementation(t *testing.T, test func(t *testing.T, newMap Factory)) {
	t.Helper()
	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			newMap, err := NewFactory(version, DefaultNumShards)
			require.NoError(t, err)
			test(t, newMap)
		})
	}
}

// itemsMap collects Items() into a map.
func itemsMap(t *testing.T, m Map) map[uint64]clock.Minutes {
	t.Helper()
	got := map[uint64]clock.Minutes{}
	_, items := m.Items()
	for key, value := range items {
		got[key] = value
	}
	return got
}

func TestNewFactory(t *testing.T) {
	t.Run("default version", func(t *testing.T) {
		newMap, err := NewFactory(DefaultImplVersion, DefaultNumShards)
		require.NoError(t, err)
		require.Equal(t, DefaultNumShards, newMap.NumShards())
		require.NotNil(t, newMap.New(8))
	})

	t.Run("unsupported version", func(t *testing.T) {
		newMap, err := NewFactory(3, DefaultNumShards)
		require.EqualError(t, err, "unsupported tenant shard map implementation version 3, supported versions are 1 and 2")
		require.Zero(t, newMap.NumShards())
	})

	t.Run("invalid shard count", func(t *testing.T) {
		// Out of range, and in range but not a power of 2: the tracker store masks a series hash
		// to get its shard, so anything but a power of 2 would leave shards unreachable.
		for _, numShards := range []int{-1, 0, MaxNumShards * 2, 3, 100, 255} {
			_, err := NewFactory(DefaultImplVersion, numShards)
			require.EqualError(t, err, fmt.Sprintf("invalid number of tenant shards %d, must be a power of 2 between 1 and %d", numShards, MaxNumShards))
		}
	})

	t.Run("shard count is reported back and reaches the maps", func(t *testing.T) {
		for _, version := range []int{1, 2} {
			t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
				// The maps turn the tenant-wide limit into a per-shard one by dividing it by the
				// shard count, so a map built for more shards grows to a smaller size.
				const totalLimit = 64 * 1024

				few, err := NewFactory(version, 1)
				require.NoError(t, err)
				require.Equal(t, 1, few.NumShards())

				many, err := NewFactory(version, MaxNumShards)
				require.NoError(t, err)
				require.Equal(t, MaxNumShards, many.NumShards())

				limit := atomic.NewUint64(totalLimit)
				grow := func(m Map) int {
					// Put enough series to force at least one limit-aware rehash.
					for i := uint64(0); i < 1024; i++ {
						m.Put(i<<7, 1, atomic.NewUint64(0), limit, false)
					}
					return m.Stats().Length
				}

				require.Greater(t, grow(few.New(8)), grow(many.New(8)))
			})
		}
	})
}

func TestMap(t *testing.T) {
	forEachImplementation(t, func(t *testing.T, newMap Factory) {
		series := atomic.NewUint64(0)
		const events = 5
		const seriesPerEvent = 5
		limit := atomic.NewUint64(uint64(events * seriesPerEvent))

		// Start small, let rehashing happen.
		m := newMap.New(seriesPerEvent)

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

			require.Equal(t, expectedSeries, int(series.Load()))
			require.Equal(t, expectedSeries, m.Count())
		}
	})
}

func TestMapValues(t *testing.T) {
	forEachImplementation(t, func(t *testing.T, newMap Factory) {
		const count = 10e3
		stored := map[uint64]clock.Minutes{}
		m := newMap.New(100)
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

		require.Equal(t, stored, itemsMap(t, m))
	})
}

func TestMapCleanup(t *testing.T) {
	forEachImplementation(t, func(t *testing.T, newMap Factory) {
		t.Run("empty map", func(t *testing.T) {
			m := newMap.New(8)
			require.Equal(t, 0, m.Cleanup(100, nil))
			require.Equal(t, 0, m.Count())
			require.Zero(t, m.Stats().Resident)
		})

		t.Run("no entries expired", func(t *testing.T) {
			m := newMap.New(8)
			m.Load(1, 50)
			m.Load(2, 60)
			m.Load(3, 70)

			require.Equal(t, 0, m.Cleanup(10, nil)) // watermark=10 is before all entries
			require.Equal(t, 3, m.Count())
			require.Equal(t, uint32(3), m.Stats().Resident)
		})

		t.Run("all entries expired", func(t *testing.T) {
			m := newMap.New(8)
			m.Load(1, 10)
			m.Load(2, 20)
			m.Load(3, 30)

			require.Equal(t, 3, m.Cleanup(30, nil)) // watermark=30 expires all
			require.Equal(t, 0, m.Count())
			require.Empty(t, itemsMap(t, m))
		})

		t.Run("some entries expired some not", func(t *testing.T) {
			m := newMap.New(16)
			m.Load(100, 10)
			m.Load(200, 20)
			m.Load(300, 30)
			m.Load(400, 40)
			m.Load(500, 50)

			require.Equal(t, 3, m.Cleanup(30, nil)) // expire entries with value <= 30
			require.Equal(t, 2, m.Count())

			// Survivors should be findable.
			got := itemsMap(t, m)
			require.Equal(t, map[uint64]clock.Minutes{400: 40, 500: 50}, got)
		})

		t.Run("survivors findable via put update path", func(t *testing.T) {
			m := newMap.New(16)
			m.Load(100, 10) // will expire
			m.Load(200, 50) // will survive
			m.Load(300, 50) // will survive
			m.Load(400, 10) // will expire

			require.Equal(t, 2, m.Cleanup(20, nil))
			require.Equal(t, 2, m.Count())

			// Update survivors via Put, which should find them instead of creating new ones.
			created, _ := m.Put(200, 60, nil, nil, false)
			require.False(t, created, "should update existing entry, not create new")
			created, _ = m.Put(300, 60, nil, nil, false)
			require.False(t, created, "should update existing entry, not create new")

			require.Equal(t, 2, m.Count())
		})

		t.Run("count and items consistent after cleanup", func(t *testing.T) {
			m := newMap.New(32)
			expected := map[uint64]clock.Minutes{}
			for i := uint64(0); i < 20; i++ {
				val := clock.Minutes(10)
				if i%2 == 0 {
					val = 50 // survivors
					expected[i] = val
				}
				m.Load(i, val)
			}

			require.Equal(t, 10, m.Cleanup(20, nil)) // expire val<=20
			require.Equal(t, len(expected), m.Count())
			require.Equal(t, expected, itemsMap(t, m))
		})

		t.Run("repeated cleanup at the same watermark is a no-op", func(t *testing.T) {
			// Removals leave a mark behind that keeps probing going. A scan that mistakes one for a
			// live entry removes it again on every pass, and the counters drift.
			m := newMap.New(16)
			survivors := map[uint64]clock.Minutes{}
			// Sequential keys all probe from the same group, so groups fill up completely and the
			// removals below have to leave their marks in them.
			for i := uint64(0); i < 16; i++ {
				val := clock.Minutes(50)
				if i%2 == 0 {
					val = 10 // expires below
				} else {
					survivors[i] = val
				}
				m.Load(i, val)
			}
			require.Equal(t, 8, m.Cleanup(10, nil))

			for range 3 {
				require.Zero(t, m.Cleanup(10, nil))
				require.Equal(t, len(survivors), m.Count())
				require.Equal(t, survivors, itemsMap(t, m))
			}
		})

		t.Run("sequential cleanups with interleaved puts", func(t *testing.T) {
			m := newMap.New(32)

			// Round 1: insert keys with value 10.
			for i := uint64(0); i < 10; i++ {
				m.Load(i, 10)
			}
			require.Equal(t, 10, m.Cleanup(10, nil)) // expire all
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
			require.Equal(t, 10, m.Cleanup(20, nil)) // expire value<=20
			require.Equal(t, 10, m.Count())

			// All remaining should be keys 110-119.
			got := itemsMap(t, m)
			for i := uint64(110); i < 120; i++ {
				require.Contains(t, got, i)
				require.Equal(t, clock.Minutes(30), got[i])
			}
		})

		t.Run("large scale correctness", func(t *testing.T) {
			// Insert many elements with mixed timestamps, cleanup, verify survivors.
			const n = 10000
			m := newMap.New(uint32(n))
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

			require.Equal(t, n-len(expected), m.Cleanup(50, nil)) // expire val <= 50
			require.Equal(t, len(expected), m.Count())
			require.Equal(t, expected, itemsMap(t, m))
		})

		t.Run("put after cleanup finds correct entries", func(t *testing.T) {
			// Regression test: after cleanup, Put must still find existing keys and not create
			// duplicates.
			m := newMap.New(32)
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
	})
}
