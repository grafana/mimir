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
		if val == 0xff {
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
