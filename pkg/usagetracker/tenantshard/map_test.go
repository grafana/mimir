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
		removed := m.Cleanup(clock.Minutes(1))
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
		maps[i].Cleanup(clock.ToMinutes(watermark))
	}
}
