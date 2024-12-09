// SPDX-License-Identifier: AGPL-3.0-only

package tenantshard

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
)

func TestMap(t *testing.T) {
	series := atomic.NewUint64(0)
	const events = 5
	const seriesPerEvent = 5
	limit := uint64(events * seriesPerEvent)

	// Start small, let rehashing happen.
	m := New(seriesPerEvent)

	storedValues := map[uint64]clock.Minutes{}
	for i := 1; i <= events; i++ {
		refs := make([]uint64, seriesPerEvent)
		for j := range refs {
			refs[j] = uint64((i*100 + j) << prefixLen)
			storedValues[refs[j]] = clock.Minutes(i)
		}

		resp := make(chan TrackResponse)
		m.Events() <- Track(
			refs,
			clock.Minutes(i),
			series,
			limit,
			resp,
		)
		response := <-resp

		require.Len(t, response.Created, seriesPerEvent, "iteration %d", i)
		require.Empty(t, response.Rejected, "iteration %d", i)
	}

	require.Equal(t, events*seriesPerEvent, m.count())
	require.Equal(t, uint64(events*seriesPerEvent), series.Load())

	{
		// No more series will fit.
		resp := make(chan TrackResponse)
		ref := uint64(65535) << prefixLen
		m.Events() <- Track(
			[]uint64{ref},
			clock.Minutes(0),
			series,
			limit,
			resp,
		)
		response := <-resp
		require.Empty(t, response.Created)
		require.Equal(t, []uint64{ref}, response.Rejected)
	}

	{
		gotValues := map[uint64]clock.Minutes{}
		cloner := make(chan func(LengthCallback, IteratorCallback))
		m.Events() <- Clone(cloner)
		iterator := <-cloner
		count := 0
		iterator(
			func(c int) {
				count = c
			},
			func(key uint64, value clock.Minutes) {
				gotValues[key] = value
			},
		)
		require.Equal(t, len(storedValues), count)
		require.Equal(t, storedValues, gotValues)
	}

	{
		// Cleanup first wave of series
		resp := make(chan struct{})
		m.Events() <- Cleanup(
			clock.Minutes(1),
			series,
			resp,
		)
		<-resp

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
		val := clock.Minutes(i) % 128 // make sure we don't try to store 0xff
		stored[key] = val
		m.put(key, val, total, 0, false)
	}
	require.Equal(t, len(stored), m.count())
	require.Equal(t, len(stored), int(total.Load()))

	got := map[uint64]clock.Minutes{}
	m.cloner()(
		func(c int) { require.Equal(t, len(stored), c) },
		func(key uint64, value clock.Minutes) { got[key] = value },
	)
	require.Equal(t, stored, got)
}
