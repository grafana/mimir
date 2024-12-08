// SPDX-License-Identifier: AGPL-3.0-only

package tenantshard

import (
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
	m := New(seriesPerEvent, 0)

	storedValues := map[uint64]clock.Minutes{}
	for i := 1; i <= events; i++ {
		refs := make([]uint64, seriesPerEvent)
		for j := range refs {
			refs[j] = uint64((i*100 + j) << valueBits)
			storedValues[refs[j]] = clock.Minutes(i)
		}

		resp := make(chan TrackSeriesResponse)
		m.Events() <- Event{
			Type:                TrackSeries,
			Refs:                refs,
			Value:               clock.Minutes(i),
			Limit:               limit,
			Series:              series,
			TrackSeriesResponse: resp,
		}
		response := <-resp

		require.Len(t, response.Created, seriesPerEvent, "iteration %d", i)
		require.Empty(t, response.Rejected, "iteration %d", i)
	}

	require.Equal(t, events*seriesPerEvent, m.count())
	require.Equal(t, uint64(events*seriesPerEvent), series.Load())

	{
		// No more series will fit.
		resp := make(chan TrackSeriesResponse)
		ref := uint64(65535) << valueBits
		m.Events() <- Event{
			Type:                TrackSeries,
			Refs:                []uint64{ref},
			Value:               clock.Minutes(0),
			Limit:               limit,
			Series:              series,
			TrackSeriesResponse: resp,
		}
		response := <-resp
		require.Empty(t, response.Created)
		require.Equal(t, []uint64{ref}, response.Rejected)
	}

	{
		gotValues := map[uint64]clock.Minutes{}
		resp := make(chan func(LengthCallback, IteratorCallback))
		m.Events() <- Event{
			Type:   Clone,
			Cloner: resp,
		}
		iterator := <-resp
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
		m.Events() <- Event{
			Type:      Cleanup,
			Watermark: clock.Minutes(1),
			Series:    series,
			Done:      resp,
		}
		<-resp

		expectedSeries := (events - 1) * seriesPerEvent

		// It's unsafe to check m.count() after Cleanup event.
		require.Equal(t, expectedSeries, int(series.Load()))
	}

}
