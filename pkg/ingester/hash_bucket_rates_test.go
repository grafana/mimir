// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"math"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

func TestHashRangeRates_NoRangesDropsSilently(t *testing.T) {
	h := newHashRangeRates(time.Second)

	h.RecordSamples(0, 100)
	h.RecordSamples(math.MaxUint32, 200)
	h.Tick()

	snap := h.Snapshot()
	assert.Empty(t, snap.Ranges)
	assert.Empty(t, snap.SamplesPerSecond)
}

func TestHashRangeRates_SetRangesAndRecord(t *testing.T) {
	h := newHashRangeRates(time.Second)
	h.SetRanges([]assignment.HashRange{
		{Lo: 0, Hi: 999},
		{Lo: 1000, Hi: 1999},
		{Lo: 2000, Hi: math.MaxUint32},
	})

	h.RecordSamples(500, 100)  // -> range 0
	h.RecordSamples(1500, 50)  // -> range 1
	h.RecordSamples(3000, 200) // -> range 2
	h.Tick()

	snap := h.Snapshot()

	require.Len(t, snap.Ranges, 3)
	require.Len(t, snap.SamplesPerSecond, 3)

	assert.Greater(t, snap.SamplesPerSecond[0], 0.0)
	assert.Greater(t, snap.SamplesPerSecond[1], 0.0)
	assert.Greater(t, snap.SamplesPerSecond[2], 0.0)

	assert.Greater(t, snap.SamplesPerSecond[2], snap.SamplesPerSecond[0],
		"range 2 (200 samples) should have higher rate than range 0 (100 samples)")
}

func TestHashRangeRates_EWMADecays(t *testing.T) {
	h := newHashRangeRates(time.Second)
	h.SetRanges([]assignment.HashRange{
		{Lo: 0, Hi: math.MaxUint32},
	})

	h.RecordSamples(0, 1000)
	h.Tick()

	rate1 := h.Snapshot().SamplesPerSecond[0]
	assert.Greater(t, rate1, 0.0)

	// Tick without new events — EWMA should decay.
	for i := 0; i < 20; i++ {
		h.Tick()
	}

	rate2 := h.Snapshot().SamplesPerSecond[0]
	assert.Less(t, rate2, rate1, "rate should decay after ticks with no new events")
}

func TestHashRangeRates_SnapshotDoesNotResetState(t *testing.T) {
	h := newHashRangeRates(time.Second)
	h.SetRanges([]assignment.HashRange{
		{Lo: 0, Hi: math.MaxUint32},
	})

	h.RecordSamples(0, 100)
	h.Tick()

	snap1 := h.Snapshot()
	assert.Greater(t, snap1.SamplesPerSecond[0], 0.0)

	// Second snapshot without tick should return the same rate.
	snap2 := h.Snapshot()
	assert.Equal(t, snap1.SamplesPerSecond[0], snap2.SamplesPerSecond[0])
}

func TestHashRangeRates_SetRangesPreservesEWMA(t *testing.T) {
	h := newHashRangeRates(time.Second)
	h.SetRanges([]assignment.HashRange{
		{Lo: 0, Hi: 999},
		{Lo: 1000, Hi: math.MaxUint32},
	})

	h.RecordSamples(500, 100)
	h.Tick()

	rateBefore := h.Snapshot().SamplesPerSecond[0]
	require.Greater(t, rateBefore, 0.0)

	// Set ranges again, keeping [0, 999] intact but adding a new split.
	h.SetRanges([]assignment.HashRange{
		{Lo: 0, Hi: 999},
		{Lo: 1000, Hi: 1999},
		{Lo: 2000, Hi: math.MaxUint32},
	})

	snap := h.Snapshot()
	require.Len(t, snap.SamplesPerSecond, 3)
	assert.Equal(t, rateBefore, snap.SamplesPerSecond[0],
		"EWMA for unchanged range should be preserved")
	assert.Equal(t, 0.0, snap.SamplesPerSecond[1],
		"new range should start at zero")
}

func TestHashRangeRates_ConcurrentAccess(t *testing.T) {
	h := newHashRangeRates(time.Second)
	h.SetRanges([]assignment.HashRange{
		{Lo: 0, Hi: math.MaxUint32 / 2},
		{Lo: math.MaxUint32/2 + 1, Hi: math.MaxUint32},
	})

	done := make(chan struct{})
	go func() {
		for i := 0; i < 10000; i++ {
			h.RecordSamples(uint32(i*1000), 1)
		}
		close(done)
	}()

	for i := 0; i < 10; i++ {
		h.Tick()
		snap := h.Snapshot()
		assert.Len(t, snap.Ranges, 2)
	}

	<-done
}

func TestHashRangeRates_HasRanges(t *testing.T) {
	h := newHashRangeRates(time.Second)
	assert.False(t, h.HasRanges())

	h.SetRanges([]assignment.HashRange{{Lo: 0, Hi: math.MaxUint32}})
	assert.True(t, h.HasRanges())
}

func TestHashRangeRates_OutOfRangeHashIgnored(t *testing.T) {
	h := newHashRangeRates(time.Second)
	h.SetRanges([]assignment.HashRange{
		{Lo: 100, Hi: 200},
	})

	h.RecordSamples(50, 100)  // below range
	h.RecordSamples(300, 100) // above range
	h.RecordSamples(150, 100) // in range
	h.Tick()

	snap := h.Snapshot()
	require.Len(t, snap.SamplesPerSecond, 1)
	assert.Greater(t, snap.SamplesPerSecond[0], 0.0)
}

func TestHashRangeRates_LogSummary(t *testing.T) {
	h := newHashRangeRates(time.Second)
	h.SetRanges([]assignment.HashRange{
		{Lo: 0, Hi: math.MaxUint32 / 2},
		{Lo: math.MaxUint32/2 + 1, Hi: math.MaxUint32},
	})

	h.RecordSamples(100, 500)
	h.Tick()

	// Should not panic with a real logger.
	h.LogSummary(log.NewNopLogger())
}
