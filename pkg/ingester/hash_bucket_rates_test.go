// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

func TestHashRangeRates_NoRangesDropsSilently(t *testing.T) {
	h := newHashRangeRates()

	h.RecordSamples(0, 100)
	h.RecordSamples(math.MaxUint32, 200)

	time.Sleep(10 * time.Millisecond)
	snap := h.Snapshot()
	assert.Empty(t, snap.Ranges)
	assert.Empty(t, snap.SamplesPerSecond)
}

func TestHashRangeRates_SetRangesAndRecord(t *testing.T) {
	h := newHashRangeRates()
	h.SetRanges([]assignment.HashRange{
		{Lo: 0, Hi: 999},
		{Lo: 1000, Hi: 1999},
		{Lo: 2000, Hi: math.MaxUint32},
	})

	h.RecordSamples(500, 100)  // -> range 0
	h.RecordSamples(1500, 50)  // -> range 1
	h.RecordSamples(3000, 200) // -> range 2

	time.Sleep(10 * time.Millisecond)
	snap := h.Snapshot()

	require.Len(t, snap.Ranges, 3)
	require.Len(t, snap.SamplesPerSecond, 3)

	assert.Greater(t, snap.SamplesPerSecond[0], 0.0)
	assert.Greater(t, snap.SamplesPerSecond[1], 0.0)
	assert.Greater(t, snap.SamplesPerSecond[2], 0.0)

	assert.Greater(t, snap.SamplesPerSecond[2], snap.SamplesPerSecond[0],
		"range 2 (200 samples) should have higher rate than range 0 (100 samples)")
}

func TestHashRangeRates_SnapshotResetsCounters(t *testing.T) {
	h := newHashRangeRates()
	h.SetRanges([]assignment.HashRange{
		{Lo: 0, Hi: math.MaxUint32},
	})

	h.RecordSamples(0, 100)
	time.Sleep(10 * time.Millisecond)

	snap1 := h.Snapshot()
	assert.Greater(t, snap1.SamplesPerSecond[0], 0.0)

	time.Sleep(10 * time.Millisecond)
	snap2 := h.Snapshot()
	assert.Equal(t, 0.0, snap2.SamplesPerSecond[0])
}

func TestHashRangeRates_SetRangesDiscardsOldCounters(t *testing.T) {
	h := newHashRangeRates()
	h.SetRanges([]assignment.HashRange{
		{Lo: 0, Hi: math.MaxUint32},
	})
	h.RecordSamples(0, 100)

	h.SetRanges([]assignment.HashRange{
		{Lo: 0, Hi: 999},
		{Lo: 1000, Hi: math.MaxUint32},
	})

	time.Sleep(10 * time.Millisecond)
	snap := h.Snapshot()
	require.Len(t, snap.SamplesPerSecond, 2)
	assert.Equal(t, 0.0, snap.SamplesPerSecond[0])
	assert.Equal(t, 0.0, snap.SamplesPerSecond[1])
}

func TestHashRangeRates_ConcurrentAccess(t *testing.T) {
	h := newHashRangeRates()
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
		snap := h.Snapshot()
		assert.Len(t, snap.Ranges, 2)
	}

	<-done
}

func TestHashRangeRates_HasRanges(t *testing.T) {
	h := newHashRangeRates()
	assert.False(t, h.HasRanges())

	h.SetRanges([]assignment.HashRange{{Lo: 0, Hi: math.MaxUint32}})
	assert.True(t, h.HasRanges())
}

func TestHashRangeRates_OutOfRangeHashIgnored(t *testing.T) {
	h := newHashRangeRates()
	h.SetRanges([]assignment.HashRange{
		{Lo: 100, Hi: 200},
	})

	h.RecordSamples(50, 100)  // below range
	h.RecordSamples(300, 100) // above range
	h.RecordSamples(150, 100) // in range

	time.Sleep(10 * time.Millisecond)
	snap := h.Snapshot()
	require.Len(t, snap.SamplesPerSecond, 1)
	assert.Greater(t, snap.SamplesPerSecond[0], 0.0)
}
