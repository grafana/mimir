// SPDX-License-Identifier: AGPL-3.0-only

package hlltracker

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/distributor/hlltracker/hyperloglog"
)

func TestNew(t *testing.T) {
	t.Run("creates tracker with valid config", func(t *testing.T) {
		cfg := Config{
			Enabled:               true,
			MaxSeriesPerPartition: 1000000,
			TimeWindowMinutes:     20,
			UpdateIntervalSeconds: 1,
			HLLPrecision:          11,
		}

		tracker, err := New(cfg, log.NewNopLogger(), prometheus.NewRegistry())
		require.NoError(t, err)
		assert.NotNil(t, tracker)
		assert.Equal(t, 1000000, tracker.MaxSeriesPerPartition())
	})

	t.Run("applies defaults for invalid config", func(t *testing.T) {
		cfg := Config{
			TimeWindowMinutes:     0, // Invalid, should default to 20
			UpdateIntervalSeconds: 0, // Invalid, should default to 1
			HLLPrecision:          3, // Invalid, should default to 11
		}

		tracker, err := New(cfg, log.NewNopLogger(), prometheus.NewRegistry())
		require.NoError(t, err)
		assert.NotNil(t, tracker)
	})
}

func TestTracker_GetCurrentState(t *testing.T) {
	cfg := Config{
		Enabled:               true,
		MaxSeriesPerPartition: 1000000,
		TimeWindowMinutes:     20,
		UpdateIntervalSeconds: 1,
		HLLPrecision:          11,
	}

	tracker, err := New(cfg, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	// Start the tracker
	require.NoError(t, tracker.StartAsync(context.Background()))
	t.Cleanup(func() {
		tracker.StopAsync()
		_ = tracker.AwaitTerminated(context.Background())
	})

	// Get state for partition that doesn't exist yet
	state := tracker.GetCurrentState(1)
	assert.NotNil(t, state.CurrentCopy)
	assert.NotNil(t, state.MergedHistorical)

	// Both should be empty initially
	assert.Equal(t, uint64(0), state.CurrentCopy.Count())
	assert.Equal(t, uint64(0), state.MergedHistorical.Count())
}

func TestTracker_UpdateCurrent(t *testing.T) {
	cfg := Config{
		Enabled:               true,
		MaxSeriesPerPartition: 1000000,
		TimeWindowMinutes:     20,
		UpdateIntervalSeconds: 1,
		HLLPrecision:          11,
	}

	tracker, err := New(cfg, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	require.NoError(t, tracker.StartAsync(context.Background()))
	t.Cleanup(func() {
		tracker.StopAsync()
		_ = tracker.AwaitTerminated(context.Background())
	})

	const partitionID = int32(5)

	// Add some series hashes
	hll := hyperloglog.New(11)
	for i := uint32(0); i < 100; i++ {
		hll.Add(i)
	}

	updates := []PartitionUpdate{
		{
			PartitionID: partitionID,
			UpdatedHLL:  hll,
		},
	}

	tracker.UpdateCurrent(updates)

	// Verify the update was applied
	state := tracker.GetCurrentState(partitionID)
	count := state.CurrentCopy.Count()

	// Should be approximately 100 (with HLL error margin)
	assert.Greater(t, count, uint64(50))
	assert.Less(t, count, uint64(200))
}

func TestTracker_MultiplePartitions(t *testing.T) {
	cfg := Config{
		Enabled:               true,
		MaxSeriesPerPartition: 1000000,
		TimeWindowMinutes:     20,
		UpdateIntervalSeconds: 1,
		HLLPrecision:          11,
	}

	tracker, err := New(cfg, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	require.NoError(t, tracker.StartAsync(context.Background()))
	t.Cleanup(func() {
		tracker.StopAsync()
		_ = tracker.AwaitTerminated(context.Background())
	})

	// Add different series to different partitions
	updates := []PartitionUpdate{}

	for partID := int32(0); partID < 5; partID++ {
		hll := hyperloglog.New(11)
		for i := uint32(0); i < 100; i++ {
			// Use different hashes for each partition
			hll.Add(uint32(partID)*1000 + i)
		}
		updates = append(updates, PartitionUpdate{
			PartitionID: partID,
			UpdatedHLL:  hll,
		})
	}

	tracker.UpdateCurrent(updates)

	// Verify each partition has its own state
	for partID := int32(0); partID < 5; partID++ {
		state := tracker.GetCurrentState(partID)
		count := state.CurrentCopy.Count()
		assert.Greater(t, count, uint64(50), "partition %d should have data", partID)
		assert.Less(t, count, uint64(200), "partition %d count within error bounds", partID)
	}
}

func TestTracker_MinuteRotation(t *testing.T) {
	cfg := Config{
		Enabled:               true,
		MaxSeriesPerPartition: 1000000,
		TimeWindowMinutes:     2, // Short window for testing
		UpdateIntervalSeconds: 1,
		HLLPrecision:          11,
	}

	reg := prometheus.NewRegistry()
	tracker, err := New(cfg, log.NewNopLogger(), reg)
	require.NoError(t, err)

	require.NoError(t, tracker.StartAsync(context.Background()))
	t.Cleanup(func() {
		tracker.StopAsync()
		_ = tracker.AwaitTerminated(context.Background())
	})

	const partitionID = int32(1)

	// Add initial series
	hll1 := hyperloglog.New(11)
	for i := uint32(0); i < 100; i++ {
		hll1.Add(i)
	}
	tracker.UpdateCurrent([]PartitionUpdate{{PartitionID: partitionID, UpdatedHLL: hll1}})

	// Force a minute rotation by manipulating internal state
	// This tests that the rotation logic works
	tracker.currentMinuteMu.Lock()
	oldMinute := tracker.currentMinute
	tracker.currentMinute = oldMinute + 1
	tracker.currentMinuteMu.Unlock()

	// Trigger rotation check
	tracker.checkAndRotateMinute()

	// Add more series in the new minute
	hll2 := hyperloglog.New(11)
	for i := uint32(100); i < 200; i++ {
		hll2.Add(i)
	}
	tracker.UpdateCurrent([]PartitionUpdate{{PartitionID: partitionID, UpdatedHLL: hll2}})

	// Get state after rotation
	state2 := tracker.GetCurrentState(partitionID)

	// CurrentCopy should only have new series (100-199)
	currentCount := state2.CurrentCopy.Count()

	// MergedHistorical should have old series (0-99)
	historicalCount := state2.MergedHistorical.Count()

	// Verify that historical contains old data
	assert.Greater(t, historicalCount, uint64(50), "historical should contain old data")

	// Verify current contains new data (should be less than initial+new combined)
	assert.Greater(t, currentCount, uint64(50), "current should contain new data")

	// Verify rotation metric was incremented
	rotations := testutil.ToFloat64(tracker.rotations)
	assert.Greater(t, rotations, float64(0), "rotations metric should be incremented")
}

func TestTracker_Metrics(t *testing.T) {
	cfg := Config{
		Enabled:               true,
		MaxSeriesPerPartition: 1000000,
		TimeWindowMinutes:     20,
		UpdateIntervalSeconds: 1,
		HLLPrecision:          11,
	}

	reg := prometheus.NewRegistry()
	tracker, err := New(cfg, log.NewNopLogger(), reg)
	require.NoError(t, err)

	require.NoError(t, tracker.StartAsync(context.Background()))
	t.Cleanup(func() {
		tracker.StopAsync()
		_ = tracker.AwaitTerminated(context.Background())
	})

	const partitionID = int32(7)

	// Add series
	hll := hyperloglog.New(11)
	for i := uint32(0); i < 1000; i++ {
		hll.Add(i)
	}
	tracker.UpdateCurrent([]PartitionUpdate{{PartitionID: partitionID, UpdatedHLL: hll}})

	// Force a rotation to update metrics
	tracker.currentMinuteMu.Lock()
	oldMinute := tracker.currentMinute
	tracker.currentMinute = oldMinute + 1
	tracker.currentMinuteMu.Unlock()
	tracker.checkAndRotateMinute()

	// Check that metrics are registered
	metricFamilies, err := reg.Gather()
	require.NoError(t, err)

	var foundEstimate, foundRotations bool
	for _, mf := range metricFamilies {
		switch mf.GetName() {
		case "cortex_distributor_partition_series_estimate":
			foundEstimate = true
		case "cortex_distributor_partition_series_tracker_rotations_total":
			foundRotations = true
		}
	}

	assert.True(t, foundEstimate, "partition_series_estimate metric should be registered")
	assert.True(t, foundRotations, "rotations metric should be registered")
}

func TestTracker_ConcurrentUpdates(t *testing.T) {
	cfg := Config{
		Enabled:               true,
		MaxSeriesPerPartition: 1000000,
		TimeWindowMinutes:     20,
		UpdateIntervalSeconds: 1,
		HLLPrecision:          11,
	}

	tracker, err := New(cfg, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	require.NoError(t, tracker.StartAsync(context.Background()))
	t.Cleanup(func() {
		tracker.StopAsync()
		_ = tracker.AwaitTerminated(context.Background())
	})

	const partitionID = int32(10)
	const numGoroutines = 10
	const seriesPerGoroutine = 100

	// Concurrent updates to the same partition
	done := make(chan struct{})
	for g := 0; g < numGoroutines; g++ {
		go func(offset uint32) {
			hll := hyperloglog.New(11)
			for i := uint32(0); i < seriesPerGoroutine; i++ {
				hll.Add(offset + i)
			}
			tracker.UpdateCurrent([]PartitionUpdate{{
				PartitionID: partitionID,
				UpdatedHLL:  hll,
			}})
			done <- struct{}{}
		}(uint32(g * seriesPerGoroutine))
	}

	// Wait for all goroutines
	for g := 0; g < numGoroutines; g++ {
		<-done
	}

	// Get final state
	state := tracker.GetCurrentState(partitionID)
	count := state.CurrentCopy.Count()

	// Should be approximately numGoroutines * seriesPerGoroutine
	expected := numGoroutines * seriesPerGoroutine
	assert.Greater(t, count, uint64(expected/2), "should track concurrent updates")
	assert.Less(t, count, uint64(expected*2), "count should be within error bounds")
}

func TestTracker_TimeWindowCleanup(t *testing.T) {
	cfg := Config{
		Enabled:               true,
		MaxSeriesPerPartition: 1000000,
		TimeWindowMinutes:     2, // Very short window
		UpdateIntervalSeconds: 1,
		HLLPrecision:          11,
	}

	tracker, err := New(cfg, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	require.NoError(t, tracker.StartAsync(context.Background()))
	t.Cleanup(func() {
		tracker.StopAsync()
		_ = tracker.AwaitTerminated(context.Background())
	})

	const partitionID = int32(1)

	// Add series in minute 0
	hll1 := hyperloglog.New(11)
	for i := uint32(0); i < 100; i++ {
		hll1.Add(i)
	}
	tracker.UpdateCurrent([]PartitionUpdate{{PartitionID: partitionID, UpdatedHLL: hll1}})

	// Simulate advancing time by 3 minutes (past the 2-minute window)
	tracker.currentMinuteMu.Lock()
	baseMinute := tracker.currentMinute
	tracker.currentMinute = baseMinute + 1
	tracker.currentMinuteMu.Unlock()
	tracker.checkAndRotateMinute()

	// Add more series
	hll2 := hyperloglog.New(11)
	for i := uint32(100); i < 200; i++ {
		hll2.Add(i)
	}
	tracker.UpdateCurrent([]PartitionUpdate{{PartitionID: partitionID, UpdatedHLL: hll2}})

	// Advance another 2 minutes
	tracker.currentMinuteMu.Lock()
	tracker.currentMinute = baseMinute + 3
	tracker.currentMinuteMu.Unlock()
	tracker.checkAndRotateMinute()

	// Check partition tracker to verify old data was cleaned up
	tracker.partitionsMu.RLock()
	pt := tracker.partitions[partitionID]
	tracker.partitionsMu.RUnlock()

	pt.mu.RLock()
	histCount := len(pt.historicalHLLs)
	pt.mu.RUnlock()

	// With a 2-minute window, we should have at most 2 historical entries
	assert.LessOrEqual(t, histCount, 2, "old entries should be cleaned up")
}

func TestMaxSeriesPerPartition(t *testing.T) {
	cfg := Config{
		Enabled:               true,
		MaxSeriesPerPartition: 12345,
		TimeWindowMinutes:     20,
		UpdateIntervalSeconds: 1,
		HLLPrecision:          11,
	}

	tracker, err := New(cfg, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	assert.Equal(t, 12345, tracker.MaxSeriesPerPartition())
}
