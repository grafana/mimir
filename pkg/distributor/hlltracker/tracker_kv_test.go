// SPDX-License-Identifier: AGPL-3.0-only

package hlltracker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/distributor/hlltracker/hyperloglog"
)

// mockKVClient is a simple in-memory KV client for testing
type mockKVClient struct {
	mu    sync.RWMutex
	store map[string]interface{}

	watchMu       sync.RWMutex
	watchPrefixes map[string][]func(string, interface{}) bool
}

func newMockKVClient() *mockKVClient {
	return &mockKVClient{
		store:         make(map[string]interface{}),
		watchPrefixes: make(map[string][]func(string, interface{}) bool),
	}
}

func (m *mockKVClient) CAS(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get existing value
	existing := m.store[key]

	// Call the CAS function
	newValue, retry, err := f(existing)
	if err != nil {
		return err
	}

	if retry {
		// In a real implementation, this would retry
		// For testing, we just return
		return nil
	}

	// Store new value
	if newValue != nil {
		m.store[key] = newValue

		// Notify watchers
		m.notifyWatchers(key, newValue)
	}

	return nil
}

func (m *mockKVClient) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	// Not implemented for this test
}

func (m *mockKVClient) WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool) {
	// Register watcher
	m.watchMu.Lock()
	m.watchPrefixes[prefix] = append(m.watchPrefixes[prefix], f)
	m.watchMu.Unlock()

	// Send current values
	m.mu.RLock()
	for key, value := range m.store {
		if len(prefix) == 0 || key[:len(prefix)] == prefix {
			if !f(key, value) {
				m.mu.RUnlock()
				return
			}
		}
	}
	m.mu.RUnlock()

	// Block until context is cancelled
	<-ctx.Done()
}

func (m *mockKVClient) Get(ctx context.Context, key string) (interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.store[key], nil
}

func (m *mockKVClient) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.store, key)
	return nil
}

func (m *mockKVClient) List(ctx context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var keys []string
	for key := range m.store {
		if len(prefix) == 0 || key[:len(prefix)] == prefix {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func (m *mockKVClient) notifyWatchers(key string, value interface{}) {
	m.watchMu.RLock()
	defer m.watchMu.RUnlock()

	for prefix, watchers := range m.watchPrefixes {
		if len(prefix) == 0 || len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			for _, watcher := range watchers {
				// Call watcher in goroutine to avoid blocking
				go watcher(key, value)
			}
		}
	}
}

func TestTracker_KVPush(t *testing.T) {
	cfg := Config{
		Enabled:               true,
		MaxSeriesPerPartition: 1000000,
		TimeWindowMinutes:     20,
		UpdateIntervalSeconds: 1,
		HLLPrecision:          11,
	}

	kvClient := newMockKVClient()

	tracker, err := New(cfg, log.NewNopLogger(), prometheus.NewRegistry(), kvClient)
	require.NoError(t, err)

	require.NoError(t, tracker.StartAsync(context.Background()))
	defer func() {
		tracker.StopAsync()
		_ = tracker.AwaitTerminated(context.Background())
	}()

	const partitionID = int32(5)

	// Wait for tracker to fully start
	time.Sleep(100 * time.Millisecond)

	// Add some series
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

	// Verify partition was created
	state := tracker.GetCurrentState(partitionID)
	require.Greater(t, state.CurrentCopy.Count(), uint64(0), "partition should have data before push")

	// Manually trigger push
	ctx := context.Background()
	tracker.pushToKV(ctx)

	// Verify state was pushed to KV
	currentMinute := time.Now().Unix() / 60
	key := MakePartitionHLLKey(partitionID, currentMinute)

	value, err := kvClient.Get(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, value, "value should be pushed to KV")

	kvState, ok := value.(*PartitionHLLState)
	require.True(t, ok)
	assert.Equal(t, partitionID, kvState.PartitionID)
	assert.Equal(t, currentMinute, kvState.UnixMinute)
	assert.Greater(t, kvState.HLL.Count(), uint64(50))
}

func TestTracker_RemoteStateMerge(t *testing.T) {
	cfg := Config{
		Enabled:               true,
		MaxSeriesPerPartition: 1000000,
		TimeWindowMinutes:     20,
		UpdateIntervalSeconds: 10, // Long interval to avoid automatic pushes
		HLLPrecision:          11,
	}

	kvClient := newMockKVClient()

	tracker, err := New(cfg, log.NewNopLogger(), prometheus.NewRegistry(), kvClient)
	require.NoError(t, err)

	require.NoError(t, tracker.StartAsync(context.Background()))
	defer func() {
		tracker.StopAsync()
		_ = tracker.AwaitTerminated(context.Background())
	}()

	const partitionID = int32(10)

	// Wait for tracker to fully start
	time.Sleep(100 * time.Millisecond)

	// Add local series (0-99)
	localHLL := hyperloglog.New(11)
	for i := uint32(0); i < 100; i++ {
		localHLL.Add(i)
	}

	tracker.UpdateCurrent([]PartitionUpdate{
		{PartitionID: partitionID, UpdatedHLL: localHLL},
	})

	// Get initial count
	stateBefore := tracker.GetCurrentState(partitionID)
	countBefore := stateBefore.CurrentCopy.Count()

	// Simulate remote update with different series (100-199)
	remoteHLL := hyperloglog.New(11)
	for i := uint32(100); i < 200; i++ {
		remoteHLL.Add(i)
	}

	currentMinute := time.Now().Unix() / 60
	remoteState := &PartitionHLLState{
		PartitionID: partitionID,
		UnixMinute:  currentMinute,
		HLL:         remoteHLL,
		UpdatedAtMs: time.Now().UnixMilli(),
	}

	// Merge remote state
	tracker.mergeRemoteState(remoteState)

	// Give it a moment to process
	time.Sleep(100 * time.Millisecond)

	// Get count after merge
	stateAfter := tracker.GetCurrentState(partitionID)
	countAfter := stateAfter.CurrentCopy.Count()

	// Count should be higher after merging remote state
	// Should be approximately 200 (100 local + 100 remote)
	assert.Greater(t, countAfter, countBefore)
	assert.Greater(t, countAfter, uint64(150)) // At least 150 out of 200
}

func TestTracker_MergeHistoricalRemoteState(t *testing.T) {
	cfg := Config{
		Enabled:               true,
		MaxSeriesPerPartition: 1000000,
		TimeWindowMinutes:     20,
		UpdateIntervalSeconds: 10,
		HLLPrecision:          11,
	}

	tracker, err := New(cfg, log.NewNopLogger(), prometheus.NewRegistry(), nil)
	require.NoError(t, err)

	require.NoError(t, tracker.StartAsync(context.Background()))
	defer func() {
		tracker.StopAsync()
		_ = tracker.AwaitTerminated(context.Background())
	}()

	const partitionID = int32(15)

	// Wait for tracker to initialize its currentMinute
	time.Sleep(100 * time.Millisecond)

	// Create remote state for a past minute
	currentMinute := time.Now().Unix() / 60
	pastMinute := currentMinute - 5 // 5 minutes ago

	remoteHLL := hyperloglog.New(11)
	for i := uint32(0); i < 50; i++ {
		remoteHLL.Add(i)
	}

	remoteState := &PartitionHLLState{
		PartitionID: partitionID,
		UnixMinute:  pastMinute,
		HLL:         remoteHLL,
		UpdatedAtMs: time.Now().UnixMilli(),
	}

	t.Logf("Merging remote state: partition=%d, pastMinute=%d, currentMinute=%d", partitionID, pastMinute, currentMinute)

	// Merge remote state
	tracker.mergeRemoteState(remoteState)

	// Verify historical state was updated
	state := tracker.GetCurrentState(partitionID)

	// Historical count should reflect the merged state
	historicalCount := state.MergedHistorical.Count()
	require.Greater(t, historicalCount, uint64(0), "historical HLL should have data from past minute. Current count: %d, expected > 0", historicalCount)
}

func TestTracker_IgnoreOldRemoteState(t *testing.T) {
	cfg := Config{
		Enabled:               true,
		MaxSeriesPerPartition: 1000000,
		TimeWindowMinutes:     20,
		UpdateIntervalSeconds: 10,
		HLLPrecision:          11,
	}

	tracker, err := New(cfg, log.NewNopLogger(), prometheus.NewRegistry(), nil)
	require.NoError(t, err)

	require.NoError(t, tracker.StartAsync(context.Background()))
	defer func() {
		tracker.StopAsync()
		_ = tracker.AwaitTerminated(context.Background())
	}()

	const partitionID = int32(20)

	// Create remote state way in the past (outside time window)
	currentMinute := time.Now().Unix() / 60
	oldMinute := currentMinute - 100 // 100 minutes ago, well outside 20-minute window

	remoteHLL := hyperloglog.New(11)
	for i := uint32(0); i < 50; i++ {
		remoteHLL.Add(i)
	}

	remoteState := &PartitionHLLState{
		PartitionID: partitionID,
		UnixMinute:  oldMinute,
		HLL:         remoteHLL,
		UpdatedAtMs: time.Now().UnixMilli(),
	}

	// Merge remote state
	tracker.mergeRemoteState(remoteState)

	// State should not be affected (old data ignored)
	state := tracker.GetCurrentState(partitionID)

	// Both current and historical should be empty
	assert.Equal(t, uint64(0), state.CurrentCopy.Count())
	assert.Equal(t, uint64(0), state.MergedHistorical.Count())
}
