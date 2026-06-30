// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/compartments"
)

const (
	testRingName = "ingester-partitions"
	testRingKey  = "ingester-partitions"
)

func TestPartitionRingWatchers_Disabled(t *testing.T) {
	kvClient, closer := consul.NewInMemoryClient(ring.GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { _ = closer.Close() })

	// Under the legacy key, exactly as today.
	writePartitions(t, kvClient, testRingKey, 0, 1, 2)

	w, err := NewPartitionRingWatchers(false, 0, testRingName, testRingKey, kvClient, log.NewNopLogger(), nil)
	require.NoError(t, err)
	startPartitionRingWatchers(t, w)

	assert.Equal(t, 1, w.Count())
	assert.Equal(t, 3, w.PartitionRing(0).PartitionsCount())
	assert.Same(t, w.Watcher(0), w.All()[0])
}

func TestPartitionRingWatchers_Enabled(t *testing.T) {
	kvClient, closer := consul.NewInMemoryClient(ring.GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { _ = closer.Close() })

	// Each read compartment has its own key with a distinct number of partitions.
	writePartitions(t, kvClient, compartments.WithReadCompartmentSuffix(testRingKey, 0), 0)
	writePartitions(t, kvClient, compartments.WithReadCompartmentSuffix(testRingKey, 1), 0, 1)
	writePartitions(t, kvClient, compartments.WithReadCompartmentSuffix(testRingKey, 2), 0, 1, 2)

	w, err := NewPartitionRingWatchers(true, 3, testRingName, testRingKey, kvClient, log.NewNopLogger(), nil)
	require.NoError(t, err)
	startPartitionRingWatchers(t, w)

	require.Equal(t, 3, w.Count())
	assert.Len(t, w.All(), 3)

	// Each accessor returns the ring keyed for its own read compartment.
	assert.Equal(t, 1, w.PartitionRing(0).PartitionsCount())
	assert.Equal(t, 2, w.PartitionRing(1).PartitionsCount())
	assert.Equal(t, 3, w.PartitionRing(2).PartitionsCount())

	assert.Same(t, w.Watcher(1).PartitionRing(), w.PartitionRing(1))
}

func TestPartitionRingWatchers_DoesNotPanicWithRealRegistererAndMultipleCompartments(t *testing.T) {
	kvClient, closer := consul.NewInMemoryClient(ring.GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { _ = closer.Close() })

	// A real registerer would panic on duplicate metric registration if the per-compartment ring
	// metrics weren't disambiguated.
	var w *PartitionRingWatchers
	require.NotPanics(t, func() {
		var err error
		w, err = NewPartitionRingWatchers(true, 4, testRingName, testRingKey, kvClient, log.NewNopLogger(), prometheus.NewPedanticRegistry())
		require.NoError(t, err)
		require.Equal(t, 4, w.Count())
	})

	// Start and stop so the watcher subservices don't leak goroutines.
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), w))
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), w))
}

func TestPartitionRingWatchers_ReturnsErrorWhenEnabledWithNoCompartments(t *testing.T) {
	kvClient, closer := consul.NewInMemoryClient(ring.GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { _ = closer.Close() })

	for _, numCompartments := range []int{0, -1} {
		w, err := NewPartitionRingWatchers(true, numCompartments, testRingName, testRingKey, kvClient, log.NewNopLogger(), nil)
		assert.Error(t, err)
		assert.Nil(t, w)
	}
}

// writePartitions stores a partition ring desc with the given partition IDs under the given key.
func writePartitions(t *testing.T, kvClient kv.Client, key string, partitionIDs ...int32) {
	t.Helper()

	require.NoError(t, kvClient.CAS(context.Background(), key, func(interface{}) (interface{}, bool, error) {
		desc := ring.NewPartitionRingDesc()
		for _, id := range partitionIDs {
			desc.AddPartition(id, ring.PartitionActive, time.Now())
		}
		return desc, true, nil
	}))
}

func startPartitionRingWatchers(t *testing.T, w *PartitionRingWatchers) {
	t.Helper()

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), w))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), w))
	})

	// Give the watchers a moment to load the initial ring state from the KV store.
	require.Eventually(t, func() bool {
		return w.PartitionRing(0).PartitionsCount() > 0
	}, time.Second, 10*time.Millisecond)
}
