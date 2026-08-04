// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
	"github.com/grafana/mimir/pkg/util/validation"
)

// startReadcacheForAssignment builds and starts a Readcache with the
// given instance ID against a 4-partition fake Kafka cluster, with no
// static partitions so ownership comes entirely from applyAssignment.
func startReadcacheForAssignment(t *testing.T, instanceID string) (*Readcache, context.Context) {
	t.Helper()

	cfg := newTestConfig(t, true, 4)
	cfg.InstanceID = instanceID
	cfg.InstanceRing.InstanceID = instanceID

	r, err := New(cfg, validation.NewOverrides(validation.Limits{}, nil), nil, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), r) })

	return r, ctx
}

// TestApplyAssignment_MatchesLeasesThroughReplicaMap is the readcache
// half of RF=2: leases name a logical slot, and every concrete mirror
// of that slot must claim the partition. Without the replica map a
// zone-a pod would see "readcache-0" in the log, fail the exact
// instance-ID comparison, and own nothing.
func TestApplyAssignment_MatchesLeasesThroughReplicaMap(t *testing.T) {
	replicaMap := readcacheassignment.ReplicaMap{
		"readcache-0": {
			{InstanceID: "readcache-zone-a-0", Zone: "zone-a"},
			{InstanceID: "readcache-zone-b-0", Zone: "zone-b"},
		},
		"readcache-1": {
			{InstanceID: "readcache-zone-a-1", Zone: "zone-a"},
			{InstanceID: "readcache-zone-b-1", Zone: "zone-b"},
		},
	}

	now := time.Now()
	// Slot 0 owns partitions 0 and 2, slot 1 owns partitions 1 and 3.
	entries := []readcacheassignment.LogEntry{
		{PartitionID: 0, InstanceID: "readcache-0", From: now.Add(-time.Minute), To: now.Add(time.Hour)},
		{PartitionID: 1, InstanceID: "readcache-1", From: now.Add(-time.Minute), To: now.Add(time.Hour)},
		{PartitionID: 2, InstanceID: "readcache-0", From: now.Add(-time.Minute), To: now.Add(time.Hour)},
		{PartitionID: 3, InstanceID: "readcache-1", From: now.Add(-time.Minute), To: now.Add(time.Hour)},
	}

	t.Run("zone-a mirror owns its logical slot's partitions", func(t *testing.T) {
		r, ctx := startReadcacheForAssignment(t, "readcache-zone-a-0")
		r.setReplicaMap(replicaMap)

		require.NoError(t, r.applyAssignment(ctx, entries, now))
		assert.Equal(t, []int32{0, 2}, r.OwnedPartitions())
	})

	t.Run("zone-b mirror owns the same partitions", func(t *testing.T) {
		r, ctx := startReadcacheForAssignment(t, "readcache-zone-b-0")
		r.setReplicaMap(replicaMap)

		require.NoError(t, r.applyAssignment(ctx, entries, now))
		assert.Equal(t, []int32{0, 2}, r.OwnedPartitions(),
			"both mirrors of a slot serve the same partitions")
	})

	t.Run("a mirror of the sibling slot owns the other partitions", func(t *testing.T) {
		r, ctx := startReadcacheForAssignment(t, "readcache-zone-a-1")
		r.setReplicaMap(replicaMap)

		require.NoError(t, r.applyAssignment(ctx, entries, now))
		assert.Equal(t, []int32{1, 3}, r.OwnedPartitions())
	})

	t.Run("expired and future leases are still filtered by time", func(t *testing.T) {
		r, ctx := startReadcacheForAssignment(t, "readcache-zone-a-0")
		r.setReplicaMap(replicaMap)

		require.NoError(t, r.applyAssignment(ctx, []readcacheassignment.LogEntry{
			{PartitionID: 0, InstanceID: "readcache-0", From: now.Add(-time.Hour), To: now},
			{PartitionID: 1, InstanceID: "readcache-0", From: now.Add(time.Minute), To: now.Add(time.Hour)},
			{PartitionID: 2, InstanceID: "readcache-0", From: now.Add(-time.Minute), To: now.Add(time.Hour)},
		}, now))
		assert.Equal(t, []int32{2}, r.OwnedPartitions(),
			"the replica map decides who owns a lease, not when it is active")
	})

	t.Run("clearing the map falls back to exact instance-ID matching", func(t *testing.T) {
		r, ctx := startReadcacheForAssignment(t, "readcache-zone-a-0")
		r.setReplicaMap(replicaMap)
		require.NoError(t, r.applyAssignment(ctx, entries, now))
		require.NotEmpty(t, r.OwnedPartitions())

		// The rebalancer went back to RF=1 and cleared the map; the
		// logical IDs in the log no longer match this pod.
		r.setReplicaMap(nil)
		require.NoError(t, r.applyAssignment(ctx, entries, now))
		assert.Empty(t, r.OwnedPartitions())
	})
}

// TestApplyAssignment_WithoutReplicaMapIsExactMatch is the RF=1
// regression guard: with no map, only leases naming this exact
// instance produce ownership.
func TestApplyAssignment_WithoutReplicaMapIsExactMatch(t *testing.T) {
	r, ctx := startReadcacheForAssignment(t, "readcache-0")

	now := time.Now()
	require.NoError(t, r.applyAssignment(ctx, []readcacheassignment.LogEntry{
		{PartitionID: 0, InstanceID: "readcache-0", From: now.Add(-time.Minute), To: now.Add(time.Hour)},
		{PartitionID: 1, InstanceID: "readcache-1", From: now.Add(-time.Minute), To: now.Add(time.Hour)},
	}, now))
	assert.Equal(t, []int32{0}, r.OwnedPartitions())
}

// TestReadcache_PrepareInstanceRingDownscaleHandler covers the
// endpoint the rollout-operator drives during scale-down: GET reports
// the current state, POST marks the ring entry read-only and stops
// keeping it on shutdown, DELETE reverts both.
func TestReadcache_PrepareInstanceRingDownscaleHandler(t *testing.T) {
	cfg := newTestConfig(t, true, 2)
	cfg.InstanceID = "readcache-zone-a-0"
	cfg.InstanceRing.InstanceID = cfg.InstanceID
	cfg.InstanceRing.KVStore = kv.Config{Store: "inmemory"}
	cfg.InstanceRing.InstanceAddr = "127.0.0.1"
	cfg.InstanceRing.InstancePort = 9095
	cfg.InstanceRing.InstanceZone = "zone-a"

	reg := prometheus.NewRegistry()
	lifecycler, err := NewInstanceRingLifecycler(cfg.InstanceRing, log.NewNopLogger(), reg)
	require.NoError(t, err)

	r, err := New(cfg, validation.NewOverrides(validation.Limits{}, nil), lifecycler, log.NewNopLogger(), reg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	call := func(t *testing.T, method string) int64 {
		t.Helper()
		req := httptest.NewRequest(method, "/readcache/prepare-instance-ring-downscale", nil)
		rec := httptest.NewRecorder()
		r.PrepareInstanceRingDownscaleHandler(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

		var body struct {
			Timestamp int64 `json:"timestamp"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		return body.Timestamp
	}

	// Before Running the endpoint must refuse to touch shutdown state.
	req := httptest.NewRequest(http.MethodPost, "/readcache/prepare-instance-ring-downscale", nil)
	rec := httptest.NewRecorder()
	r.PrepareInstanceRingDownscaleHandler(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	defer func() { _ = services.StopAndAwaitTerminated(context.Background(), r) }()

	assert.Zero(t, call(t, http.MethodGet), "a fresh instance is read-write")
	require.True(t, lifecycler.ShouldKeepInstanceInTheRingOnShutdown())

	ts := call(t, http.MethodPost)
	assert.NotZero(t, ts, "POST must report when the entry became read-only")
	readOnly, _ := lifecycler.GetReadOnlyState()
	assert.True(t, readOnly)
	assert.False(t, lifecycler.ShouldKeepInstanceInTheRingOnShutdown(),
		"a prepared instance must unregister on shutdown so the rebalancer drops it promptly")

	assert.Equal(t, ts, call(t, http.MethodPost), "repeated POSTs must not move the timestamp")
	assert.Equal(t, ts, call(t, http.MethodGet))

	assert.Zero(t, call(t, http.MethodDelete), "DELETE reverts to read-write")
	readOnly, _ = lifecycler.GetReadOnlyState()
	assert.False(t, readOnly)
	assert.True(t, lifecycler.ShouldKeepInstanceInTheRingOnShutdown(),
		"an aborted scale-down must leave no trace")
}

// TestReadcache_PrepareInstanceRingDownscaleHandler_NoRing checks the
// degraded configuration: without an instance ring there is nothing to
// mark read-only, so the endpoint says so rather than panicking.
func TestReadcache_PrepareInstanceRingDownscaleHandler_NoRing(t *testing.T) {
	r := &Readcache{logger: log.NewNopLogger()}

	rec := httptest.NewRecorder()
	r.PrepareInstanceRingDownscaleHandler(rec, httptest.NewRequest(http.MethodGet, "/readcache/prepare-instance-ring-downscale", nil))
	assert.Equal(t, http.StatusNotImplemented, rec.Code)
}
