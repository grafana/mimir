// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/compartments"
)

func TestPartitionInstanceRings(t *testing.T) {
	kvClient, closer := consul.NewInMemoryClient(ring.GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { _ = closer.Close() })

	// Each read compartment has a single partition owned by a distinct ingester.
	writePartitionAndOwner(t, kvClient, compartments.ReadCompartmentRingKey(0, testRingKey), 0, "ingester-c0")
	writePartitionAndOwner(t, kvClient, compartments.ReadCompartmentRingKey(1, testRingKey), 0, "ingester-c1")

	watcher, err := NewPartitionRingWatchers(true, 2, testRingName, testRingKey, kvClient, log.NewNopLogger(), nil)
	require.NoError(t, err)
	startPartitionRingWatchers(t, watcher)

	now := time.Now()
	instanceRing := fakeInstanceRing{instances: map[string]ring.InstanceDesc{
		"ingester-c0": {Addr: "c0", State: ring.ACTIVE, Timestamp: now.Unix(), Zone: "zone-a"},
		"ingester-c1": {Addr: "c1", State: ring.ACTIVE, Timestamp: now.Unix(), Zone: "zone-a"},
	}}

	rings := NewPartitionInstanceRings(watcher, instanceRing, time.Minute)
	require.Equal(t, 2, rings.Count())

	// Each ring exposes its own compartment's partition ring.
	assert.Equal(t, 1, rings.Get(0).PartitionRing().PartitionsCount())
	assert.Equal(t, 1, rings.Get(1).PartitionRing().PartitionsCount())

	// Compartment 0's ring resolves only compartment 0's partition owner against the shared instance
	// ring; compartment 1's instance is never an owner of compartment 0's partitions.
	sets, err := rings.Get(0).GetReplicationSetsForOperation(ring.Read)
	require.NoError(t, err)
	require.Len(t, sets, 1)
	require.Len(t, sets[0].Instances, 1)
	assert.Equal(t, "c0", sets[0].Instances[0].Addr)
}

func TestPartitionInstanceRings_Disabled(t *testing.T) {
	kvClient, closer := consul.NewInMemoryClient(ring.GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { _ = closer.Close() })

	writePartitionAndOwner(t, kvClient, testRingKey, 0, "ingester-0")

	watcher, err := NewPartitionRingWatchers(false, 0, testRingName, testRingKey, kvClient, log.NewNopLogger(), nil)
	require.NoError(t, err)
	startPartitionRingWatchers(t, watcher)

	rings := NewPartitionInstanceRings(watcher, fakeInstanceRing{}, time.Minute)
	assert.Equal(t, 1, rings.Count())
	assert.Same(t, watcher.Watcher(0).PartitionRing(), rings.Get(0).PartitionRing())
}

// fakeInstanceRing is a minimal ring.InstanceRingReader backed by a static map.
type fakeInstanceRing struct {
	instances map[string]ring.InstanceDesc
}

func (f fakeInstanceRing) GetInstance(id string) (ring.InstanceDesc, error) {
	inst, ok := f.instances[id]
	if !ok {
		return ring.InstanceDesc{}, fmt.Errorf("instance %s not found", id)
	}
	return inst, nil
}

func (f fakeInstanceRing) InstancesCount() int { return len(f.instances) }

// writePartitionAndOwner stores a partition ring desc with a single active partition owned by the given instance.
func writePartitionAndOwner(t *testing.T, kvClient kv.Client, key string, partitionID int32, ownerID string) {
	t.Helper()
	require.NoError(t, kvClient.CAS(context.Background(), key, func(interface{}) (interface{}, bool, error) {
		desc := ring.NewPartitionRingDesc()
		desc.AddPartition(partitionID, ring.PartitionActive, time.Now())
		desc.AddOrUpdateOwner(ownerID, ring.OwnerActive, partitionID, time.Now())
		return desc, true, nil
	}))
}
