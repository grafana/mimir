// SPDX-License-Identifier: AGPL-3.0-only

package exporter

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOverridesExporter_emptyRing(t *testing.T) {
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Create an empty ring.
	ctx := context.Background()
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		return ring.NewDesc(), true, nil
	}))

	cfg := RingConfig{Enabled: true}
	cfg.KVStore.Mock = ringStore

	cfg.InstanceID = "instance-1"
	cfg.InstanceAddr = "127.0.0.1"
	i1, err := newRing(cfg, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, i1.client))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, i1.client)) })

	_, err = i1.isLeader()
	require.ErrorIs(t, err, ring.ErrEmptyRing)
}

// TestOverridesExporterRing_scaleDownAndUp tests that a maximum of one leader
// replica exists at any point in time while the number of replicas is scaled.
func TestOverridesExporterRing_scaleDown(t *testing.T) {
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg := RingConfig{Enabled: true}
	cfg.KVStore.Mock = ringStore
	cfg.HeartbeatTimeout = 15 * time.Second

	cfg.InstanceID = "instance-1"
	cfg.InstanceAddr = "127.0.0.1"
	i1, err := newRing(cfg, log.NewNopLogger(), nil)
	require.NoError(t, err)
	l1 := i1.lifecycler

	cfg.InstanceID = "instance-2"
	cfg.InstanceAddr = "127.0.0.2"
	i2, err := newRing(cfg, log.NewNopLogger(), nil)
	require.NoError(t, err)
	l2 := i2.lifecycler

	// Register instances in the ring (manually, to be able to assign registered timestamps and tokens).
	ctx := context.Background()
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := ring.NewDesc()
		desc.AddIngester(l1.GetInstanceID(), l1.GetInstanceAddr(), "", []uint32{leaderToken + 1}, ring.ACTIVE, time.Now())
		desc.AddIngester(l2.GetInstanceID(), l2.GetInstanceAddr(), "", []uint32{leaderToken + 2}, ring.ACTIVE, time.Now())
		return desc, true, nil
	}))

	require.NoError(t, services.StartAndAwaitRunning(ctx, i1))
	require.NoError(t, services.StartAndAwaitRunning(ctx, i2))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, i2)) })

	// Wait until the clients have received the ring update.
	test.Poll(t, time.Second, []int{2, 2}, func() interface{} {
		rs1, _ := i1.client.GetAllHealthy(ringOp)
		rs2, _ := i2.client.GetAllHealthy(ringOp)
		return []int{len(rs1.Instances), len(rs2.Instances)}
	})

	// instance-1 should be the leader
	i1IsLeader, err := i1.isLeader()
	require.NoError(t, err)
	i2IsLeader, err := i2.isLeader()
	require.NoError(t, err)

	require.True(t, i1IsLeader)
	require.False(t, i2IsLeader)

	// --- Scale down ---

	// Stop instance-1.
	require.NoError(t, services.StopAndAwaitTerminated(ctx, i1))

	// Wait for the leader to have advertised its leaving state to the ring
	test.Poll(t, time.Second, ring.LEAVING, func() interface{} {
		rs, _ := i1.client.GetAllHealthy(ringOp)
		for _, instance := range rs.Instances {
			if instance.Addr == l1.GetInstanceAddr() {
				return instance.GetState()
			}
		}
		return nil
	})

	i2IsLeader, err = i2.isLeader()
	require.NoError(t, err)
	// Since the previous leader is still in the ring but in state ring.LEAVING,
	// no other instance should be the leader now.
	require.False(t, i2IsLeader)

	// After a certain period of time (ringAutoForgetUnhealthyPeriods *
	// cfg.HeartbeatTimeout) the instance's heartbeat will expire. If the instance
	// becomes healthy again during this period (e.g. during rollout), it will rejoin
	// the ring and resume its function as the leader. Otherwise, it will be
	// auto-forgotten from the ring and a different replica will become the leader.
	// This is done manually in the unit test.
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := in.(*ring.Desc)
		desc.RemoveIngester(i1.lifecycler.GetInstanceID())
		return desc, true, nil
	}))

	// Wait for ring update to be observed by the client.
	test.Poll(t, time.Second, false, func() interface{} {
		return i2.client.HasInstance(l1.GetInstanceID())
	})

	i2IsLeader, err = i2.isLeader()
	require.NoError(t, err)
	// instance-2 should now be the new leader.
	require.True(t, i2IsLeader)
}
