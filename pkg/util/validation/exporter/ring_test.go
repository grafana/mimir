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

func TestEmptyRing(t *testing.T) {
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Create an empty ring.
	ctx := context.Background()
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		return ring.NewDesc(), true, nil
	}))

	cfg := RingConfig{Enabled: true}
	cfg.KVStore.Mock = ringStore
	cfg.HeartbeatTimeout = 15 * time.Second

	cfg.InstanceID = "instance-1"
	cfg.InstanceAddr = "127.0.0.1"
	i1, err := newRing(cfg, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, i1.client))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, i1.client)) })

	_, err = i1.isLeader(time.Now())
	require.ErrorIs(t, err, ring.ErrEmptyRing)
}

// TestOverridesExporterRing_scaleDownAndUp tests that a maximum of one leader
// replica exists at any point in time while the number of replicas is scaled.
func TestOverridesExporterRing_scaleDownAndUp(t *testing.T) {
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Create an empty ring.
	ctx := context.Background()
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		return ring.NewDesc(), true, nil
	}))

	cfg := RingConfig{Enabled: true}
	cfg.KVStore.Mock = ringStore
	cfg.HeartbeatTimeout = 15 * time.Second

	cfg.InstanceID = "instance-1"
	cfg.InstanceAddr = "127.0.0.1"
	i1, err := newRing(cfg, log.NewNopLogger(), nil)
	l1 := i1.lifecycler
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, i1.client))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, i1.client)) })

	cfg.InstanceID = "instance-2"
	cfg.InstanceAddr = "127.0.0.2"
	i2, err := newRing(cfg, log.NewNopLogger(), nil)
	l2 := i2.lifecycler
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, i2.client))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, i2.client)) })

	// Register instances in the ring (manually, to be able to assign registered timestamps and tokens).
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := in.(*ring.Desc)
		desc.AddIngester(l1.GetInstanceID(), l1.GetInstanceAddr(), "", []uint32{leaderToken + 2}, ring.ACTIVE, time.Now().Add(-time.Hour))
		desc.AddIngester(l2.GetInstanceID(), l2.GetInstanceAddr(), "", []uint32{leaderToken + 3}, ring.ACTIVE, time.Now().Add(-59*time.Minute))
		return desc, true, nil
	}))

	// Wait until the clients have received the ring update.
	test.Poll(t, time.Second, []int{2, 2}, func() interface{} {
		rs1, _ := i1.client.GetAllHealthy(ringOp)
		rs2, _ := i2.client.GetAllHealthy(ringOp)
		return []int{len(rs1.Instances), len(rs2.Instances)}
	})

	// instance-1 should be the leader
	i1IsLeader, err := i1.isLeader(time.Now())
	require.NoError(t, err)
	i2IsLeader, err := i2.isLeader(time.Now())
	require.NoError(t, err)
	require.True(t, i1IsLeader && !i2IsLeader)

	// --- Scale down ---

	// Start and immediately stop the leader's lifecycler to make it unregister itself from the ring.
	require.NoError(t, services.StartAndAwaitRunning(ctx, i1.lifecycler))
	require.NoError(t, services.StopAndAwaitTerminated(ctx, i1.lifecycler))

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

	i1IsLeader, err = i1.isLeader(time.Now())
	require.NoError(t, err)
	i2IsLeader, err = i2.isLeader(time.Now())
	require.NoError(t, err)
	// Since the previous leader is still in the ring but in state ring.LEAVING, there should be no leader now.
	require.True(t, !i1IsLeader && !i2IsLeader)

	// Remove instance-1 from the ring.
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := in.(*ring.Desc)
		desc.RemoveIngester(i1.lifecycler.GetInstanceID())
		return desc, true, nil
	}))

	test.Poll(t, time.Second, false, func() interface{} {
		return i2.client.HasInstance(i1.lifecycler.GetInstanceID())
	})

	i1IsLeader, err = i1.isLeader(time.Now())
	require.NoError(t, err)
	i2IsLeader, err = i2.isLeader(time.Now())
	require.NoError(t, err)
	// instance-2 should now be the new leader.
	require.True(t, !i1IsLeader && i2IsLeader)

	// --- Scale up ---

	// Create and start a new instance.
	cfg.InstanceID = "instance-3"
	cfg.InstanceAddr = "127.0.0.3"
	i3, err := newRing(cfg, log.NewNopLogger(), nil)
	l3 := i3.lifecycler
	require.NoError(t, err)
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := in.(*ring.Desc)
		desc.AddIngester(l3.GetInstanceID(), l3.GetInstanceAddr(), "", []uint32{leaderToken + 1}, ring.ACTIVE, time.Now())
		return desc, true, nil
	}))

	require.NoError(t, services.StartAndAwaitRunning(ctx, i3))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, i3)) })

	// Wait until the new instance is observed in the ring.
	test.Poll(t, time.Second, true, func() interface{} {
		rs2, _ := i2.client.GetAllHealthy(ringOp)
		rs3, _ := i3.client.GetAllHealthy(ringOp)
		return rs2.Includes(l3.GetInstanceAddr()) && rs3.Includes(l3.GetInstanceAddr())
	})

	i2IsLeader, err = i2.isLeader(time.Now())
	require.NoError(t, err)
	i3IsLeader, err := i3.isLeader(time.Now())
	require.NoError(t, err)

	// instance-3 is registered with a token that makes it the new leader, but
	// because of the wait time it won't be the leader immediately.
	require.True(t, !i2IsLeader && !i3IsLeader)

	// However, instance-3 should become the leader once the wait time has passed and
	// instance-2 has had time to become aware of the new ring leader.
	i3WillBeLeader, err := i3.isLeader(time.Now().Add((ringAutoForgetUnhealthyPeriods) * cfg.HeartbeatTimeout))
	require.NoError(t, err)
	require.True(t, i3WillBeLeader)
}
