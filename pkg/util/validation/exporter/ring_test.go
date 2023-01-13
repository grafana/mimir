// SPDX-License-Identifier: AGPL-3.0-only

package exporter

import (
	"context"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// TestOverridesExporterRollout tests that metric duplicates are not exported on
// scale-down
func TestRing_scaleDown(t *testing.T) {
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
	r1, err := newRing(cfg, log.NewNopLogger(), nil)
	l1 := r1.lifecycler
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, r1.client))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, r1.client)) })

	cfg.InstanceID = "instance-2"
	cfg.InstanceAddr = "127.0.0.2"
	r2, err := newRing(cfg, log.NewNopLogger(), nil)
	l2 := r2.lifecycler
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, r2.client))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, r2.client)) })

	// Register instances in the ring
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := in.(*ring.Desc)
		desc.AddIngester(l1.GetInstanceID(), l1.GetInstanceAddr(), "", nil, ring.ACTIVE, time.Now().Add(-time.Hour))
		desc.AddIngester(l2.GetInstanceID(), l2.GetInstanceAddr(), "", nil, ring.ACTIVE, time.Now().Add(-59*time.Minute))
		return desc, true, nil
	}))

	// Wait until the client has received the ring update
	test.Poll(t, time.Second, 2, func() interface{} {
		rs, _ := r1.client.GetAllHealthy(ringOp)
		return len(rs.Instances)
	})

	r1IsLeader, err := r1.isLeader()
	require.NoError(t, err)
	r2IsLeader, err := r2.isLeader()
	require.NoError(t, err)
	require.True(t, r1IsLeader && !r2IsLeader)

	// Start and immediately stop the leader's lifecycler to make it unregister itself from the ring
	require.NoError(t, services.StartAndAwaitRunning(ctx, r1.lifecycler))
	require.NoError(t, services.StopAndAwaitTerminated(ctx, r1.lifecycler))

	test.Poll(t, time.Second, ring.LEAVING, func() interface{} {
		rs, _ := r1.client.GetAllHealthy(ringOp)
		for _, instance := range rs.Instances {
			if instance.Addr == l1.GetInstanceAddr() {
				return instance.GetState()
			}
		}
		return nil
	})

	r1IsLeader, err = r1.isLeader()
	require.NoError(t, err)
	r2IsLeader, err = r2.isLeader()
	require.NoError(t, err)
	// Since the previous leader is still in the ring but in state ring.LEAVING, there should be no leader now
	require.True(t, !r1IsLeader && !r2IsLeader)

	// Set the last heartbeat to a point in the past to allow the remaining instance's lifecycler to remove it from the ring
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := in.(*ring.Desc)
		instance := desc.Ingesters[l1.GetInstanceID()]
		instance.Timestamp = time.Now().Add(-(ringAutoForgetUnhealthyPeriods + 1) * cfg.HeartbeatTimeout).Unix()
		desc.Ingesters[l1.GetInstanceID()] = instance
		return desc, true, nil
	}))

	require.NoError(t, services.StartAndAwaitRunning(ctx, r2.lifecycler))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, r2.lifecycler)) })

	test.Poll(t, time.Second, 1, func() interface{} {
		rs, _ := r2.client.GetAllHealthy(ringOp)
		return len(rs.Instances)
	})

	r1IsLeader, err = r1.isLeader()
	require.NoError(t, err)
	r2IsLeader, err = r2.isLeader()
	require.NoError(t, err)
	require.True(t, !r1IsLeader && r2IsLeader)
}

func TestRing_scaleUp(t *testing.T) {

}
