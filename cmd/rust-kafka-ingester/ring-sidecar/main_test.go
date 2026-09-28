// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/distributor"
)

func TestRegistrationFollowsRustReadiness(t *testing.T) {
	logger := log.NewNopLogger()
	instanceClient, closeInstance := consul.NewInMemoryClient(ring.GetCodec(), logger, nil)
	defer closeInstance.Close()
	partitionClient, closePartition := consul.NewInMemoryClient(ring.GetPartitionRingCodec(), logger, nil)
	defer closePartition.Close()

	portReservation, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := portReservation.Addr().(*net.TCPAddr).Port
	require.NoError(t, portReservation.Close())

	cfg := config{
		instanceID: "shadow-0", podIP: "127.0.0.1", zone: "zone-c", rustPort: port,
		partition: 0, instanceRingKey: "shadow/ring", partitionRingKey: "shadow-partitions",
		pollInterval: 20 * time.Millisecond,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var ready atomic.Bool
	done := make(chan error, 1)
	go func() {
		done <- manage(ctx, cfg, instanceClient, partitionClient, func() bool { return true }, &ready, logger)
	}()
	time.Sleep(100 * time.Millisecond)
	require.False(t, ready.Load())
	value, err := instanceClient.Get(ctx, cfg.instanceRingKey)
	require.NoError(t, err)
	require.Nil(t, value)

	rustPort, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	require.NoError(t, err)
	require.Eventually(t, ready.Load, 5*time.Second, 20*time.Millisecond)
	value, err = instanceClient.Get(ctx, cfg.instanceRingKey)
	require.NoError(t, err)
	instanceRing := value.(*ring.Desc)
	require.Equal(t, ring.ACTIVE, instanceRing.Ingesters[cfg.instanceID].State)
	require.Equal(t, net.JoinHostPort(cfg.podIP, strconv.Itoa(port)), instanceRing.Ingesters[cfg.instanceID].Addr)
	value, err = partitionClient.Get(ctx, cfg.partitionRingKey)
	require.NoError(t, err)
	partitionRing := value.(*ring.PartitionRingDesc)
	require.Equal(t, ring.PartitionActive, partitionRing.Partitions[0].State)
	require.Contains(t, partitionRing.Owners, cfg.instanceID)

	require.NoError(t, rustPort.Close())
	require.Eventually(t, func() bool {
		value, err := instanceClient.Get(ctx, cfg.instanceRingKey)
		if err != nil || value == nil {
			return false
		}
		_, exists := value.(*ring.Desc).Ingesters[cfg.instanceID]
		return !ready.Load() && !exists
	}, 5*time.Second, 20*time.Millisecond)
	value, err = partitionClient.Get(ctx, cfg.partitionRingKey)
	require.NoError(t, err)
	require.NotContains(t, value.(*ring.PartitionRingDesc).Owners, cfg.instanceID)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestMemberlistTransportKeepsTimeoutDefaults(t *testing.T) {
	cfg := newMemberlistConfig(config{instanceID: "shadow-0", podIP: "127.0.0.1", memberlistPort: 7946, join: "127.0.0.1:7946"})
	require.Equal(t, "127.0.0.1", cfg.TCPTransport.BindAddrs[0])
	require.Equal(t, 7946, cfg.TCPTransport.BindPort)
	require.Equal(t, 3, cfg.TCPTransport.MaxConcurrentWrites)
	require.Equal(t, 250*time.Millisecond, cfg.TCPTransport.AcquireWriterTimeout)
	require.Equal(t, 2*time.Second, cfg.TCPTransport.PacketDialTimeout)
	require.Equal(t, 5*time.Second, cfg.TCPTransport.PacketWriteTimeout)
}

func TestShadowRingGossipsToGoMemberlistBridge(t *testing.T) {
	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()
	var bridgeConfig memberlist.KVConfig
	flagext.DefaultValues(&bridgeConfig)
	bridgeConfig.NodeName = "go-bridge-zone-a"
	bridgeConfig.ClusterLabel = "shadow-test"
	bridgeConfig.TCPTransport.BindAddrs = []string{"127.0.0.1"}
	bridgeConfig.TCPTransport.BindPort = 0
	bridgeConfig.Codecs = []codec.Codec{ring.GetCodec(), ring.GetPartitionRingCodec(), distributor.GetReplicaDescCodec()}
	bridgeConfig.ZoneAwareRouting = memberlist.ZoneAwareRoutingConfig{Enabled: true, Zone: "zone-a", Role: "bridge"}
	bridgeConfig.LeaveTimeout = 100 * time.Millisecond
	bridgeConfig.BroadcastTimeoutForLocalUpdatesOnShutdown = 100 * time.Millisecond
	bridge := memberlist.NewKV(bridgeConfig, logger, dns.NewProvider(dns.GolangResolverType, 0, logger, registry), registry)
	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, bridge))
	defer services.StopAndAwaitTerminated(ctx, bridge)

	rustPort, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer rustPort.Close()
	cfg := config{
		instanceID: "shadow-0", podIP: "127.0.0.1", zone: "zone-c", memberlistRole: "bridge",
		memberlistPort: 0, rustPort: rustPort.Addr().(*net.TCPAddr).Port,
		partition: 0, clusterLabel: "shadow-test", listen: "127.0.0.1:0",
		join:            net.JoinHostPort("127.0.0.1", strconv.Itoa(bridge.GetListeningPort())),
		instanceRingKey: "shadow/ring", partitionRingKey: "shadow-partitions",
	}
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(childCtx, cfg) }()
	require.Eventually(t, func() bool {
		value, err := bridge.Get(cfg.instanceRingKey, ring.GetCodec())
		if err != nil || value == nil {
			return false
		}
		_, exists := value.(*ring.Desc).Ingesters[cfg.instanceID]
		return exists
	}, 10*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool {
		value, err := bridge.Get(cfg.partitionRingKey, ring.GetPartitionRingCodec())
		if err != nil || value == nil {
			return false
		}
		_, exists := value.(*ring.PartitionRingDesc).Owners[cfg.instanceID]
		return exists
	}, 10*time.Second, 50*time.Millisecond)
	productionInstanceRing, err := bridge.Get("partition-ingesters/ring", ring.GetCodec())
	require.NoError(t, err)
	require.Nil(t, productionInstanceRing)
	productionPartitionRing, err := bridge.Get("ingester-partitions", ring.GetPartitionRingCodec())
	require.NoError(t, err)
	require.Nil(t, productionPartitionRing)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Eventually(t, func() bool {
		instanceValue, instanceErr := bridge.Get(cfg.instanceRingKey, ring.GetCodec())
		partitionValue, partitionErr := bridge.Get(cfg.partitionRingKey, ring.GetPartitionRingCodec())
		if instanceErr != nil || partitionErr != nil || instanceValue == nil || partitionValue == nil {
			return false
		}
		_, instancePresent := instanceValue.(*ring.Desc).Ingesters[cfg.instanceID]
		_, ownerPresent := partitionValue.(*ring.PartitionRingDesc).Owners[cfg.instanceID]
		return !instancePresent && !ownerPresent
	}, 10*time.Second, 50*time.Millisecond)
}

func TestSharedRingNeverCreatesOrActivatesPartitions(t *testing.T) {
	logger := log.NewNopLogger()
	instanceClient, closeInstance := consul.NewInMemoryClient(ring.GetCodec(), logger, nil)
	defer closeInstance.Close()
	partitionClient, closePartition := consul.NewInMemoryClient(ring.GetPartitionRingCodec(), logger, nil)
	defer closePartition.Close()

	rustPort, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer rustPort.Close()
	coverageFile := filepath.Join(t.TempDir(), "coverage")
	require.NoError(t, os.WriteFile(coverageFile, []byte(strconv.FormatInt(time.Now().UnixMilli(), 10)), 0o600))
	cfg := config{
		instanceID: "ingester-julien-0", podIP: "127.0.0.1", zone: "zone-c",
		rustPort: rustPort.Addr().(*net.TCPAddr).Port, partition: 0,
		instanceRingKey: "shared/ring", partitionRingKey: "shared-partitions",
		pollInterval: 20 * time.Millisecond, sharedRing: true,
		coverageFile: coverageFile, minCoverage: time.Hour,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var ready atomic.Bool
	done := make(chan error, 1)
	go func() {
		done <- manage(ctx, cfg, instanceClient, partitionClient, func() bool { return true }, &ready, logger)
	}()
	partitionRing := func() *ring.PartitionRingDesc {
		value, err := partitionClient.Get(ctx, cfg.partitionRingKey)
		require.NoError(t, err)
		if value == nil {
			return ring.NewPartitionRingDesc()
		}
		return value.(*ring.PartitionRingDesc)
	}
	setPartition := func(state ring.PartitionState) {
		require.NoError(t, partitionClient.CAS(ctx, cfg.partitionRingKey, func(in any) (any, bool, error) {
			desc := ring.NewPartitionRingDesc()
			if in != nil {
				desc = in.(*ring.PartitionRingDesc)
			}
			if !desc.HasPartition(0) {
				desc.AddPartition(0, state, time.Now())
				desc.AddOrUpdateOwner("ingester-zone-a-0", ring.OwnerActive, 0, time.Now())
			} else {
				desc.UpdatePartitionState(0, state, time.Now())
			}
			return desc, true, nil
		}))
	}

	time.Sleep(200 * time.Millisecond)
	require.False(t, partitionRing().HasPartition(0), "a missing partition is never created")

	setPartition(ring.PartitionPending)
	time.Sleep(200 * time.Millisecond)
	require.NotContains(t, partitionRing().Owners, cfg.instanceID, "no registration without coverage")

	require.NoError(t, os.WriteFile(coverageFile, []byte(strconv.FormatInt(time.Now().Add(-2*time.Hour).UnixMilli(), 10)), 0o600))
	require.Eventually(t, func() bool {
		_, owned := partitionRing().Owners[cfg.instanceID]
		return owned
	}, 5*time.Second, 20*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, ring.PartitionPending, partitionRing().Partitions[0].State, "a pending partition is never activated")
	require.False(t, ready.Load())

	setPartition(ring.PartitionActive)
	require.Eventually(t, ready.Load, 5*time.Second, 20*time.Millisecond)

	setPartition(ring.PartitionInactive)
	require.Eventually(t, func() bool {
		_, owned := partitionRing().Owners[cfg.instanceID]
		return !owned && !ready.Load()
	}, 5*time.Second, 20*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	_, owned := partitionRing().Owners[cfg.instanceID]
	require.False(t, owned, "an inactive partition is not owned again")
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}
