// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/cmd/rust-kafka-ingester/ring-sidecar/handlers"
	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/util/shutdownmarker"
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
		done <- manage(ctx, cfg, instanceClient, partitionClient, func() bool { return true }, &ready, nil, logger)
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
		unregisterOnShutdown: true,
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
		// Like Go's partition lifecycler, the owner stays unless shutdown was prepared.
		_, ownerPresent := partitionValue.(*ring.PartitionRingDesc).Owners[cfg.instanceID]
		return !instancePresent && ownerPresent
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
		done <- manage(ctx, cfg, instanceClient, partitionClient, func() bool { return true }, &ready, nil, logger)
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

func TestActivePartitionsCountsOnlyActivePartitions(t *testing.T) {
	desc := ring.NewPartitionRingDesc()
	desc.AddPartition(0, ring.PartitionActive, time.Now())
	desc.AddPartition(1, ring.PartitionActive, time.Now())
	desc.AddPartition(2, ring.PartitionPending, time.Now())
	desc.AddPartition(3, ring.PartitionInactive, time.Now())
	require.Equal(t, 2, activePartitions(desc))
}

func startIsolatedSidecar(t *testing.T, cfg config, lifecycle *lifecycleState) (context.CancelFunc, chan error, kv.Client, kv.Client, *atomic.Bool) {
	t.Helper()
	logger := log.NewNopLogger()
	instanceClient, closeInstance := consul.NewInMemoryClient(ring.GetCodec(), logger, nil)
	t.Cleanup(func() { _ = closeInstance.Close() })
	partitionClient, closePartition := consul.NewInMemoryClient(ring.GetPartitionRingCodec(), logger, nil)
	t.Cleanup(func() { _ = closePartition.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if lifecycle.prepared.Load() {
		// A prepared ingester never creates its partition.
		require.NoError(t, partitionClient.CAS(ctx, cfg.partitionRingKey, func(any) (any, bool, error) {
			desc := ring.NewPartitionRingDesc()
			desc.AddPartition(int32(cfg.partition), ring.PartitionActive, time.Now())
			return desc, true, nil
		}))
	}
	var ready atomic.Bool
	done := make(chan error, 1)
	go func() {
		done <- manage(ctx, cfg, instanceClient, partitionClient, func() bool { return true }, &ready, lifecycle, logger)
	}()
	require.Eventually(t, ready.Load, 5*time.Second, 20*time.Millisecond)
	return cancel, done, instanceClient, partitionClient, &ready
}

func rustListener(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	return listener.Addr().(*net.TCPAddr).Port
}

func TestShutdownKeepsRegistrationsLikeGoUnlessPrepared(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		t.Run(fmt.Sprintf("prepared=%v", prepared), func(t *testing.T) {
			cfg := config{
				instanceID: "shadow-0", podIP: "127.0.0.1", zone: "zone-c", rustPort: rustListener(t),
				partition: 0, instanceRingKey: "shadow/ring", partitionRingKey: "shadow-partitions",
				pollInterval: 20 * time.Millisecond, unregisterOnShutdown: false,
			}
			lifecycle := &lifecycleState{}
			lifecycle.prepared.Store(prepared)
			cancel, done, instanceClient, partitionClient, _ := startIsolatedSidecar(t, cfg, lifecycle)
			cancel()
			require.ErrorIs(t, <-done, context.Canceled)

			value, err := instanceClient.Get(context.Background(), cfg.instanceRingKey)
			require.NoError(t, err)
			instance, registered := value.(*ring.Desc).Ingesters[cfg.instanceID]
			value, err = partitionClient.Get(context.Background(), cfg.partitionRingKey)
			require.NoError(t, err)
			_, owner := value.(*ring.PartitionRingDesc).Owners[cfg.instanceID]
			if prepared {
				require.False(t, registered, "a prepared shutdown leaves the instance ring")
				require.False(t, owner, "a prepared shutdown removes the partition owner")
			} else {
				require.True(t, registered, "-ingester.ring.unregister-on-shutdown=false keeps the instance")
				require.Equal(t, ring.LEAVING, instance.State)
				require.True(t, owner, "the partition owner stays, like Go's default")
			}
		})
	}
}

func TestPrepareShutdownHandlerPersistsAMarker(t *testing.T) {
	cfg := config{shutdownMarkerDir: t.TempDir()}
	lifecycle := &lifecycleState{}
	call := func(method string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		handlers.PrepareShutdown(recorder, httptest.NewRequest(method, "/ingester/prepare-shutdown", nil), cfg.shutdownMarkerDir, &lifecycle.prepared, log.NewNopLogger())
		return recorder
	}
	require.Equal(t, "unset\n", call(http.MethodGet).Body.String())
	require.Equal(t, http.StatusNoContent, call(http.MethodPost).Code)
	require.Equal(t, "set\n", call(http.MethodGet).Body.String())
	// Like Go with ingest storage, the preparation can't be reverted.
	require.Equal(t, http.StatusMethodNotAllowed, call(http.MethodDelete).Code)
	exists, err := shutdownmarker.Exists(shutdownmarker.GetPath(cfg.shutdownMarkerDir))
	require.NoError(t, err)
	require.True(t, exists, "the marker survives a restart")
}

func TestPreparePartitionDownscaleSwitchesPartitionState(t *testing.T) {
	cfg := config{
		instanceID: "shadow-0", podIP: "127.0.0.1", zone: "zone-c", rustPort: rustListener(t),
		partition: 0, instanceRingKey: "shadow/ring", partitionRingKey: "shadow-partitions",
		pollInterval: 20 * time.Millisecond,
	}
	lifecycle := &lifecycleState{}
	_, _, _, partitionClient, _ := startIsolatedSidecar(t, cfg, lifecycle)
	call := func(cfg config, method string) (int, map[string]int64) {
		recorder := httptest.NewRecorder()
		preparePartitionDownscaleHandler(recorder, httptest.NewRequest(method, "/ingester/prepare-partition-downscale", nil), cfg, lifecycle, log.NewNopLogger())
		var body map[string]int64
		_ = json.Unmarshal(recorder.Body.Bytes(), &body)
		return recorder.Code, body
	}
	state := func() ring.PartitionState {
		value, err := partitionClient.Get(context.Background(), cfg.partitionRingKey)
		require.NoError(t, err)
		return value.(*ring.PartitionRingDesc).Partitions[0].State
	}
	code, body := call(cfg, http.MethodGet)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, int64(0), body["timestamp"])
	code, body = call(cfg, http.MethodPost)
	require.Equal(t, http.StatusOK, code)
	require.Positive(t, body["timestamp"])
	require.Equal(t, ring.PartitionInactive, state())
	code, body = call(cfg, http.MethodDelete)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, int64(0), body["timestamp"])
	require.Equal(t, ring.PartitionActive, state())
	// The shared ring's partition states belong to the Go ingesters.
	shared := cfg
	shared.sharedRing = true
	code, _ = call(shared, http.MethodPost)
	require.Equal(t, http.StatusConflict, code)
	require.Equal(t, ring.PartitionActive, state())
}

func TestOwnedTokenRangesFollowTheTenantShuffleShard(t *testing.T) {
	desc := ring.NewPartitionRingDesc()
	for id := int32(0); id < 3; id++ {
		desc.AddPartition(id, ring.PartitionActive, time.Now())
	}
	partitionRing, err := ring.NewPartitionRing(*desc)
	require.NoError(t, err)
	owners := 0
	for id := int32(0); id < 3; id++ {
		ranges, err := ownedTokenRanges(desc, id, map[string]int{"single": 1, "all": 0})
		require.NoError(t, err)
		require.NotNil(t, ranges["all"], "shard size 0 uses every partition")
		expected, err := partitionRing.GetTokenRangesForPartition(id)
		require.NoError(t, err)
		require.Equal(t, []uint32(expected), ranges["all"])
		if ranges["single"] != nil {
			owners++
		}
	}
	require.Equal(t, 1, owners, "a shard of one partition is owned by exactly one partition")
}
