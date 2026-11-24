// SPDX-License-Identifier: AGPL-3.0-only

package rustkafkaingester_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
)

func TestRustReadsGoMemberlistRingSnapshot(t *testing.T) {
	ctx := t.Context()
	cmd := exec.CommandContext(ctx, "cargo", "build", "--release", "--locked", "--offline", "--example", "ring_snapshot")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
	targetDir := os.Getenv("CARGO_TARGET_DIR")
	if targetDir == "" {
		targetDir = "target"
	}

	for _, compression := range []string{"disabled", "lzw", "snappy"} {
		t.Run(compression, func(t *testing.T) {
			ctx := t.Context()
			logger := log.NewNopLogger()
			registry := prometheus.NewRegistry()
			var config memberlist.KVConfig
			flagext.DefaultValues(&config)
			config.NodeName = "go-ingester"
			config.ClusterLabel = "mimir-ring-test"
			config.TCPTransport.BindAddrs = []string{"127.0.0.1"}
			config.TCPTransport.BindPort = 0
			config.EnableCompression = compression != "disabled"
			if config.EnableCompression {
				config.CompressionAlgorithm = compression
			}
			config.Codecs = []codec.Codec{ring.GetCodec(), ring.GetPartitionRingCodec()}
			store := memberlist.NewKV(config, logger, dns.NewProvider(dns.GolangResolverType, 0, logger, registry), registry)
			require.NoError(t, services.StartAndAwaitRunning(ctx, store))
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				require.NoError(t, services.StopAndAwaitTerminated(ctx, store))
			})

			instanceClient, err := memberlist.NewClient(store, ring.GetCodec())
			require.NoError(t, err)
			partitionClient, err := memberlist.NewClient(store, ring.GetPartitionRingCodec())
			require.NoError(t, err)

			now := time.Now()
			require.NoError(t, instanceClient.CAS(ctx, "partition-ingesters/ring", func(any) (any, bool, error) {
				desc := ring.NewDesc()
				desc.AddIngester("go-ingester", "127.0.0.1:9095", "zone-a", nil, ring.ACTIVE, now, false, now, nil)
				return desc, true, nil
			}))
			require.NoError(t, partitionClient.CAS(ctx, "ingester-partitions", func(any) (any, bool, error) {
				desc := ring.NewPartitionRingDesc()
				desc.AddPartition(17, ring.PartitionActive, now)
				desc.AddOrUpdateOwner("go-ingester", ring.OwnerActive, 17, now)
				for id := int32(20); id < 100; id++ {
					desc.AddPartition(id, ring.PartitionInactive, now)
					desc.AddOrUpdateOwner(strings.Repeat("owner-", 16)+strconv.Itoa(int(id)), ring.OwnerActive, id, now)
				}
				return desc, true, nil
			}))

			peer := net.JoinHostPort("127.0.0.1", strconv.Itoa(store.GetListeningPort()))
			probe := exec.CommandContext(ctx, filepath.Join(targetDir, "release", "examples", "ring_snapshot"), peer, "--cluster-label", config.ClusterLabel)
			output, err := probe.CombinedOutput()
			require.NoError(t, err, "%s", output)

			var snapshot struct {
				Instances map[string]struct {
					Addr string
					Zone string
				}
				Partitions map[int32]struct{ State int32 }
				Owners     map[string]struct {
					Partition int32 `json:"owned_partition"`
				}
			}
			require.NoError(t, json.Unmarshal(output, &snapshot), "%s", output)
			require.Equal(t, "127.0.0.1:9095", snapshot.Instances["go-ingester"].Addr)
			require.Equal(t, "zone-a", snapshot.Instances["go-ingester"].Zone)
			require.EqualValues(t, ring.PartitionActive, snapshot.Partitions[17].State)
			require.EqualValues(t, 17, snapshot.Owners["go-ingester"].Partition)
			require.Len(t, snapshot.Partitions, 81)
		})
	}
}

func TestRustSharedRingLifecycleWithGoPeers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	const label = "rust-shared-ring-test"
	const instanceKey = "partition-ingesters/ring"
	const partitionKey = "ingester-partitions"
	const partitionID = 17
	const instanceID = "rust-ingester"
	const originalOwner = "go-ingester"

	seed := startRingPeer(t, "go-seed", label)
	replica := startRingPeer(t, "go-replica", label)
	_, err := replica.JoinMembers([]string{ringPeerAddress(seed)})
	require.NoError(t, err)
	seedInstances, err := memberlist.NewClient(seed, ring.GetCodec())
	require.NoError(t, err)
	seedPartitions, err := memberlist.NewClient(seed, ring.GetPartitionRingCodec())
	require.NoError(t, err)
	replicaInstances, err := memberlist.NewClient(replica, ring.GetCodec())
	require.NoError(t, err)
	replicaPartitions, err := memberlist.NewClient(replica, ring.GetPartitionRingCodec())
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, seedInstances.CAS(ctx, instanceKey, func(any) (any, bool, error) {
		desc := ring.NewDesc()
		desc.AddIngester(originalOwner, "127.0.0.1:9095", "zone-a", nil, ring.ACTIVE, now, false, now, nil)
		return desc, true, nil
	}))
	partitions := ring.NewPartitionRingDesc()
	for id := int32(0); id <= 20; id++ {
		state := ring.PartitionActive
		if id%5 == 0 {
			state = ring.PartitionInactive
		}
		if id%5 == 1 {
			state = ring.PartitionPending
		}
		if id%3 == 0 {
			partitions.AddPartition(id, state, now)
			p := partitions.Partitions[id]
			p.Tokens = []uint32{uint32(id)*14213579 + 1, uint32(id)*28125471 + 3}
			partitions.Partitions[id] = p
		} else {
			partitions.AddPartitionWithDerivedTokens(id, state, now)
		}
	}
	partitions.AddOrUpdateOwner(originalOwner, ring.OwnerActive, partitionID, now)
	require.NoError(t, seedPartitions.CAS(ctx, partitionKey, func(any) (any, bool, error) {
		return partitions, true, nil
	}))

	table, err := ring.NewPartitionTokenTable(21, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)
	view, err := ring.NewPartitionRingWithOptions(*partitions, ring.PartitionRingOptions{TokenGenerator: table})
	require.NoError(t, err)
	type shardCase struct {
		Tenant string           `json:"tenant"`
		Size   int              `json:"size"`
		Ranges ring.TokenRanges `json:"ranges"`
	}
	var cases []shardCase
	for i := 0; i < 20; i++ {
		tenant := "tenant-" + strconv.Itoa(i)
		for _, size := range []int{-1, 0, 1, 3, 7, 20, 30} {
			subring, err := view.ShuffleShard(tenant, size)
			require.NoError(t, err)
			ranges, err := subring.GetTokenRangesForPartition(partitionID)
			if err != nil {
				require.ErrorIs(t, err, ring.ErrPartitionDoesNotExist)
			}
			cases = append(cases, shardCase{Tenant: tenant, Size: size, Ranges: ranges})
		}
	}
	fixture, err := json.Marshal(cases)
	require.NoError(t, err)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ranges.json"), fixture, 0600))

	cmd := exec.CommandContext(ctx, "cargo", "test", "--release", "--locked", "--offline", "--lib", "mixed_go_lifecycle_and_ownership", "--", "--ignored", "--nocapture")
	cmd.Env = append(os.Environ(),
		"MIMIR_TEST_RING_PEER="+ringPeerAddress(seed),
		"MIMIR_TEST_RING_REPLICA="+ringPeerAddress(replica),
		"MIMIR_TEST_RING_DATA_DIR="+dir,
	)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		if t.Failed() {
			cancel()
		}
		err := <-done
		if err != nil && !t.Failed() {
			t.Errorf("Rust ring test failed: %v", err)
		}
		if t.Failed() {
			t.Log(output.String())
		}
	})

	// Observe the registrations via a different Go peer, so the test also
	// exercises normal dskit rebroadcasts of deltas received from Rust.
	var firstHeartbeat int64
	require.Eventually(t, func() bool {
		instances, err := replicaInstances.Get(ctx, instanceKey)
		if err != nil || instances == nil {
			return false
		}
		instance, found := instances.(*ring.Desc).Ingesters[instanceID]
		if !found || instance.State != ring.ACTIVE || instance.Addr != "127.0.0.1:19095" {
			return false
		}
		desc, err := replicaPartitions.Get(ctx, partitionKey)
		if err != nil || desc == nil {
			return false
		}
		owner := desc.(*ring.PartitionRingDesc).Owners[instanceID]
		if owner.State != ring.OwnerActive || owner.OwnedPartition != partitionID {
			return false
		}
		if firstHeartbeat == 0 {
			firstHeartbeat = instance.Timestamp
		}
		return instance.Timestamp > firstHeartbeat
	}, 95*time.Second, 100*time.Millisecond)

	require.NoError(t, seedPartitions.CAS(ctx, partitionKey, func(in any) (any, bool, error) {
		desc := in.(*ring.PartitionRingDesc)
		desc.UpdatePartitionState(partitionID, ring.PartitionInactive, time.Now())
		return desc, true, nil
	}))
	require.Eventually(t, func() bool {
		desc, err := replicaPartitions.Get(ctx, partitionKey)
		if err != nil || desc == nil {
			return false
		}
		partitions := desc.(*ring.PartitionRingDesc)
		_, stillOwned := partitions.Owners[instanceID]
		if partitions.Partitions[partitionID].State != ring.PartitionInactive || stillOwned ||
			partitions.Owners[originalOwner].State != ring.OwnerActive {
			return false
		}
		instances, err := replicaInstances.Get(ctx, instanceKey)
		if err != nil || instances == nil {
			return false
		}
		_, stillRegistered := instances.(*ring.Desc).Ingesters[instanceID]
		return !stillRegistered
	}, 25*time.Second, 100*time.Millisecond)
	require.FileExists(t, filepath.Join(dir, "shutdown-requested.txt"))
}

func startRingPeer(t *testing.T, name, label string) *memberlist.KV {
	t.Helper()
	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()
	var config memberlist.KVConfig
	flagext.DefaultValues(&config)
	config.NodeName = name
	config.ClusterLabel = label
	config.TCPTransport.BindAddrs = []string{"127.0.0.1"}
	config.TCPTransport.BindPort = 0
	config.LeaveTimeout = 100 * time.Millisecond
	config.BroadcastTimeoutForLocalUpdatesOnShutdown = 100 * time.Millisecond
	config.Codecs = []codec.Codec{ring.GetCodec(), ring.GetPartitionRingCodec()}
	store := memberlist.NewKV(config, logger, dns.NewProvider(dns.GolangResolverType, 0, logger, registry), registry)
	require.NoError(t, services.StartAndAwaitRunning(t.Context(), store))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, services.StopAndAwaitTerminated(ctx, store))
	})
	return store
}

func ringPeerAddress(peer *memberlist.KV) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(peer.GetListeningPort()))
}

func TestRustIngesterServesWithNativeRing(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	const label = "rust-serve-ring-test"
	const instanceID = "rust-serve-ingester"
	const topic = "ingest"
	peer := startRingPeer(t, "go-ingester", label)
	instances, err := memberlist.NewClient(peer, ring.GetCodec())
	require.NoError(t, err)
	partitions, err := memberlist.NewClient(peer, ring.GetPartitionRingCodec())
	require.NoError(t, err)
	now := time.Now()
	require.NoError(t, instances.CAS(ctx, "partition-ingesters/ring", func(any) (any, bool, error) {
		desc := ring.NewDesc()
		desc.AddIngester("go-ingester", "127.0.0.1:19091", "zone-a", nil, ring.ACTIVE, now, false, now, nil)
		return desc, true, nil
	}))
	require.NoError(t, partitions.CAS(ctx, "ingester-partitions", func(any) (any, bool, error) {
		desc := ring.NewPartitionRingDesc()
		desc.AddPartition(0, ring.PartitionActive, now)
		desc.AddOrUpdateOwner("go-ingester", ring.OwnerActive, 0, now)
		return desc, true, nil
	}))
	kafka, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	require.NoError(t, err)
	t.Cleanup(kafka.Close)
	cmd := exec.CommandContext(ctx, "cargo", "build", "--release", "--locked", "--offline", "--bin", "mimir-rust-kafka-ingester")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)

	address := func() string {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		require.NoError(t, listener.Close())
		return listener.Addr().String()
	}
	httpAddr, grpcAddr := address(), address()
	targetDir := os.Getenv("CARGO_TARGET_DIR")
	if targetDir == "" {
		targetDir = "target"
	}
	cmd = exec.CommandContext(ctx, filepath.Join(targetDir, "release", "mimir-rust-kafka-ingester"), "serve",
		"--brokers", strings.Join(kafka.ListenAddrs(), ","), "--topic", topic,
		"--data-dir", t.TempDir(), "--partition", "0", "--start-offset", "earliest",
		"--store-shards", "1", "--ingest-threads", "1", "--listen", grpcAddr, "--metrics-listen", httpAddr,
		"--ring-peer", ringPeerAddress(peer), "--ring-cluster-label", label,
		"--ring-advertise-address", grpcAddr, "--ring-instance-id", instanceID, "--ring-zone", "zone-b")
	var logs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &logs, &logs
	require.NoError(t, cmd.Start())
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
		if t.Failed() {
			t.Log(logs.String())
		}
	}()
	ready := "http://" + httpAddr + "/ready"
	client := &http.Client{Timeout: time.Second}
	require.Eventually(t, func() bool {
		response, err := client.Get(ready)
		if err != nil {
			return false
		}
		_ = response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, 25*time.Second, 100*time.Millisecond)
	connection, err := net.DialTimeout("tcp", grpcAddr, time.Second)
	require.NoError(t, err)
	require.NoError(t, connection.Close())
	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, cmd.Wait(), "%s", logs.String())
	require.Eventually(t, func() bool {
		value, err := instances.Get(ctx, "partition-ingesters/ring")
		if err != nil || value == nil {
			return false
		}
		_, registered := value.(*ring.Desc).Ingesters[instanceID]
		return !registered
	}, 3*time.Second, 100*time.Millisecond)
	// An ordinary restart leaves the Kafka partition owner, like the Go sidecar.
	value, err := partitions.Get(ctx, "ingester-partitions")
	require.NoError(t, err)
	require.Equal(t, ring.OwnerActive, value.(*ring.PartitionRingDesc).Owners[instanceID].State)
}
