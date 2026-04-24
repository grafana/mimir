// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestNewNullIngester_RequiresIngestStorage(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	_, err := NewNullIngester(cfg, log.NewNopLogger(), prometheus.NewRegistry())
	require.EqualError(t, err, "ingest storage must be enabled for null ingester")
}

func TestNewNullIngester_RequiresCompartments(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.IngestStorageConfig.Enabled = true
	_, err := NewNullIngester(cfg, log.NewNopLogger(), prometheus.NewRegistry())
	require.EqualError(t, err, "compartments must be enabled for null ingester")
}

func TestNullIngester_RegistersAndDeregistersInPartitionRing(t *testing.T) {
	ctx := context.Background()

	kvClient, closer := consul.NewInMemoryClient(ring.GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	_, kafkaAddr := testkafka.CreateCluster(t, 1, "test-comp-0")

	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.InstanceID = "ingester-zone-a-0"
	cfg.IngestStorageConfig.Enabled = true
	cfg.IngestStorageConfig.Compartments.Enabled = true
	cfg.IngestStorageConfig.Compartments.NumCompartments = 1
	cfg.IngestStorageConfig.Compartments.TopicFormat = "test-comp-<compartment-id>"
	cfg.IngestStorageConfig.Compartments.ReadCompartmentID = 0
	cfg.IngestStorageConfig.Compartments.WriteKafkaAddressFormat = kafkaAddr
	cfg.IngesterPartitionRing.KVStore.Mock = kvClient
	cfg.IngesterPartitionRing.MinOwnersDuration = 0
	cfg.IngesterPartitionRing.lifecyclerPollingInterval = 10 * time.Millisecond

	ringName := fmt.Sprintf("%s-rc-%d", PartitionRingName, 0)
	ringKey := fmt.Sprintf("%s-rc-%d", PartitionRingKey, 0)
	watcher := ring.NewPartitionRingWatcher(ringName, ringKey, kvClient, log.NewNopLogger(), nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, watcher))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, watcher)) })

	ni, err := NewNullIngester(cfg, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, ni))

	// After startup, partition 0 should be ACTIVE and owned by this instance.
	require.Eventually(t, func() bool {
		pr := watcher.PartitionRing()
		return slices.Contains(pr.ActivePartitionIDs(), int32(0)) &&
			slices.Equal(pr.PartitionOwnerIDs(0), []string{"ingester-zone-a-0"})
	}, 5*time.Second, 10*time.Millisecond, "partition 0 should be ACTIVE and owned by ingester-zone-a-0")

	// After stop, the instance should be removed as owner.
	require.NoError(t, services.StopAndAwaitTerminated(ctx, ni))
	require.Eventually(t, func() bool {
		return slices.Equal(watcher.PartitionRing().PartitionOwnerIDs(0), []string{})
	}, 5*time.Second, 10*time.Millisecond, "partition 0 should have no owners after stop")
}
