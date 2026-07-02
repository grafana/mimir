// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestMultiClusterPartitionReader_ConsumesFromAllWriteCompartments(t *testing.T) {
	const (
		readTopic            = "ingest-rc-0"
		partitionID          = int32(0)
		tenantID             = "user-1"
		numWriteCompartments = 3
	)

	ctx := context.Background()

	// Run one Kafka cluster per write compartment, and produce a distinct series to each so we can
	// assert the reader unions records from every cluster.
	clusterConfigs := make([]KafkaConfig, numWriteCompartments)
	expectedMetricNames := make(map[string]struct{}, numWriteCompartments)
	for writeCompartmentID := 0; writeCompartmentID < numWriteCompartments; writeCompartmentID++ {
		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, readTopic)
		clusterConfigs[writeCompartmentID] = createTestKafkaConfig(clusterAddr, readTopic)

		writer, _ := createTestWriter(t, clusterConfigs[writeCompartmentID])
		metricName := fmt.Sprintf("series_wc_%d", writeCompartmentID)
		expectedMetricNames[metricName] = struct{}{}
		req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries(metricName)}, Source: mimirpb.API}
		require.NoError(t, writer.WriteSync(ctx, readTopic, partitionID, tenantID, req))
	}

	var mtx sync.Mutex
	receivedMetricNames := map[string]struct{}{}
	pusher := pusherFunc(func(_ context.Context, req *mimirpb.WriteRequest) error {
		mtx.Lock()
		defer mtx.Unlock()
		for _, ts := range req.Timeseries {
			for _, lbl := range ts.Labels {
				if lbl.Name == "__name__" {
					receivedMetricNames[lbl.Value] = struct{}{}
				}
			}
		}
		return nil
	})

	reg := prometheus.NewPedanticRegistry()
	reader, err := NewMultiClusterPartitionReader(clusterConfigs, partitionID, "ingester-0", multiClusterTestOffsetFilePath(t), pusher, log.NewNopLogger(), reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, reader)) })

	// Every cluster's record is eventually pushed.
	require.Eventually(t, func() bool {
		mtx.Lock()
		defer mtx.Unlock()
		return len(receivedMetricNames) == numWriteCompartments
	}, 10*time.Second, 100*time.Millisecond)

	mtx.Lock()
	assert.Equal(t, expectedMetricNames, receivedMetricNames)
	mtx.Unlock()

	// Now that everything produced so far has been consumed, waiting for read consistency returns
	// promptly across all clusters.
	require.NoError(t, reader.WaitReadConsistencyUntilLastProducedOffset(ctx))

	// Reader metrics are registered per cluster (each per-cluster reader's registerer is wrapped with a
	// distinct write_compartment label). Each cluster consumed exactly one record, so it pushed one write
	// request and missed no records.
	require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingest_storage_reader_requests_total Number of attempted write requests after batching records from Kafka.
		# TYPE cortex_ingest_storage_reader_requests_total counter
		cortex_ingest_storage_reader_requests_total{write_compartment="0"} 1
		cortex_ingest_storage_reader_requests_total{write_compartment="1"} 1
		cortex_ingest_storage_reader_requests_total{write_compartment="2"} 1

		# HELP cortex_ingest_storage_reader_missed_records_total The number of offsets that were never consumed by the reader because they weren't fetched.
		# TYPE cortex_ingest_storage_reader_missed_records_total counter
		cortex_ingest_storage_reader_missed_records_total{write_compartment="0"} 0
		cortex_ingest_storage_reader_missed_records_total{write_compartment="1"} 0
		cortex_ingest_storage_reader_missed_records_total{write_compartment="2"} 0
	`),
		"cortex_ingest_storage_reader_requests_total",
		"cortex_ingest_storage_reader_missed_records_total"))
}

func TestMultiClusterPartitionReader_FailsToStartIfAnyClusterReaderFailsToStart(t *testing.T) {
	const (
		readTopic   = "ingest-rc-0"
		partitionID = int32(0)
	)

	ctx := context.Background()

	// Write compartment 0 points at a healthy cluster.
	_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, readTopic)
	healthyCfg := createTestKafkaConfig(clusterAddr, readTopic)

	// Write compartment 1 is misconfigured so that its reader fails to start: with file-based offset
	// enforcement enabled but no replay period and consumption from the last committed offset,
	// getStartOffset returns an error synchronously at startup.
	brokenCfg := createTestKafkaConfig(clusterAddr, readTopic)
	brokenCfg.ConsumerGroupOffsetCommitFileEnforced = true
	brokenCfg.MaxReplayPeriod = 0
	brokenCfg.ConsumeFromPositionAtStartup = consumeFromLastOffset

	reader, err := NewMultiClusterPartitionReader([]KafkaConfig{healthyCfg, brokenCfg}, partitionID, "ingester-0", multiClusterTestOffsetFilePath(t), pusherFunc(func(context.Context, *mimirpb.WriteRequest) error { return nil }), log.NewNopLogger(), prometheus.NewPedanticRegistry())
	require.NoError(t, err)

	// Starting must fail: an ingester must not start if it cannot consume from every write compartment.
	err = services.StartAndAwaitRunning(ctx, reader)
	require.Error(t, err)

	// Ensure the reader is fully terminated.
	_ = services.StopAndAwaitTerminated(ctx, reader)
}

func TestNewMultiClusterPartitionReader(t *testing.T) {
	noopPusher := pusherFunc(func(context.Context, *mimirpb.WriteRequest) error { return nil })

	t.Run("rejects empty cluster configs", func(t *testing.T) {
		_, err := NewMultiClusterPartitionReader(nil, 0, "ingester-0", multiClusterTestOffsetFilePath(t), noopPusher, log.NewNopLogger(), prometheus.NewPedanticRegistry())
		require.Error(t, err)
	})

	t.Run("rejects offset file path without the write compartment placeholder", func(t *testing.T) {
		_, clusterAddr := testkafka.CreateCluster(t, 1, "ingest-rc-0")
		clusterConfigs := []KafkaConfig{createTestKafkaConfig(clusterAddr, "ingest-rc-0")}

		// The offset file path is missing the write compartment placeholder, so the per-cluster offset
		// files would collide.
		offsetFilePath := filepath.Join(t.TempDir(), "kafka-offset.json")
		_, err := NewMultiClusterPartitionReader(clusterConfigs, 0, "ingester-0", offsetFilePath, noopPusher, log.NewNopLogger(), prometheus.NewPedanticRegistry())
		require.ErrorContains(t, err, "must contain")
	})
}

// multiClusterTestOffsetFilePath returns an offset file path template containing the write compartment
// placeholder, as required by NewMultiClusterPartitionReader.
func multiClusterTestOffsetFilePath(t *testing.T) string {
	return filepath.Join(t.TempDir(), "kafka-offset-wc-"+compartments.WriteCompartmentIDPlaceholder+".json")
}

func TestMultiClusterPartitionReader_WaitReadConsistencyUntilOffsets_RejectsKafkaClusterCountMismatch(t *testing.T) {
	const (
		readTopic   = "ingest-rc-0"
		partitionID = int32(0)
	)

	// A reader consuming from 2 Kafka clusters. The clusters don't need to be running: the cluster count
	// invariant is checked before any cluster interaction.
	_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, readTopic)
	clusterConfigs := []KafkaConfig{createTestKafkaConfig(clusterAddr, readTopic), createTestKafkaConfig(clusterAddr, readTopic)}

	reader, err := NewMultiClusterPartitionReader(clusterConfigs, partitionID, "ingester-0", multiClusterTestOffsetFilePath(t), pusherFunc(func(context.Context, *mimirpb.WriteRequest) error { return nil }), log.NewNopLogger(), prometheus.NewPedanticRegistry())
	require.NoError(t, err)

	// Fewer offsets than Kafka clusters is an invariant violation.
	err = reader.WaitReadConsistencyUntilOffsets(context.Background(), kmeta.NewSingleClusterPartitionOffsets(10))
	require.ErrorContains(t, err, "consumes from 2 Kafka clusters but was given read consistency offsets for 1")

	// More offsets than Kafka clusters is an invariant violation too.
	err = reader.WaitReadConsistencyUntilOffsets(context.Background(), kmeta.NewMultiClusterPartitionOffsets([]int64{1, 2, 3}))
	require.ErrorContains(t, err, "consumes from 2 Kafka clusters but was given read consistency offsets for 3")
}

func TestMultiClusterPartitionReader_WaitReadConsistencyUntilOffsets_RoutesOffsetsPerCluster(t *testing.T) {
	const (
		readTopic   = "ingest-rc-0"
		partitionID = int32(0)
		tenantID    = "user-1"
	)

	ctx := context.Background()

	// Two clusters with a different number of records on the same partition, so each ends up with a distinct
	// last-seen offset (cluster 0 -> 2, cluster 1 -> 0). This lets us prove each per-cluster offset is routed
	// to the matching cluster's reader.
	_, addr0 := testkafka.CreateCluster(t, partitionID+1, readTopic)
	_, addr1 := testkafka.CreateCluster(t, partitionID+1, readTopic)
	cfg0 := createTestKafkaConfig(addr0, readTopic)
	cfg1 := createTestKafkaConfig(addr1, readTopic)

	writer0, _ := createTestWriter(t, cfg0)
	writer1, _ := createTestWriter(t, cfg1)
	writeReq := func(name string) *mimirpb.WriteRequest {
		return &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries(name)}, Source: mimirpb.API}
	}
	for i := 0; i < 3; i++ {
		require.NoError(t, writer0.WriteSync(ctx, readTopic, partitionID, tenantID, writeReq(fmt.Sprintf("c0_%d", i))))
	}
	require.NoError(t, writer1.WriteSync(ctx, readTopic, partitionID, tenantID, writeReq("c1_0")))

	pusher := pusherFunc(func(context.Context, *mimirpb.WriteRequest) error { return nil })
	reader, err := NewMultiClusterPartitionReader([]KafkaConfig{cfg0, cfg1}, partitionID, "ingester-0", multiClusterTestOffsetFilePath(t), pusher, log.NewNopLogger(), prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, reader)) })

	// Ensure everything produced has been consumed, then read each cluster's last-seen offset.
	require.NoError(t, reader.WaitReadConsistencyUntilLastProducedOffset(ctx))
	require.Equal(t, kmeta.NewMultiClusterPartitionOffsets([]int64{2, 0}), reader.LastSeenOffsets())

	// Each cluster's own last-seen offset is already satisfied, so this returns promptly.
	require.NoError(t, reader.WaitReadConsistencyUntilOffsets(ctx, kmeta.NewMultiClusterPartitionOffsets([]int64{2, 0})))

	// Swap the offsets: cluster 0 is asked for 0 (already satisfied) while cluster 1 is asked for 2 (never
	// produced on cluster 1). With correct routing this blocks on cluster 1 until the context is canceled; if
	// the offsets were misrouted, both would be satisfied and it would return nil.
	swapCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err = reader.WaitReadConsistencyUntilOffsets(swapCtx, kmeta.NewMultiClusterPartitionOffsets([]int64{0, 2}))
	require.Error(t, err)
	require.ErrorContains(t, err, "write compartment 1")
}
