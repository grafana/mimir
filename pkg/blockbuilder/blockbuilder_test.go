// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	testTopic = "test"
	testGroup = "testgroup"
)

func blockBuilderConfig(t *testing.T, addr string) (Config, *validation.Overrides) {
	cfg := Config{}
	flagext.DefaultValues(&cfg)

	cfg.InstanceID = "block-builder-0"
	cfg.PartitionAssignment = map[string][]int32{
		"block-builder-0": {0}, // instance 0 -> partition 0
	}

	// Kafka related options.
	cfg.Kafka.Address = addr
	cfg.Kafka.Topic = testTopic
	cfg.Kafka.ConsumerGroup = testGroup

	// Block storage related options.
	cfg.BlocksStorageConfig.TSDB.Dir = t.TempDir()
	cfg.BlocksStorageConfig.Bucket.StorageBackendConfig.Backend = bucket.Filesystem
	cfg.BlocksStorageConfig.Bucket.Filesystem.Directory = t.TempDir()

	limits := defaultLimitsTestConfig()
	limits.OutOfOrderTimeWindow = 2 * model.Duration(time.Hour)
	limits.NativeHistogramsIngestionEnabled = true
	overrides, err := validation.NewOverrides(limits, nil)
	require.NoError(t, err)

	return cfg, overrides
}

func TestBlockBuilder_NextConsumeCycle(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	const numPartitions = 2

	_, addr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)
	kafkaClient := mustKafkaClient(t, addr)

	produceRecords(ctx, t, kafkaClient, time.Now().Add(-time.Hour), "1", testTopic, 0, []byte(`test value`))

	cfg, overrides := blockBuilderConfig(t, addr)
	cfg.PartitionAssignment = map[string][]int32{
		"block-builder-0": {0, 1}, // instance 0 -> partitions 0, 1
	}

	reg := prometheus.NewPedanticRegistry()
	bb, err := New(cfg, test.NewTestingLogger(t), reg, overrides)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, bb))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, bb))
	})

	// Explicitly call NextConsumeCycle and verify that we observed the expected per-partition lag.
	err = bb.NextConsumeCycle(ctx, time.Now())
	require.NoError(t, err)

	require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_blockbuilder_consumer_lag_records The per-topic-partition number of records, instance needs to work through each cycle.
		# TYPE cortex_blockbuilder_consumer_lag_records gauge
		cortex_blockbuilder_consumer_lag_records{partition="0",topic="test"} 1
		cortex_blockbuilder_consumer_lag_records{partition="1",topic="test"} 0
	`), "cortex_blockbuilder_consumer_lag_records"))
}
