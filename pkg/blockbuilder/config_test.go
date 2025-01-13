// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestConfig_Validate(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		cfg, _ := blockBuilderConfig(t, "kafka:9092")
		require.NoError(t, cfg.Validate())
	})

	t.Run("empty partition assignment", func(t *testing.T) {
		cfg, _ := blockBuilderConfig(t, "kafka:9092")

		cfg.PartitionAssignment = map[string][]int32{}
		require.Error(t, cfg.Validate())
	})

	t.Run("empty instance-id assignment", func(t *testing.T) {
		cfg, _ := blockBuilderConfig(t, "kafka:9092")

		cfg.InstanceID = ""
		// Instance 0 isn't present in the assignment
		cfg.PartitionAssignment = map[string][]int32{
			"block-builder-0": {1},
		}
		require.Error(t, cfg.Validate())
	})

	t.Run("bad partition assignment", func(t *testing.T) {
		cfg, _ := blockBuilderConfig(t, "kafka:9092")

		cfg.InstanceID = "block-builder-0"
		// Instance 0 isn't present in the assignment
		cfg.PartitionAssignment = map[string][]int32{
			"block-builder-1":   {1},
			"block-builder-100": {10},
		}
		require.Error(t, cfg.Validate())
	})

	t.Run("empty data dir", func(t *testing.T) {
		cfg, _ := blockBuilderConfig(t, "kafka:9092")

		cfg.DataDir = ""
		require.Error(t, cfg.Validate())
	})

	t.Run("bad consume-interval", func(t *testing.T) {
		cfg, _ := blockBuilderConfig(t, "kafka:9092")

		cfg.ConsumeInterval = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("bad lookback_on_no_commit", func(t *testing.T) {
		cfg, _ := blockBuilderConfig(t, "kafka:9092")

		cfg.LookbackOnNoCommit = -1
		require.Error(t, cfg.Validate())
	})
}

const (
	testTopic = "test"
	testGroup = "testgroup"

	numPartitions = 2
)

func blockBuilderConfig(t *testing.T, addr string) (Config, *validation.Overrides) {
	cfg := Config{}
	flagext.DefaultValues(&cfg)

	cfg.InstanceID = "block-builder-0"
	cfg.PartitionAssignment = map[string][]int32{
		"block-builder-0": {0, 1}, // instance 0 -> partitions 0 and 1
	}
	cfg.ConsumerGroup = testGroup
	cfg.DataDir = t.TempDir()

	// Kafka related options.
	flagext.DefaultValues(&cfg.Kafka)
	cfg.Kafka.Address = addr
	cfg.Kafka.Topic = testTopic

	// Block storage related options.
	flagext.DefaultValues(&cfg.BlocksStorage)
	cfg.BlocksStorage.Bucket.StorageBackendConfig.Backend = bucket.Filesystem
	cfg.BlocksStorage.Bucket.Filesystem.Directory = t.TempDir()

	limits := defaultLimitsTestConfig()
	limits.OutOfOrderTimeWindow = 2 * model.Duration(time.Hour)
	limits.NativeHistogramsIngestionEnabled = true
	overrides, err := validation.NewOverrides(limits, nil)
	require.NoError(t, err)

	return cfg, overrides
}

func blockBuilderPullModeConfig(t *testing.T, addr string) (Config, *validation.Overrides) {
	cfg, overrides := blockBuilderConfig(t, addr)
	cfg.SchedulerConfig = SchedulerConfig{
		Address:        "localhost:099", // Trigger pull mode initialization.
		UpdateInterval: 20 * time.Millisecond,
		MaxUpdateAge:   1 * time.Second,
	}
	return cfg, overrides
}
