// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestConfig_Validate(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		cfg, _ := blockBuilderConfig(t, "kafka:9092", nil)
		require.NoError(t, cfg.Validate())
	})

	t.Run("empty instance-id assignment", func(t *testing.T) {
		cfg, _ := blockBuilderConfig(t, "kafka:9092", nil)

		cfg.InstanceID = ""
		require.Error(t, cfg.Validate())
	})

	t.Run("empty data dir", func(t *testing.T) {
		cfg, _ := blockBuilderConfig(t, "kafka:9092", nil)

		cfg.DataDir = ""
		require.Error(t, cfg.Validate())
	})
}

// Asserts that a single base config is returned when compartments are disabled, and one per-cluster
// config (with the write compartment ID templated into the address) when enabled.
func TestConfig_kafkaClientConfigs(t *testing.T) {
	t.Run("compartments disabled returns the single base config", func(t *testing.T) {
		cfg, _ := blockBuilderConfig(t, "kafka:9092", nil)
		require.False(t, cfg.Compartments.Enabled)

		cfgs := cfg.kafkaClientConfigs()
		require.Len(t, cfgs, 1)
		require.Equal(t, cfg.Kafka.Address, cfgs[0].Address)
	})

	t.Run("compartments enabled returns one config per write compartment", func(t *testing.T) {
		cfg, _ := blockBuilderConfig(t, "kafka-"+compartments.WriteCompartmentIDPlaceholder+":9092", nil)
		cfg.Compartments = compartments.Config{
			Enabled: true,
			Read:    compartments.ReadConfig{NumCompartments: 1},
			Write:   compartments.WriteConfig{NumCompartments: 3},
		}

		cfgs := cfg.kafkaClientConfigs()
		require.Len(t, cfgs, 3)
		// Each config targets its own write compartment's Kafka cluster.
		require.Equal(t, flagext.StringSliceCSV{"kafka-0:9092"}, cfgs[0].Address)
		require.Equal(t, flagext.StringSliceCSV{"kafka-1:9092"}, cfgs[1].Address)
		require.Equal(t, flagext.StringSliceCSV{"kafka-2:9092"}, cfgs[2].Address)
	})
}

const (
	testTopic = "test"

	numPartitions = 2
)

func blockBuilderConfig(t testing.TB, kafkaAddr string, tenantLimits validation.TenantLimits) (Config, *validation.Overrides) {
	cfg := Config{}
	flagext.DefaultValues(&cfg)

	cfg.InstanceID = "block-builder-0"
	cfg.DataDir = t.TempDir()

	cfg.SchedulerConfig = SchedulerConfig{
		Address:        "localhost:099",
		UpdateInterval: 20 * time.Millisecond,
		MaxUpdateAge:   1 * time.Second,
	}

	// Kafka related options.
	flagext.DefaultValues(&cfg.Kafka)
	cfg.Kafka.Address = flagext.StringSliceCSV{kafkaAddr}
	cfg.Kafka.Topic = testTopic

	// Block storage related options.
	flagext.DefaultValues(&cfg.BlocksStorage)
	cfg.BlocksStorage.Bucket.Backend = bucket.Filesystem
	cfg.BlocksStorage.Bucket.Filesystem.Directory = t.TempDir()

	limits := defaultLimitsTestConfig()
	limits.OutOfOrderTimeWindow = 2 * model.Duration(time.Hour)
	limits.OutOfOrderBlocksExternalLabelEnabled = true // Needed to reproduce a panic.
	limits.NativeHistogramsIngestionEnabled = true
	overrides := validation.NewOverrides(limits, tenantLimits)

	return cfg, overrides
}
