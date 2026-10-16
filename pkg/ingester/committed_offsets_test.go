// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"testing"

	"fmt"

	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/require"
)

func TestCommittedOffsetsKafkaConfigs(t *testing.T) {
	t.Run("without compartments returns the explicit config unchanged", func(t *testing.T) {
		cfg := defaultIngesterTestConfig(t)
		cfg.Compartments.Enabled = false
		cfg.BlocksStorageConfig.TSDB.OffsetCatalogue.Kafka.Address = flagext.StringSliceCSV{"kafka-mirror:9092"}
		cfg.BlocksStorageConfig.TSDB.OffsetCatalogue.Kafka.Topic = "mimir-ingest"

		configs := committedOffsetsKafkaConfigs(cfg)

		require.Len(t, configs, 1)
		require.Equal(t, flagext.StringSliceCSV{"kafka-mirror:9092"}, configs[0].Address)
		require.Equal(t, "mimir-ingest", configs[0].Topic)
	})

	t.Run("resolves the read compartment of the topic", func(t *testing.T) {
		cfg := defaultIngesterTestConfig(t)
		cfg.Compartments.Enabled = false
		cfg.ReadCompartmentID = 2
		cfg.BlocksStorageConfig.TSDB.OffsetCatalogue.Kafka.Address = flagext.StringSliceCSV{"kafka-mirror:9092"}
		cfg.BlocksStorageConfig.TSDB.OffsetCatalogue.Kafka.Topic = "mimir-ingest-rc-<read-compartment-id>"

		configs := committedOffsetsKafkaConfigs(cfg)

		require.Len(t, configs, 1)
		require.Equal(t, "mimir-ingest-rc-2", configs[0].Topic)
	})

	t.Run("resolves one mirror cluster per write compartment", func(t *testing.T) {
		cfg := defaultIngesterTestConfig(t)
		cfg.Compartments.Enabled = true
		cfg.Compartments.Read.NumCompartments = 2
		cfg.Compartments.Write.NumCompartments = 2
		cfg.ReadCompartmentID = 1
		cfg.BlocksStorageConfig.TSDB.OffsetCatalogue.Kafka.Address = flagext.StringSliceCSV{"kafka-mirror-wc-<write-compartment-id>:9092"}
		cfg.BlocksStorageConfig.TSDB.OffsetCatalogue.Kafka.Topic = "mimir-ingest-rc-<read-compartment-id>"
		cfg.BlocksStorageConfig.TSDB.OffsetCatalogue.Kafka.SASL.Username = "user-wc-<write-compartment-id>"

		configs := committedOffsetsKafkaConfigs(cfg)

		require.Len(t, configs, 2)
		for clusterID, kafkaCfg := range configs {
			// WriteCompartmentConfigs resolves the write-compartment placeholders in the address
			// and SASL credentials, mirroring how the ingester resolves its consumption clusters.
			require.Equal(t, flagext.StringSliceCSV{"kafka-mirror-wc-" + fmt.Sprint(clusterID) + ":9092"}, kafkaCfg.Address)
			require.Equal(t, "mimir-ingest-rc-1", kafkaCfg.Topic)
			require.Equal(t, "user-wc-"+fmt.Sprint(clusterID), kafkaCfg.SASL.Username)
		}
	})
}
