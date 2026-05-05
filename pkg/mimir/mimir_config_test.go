// SPDX-License-Identifier: AGPL-3.0-only

package mimir_test

import (
	"flag"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/clusterutil"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/mimir"
	"github.com/grafana/mimir/pkg/storage/bucket"
)

func TestCommonConfigCanBeExtended(t *testing.T) {
	t.Run("flag inheritance", func(t *testing.T) {
		var cfg customExtendedConfig
		fs := flag.NewFlagSet("test", flag.PanicOnError)
		cfg.RegisterFlags(fs, log.NewNopLogger())

		args := []string{
			"-common.storage.backend", "s3",
			"-common.client-cluster-validation.label", "client-cluster",
			"-common.instrument-reference-leaks.percentage", "13.37",
			"-common.instrument-reference-leaks.before-reuse-period", "20h",
			"-common.instrument-reference-leaks.max-inflight-instrumented-bytes", "1048576",
		}
		require.NoError(t, fs.Parse(args))

		require.NoError(t, mimir.InheritCommonFlagValues(log.NewNopLogger(), fs, cfg.MimirConfig.Common, &cfg.MimirConfig, &cfg))

		// Values should be properly inherited.
		require.Equal(t, "s3", cfg.CustomStorage.Backend)
		require.Equal(t, "client-cluster", cfg.CustomClientClusterValidation.Label)

		// Mimir's inheritance should still work.
		checkAllClusterValidationLabels(t, cfg, "client-cluster")

		// Non-inherited flags still work.
		require.Equal(t, 13.37, cfg.MimirConfig.Common.InstrumentRefLeaks.Percentage)
		require.Equal(t, 20*time.Hour, cfg.MimirConfig.Common.InstrumentRefLeaks.BeforeReusePeriod)
		require.Equal(t, uint64(1048576), cfg.MimirConfig.Common.InstrumentRefLeaks.MaxInflightInstrumentedBytes)
	})

	t.Run("yaml inheritance", func(t *testing.T) {
		const commonYAMLConfig = `
common:
  storage:
    backend: s3
  client_cluster_validation:
    label: client-cluster
  instrument_ref_leaks:
    percentage: 13.37
    before_reuse_period: 20h
    max_inflight_instrumented_bytes: 2097152
`

		var cfg customExtendedConfig
		fs := flag.NewFlagSet("test", flag.PanicOnError)
		cfg.RegisterFlags(fs, log.NewNopLogger())

		err := yaml.Unmarshal([]byte(commonYAMLConfig), &cfg)
		require.NoError(t, err)

		// Values should be properly inherited.
		require.Equal(t, "s3", cfg.CustomStorage.Backend)
		require.Equal(t, "client-cluster", cfg.CustomClientClusterValidation.Label)

		// Mimir's inheritance should still work.
		checkAllClusterValidationLabels(t, cfg, "client-cluster")

		// Non-inherited flags should still work.
		require.Equal(t, 13.37, cfg.MimirConfig.Common.InstrumentRefLeaks.Percentage)
		require.Equal(t, 20*time.Hour, cfg.MimirConfig.Common.InstrumentRefLeaks.BeforeReusePeriod)
		require.Equal(t, uint64(2097152), cfg.MimirConfig.Common.InstrumentRefLeaks.MaxInflightInstrumentedBytes)
	})
}

func TestDirectConfigOverwritesCommonConfig(t *testing.T) {
	t.Run("flag inheritance", func(t *testing.T) {
		var cfg customExtendedConfig
		fs := flag.NewFlagSet("test", flag.PanicOnError)
		cfg.RegisterFlags(fs, log.NewNopLogger())

		args := []string{
			"-ingester.client.cluster-validation.label", "ingester-cluster",
			"-common.client-cluster-validation.label", "common-cluster",
			"-querier.store-gateway-client.cluster-validation.label", "store-gateway-cluster",
		}
		require.NoError(t, fs.Parse(args))

		require.NoError(t, mimir.InheritCommonFlagValues(log.NewNopLogger(), fs, cfg.MimirConfig.Common, &cfg.MimirConfig, &cfg))

		// common configuration is correctly set
		require.Equal(t, "common-cluster", cfg.CustomClientClusterValidation.Label)

		// overwritten configurations are correctly set
		require.Equal(t, "ingester-cluster", cfg.MimirConfig.IngesterClient.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "store-gateway-cluster", cfg.MimirConfig.Querier.StoreGatewayClient.ClusterValidation.Label)

		// all other configurations correctly inherit the common configuration
		require.Equal(t, "common-cluster", cfg.MimirConfig.Worker.QueryFrontendGRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.Worker.QuerySchedulerGRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.BlockBuilder.SchedulerConfig.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.Frontend.FrontendV2.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.QueryScheduler.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.Ruler.ClientTLSConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.Ruler.QueryFrontend.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.Alertmanager.AlertmanagerClient.GRPCClientConfig.ClusterValidation.Label)
	})

	t.Run("yaml inheritance", func(t *testing.T) {
		const commonYAMLConfig = `
ingester_client:
  grpc_client_config:
    cluster_validation:
      label: ingester-cluster
common:
  client_cluster_validation:
    label: common-cluster
querier:
  store_gateway_client:
    cluster_validation:
      label: store-gateway-cluster
`

		var cfg customExtendedConfig
		fs := flag.NewFlagSet("test", flag.PanicOnError)
		cfg.RegisterFlags(fs, log.NewNopLogger())

		err := yaml.Unmarshal([]byte(commonYAMLConfig), &cfg)
		require.NoError(t, err)

		// common configuration is correctly set
		require.Equal(t, "common-cluster", cfg.CustomClientClusterValidation.Label)

		// overwritten configurations are correctly set
		require.Equal(t, "ingester-cluster", cfg.MimirConfig.IngesterClient.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "store-gateway-cluster", cfg.MimirConfig.Querier.StoreGatewayClient.ClusterValidation.Label)

		// all other configurations correctly inherit the common configuration
		require.Equal(t, "common-cluster", cfg.MimirConfig.Worker.QueryFrontendGRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.Worker.QuerySchedulerGRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.BlockBuilder.SchedulerConfig.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.Frontend.FrontendV2.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.QueryScheduler.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.Ruler.ClientTLSConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.Ruler.QueryFrontend.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.Alertmanager.AlertmanagerClient.GRPCClientConfig.ClusterValidation.Label)
	})
}

func TestDirectConfigUnsetCommonConfig(t *testing.T) {
	t.Run("flag inheritance", func(t *testing.T) {
		var cfg customExtendedConfig
		fs := flag.NewFlagSet("test", flag.PanicOnError)
		cfg.RegisterFlags(fs, log.NewNopLogger())

		args := []string{
			"-ingester.client.cluster-validation.label", "",
			"-common.client-cluster-validation.label", "common-cluster",
			"-alertmanager.alertmanager-client.cluster-validation.label", "",
		}
		require.NoError(t, fs.Parse(args))

		require.NoError(t, mimir.InheritCommonFlagValues(log.NewNopLogger(), fs, cfg.MimirConfig.Common, &cfg.MimirConfig, &cfg))

		// common configuration is correctly set
		require.Equal(t, "common-cluster", cfg.CustomClientClusterValidation.Label)

		// overwritten configurations are correctly set
		require.Equal(t, "", cfg.MimirConfig.IngesterClient.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "", cfg.MimirConfig.Alertmanager.AlertmanagerClient.GRPCClientConfig.ClusterValidation.Label)

		// all other configurations correctly inherit the common configuration
		require.Equal(t, "common-cluster", cfg.MimirConfig.Worker.QueryFrontendGRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.Worker.QuerySchedulerGRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.BlockBuilder.SchedulerConfig.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.Frontend.FrontendV2.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.Querier.StoreGatewayClient.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.QueryScheduler.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.Ruler.ClientTLSConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.Ruler.QueryFrontend.GRPCClientConfig.ClusterValidation.Label)
	})

	t.Run("yaml inheritance", func(t *testing.T) {
		const commonYAMLConfig = `
ingester_client:
  grpc_client_config:
    cluster_validation:
      label: ''
common:
  client_cluster_validation:
    label: common-cluster
alertmanager:
  alertmanager_client:
    cluster_validation:
      label: ''
`

		var cfg customExtendedConfig
		fs := flag.NewFlagSet("test", flag.PanicOnError)
		cfg.RegisterFlags(fs, log.NewNopLogger())

		err := yaml.Unmarshal([]byte(commonYAMLConfig), &cfg)
		require.NoError(t, err)

		// common configuration is correctly set
		require.Equal(t, "common-cluster", cfg.CustomClientClusterValidation.Label)

		// unset configurations are empty
		require.Equal(t, "", cfg.MimirConfig.IngesterClient.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "", cfg.MimirConfig.Alertmanager.AlertmanagerClient.GRPCClientConfig.ClusterValidation.Label)

		// all other configurations correctly inherit the common configuration
		require.Equal(t, "common-cluster", cfg.MimirConfig.Worker.QueryFrontendGRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.Worker.QuerySchedulerGRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.BlockBuilder.SchedulerConfig.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.Frontend.FrontendV2.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.Querier.StoreGatewayClient.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.QueryScheduler.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.Ruler.ClientTLSConfig.ClusterValidation.Label)
		require.Equal(t, "common-cluster", cfg.MimirConfig.Ruler.QueryFrontend.GRPCClientConfig.ClusterValidation.Label)
	})
}

type customExtendedConfig struct {
	MimirConfig                   mimir.Config                        `yaml:",inline"`
	CustomStorage                 bucket.Config                       `yaml:"custom_storage"`
	CustomClientClusterValidation clusterutil.ClusterValidationConfig `yaml:"custom_client_cluster_validation"`
}

func (c *customExtendedConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	c.MimirConfig.RegisterFlags(f, logger)
	c.CustomStorage.RegisterFlagsWithPrefix("custom-storage", f)
	c.CustomClientClusterValidation.RegisterFlagsWithPrefix("custom-client-cluster-validation", f)
}

func (c *customExtendedConfig) CommonConfigInheritance() mimir.CommonConfigInheritance {
	return mimir.CommonConfigInheritance{
		Storage: map[string]*bucket.StorageBackendConfig{
			"custom": &c.CustomStorage.StorageBackendConfig,
		},
		ClientClusterValidation: map[string]*clusterutil.ClusterValidationConfig{
			"custom_client_cluster_validation": &c.CustomClientClusterValidation,
		},
	}
}

func (c *customExtendedConfig) UnmarshalYAML(value *yaml.Node) error {
	if err := mimir.UnmarshalCommonYAML(value, &c.MimirConfig, c); err != nil {
		return err
	}

	type plain customExtendedConfig
	return value.DecodeWithOptions((*plain)(c), yaml.DecodeOptions{KnownFields: true})
}

func TestMimirConfigCanBeInlined(t *testing.T) {
	const commonYAMLConfig = `
custom_storage:
  backend: s3
custom_client_cluster_validation:
  label: client-cluster
`

	var cfg customExtendedConfig
	fs := flag.NewFlagSet("test", flag.PanicOnError)
	cfg.RegisterFlags(fs, log.NewNopLogger())

	err := yaml.Unmarshal([]byte(commonYAMLConfig), &cfg)
	require.NoError(t, err)

	// Value should be properly set.
	require.Equal(t, "s3", cfg.CustomStorage.Backend)
	require.Equal(t, "client-cluster", cfg.CustomClientClusterValidation.Label)
}

func TestRemovedGlobalHATrackerTimeouts(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name       string
		yamlConfig string
	}{{
		name: "keeps values in limit",
		yamlConfig: `
limits:
  ha_tracker_update_timeout: 13s
  ha_tracker_update_timeout_jitter_max: 3s
  ha_tracker_failover_timeout: 26s
  `,
	}, {
		name: "ignores values in removed distributor.ha_tracker",
		yamlConfig: `
limits:
  ha_tracker_update_timeout: 13s
  ha_tracker_update_timeout_jitter_max: 3s
  ha_tracker_failover_timeout: 26s

distributor:
  ha_tracker:
    ha_tracker_update_timeout: 1s
    ha_tracker_update_timeout_jitter_max: 1s
    ha_tracker_failover_timeout: 3s
  `,
	}} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			var cfg mimir.Config
			fs := flag.NewFlagSet("test", flag.PanicOnError)
			cfg.RegisterFlags(fs, log.NewNopLogger())

			err := yaml.Unmarshal([]byte(testCase.yamlConfig), &cfg)
			require.NoError(t, err)

			err = cfg.Validate(log.NewNopLogger())
			require.NoError(t, err)

			require.Equal(t, 13*time.Second, time.Duration(cfg.LimitsConfig.HATrackerUpdateTimeout))
			require.Equal(t, 3*time.Second, time.Duration(cfg.LimitsConfig.HATrackerUpdateTimeoutJitterMax))
			require.Equal(t, 26*time.Second, time.Duration(cfg.LimitsConfig.HATrackerFailoverTimeout))

		})
	}
}

func checkAllClusterValidationLabels(t *testing.T, cfg customExtendedConfig, expectedValue string) {
	require.Equal(t, expectedValue, cfg.MimirConfig.IngesterClient.GRPCClientConfig.ClusterValidation.Label)
	require.Equal(t, expectedValue, cfg.MimirConfig.Worker.QueryFrontendGRPCClientConfig.ClusterValidation.Label)
	require.Equal(t, expectedValue, cfg.MimirConfig.Worker.QuerySchedulerGRPCClientConfig.ClusterValidation.Label)
	require.Equal(t, expectedValue, cfg.MimirConfig.BlockBuilder.SchedulerConfig.GRPCClientConfig.ClusterValidation.Label)
	require.Equal(t, expectedValue, cfg.MimirConfig.Frontend.FrontendV2.GRPCClientConfig.ClusterValidation.Label)
	require.Equal(t, expectedValue, cfg.MimirConfig.Querier.StoreGatewayClient.ClusterValidation.Label)
	require.Equal(t, expectedValue, cfg.MimirConfig.QueryScheduler.GRPCClientConfig.ClusterValidation.Label)
	require.Equal(t, expectedValue, cfg.MimirConfig.Ruler.ClientTLSConfig.ClusterValidation.Label)
	require.Equal(t, expectedValue, cfg.MimirConfig.Ruler.QueryFrontend.GRPCClientConfig.ClusterValidation.Label)
	require.Equal(t, expectedValue, cfg.MimirConfig.Alertmanager.AlertmanagerClient.GRPCClientConfig.ClusterValidation.Label)
}
