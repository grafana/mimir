// SPDX-License-Identifier: AGPL-3.0-only

package mimir_test

import (
	"flag"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/clusterutil"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

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
		}
		require.NoError(t, fs.Parse(args))

		require.NoError(t, mimir.InheritCommonFlagValues(log.NewNopLogger(), fs, cfg.MimirConfig.Common, &cfg.MimirConfig, &cfg))

		// Values should be properly inherited.
		require.Equal(t, "s3", cfg.CustomStorage.Backend)
		require.Equal(t, "client-cluster", cfg.CustomClientClusterValidation.Label)

		// Mimir's inheritance should still work.
		require.Equal(t, "s3", cfg.MimirConfig.BlocksStorage.Bucket.Backend)
		require.Equal(t, "client-cluster", cfg.MimirConfig.IngesterClient.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "client-cluster", cfg.MimirConfig.Worker.QueryFrontendGRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "client-cluster", cfg.MimirConfig.Worker.QuerySchedulerGRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "client-cluster", cfg.MimirConfig.BlockBuilder.SchedulerConfig.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "client-cluster", cfg.MimirConfig.Frontend.FrontendV2.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "client-cluster", cfg.MimirConfig.Querier.StoreGatewayClient.ClusterValidation.Label)
		require.Equal(t, "client-cluster", cfg.MimirConfig.QueryScheduler.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "client-cluster", cfg.MimirConfig.Ruler.ClientTLSConfig.ClusterValidation.Label)
		require.Equal(t, "client-cluster", cfg.MimirConfig.Ruler.QueryFrontend.GRPCClientConfig.ClusterValidation.Label)
	})

	t.Run("yaml inheritance", func(t *testing.T) {
		const commonYAMLConfig = `
common:
  storage:
    backend: s3
  client_cluster_validation: 
    label: client-cluster
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
		require.Equal(t, "s3", cfg.MimirConfig.BlocksStorage.Bucket.Backend)
		require.Equal(t, "client-cluster", cfg.MimirConfig.IngesterClient.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "client-cluster", cfg.MimirConfig.Worker.QueryFrontendGRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "client-cluster", cfg.MimirConfig.Worker.QuerySchedulerGRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "client-cluster", cfg.MimirConfig.BlockBuilder.SchedulerConfig.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "client-cluster", cfg.MimirConfig.Frontend.FrontendV2.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "client-cluster", cfg.MimirConfig.Querier.StoreGatewayClient.ClusterValidation.Label)
		require.Equal(t, "client-cluster", cfg.MimirConfig.QueryScheduler.GRPCClientConfig.ClusterValidation.Label)
		require.Equal(t, "client-cluster", cfg.MimirConfig.Ruler.ClientTLSConfig.ClusterValidation.Label)
		require.Equal(t, "client-cluster", cfg.MimirConfig.Ruler.QueryFrontend.GRPCClientConfig.ClusterValidation.Label)
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
