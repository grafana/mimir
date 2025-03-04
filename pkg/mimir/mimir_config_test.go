// SPDX-License-Identifier: AGPL-3.0-only

package mimir_test

import (
	"flag"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"

	"github.com/grafana/mimir/pkg/mimir"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/util"
)

func TestCommonConfigCanBeExtended(t *testing.T) {
	t.Run("flag inheritance", func(t *testing.T) {
		var cfg customExtendedConfig
		fs := flag.NewFlagSet("test", flag.PanicOnError)
		cfg.RegisterFlags(fs, log.NewNopLogger())

		args := []string{
			"-common.storage.backend", "s3",
			"-common.grpc-client.grpc-max-recv-msg-size", "1000000",
			"-common.grpc-client.cluster-validation-label", "cluster",
		}
		require.NoError(t, fs.Parse(args))

		require.NoError(t, mimir.InheritCommonFlagValues(log.NewNopLogger(), fs, cfg.MimirConfig.Common, &cfg.MimirConfig, &cfg))

		// Values should be properly inherited.
		require.Equal(t, "s3", cfg.CustomStorage.Backend)
		require.Equal(t, 1000000, cfg.CustomIngesterClient.GRPCClientConfig.MaxRecvMsgSize)
		require.Equal(t, "cluster", cfg.CustomIngesterClient.GRPCClientConfig.ClusterValidationLabel)

		// Mimir's inheritance should still work.
		require.Equal(t, "s3", cfg.MimirConfig.BlocksStorage.Bucket.Backend)
		require.Equal(t, 1000000, cfg.MimirConfig.IngesterClient.GRPCClientConfig.MaxRecvMsgSize)
		require.Equal(t, "cluster", cfg.MimirConfig.IngesterClient.GRPCClientConfig.ClusterValidationLabel)
	})

	t.Run("yaml inheritance", func(t *testing.T) {
		const commonYAMLConfig = `
common:
  storage:
    backend: s3
  grpc_client:
    max_recv_msg_size: 1000000
    cluster_validation_label: cluster
`

		var cfg customExtendedConfig
		fs := flag.NewFlagSet("test", flag.PanicOnError)
		cfg.RegisterFlags(fs, log.NewNopLogger())

		err := yaml.Unmarshal([]byte(commonYAMLConfig), &cfg)
		require.NoError(t, err)

		// Values should be properly inherited.
		require.Equal(t, "s3", cfg.CustomStorage.Backend)
		require.Equal(t, 1000000, cfg.CustomIngesterClient.GRPCClientConfig.MaxRecvMsgSize)
		require.Equal(t, "cluster", cfg.CustomIngesterClient.GRPCClientConfig.ClusterValidationLabel)

		// Mimir's inheritance should still work.
		require.Equal(t, "s3", cfg.MimirConfig.BlocksStorage.Bucket.Backend)
		require.Equal(t, 1000000, cfg.MimirConfig.IngesterClient.GRPCClientConfig.MaxRecvMsgSize)
		require.Equal(t, "cluster", cfg.MimirConfig.IngesterClient.GRPCClientConfig.ClusterValidationLabel)
	})
}

type customExtendedConfig struct {
	MimirConfig          mimir.Config           `yaml:",inline"`
	CustomStorage        bucket.Config          `yaml:"custom_storage"`
	CustomIngesterClient ingester_client.Config `yaml:"custom_ingester_client"`
}

func (c *customExtendedConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	c.MimirConfig.RegisterFlags(f, logger)
	c.CustomStorage.RegisterFlagsWithPrefix("custom-storage", f)
	c.CustomIngesterClient.RegisterFlagsWithPrefix("custom-ingester-client", f)
}

func (c *customExtendedConfig) CommonConfigInheritance() mimir.CommonConfigInheritance {
	return mimir.CommonConfigInheritance{
		Storage: map[string]*bucket.StorageBackendConfig{
			"custom": &c.CustomStorage.StorageBackendConfig,
		},
		GRPCClient: map[string]*util.GRPCClientConfig{
			"custom_grpc_client": &c.CustomIngesterClient.GRPCClientConfig,
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
custom_ingester_client:
  grpc_client_config:
    max_recv_msg_size: 1000000
    cluster_validation_label: cluster
`

	var cfg customExtendedConfig
	fs := flag.NewFlagSet("test", flag.PanicOnError)
	cfg.RegisterFlags(fs, log.NewNopLogger())

	err := yaml.Unmarshal([]byte(commonYAMLConfig), &cfg)
	require.NoError(t, err)

	// Value should be properly set.
	require.Equal(t, "s3", cfg.CustomStorage.Backend)
	require.Equal(t, 1000000, cfg.CustomIngesterClient.GRPCClientConfig.MaxRecvMsgSize)
	require.Equal(t, "cluster", cfg.CustomIngesterClient.GRPCClientConfig.ClusterValidationLabel)
}
