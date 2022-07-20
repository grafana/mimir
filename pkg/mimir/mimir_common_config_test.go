// SPDX-License-Identifier: AGPL-3.0-only

package mimir_test

import (
	"flag"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/mimir"
	"github.com/grafana/mimir/pkg/storage/bucket"
)

func TestCommonConfigCanBeExtended(t *testing.T) {
	t.Run("flag inheritance", func(t *testing.T) {
		var cfg customExtendedConfig
		cfg.MimirConfig.Common.ExtraSpecificStorageConfigs = map[string]*bucket.StorageBackendConfig{
			"custom_storage": &cfg.CustomStorage.StorageBackendConfig,
		}

		fs := flag.NewFlagSet("test", flag.PanicOnError)
		cfg.RegisterFlags(fs, log.NewNopLogger())

		args := []string{"-common.storage.backend", "s3"}
		require.NoError(t, fs.Parse(args))

		require.NoError(t, cfg.MimirConfig.InheritCommonFlagValues(log.NewNopLogger(), fs))

		// Value should be properly inherited.
		require.Equal(t, "s3", cfg.CustomStorage.Backend)

		// Mimir's inheritance should still work.
		require.Equal(t, "s3", cfg.MimirConfig.BlocksStorage.Bucket.Backend)
	})

	t.Run("yaml inheritance", func(t *testing.T) {
		const commonYAMLConfig = `
common:
  storage:
    backend: s3
`

		var cfg customExtendedConfig
		cfg.MimirConfig.Common.ExtraSpecificStorageConfigs = map[string]*bucket.StorageBackendConfig{
			"custom_storage": &cfg.CustomStorage.StorageBackendConfig,
		}

		fs := flag.NewFlagSet("test", flag.PanicOnError)
		cfg.RegisterFlags(fs, log.NewNopLogger())

		err := yaml.Unmarshal([]byte(commonYAMLConfig), &cfg)
		require.NoError(t, err)

		// Value should be properly inherited.
		require.Equal(t, "s3", cfg.CustomStorage.Backend)

		// Mimir's inheritance should still work.
		require.Equal(t, "s3", cfg.MimirConfig.BlocksStorage.Bucket.Backend)
	})

}

type customExtendedConfig struct {
	MimirConfig   mimir.Config  `yaml:",inline"`
	CustomStorage bucket.Config `yaml:"custom_storage"`
}

func (c *customExtendedConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	c.MimirConfig.RegisterFlags(f, logger)
	c.CustomStorage.RegisterFlagsWithPrefix("custom-storage", f)
}
