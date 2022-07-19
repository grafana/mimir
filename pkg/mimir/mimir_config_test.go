package mimir_test

import (
	"flag"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimir"
	"github.com/grafana/mimir/pkg/storage/bucket"
)

func TestCommonConfigCanBeExtended(t *testing.T) {
	t.Run("flags", func(t *testing.T) {
		// customConfig is a type that an upstream defines.
		type customConfig struct {
			mimir.Config
			CustomStorage bucket.Config
		}

		// Register flags as usual.
		var cfg customConfig
		fs := flag.NewFlagSet("test", flag.PanicOnError)
		cfg.RegisterFlags(fs, log.NewNopLogger())

		// And also register the custom-specific flags.
		cfg.CustomStorage.RegisterFlagsWithPrefix("custom-storage.", fs)

		// Parse the flags.
		args := []string{"-common.storage.backend", "s3"}
		err := fs.Parse(args)
		require.NoError(t, err)

		// Define our extra inheritance by extending the mimir's config flag inheritance.
		inheritance := append(cfg.CommonFlagInheritance(), mimir.FlagInheritance{
			Common: cfg.Common.Storage.RegisteredFlags, Specific: cfg.CustomStorage.RegisteredFlags,
		})
		// And inherit the values.
		err = mimir.InheritFlagValues(log.NewNopLogger(), fs, inheritance)
		require.NoError(t, err)

		// Check that CustomStorage value was actually inherited.
		require.Equal(t, "s3", cfg.CustomStorage.Backend)
		// Also check that mimir's inheritance didn't break.
		require.Equal(t, "s3", cfg.BlocksStorage.Bucket.Backend)
	})
}
