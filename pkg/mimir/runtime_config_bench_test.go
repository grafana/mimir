// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"context"
	"os"
	"testing"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

const (
	benchRuntimeConfigBaseFile      = "testdata/base.yaml"
	benchRuntimeConfigOverridesFile = "testdata/overrides.json"
)

func BenchmarkNewRuntimeManager(b *testing.B) {
	for _, p := range []string{benchRuntimeConfigBaseFile, benchRuntimeConfigOverridesFile} {
		if _, err := os.Stat(p); err != nil {
			b.Skipf("skipping because fixture %s does not exist", p)
		}
	}

	newConfig := func() *Config {
		cfg := &Config{}
		flagext.DefaultValues(cfg)
		require.NoError(b, cfg.RuntimeConfig.LoadPath.Set(benchRuntimeConfigBaseFile+","+benchRuntimeConfigOverridesFile))
		return cfg
	}

	loadOnce := func(cfg *Config) {
		manager, err := NewRuntimeManager(cfg, "benchmark", nil, log.NewNopLogger())
		require.NoError(b, err)
		require.NoError(b, services.StartAndAwaitRunning(context.Background(), manager))
		require.NoError(b, services.StopAndAwaitTerminated(context.Background(), manager))
	}

	b.Run("default", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			cfg := newConfig()
			b.StartTimer()

			loadOnce(cfg)
		}
	})

	b.Run("with YAML loader", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			cfg := newConfig()
			loader := runtimeConfigLoader{validate: cfg.ValidateLimits}
			cfg.RuntimeConfig.Loader = loader.load
			b.StartTimer()

			loadOnce(cfg)
		}
	})
}
