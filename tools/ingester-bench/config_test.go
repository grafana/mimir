// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeConfig(t *testing.T, content string) string {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return path
}

func TestConfigLayersOverTheDefaults(t *testing.T) {
	first := writeConfig(t, `
common:
  args:
    -log.level: info
    blocks-storage.tsdb.head-compaction-interval: 5m
    ingester.error-sample-rate: null
a:
  name: release
  image: example.com/mimir:release
  args:
    blocks-storage.tsdb.head-compaction-interval: 1m
b:
  name: tree
  args:
    some.flag: true
    other.flag: 5
`)
	second := writeConfig(t, `
b:
  args:
    other.flag: null
    blocks-storage.tsdb.float-chunk-encoding: xor
`)
	cfg, err := loadConfig([]string{first, second})
	require.NoError(t, err)

	a, b := cfg.ingester(0), cfg.ingester(1)
	require.Equal(t, "release", a.Name)
	require.Equal(t, "example.com/mimir:release", a.Image)
	require.Equal(t, "tree", b.Name)
	require.Empty(t, b.Image)

	base := []string{"-target=ingester", "-log.level=warn", "-ingester.error-sample-rate=0"}
	argsA, argsB := a.withArgs(base), b.withArgs(base)
	// The bench's flags keep their place, with the configuration's values; the others follow, by name.
	require.Equal(t, "-target=ingester", argsA[0])
	require.Equal(t, "-log.level=info", argsA[1])
	require.Contains(t, argsA, "-blocks-storage.tsdb.head-compaction-interval=1m")
	require.Contains(t, argsB, "-blocks-storage.tsdb.head-compaction-interval=5m")
	require.Contains(t, argsA, "-blocks-storage.tsdb.float-chunk-encoding=xor2")
	require.Contains(t, argsB, "-blocks-storage.tsdb.float-chunk-encoding=xor")
	require.Contains(t, argsB, "-some.flag=true")
	for _, args := range [][]string{argsA, argsB} {
		require.NotContains(t, args, "-ingester.error-sample-rate=0")
		require.NotContains(t, args, "-ingester.error-sample-rate=10")
	}
	require.NotContains(t, argsB, "-other.flag=5")
	require.IsNonDecreasing(t, argsA[len(base)-1:])
}

func TestConfigPrintedLoadsAsItself(t *testing.T) {
	cfg, err := loadConfig([]string{writeConfig(t, "a:\n  args:\n    ingester.error-sample-rate: null\n    some.flag: x\n")})
	require.NoError(t, err)
	data, err := json.Marshal(cfg)
	require.NoError(t, err)

	again, err := loadConfig([]string{writeConfig(t, string(data))})
	require.NoError(t, err)
	require.Equal(t, cfg, again)
	require.NotContains(t, again.ingester(0).withArgs(nil), "-ingester.error-sample-rate=10")
}

func TestConfigRejectsUnknownFields(t *testing.T) {
	_, err := loadConfig([]string{writeConfig(t, "a:\n  arg:\n    some.flag: x\n")})
	require.ErrorContains(t, err, "field arg not found")
}

func TestConfigEmptyFileIsTheDefaults(t *testing.T) {
	cfg, err := loadConfig([]string{writeConfig(t, "")})
	require.NoError(t, err)
	defaults, err := loadConfig(nil)
	require.NoError(t, err)
	require.Equal(t, defaults, cfg)
	require.NotEmpty(t, defaults.Common.Args)
}
