// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/ingester"
)

func TestIngesterConfigOptions(t *testing.T) {
	t.Run("no options", func(t *testing.T) {
		// This just tests that the function signature works with no options
		tempDir := t.TempDir()
		addr, cleanup, err := StartIngesterAndLoadData(tempDir, []int{})
		assert.NoError(t, err)
		assert.NotEmpty(t, addr)
		cleanup()
	})

	t.Run("with custom head compaction interval", func(t *testing.T) {
		tempDir := t.TempDir()

		customInterval := 5 * time.Minute
		configOpt := func(cfg *ingester.Config) {
			cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = customInterval
		}

		addr, cleanup, err := StartIngesterAndLoadData(tempDir, []int{}, configOpt)
		assert.NoError(t, err)
		assert.NotEmpty(t, addr)
		cleanup()
	})

	t.Run("with multiple config options", func(t *testing.T) {
		tempDir := t.TempDir()

		opt1 := func(cfg *ingester.Config) {
			cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = 5 * time.Minute
		}

		opt2 := func(cfg *ingester.Config) {
			cfg.BlocksStorageConfig.TSDB.BlockRanges = []time.Duration{
				2 * time.Hour,
				6 * time.Hour,
			}
		}

		addr, cleanup, err := StartIngesterAndLoadData(tempDir, []int{}, opt1, opt2)
		assert.NoError(t, err)
		assert.NotEmpty(t, addr)
		cleanup()
	})
}
