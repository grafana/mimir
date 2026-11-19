// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

// An ingester's switch of engines on its kept volume, as a zone's is when it isn't moved onto fresh volumes: each
// engine opens the other's data directory without failing, leaves what the other wrote there alone, and resumes from
// the file-stored Kafka offset the other kept. So it starts without the other engine's samples, rather than replaying
// them. Back on the TSDB engine, the TSDB has what it had before the switch.
func TestIngester_SwitchEngineOnKeptVolume(t *testing.T) {
	skipIfSeriesstore(t, "it sets the engine itself")
	h := newNativeInfoHarness(t, mimir_tsdb.EnginePrometheus, 1, defaultLimitsTestConfig())
	base := time.Now().Truncate(time.Minute).Add(-30 * time.Minute)
	at := func(minute int) time.Time { return base.Add(time.Duration(minute) * time.Minute) }
	push := func(from, to int) {
		for minute := from; minute <= to; minute++ {
			h.pushOTLP(t, otlpGauge(at(minute), otlpResource{name: "api", instance: "a"}, 0))
		}
	}
	minutes := func(from, to int) []int {
		var all []int
		for minute := from; minute <= to; minute++ {
			all = append(all, minute)
		}
		return all
	}
	// sampleMinutes are the minutes the series has a sample at, as the ingester answers: each step's count over the
	// minute before it counts the sample pushed at the step.
	sampleMinutes := func() []int {
		var found []int
		for _, s := range h.rangeQuery(t, false, `count_over_time(requests[1m])`, at(0), at(29), time.Minute) {
			for _, p := range s.Floats {
				found = append(found, int(time.UnixMilli(p.T).Sub(base)/time.Minute))
			}
		}
		return found
	}
	// The cell's settings for either engine: the file-stored offset enforced, and what the seriesstore engine requires.
	switchTo := func(engine string) func(*Config) {
		return func(cfg *Config) {
			// The offset the previous engine kept, which the next one resumes from.
			require.FileExists(t, filepath.Join(cfg.BlocksStorageConfig.TSDB.Dir, "kafka-offset.json"))
			cfg.BlocksStorageConfig.TSDB.Engine = engine
			cfg.IngestStorageConfig.KafkaConfig.ConsumerGroupOffsetCommitFileEnforced = true
			cfg.BlocksStorageConfig.TSDB.ShipInterval = 0
			cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
			cfg.BlocksStorageConfig.TSDB.MemorySnapshotOnShutdown = false
			cfg.BlocksStorageConfig.TSDB.OffsetCatalogue.Enabled = false
			require.NoError(t, ValidateSeriesstoreEngine(cfg.BlocksStorageConfig.TSDB, *cfg, cfg.IngestStorageConfig))
		}
	}
	tenantDir := filepath.Join(h.ingesters[0].cfg.BlocksStorageConfig.TSDB.Dir, h.tenant)
	// The TSDB's blocks on the volume, which the seriesstore engine mustn't serve.
	blocks := func() []string {
		entries, err := os.ReadDir(tenantDir)
		require.NoError(t, err)
		var ids []string
		for _, e := range entries {
			if _, err := ulid.Parse(e.Name()); err == nil && e.IsDir() {
				ids = append(ids, e.Name())
			}
		}
		return ids
	}

	// The TSDB, with a compacted block and a head after it, which leave a block, a WAL and head chunks on the volume.
	push(0, 9)
	h.ingesters[0].ingester.compactBlocks(context.Background(), true, math.MaxInt64, nil)
	tsdbBlocks := blocks()
	require.NotEmpty(t, tsdbBlocks, "the forced compaction wrote a block")
	push(10, 14)
	require.Equal(t, minutes(0, 14), sampleMinutes())

	// The seriesstore engine opens beside the TSDB's files, which stay, and resumes from their offset.
	h.restartWith(t, 0, switchTo(mimir_tsdb.EngineSeriesstore))
	push(15, 19)
	require.Equal(t, minutes(15, 19), sampleMinutes(), "the TSDB's samples are lost to the seriesstore engine")
	require.DirExists(t, filepath.Join(tenantDir, "seriesstore"))
	require.DirExists(t, filepath.Join(tenantDir, "wal"))
	require.Equal(t, tsdbBlocks, blocks(), "the TSDB's block stays, unserved")

	// Its own restart restores its head, with the TSDB's files beside it.
	h.restart(t, 0)
	push(20, 24)
	require.Equal(t, minutes(15, 24), sampleMinutes())

	// Back on the TSDB engine: it reopens its block and WAL, and resumes from the offset the seriesstore engine kept.
	h.restartWith(t, 0, switchTo(mimir_tsdb.EnginePrometheus))
	push(25, 29)
	require.Equal(t, append(minutes(0, 14), minutes(25, 29)...), sampleMinutes(), "the seriesstore engine's samples are lost to the TSDB")
}
