// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/seriesstore/store"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

// Without a WAL, a tenant the engine opens without a clean shutdown's snapshot lost its head: the
// ingester then replays Kafka from the retention period rather than from the stored offset.
func TestIngester_SeriesstoreResetsKafkaOffsetsAfterUncleanShutdown(t *testing.T) {
	for name, clean := range map[string]bool{"clean shutdown": true, "unclean shutdown": false} {
		t.Run(name, func(t *testing.T) {
			dataDir := t.TempDir()
			engine, err := store.OpenEngine(filepath.Join(dataDir, userID), userID, store.EngineOptions{Shards: 2})
			require.NoError(t, err)
			app := engine.Appender(context.Background())
			_, err = app.Append(0, labels.FromStrings(labels.MetricName, "up"), 1000, 1)
			require.NoError(t, err)
			require.NoError(t, app.Commit())
			if clean {
				require.NoError(t, engine.Close())
			} else {
				// Its data files are there, but no snapshot of its head.
				require.NoError(t, os.WriteFile(filepath.Join(dataDir, userID, "chunks-in-progress"), []byte("x"), 0o644))
			}
			offsetFile := filepath.Join(dataDir, "kafka-offset.json")
			require.NoError(t, os.WriteFile(offsetFile, []byte(`{"version":1,"partition_id":0,"offset":42}`), 0o644))

			cfg := defaultIngesterTestConfig(t)
			cfg.BlocksStorageConfig.TSDB.Engine = mimir_tsdb.EngineSeriesstore
			ingester, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), nil, dataDir, nil)
			require.NoError(t, err)
			startAndWaitHealthy(t, ingester, r)
			t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingester)) })

			_, err = os.Stat(offsetFile)
			if clean {
				require.NoError(t, err, "a clean shutdown keeps the stored offset")
				require.Equal(t, uint64(1), ingester.getTSDB(userID).Head().NumSeries())
			} else {
				require.ErrorIs(t, err, os.ErrNotExist, "an unclean shutdown removes the stored offset")
			}
		})
	}
}

func TestValidateSeriesstoreEngine(t *testing.T) {
	valid := func() (mimir_tsdb.TSDBConfig, Config, ingest.Config) {
		var tsdbCfg mimir_tsdb.TSDBConfig
		flagext.DefaultValues(&tsdbCfg)
		tsdbCfg.Engine = mimir_tsdb.EngineSeriesstore
		tsdbCfg.ShipInterval = 0
		var ingesterCfg Config
		flagext.DefaultValues(&ingesterCfg)
		ingesterCfg.PushGrpcMethodEnabled = false
		var ingestCfg ingest.Config
		flagext.DefaultValues(&ingestCfg)
		ingestCfg.Enabled = true
		ingestCfg.KafkaConfig.ConsumerGroupOffsetCommitFileEnforced = true
		return tsdbCfg, ingesterCfg, ingestCfg
	}
	tsdbCfg, ingesterCfg, ingestCfg := valid()
	require.NoError(t, ValidateSeriesstoreEngine(tsdbCfg, ingesterCfg, ingestCfg))

	for name, breakIt := range map[string]func(*mimir_tsdb.TSDBConfig, *Config, *ingest.Config){
		"ingest storage disabled": func(_ *mimir_tsdb.TSDBConfig, _ *Config, c *ingest.Config) { c.Enabled = false },
		"gRPC push enabled":       func(_ *mimir_tsdb.TSDBConfig, c *Config, _ *ingest.Config) { c.PushGrpcMethodEnabled = true },
		"offset file not enforced": func(_ *mimir_tsdb.TSDBConfig, _ *Config, c *ingest.Config) {
			c.KafkaConfig.ConsumerGroupOffsetCommitFileEnforced = false
		},
		"block shipping":              func(c *mimir_tsdb.TSDBConfig, _ *Config, _ *ingest.Config) { c.ShipInterval = 1 },
		"flush blocks on shutdown":    func(c *mimir_tsdb.TSDBConfig, _ *Config, _ *ingest.Config) { c.FlushBlocksOnShutdown = true },
		"offset catalogue":            func(c *mimir_tsdb.TSDBConfig, _ *Config, _ *ingest.Config) { c.OffsetCatalogue.Enabled = true },
		"memory snapshot on shutdown": func(c *mimir_tsdb.TSDBConfig, _ *Config, _ *ingest.Config) { c.MemorySnapshotOnShutdown = true },
	} {
		t.Run(name, func(t *testing.T) {
			tsdbCfg, ingesterCfg, ingestCfg := valid()
			breakIt(&tsdbCfg, &ingesterCfg, &ingestCfg)
			require.Error(t, ValidateSeriesstoreEngine(tsdbCfg, ingesterCfg, ingestCfg))
		})
	}

	// The Prometheus engine has none of these constraints.
	tsdbCfg, ingesterCfg, ingestCfg = valid()
	tsdbCfg.Engine = mimir_tsdb.EnginePrometheus
	ingestCfg.Enabled = false
	require.NoError(t, ValidateSeriesstoreEngine(tsdbCfg, ingesterCfg, ingestCfg))
}
