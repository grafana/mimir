// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/mimir/pkg/costattribution"
	"github.com/grafana/mimir/pkg/costattribution/costattributionmodel"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/seriesstore/store"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/validation"
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

// An ingester stopped while it replays Kafka at startup still closes its seriesstore TSDBs, so
// the next start restores their heads instead of replaying the retention period.
func TestIngester_SeriesstoreSnapshotsWhenStoppedWhileStarting(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.Engine = mimir_tsdb.EngineSeriesstore
	cfg.BlocksStorageConfig.TSDB.Dir = t.TempDir()
	tenantDir := filepath.Join(cfg.BlocksStorageConfig.TSDB.Dir, userID)
	engine, err := store.OpenEngine(tenantDir, userID, store.EngineOptions{Shards: 2})
	require.NoError(t, err)
	app := engine.Appender(context.Background())
	_, err = app.Append(0, labels.FromStrings(labels.MetricName, "up"), 1000, 1)
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.NoError(t, engine.Close())

	ingester, kafkaCluster, _ := createTestIngesterWithIngestStorage(t, &cfg, validation.NewOverrides(defaultLimitsTestConfig(), nil), nil, nil, nil)
	// Fetches fail, so the ingester never finishes replaying its partition.
	kafkaCluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
		kafkaCluster.KeepControl()
		return nil, errors.New("mocked error"), true
	})
	require.NoError(t, ingester.StartAsync(context.Background()))
	test.Poll(t, 10*time.Second, true, func() interface{} {
		return ingester.State() == services.Starting && ingester.getTSDB(userID) != nil
	})
	ingester.StopAsync()
	require.Error(t, ingester.AwaitTerminated(context.Background()), "it failed to start")

	restored, err := store.OpenEngine(tenantDir, userID, store.EngineOptions{Shards: 2})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restored.Close()) })
	require.True(t, restored.Restored(), "closed while starting, the head was snapshotted again")
	require.Equal(t, uint64(1), restored.NumSeries())
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

// The engine reports a tenant's active series to cost attribution as the tracker does, for series becoming active,
// going idle, and growing their histograms.
func TestIngester_SeriesstoreCostAttributionMatchesTracker(t *testing.T) {
	attributed := func(native bool) []map[string]float64 {
		previous := testNoNativeActive
		testNoNativeActive = !native
		t.Cleanup(func() { testNoNativeActive = previous })

		ctx := user.InjectOrgID(context.Background(), userID)
		cfg := defaultIngesterTestConfig(t)
		cfg.BlocksStorageConfig.TSDB.Engine = mimir_tsdb.EngineSeriesstore
		cfg.ActiveSeriesMetrics.IdleTimeout = 500 * time.Millisecond
		limitsCfg := defaultLimitsTestConfig()
		limitsCfg.NativeHistogramsIngestionEnabled = true
		limitsCfg.CostAttributionBaseTrackers = costattributionmodel.TrackerConfigs{
			costattributionmodel.DefaultTrackerName: {Labels: costattributionmodel.Labels{{Input: "cpu"}}},
		}
		limitsCfg.MaxCostAttributionCardinality = 100
		overrides := validation.NewOverrides(limitsCfg, nil)
		registry, attributionRegistry := prometheus.NewRegistry(), prometheus.NewRegistry()
		manager, err := costattribution.NewManager(5*time.Second, 10*time.Second, nil, overrides, registry, attributionRegistry)
		require.NoError(t, err)
		ingester, r, err := prepareIngesterWithBlockStorageOverridesAndCostAttribution(t, cfg, overrides, nil, "", "", registry, manager)
		require.NoError(t, err)
		startAndWaitHealthy(t, ingester, r)
		t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingester)) })

		read := func() map[string]float64 {
			families, err := attributionRegistry.Gather()
			require.NoError(t, err)
			out := map[string]float64{}
			for _, family := range families {
				if family.GetName() != "cortex_ingester_attributed_active_series" {
					continue
				}
				for _, m := range family.GetMetric() {
					for _, label := range m.GetLabel() {
						if label.GetName() == "cpu" {
							out[label.GetValue()] += m.GetGauge().GetValue()
						}
					}
				}
			}
			return out
		}
		push := func(at time.Time, series ...[]mimirpb.LabelAdapter) {
			samples := make([]mimirpb.Sample, len(series))
			for i := range samples {
				samples[i] = mimirpb.Sample{TimestampMs: at.UnixMilli(), Value: 1}
			}
			_, err := ingester.Push(ctx, mimirpb.ToWriteRequest(series, samples, nil, nil, mimirpb.API))
			require.NoError(t, err)
		}
		series := func(name, cpu string) []mimirpb.LabelAdapter {
			return []mimirpb.LabelAdapter{{Name: "__name__", Value: name}, {Name: "cpu", Value: cpu}}
		}

		var out []map[string]float64
		now := time.Now()
		push(now, series("a", "1"), series("b", "1"), series("c", "2"))
		require.Equal(t, native, !ingester.getTSDB(userID).trackerActive(), "the engine, not the tracker, counts the tenant's active series with cost attribution")
		ingester.updateActiveSeries(now)
		out = append(out, read())
		// Another series, and the first ones go idle: samples count as ingested when they arrive.
		time.Sleep(700 * time.Millisecond)
		push(time.Now(), series("d", "2"))
		ingester.updateActiveSeries(time.Now())
		out = append(out, read())
		ingester.updateActiveSeries(time.Now().Add(5 * time.Second))
		out = append(out, read())
		return out
	}
	tracker := attributed(false)
	require.Equal(t, []map[string]float64{{"1": 2, "2": 1}, {"2": 1}, {}}, tracker)
	require.Equal(t, tracker, attributed(true))
}
