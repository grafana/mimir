// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/mimirengine/store"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/validation"
)

// Without a WAL, a tenant the engine opens without a clean shutdown's snapshot lost its head: the
// ingester then replays Kafka from the retention period rather than from the stored offset.
func TestIngester_MimirEngineResetsKafkaOffsetsAfterUncleanShutdown(t *testing.T) {
	for name, clean := range map[string]bool{"clean shutdown": true, "unclean shutdown": false} {
		t.Run(name, func(t *testing.T) {
			dataDir := t.TempDir()
			engine, err := store.OpenEngine(filepath.Join(dataDir, userID), userID, store.EngineOptions{Shards: 2})
			require.NoError(t, err)
			app := engine.Appender(context.Background())
			_, err = app.Append(0, labels.FromStrings(model.MetricNameLabel, "up"), 0, 1000, 1, nil, nil, storage.AppendV2Options{})
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
			cfg.BlocksStorageConfig.TSDB.Engine = mimir_tsdb.EngineMimirEngine
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

// Tenants restored from snapshots taken while ingesting miss what came after them: the ingester replays Kafka from the
// oldest, or from the retention period for an offset file one of them has no offset for.
func TestIngester_MimirEngineRewindsKafkaOffsetsToCheckpoints(t *testing.T) {
	for name, tc := range map[string]struct {
		checkpoints []string
		expected    int64
	}{
		"to the oldest checkpoint":          {checkpoints: []string{`{"kafka-offset.json":30}`, `{"kafka-offset.json":20}`}, expected: 20},
		"without a checkpoint for the file": {checkpoints: []string{`{"kafka-offset.json":30}`, `{}`}, expected: -1},
	} {
		t.Run(name, func(t *testing.T) {
			dataDir := t.TempDir()
			for index, checkpoint := range tc.checkpoints {
				tenant := fmt.Sprintf("user-%d", index)
				engine, err := store.OpenEngine(filepath.Join(dataDir, tenant), tenant, store.EngineOptions{Shards: 2, Checkpoints: true})
				require.NoError(t, err)
				t.Cleanup(func() { _ = engine.Close() })
				app := engine.Appender(context.Background())
				_, err = app.Append(0, labels.FromStrings(model.MetricNameLabel, "up"), 0, 1000, 1, nil, nil, storage.AppendV2Options{})
				require.NoError(t, err)
				require.NoError(t, app.Commit())
				// The engine stays open, like one that crashed.
				require.NoError(t, engine.Checkpoint(context.Background(), []byte(checkpoint), 0))
			}
			offsetFile := filepath.Join(dataDir, "kafka-offset.json")
			require.NoError(t, os.WriteFile(offsetFile, []byte(`{"version":1,"partition_id":0,"offset":42}`), 0o644))

			cfg := defaultIngesterTestConfig(t)
			cfg.BlocksStorageConfig.TSDB.Engine = mimir_tsdb.EngineMimirEngine
			ingester, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), nil, dataDir, nil)
			require.NoError(t, err)
			startAndWaitHealthy(t, ingester, r)
			t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingester)) })

			for index := range tc.checkpoints {
				require.Equal(t, uint64(1), ingester.getTSDB(fmt.Sprintf("user-%d", index)).Head().NumSeries())
			}
			offset, ok := ingest.ReadOffsetFile(offsetFile, 0, ingester.logger)
			if tc.expected < 0 {
				require.False(t, ok, "the offset file is removed")
				return
			}
			require.True(t, ok)
			require.Equal(t, tc.expected, offset)
		})
	}
}

func TestIngester_MimirEngineRestartsWithTenantOpenedAfterSnapshotRound(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.Engine = mimir_tsdb.EngineMimirEngine
	cfg.BlocksStorageConfig.TSDB.MimirEngineSnapshotInterval = time.Hour
	dataDir := t.TempDir()
	before, ring, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), nil, dataDir, nil)
	require.NoError(t, err)
	startAndWaitHealthy(t, before, ring)
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), before)) })

	writeOffset := func(offset int64) {
		t.Helper()
		require.NoError(t, ingest.WriteOffsetFile(filepath.Join(dataDir, "kafka-offset.json"), 0, offset, before.logger))
	}
	push := func(i *Ingester, tenantID string) {
		t.Helper()
		ctx := user.InjectOrgID(t.Context(), tenantID)
		req := mimirpb.ToWriteRequest([][]mimirpb.LabelAdapter{{{Name: model.MetricNameLabel, Value: "up"}}}, []mimirpb.Sample{{TimestampMs: 1000, Value: 1}}, nil, nil, mimirpb.API)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	writeOffset(40)
	push(before, "existing-tenant")
	require.NoError(t, before.snapshotMimirEngineHeads(t.Context(), 0))

	writeOffset(60)
	push(before, "new-tenant")
	writeOffset(80)

	// Copy the state before a graceful shutdown can write the new tenant's head.
	crashDir := t.TempDir()
	require.NoError(t, os.CopyFS(crashDir, os.DirFS(dataDir)))
	after, ring, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), nil, crashDir, nil)
	require.NoError(t, err)
	startAndWaitHealthy(t, after, ring)
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), after)) })

	offset, ok := ingest.ReadOffsetFile(filepath.Join(crashDir, "kafka-offset.json"), 0, after.logger)
	require.True(t, ok, "a new tenant should not force a rewind to the retention period")
	require.Equal(t, int64(40), offset)
	require.Equal(t, uint64(1), after.getTSDB("existing-tenant").Head().NumSeries())
	fresh := after.getTSDB("new-tenant")
	require.JSONEq(t, `{"kafka-offset.json":60}`, string(restoredCheckpoint(fresh.db)))
	require.Zero(t, fresh.Head().NumSeries())
	push(after, "new-tenant") // Replay the write that followed the empty checkpoint.
	require.Equal(t, uint64(1), fresh.Head().NumSeries())
}

// Each tenant's checkpoint has the offsets committed before it started.
func TestIngester_MimirEngineSnapshotsHeadsWithTheCommittedOffsets(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.Engine = mimir_tsdb.EngineMimirEngine
	dataDir := t.TempDir()
	reg := prometheus.NewPedanticRegistry()
	ingester, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), nil, dataDir, reg)
	require.NoError(t, err)
	startAndWaitHealthy(t, ingester, r)
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingester)) })
	ctx := user.InjectOrgID(context.Background(), userID)
	req := mimirpb.ToWriteRequest([][]mimirpb.LabelAdapter{{{Name: model.MetricNameLabel, Value: "up"}}}, []mimirpb.Sample{{TimestampMs: 1000, Value: 1}}, nil, nil, mimirpb.API)
	_, err = ingester.Push(ctx, req)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "kafka-offset.json"), []byte(`{"version":1,"partition_id":0,"offset":42}`), 0o644))

	require.NoError(t, ingester.snapshotMimirEngineHeads(t.Context(), 0))
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_mimirengine_head_snapshots_total Total number of tenants' heads the MimirEngine snapshotted while ingesting, by outcome.
		# TYPE cortex_ingester_mimirengine_head_snapshots_total counter
		cortex_ingester_mimirengine_head_snapshots_total{outcome="failure"} 0
		cortex_ingester_mimirengine_head_snapshots_total{outcome="success"} 1
	`), "cortex_ingester_mimirengine_head_snapshots_total"))
	require.Equal(t, 1, testutil.CollectAndCount(ingester.metrics.mimirengineSnapshotRoundDuration))

	crashed, err := store.OpenEngine(filepath.Join(dataDir, userID), userID, store.EngineOptions{Shards: 2, Checkpoints: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = crashed.Close() })
	require.JSONEq(t, `{"kafka-offset.json":42}`, string(crashed.RestoredCheckpoint()))
	require.Equal(t, uint64(1), crashed.NumSeries())
}

func TestIngester_MimirEngineCheckpointsAfterStartupReplay(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.Engine = mimir_tsdb.EngineMimirEngine
	cfg.BlocksStorageConfig.TSDB.MimirEngineSnapshotInterval = time.Hour
	cfg.BlocksStorageConfig.TSDB.Dir = t.TempDir()
	cfg.IngestStorageConfig.KafkaConfig.ConsumerGroupOffsetCommitFileEnforced = true
	cfg.IngestStorageConfig.KafkaConfig.ConsumerGroupOffsetCommitInterval = time.Hour
	offsetFile := "kafka-offset.json"

	engine, err := store.OpenEngine(filepath.Join(cfg.BlocksStorageConfig.TSDB.Dir, userID), userID, store.EngineOptions{Shards: 2})
	require.NoError(t, err)
	now := time.Now().UnixMilli()
	app := engine.Appender(t.Context())
	_, err = app.Append(0, labels.FromStrings(model.MetricNameLabel, "already_consumed"), 0, now, 1, nil, nil, storage.AppendV2Options{})
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.NoError(t, engine.Close())

	ingester, _, _ := createTestIngesterWithIngestStorage(t, &cfg, validation.NewOverrides(defaultLimitsTestConfig(), nil), nil, nil, nil)
	writer := ingest.NewWriter(cfg.IngestStorageConfig.KafkaConfig, log.NewNopLogger(), nil)
	require.NoError(t, services.StartAndAwaitRunning(t.Context(), writer))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), writer)) })
	for _, name := range []string{"already_consumed", "replayed_at_startup"} {
		req := mimirpb.ToWriteRequest([][]mimirpb.LabelAdapter{{{Name: model.MetricNameLabel, Value: name}}}, []mimirpb.Sample{{TimestampMs: now, Value: 1}}, nil, nil, mimirpb.API)
		require.NoError(t, writer.WriteSync(t.Context(), cfg.IngestStorageConfig.KafkaConfig.Topic, ingester.ingestPartitionID, userID, req))
	}
	require.NoError(t, ingest.WriteOffsetFile(filepath.Join(cfg.BlocksStorageConfig.TSDB.Dir, offsetFile), ingester.ingestPartitionID, 0, ingester.logger))
	require.NoError(t, services.StartAndAwaitRunning(t.Context(), ingester))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingester)) })

	// Copy the live files to simulate a crash before either a periodic snapshot or a clean shutdown.
	crashDir := t.TempDir()
	require.NoError(t, os.CopyFS(crashDir, os.DirFS(cfg.BlocksStorageConfig.TSDB.Dir)))
	require.NoError(t, ingest.WriteOffsetFile(filepath.Join(crashDir, offsetFile), ingester.ingestPartitionID, 1, ingester.logger))
	restartCfg := defaultIngesterTestConfig(t)
	restartCfg.BlocksStorageConfig.TSDB.Engine = mimir_tsdb.EngineMimirEngine
	restarted, ring, err := prepareIngesterWithBlocksStorageAndLimits(t, restartCfg, defaultLimitsTestConfig(), nil, crashDir, nil)
	require.NoError(t, err)
	startAndWaitHealthy(t, restarted, ring)
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), restarted)) })

	db := restarted.getTSDB(userID)
	require.NotNil(t, db)
	require.JSONEq(t, `{"kafka-offset.json":0}`, string(restoredCheckpoint(db.db)))
	require.Equal(t, uint64(2), db.Head().NumSeries(), "the checkpoint includes the replayed series")
	offset, ok := ingest.ReadOffsetFile(filepath.Join(crashDir, offsetFile), restarted.ingestPartitionID, restarted.logger)
	require.True(t, ok, "the recovered head should keep a Kafka resume offset")
	require.Zero(t, offset)
}

// An ingester stopped while it replays Kafka at startup still closes its mimirengine TSDBs, so
// the next start restores their heads instead of replaying the retention period.
func TestIngester_MimirEngineSnapshotsWhenStoppedWhileStarting(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.Engine = mimir_tsdb.EngineMimirEngine
	cfg.BlocksStorageConfig.TSDB.Dir = t.TempDir()
	tenantDir := filepath.Join(cfg.BlocksStorageConfig.TSDB.Dir, userID)
	engine, err := store.OpenEngine(tenantDir, userID, store.EngineOptions{Shards: 2})
	require.NoError(t, err)
	app := engine.Appender(context.Background())
	_, err = app.Append(0, labels.FromStrings(model.MetricNameLabel, "up"), 0, 1000, 1, nil, nil, storage.AppendV2Options{})
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.NoError(t, engine.Close())

	ingester, kafkaCluster, _ := createTestIngesterWithIngestStorage(t, &cfg, validation.NewOverrides(defaultLimitsTestConfig(), nil), nil, nil, nil)
	// A pending record makes startup wait on replay even when the broker has no other traffic.
	kafkaCfg := cfg.IngestStorageConfig.KafkaConfig
	writer := ingest.NewWriter(kafkaCfg, log.NewNopLogger(), nil)
	require.NoError(t, services.StartAndAwaitRunning(t.Context(), writer))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), writer)) })
	req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{
		Labels:  []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "pending"}},
		Samples: []mimirpb.Sample{{TimestampMs: time.Now().UnixMilli(), Value: 1}},
	}}}}
	require.NoError(t, writer.WriteSync(t.Context(), kafkaCfg.Topic, ingester.ingestPartitionID, userID, req))

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

func TestValidateMimirEngine(t *testing.T) {
	valid := func() (mimir_tsdb.TSDBConfig, Config, ingest.Config) {
		var tsdbCfg mimir_tsdb.TSDBConfig
		flagext.DefaultValues(&tsdbCfg)
		tsdbCfg.Engine = mimir_tsdb.EngineMimirEngine
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
	require.NoError(t, ValidateMimirEngine(tsdbCfg, ingesterCfg, ingestCfg))

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
			require.Error(t, ValidateMimirEngine(tsdbCfg, ingesterCfg, ingestCfg))
		})
	}

	// The Prometheus engine has none of these constraints.
	tsdbCfg, ingesterCfg, ingestCfg = valid()
	tsdbCfg.Engine = mimir_tsdb.EnginePrometheus
	ingestCfg.Enabled = false
	require.NoError(t, ValidateMimirEngine(tsdbCfg, ingesterCfg, ingestCfg))
}

// The engine reports a tenant's active series to cost attribution as the tracker does, for series becoming active,
// going idle, and growing their histograms.

// recordingStream keeps the marshaled bytes of the response messages it is sent, as the messages' slices are reused.
type recordingStream struct {
	grpc.ServerStream
	ctx      context.Context
	messages [][]byte
}

func (s *recordingStream) Send(response *client.QueryStreamResponse) error {
	encoded, err := response.Marshal()
	if err != nil {
		return err
	}
	s.messages = append(s.messages, encoded)
	return nil
}

func (s *recordingStream) Context() context.Context { return s.ctx }

// The mimirengine builds the streamed response itself: it has to send what sendStreamingQuerySeries and
// sendStreamingQueryChunks send for the same querier, message for message.
func TestIngester_MimirEngineStreamsTheSameResponseAsTheGenericPath(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.Engine = mimir_tsdb.EngineMimirEngine
	limitsCfg := defaultLimitsTestConfig()
	limitsCfg.NativeHistogramsIngestionEnabled = true
	ingester, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limitsCfg, nil, t.TempDir(), nil)
	require.NoError(t, err)
	startAndWaitHealthy(t, ingester, r)
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingester)) })

	ctx := user.InjectOrgID(context.Background(), userID)
	now := time.Now().UnixMilli()
	// Series with floats, with histograms and with both, with many samples so the chunks cut, and enough of them for
	// more than one batch of each kind.
	var series []mimirpb.PreallocTimeseries
	for n := range 300 {
		ts := &mimirpb.TimeSeries{Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: fmt.Sprintf("metric_%d", n%3)}, {Name: "pod", Value: fmt.Sprintf("pod-%03d", n)}}}
		for sample := range 130 {
			if n%5 == 4 {
				ts.Histograms = append(ts.Histograms, mimirpb.FromHistogramToHistogramProto(now-int64(130-sample)*1000, streamTestHistogram(sample)))
			} else {
				ts.Samples = append(ts.Samples, mimirpb.Sample{TimestampMs: now - int64(130-sample)*1000, Value: float64(sample * n)})
			}
		}
		series = append(series, mimirpb.PreallocTimeseries{TimeSeries: ts})
	}
	_, err = ingester.Push(ctx, &mimirpb.WriteRequest{Timeseries: series, Source: mimirpb.API})
	require.NoError(t, err)

	db := ingester.getTSDB(userID)
	require.NotNil(t, db)
	hints := initSelectHints(now-1_000_000, now+1000)
	hints = configSelectHintsWithDisabledTrimming(hints)
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "__name__", "metric_.*")}

	q, err := db.ChunkQuerier(hints.Start, hints.End)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	streaming, ok := q.(streamingQuerier)
	require.True(t, ok, "the mimirengine's querier builds the response itself")

	const chunksBatchSize = 7
	viaEngine := &recordingStream{ctx: ctx}
	stats, err := streaming.QueryStream(ctx, hints, matchers, streamBatching{SeriesBatchSize: queryStreamBatchSize, ChunksBatchSize: chunksBatchSize, ChunksBatchMessageBytes: queryStreamBatchMessageSize}, func(response *client.QueryStreamResponse) error { return client.SendQueryStream(viaEngine, response) })
	require.NoError(t, err)

	generic := &recordingStream{ctx: ctx}
	underlying := q.(streamingChunkQuerier).ChunkQuerier
	allSeries, count, err := ingester.sendStreamingQuerySeries(ctx, underlying, hints, matchers, generic)
	require.NoError(t, err)
	samples, chunks, batches, err := ingester.sendStreamingQueryChunks(allSeries, generic, chunksBatchSize)
	require.NoError(t, err)

	require.Equal(t, count, stats.Series)
	require.Equal(t, samples, stats.Samples)
	require.Equal(t, chunks, stats.Chunks)
	require.Equal(t, batches, stats.Batches)
	require.Greater(t, len(generic.messages), 3)
	require.Equal(t, generic.messages, viaEngine.messages)

	// Again, with the slices the first response kept.
	again := &recordingStream{ctx: ctx}
	_, err = streaming.QueryStream(ctx, hints, matchers, streamBatching{SeriesBatchSize: queryStreamBatchSize, ChunksBatchSize: chunksBatchSize, ChunksBatchMessageBytes: queryStreamBatchMessageSize}, func(response *client.QueryStreamResponse) error { return client.SendQueryStream(again, response) })
	require.NoError(t, err)
	require.Equal(t, generic.messages, again.messages)
}

func streamTestHistogram(base int) *histogram.Histogram {
	return &histogram.Histogram{
		Count:           uint64(2*base + 3),
		Sum:             float64(base) + 1.5,
		ZeroThreshold:   0.001,
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: 2}},
		PositiveBuckets: []int64{int64(base) + 1, 1},
	}
}

func TestIngester_MimirEngineIngestsAnExistingAndNewSeriesFromOneRecord(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.Engine = mimir_tsdb.EngineMimirEngine
	cfg.ActiveSeriesMetrics.Enabled = true
	i, ring, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), nil, t.TempDir(), nil)
	require.NoError(t, err)
	startAndWaitHealthy(t, i, ring)

	ctx := user.InjectOrgID(t.Context(), "synthetic-tenant")
	now := time.Now().UnixMilli()
	makeSeries := func(name string, timestamp int64) mimirpb.PreallocTimeseries {
		return mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels:  []mimirpb.LabelAdapter{{Name: "__name__", Value: name}},
			Samples: []mimirpb.Sample{{TimestampMs: timestamp, Value: 1}},
		}}
	}
	initial := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{makeSeries("existing", now)}}
	initialBytes, err := initial.Marshal()
	require.NoError(t, err)
	require.NoError(t, i.IngestRecord(ctx, initialBytes, 1))

	req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{makeSeries("existing", now+1), makeSeries("new_series", now+1)}}
	rw2, err := mimirpb.FromWriteRequestToRW2Request(req, ingest.V2CommonSymbols, ingest.V2RecordSymbolOffset)
	require.NoError(t, err)
	content, err := rw2.Marshal()
	require.NoError(t, err)

	db := i.getTSDB("synthetic-tenant")
	require.NotNil(t, db)
	record := db.engine().(directIngestEngine).ReadRecord(content, 2)
	require.NotNil(t, record)
	record.Release()
	require.NoError(t, i.IngestRecord(ctx, content, 2))
	require.Equal(t, 2, db.activeSeriesCounts(time.Now()).Total)

	q, err := db.ChunkQuerier(now, now+1)
	require.NoError(t, err)
	defer q.Close()
	for name, expected := range map[string][]int64{"existing": {now, now + 1}, "new_series": {now + 1}} {
		set := q.Select(ctx, false, nil, labels.MustNewMatcher(labels.MatchEqual, "__name__", name))
		require.True(t, set.Next())
		chunks := set.At().Iterator(nil)
		var timestamps []int64
		for chunks.Next() {
			it := chunks.At().Chunk.Iterator(nil)
			for it.Next() == chunkenc.ValFloat {
				ts, _ := it.At()
				timestamps = append(timestamps, ts)
			}
			require.NoError(t, it.Err())
		}
		require.Equal(t, expected, timestamps)
		require.NoError(t, chunks.Err())
		require.False(t, set.Next())
		require.NoError(t, set.Err())
	}
}
