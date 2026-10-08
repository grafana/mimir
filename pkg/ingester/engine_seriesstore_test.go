// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/costattribution"
	"github.com/grafana/mimir/pkg/costattribution/costattributionmodel"
	"github.com/grafana/mimir/pkg/ingester/client"
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

// The seriesstore builds the streamed response itself: it has to send what sendStreamingQuerySeries and
// sendStreamingQueryChunks send for the same querier, message for message.
func TestIngester_SeriesstoreStreamsTheSameResponseAsTheGenericPath(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.Engine = mimir_tsdb.EngineSeriesstore
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
	require.True(t, ok, "the seriesstore's querier builds the response itself")

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

type marshalingStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *marshalingStream) Send(response *client.QueryStreamResponse) error {
	_, err := response.Marshal()
	return err
}

func (s *marshalingStream) Context() context.Context { return s.ctx }

// BenchmarkIngester_SeriesstoreQueryStream is a streamed response of 5000 series of 130 samples each, built by the
// generic path and by the seriesstore's.
func BenchmarkIngester_SeriesstoreQueryStream(b *testing.B) {
	cfg := defaultIngesterTestConfig(b)
	cfg.BlocksStorageConfig.TSDB.Engine = mimir_tsdb.EngineSeriesstore
	limitsCfg := defaultLimitsTestConfig()
	limitsCfg.MaxGlobalSeriesPerMetric = 0
	limitsCfg.MaxGlobalSeriesPerUser = 0
	ingester, r, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limitsCfg, nil, b.TempDir(), nil)
	require.NoError(b, err)
	startAndWaitHealthy(b, ingester, r)

	ctx := user.InjectOrgID(context.Background(), userID)
	now := time.Now().UnixMilli()
	for first := 0; first < 5000; first += 500 {
		var series []mimirpb.PreallocTimeseries
		for n := first; n < first+500; n++ {
			ts := &mimirpb.TimeSeries{Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric"}, {Name: "cluster", Value: "c1"}, {Name: "namespace", Value: fmt.Sprintf("ns-%d", n%50)}, {Name: "pod", Value: fmt.Sprintf("pod-%05d", n)}}}
			for sample := range 130 {
				ts.Samples = append(ts.Samples, mimirpb.Sample{TimestampMs: now - int64(130-sample)*1000, Value: float64(sample * n)})
			}
			series = append(series, mimirpb.PreallocTimeseries{TimeSeries: ts})
		}
		_, err = ingester.Push(ctx, &mimirpb.WriteRequest{Timeseries: series, Source: mimirpb.API})
		require.NoError(b, err)
	}
	hints := configSelectHintsWithDisabledTrimming(initSelectHints(now-1_000_000, now+1000))
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "metric")}
	stream := &marshalingStream{ctx: ctx}
	db := ingester.getTSDB(userID)

	for _, mode := range []string{"generic", "engine"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				q, err := db.ChunkQuerier(hints.Start, hints.End)
				require.NoError(b, err)
				if mode == "engine" {
					_, err = q.(streamingQuerier).QueryStream(ctx, hints, matchers, streamBatching{SeriesBatchSize: queryStreamBatchSize, ChunksBatchSize: 128, ChunksBatchMessageBytes: queryStreamBatchMessageSize}, func(response *client.QueryStreamResponse) error { return client.SendQueryStream(stream, response) })
				} else {
					underlying := q.(streamingChunkQuerier).ChunkQuerier
					var allSeries *chunkSeriesNode
					allSeries, _, err = ingester.sendStreamingQuerySeries(ctx, underlying, hints, matchers, stream)
					if err == nil {
						_, _, _, err = ingester.sendStreamingQueryChunks(allSeries, stream, 128)
					}
				}
				require.NoError(b, err)
				require.NoError(b, q.Close())
			}
		})
	}
}
