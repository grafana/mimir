// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/validation"
)

func testResourceEntry(service, host string) mimirpb.ResourceEntry {
	return mimirpb.ResourceEntry{Attributes: []mimirpb.ResourceAttribute{
		{Key: "service.name", StringValue: service},
		{Key: "service.instance.id", StringValue: "1"},
		{Key: "host.name", StringValue: host},
		{Key: "host.cores", Type: mimirpb.RESOURCE_ATTRIBUTE_INT, IntValue: 8},
	}}
}

// Each engine stores each series' resource from records, with the sequential and the parallel pusher, which keep
// the series' resources when they rebatch them, and serves them through the engine. #17740 keeps a series' first
// resource per appender, so the parallel pusher, which may append both records with one, may record a change only
// with a later batch.
func TestEnginesServeNativeResources(t *testing.T) {
	skipIfSeriesstore(t, "it runs the engines itself")
	for _, concurrency := range []int{0, 8} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("engine=%s concurrency=%d enabled=%t", mimir_tsdb.EnginePrometheus, concurrency, enabled), func(t *testing.T) {
				limits := defaultLimitsTestConfig()
				limits.NativeResourcesEnabled = enabled
				cfg := defaultIngesterTestConfig(t)
				cfg.IngestStorageConfig.KafkaConfig.ConsumeFromPositionAtStartup = "start"
				cfg.BlocksStorageConfig.TSDB.Engine = mimir_tsdb.EnginePrometheus
				cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyMax = concurrency
				if concurrency > 0 {
					cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyBatchSize = 150
					cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyEstimatedBytesPerSample = 200
					cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyQueueCapacity = 3
					cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyTargetFlushesPerShard = 40
				}
				ingester, _, _ := createTestIngesterWithIngestStorage(t, &cfg, validation.NewOverrides(limits, nil), nil, nil, nil)

				// Two requests a minute apart, for the series of two resources. The second's host changes, the first's
				// doesn't.
				base := time.Now().Truncate(time.Minute).Add(-time.Hour).UnixMilli()
				write, last := compatProducer(t, cfg.IngestStorageConfig.KafkaConfig)
				for step, secondHost := range []string{"node-2", "node-3"} {
					timestamp := base + int64(step)*60_000
					req := &mimirpb.WriteRequest{ResourceTable: []mimirpb.ResourceEntry{testResourceEntry("frontend", "node-1"), testResourceEntry("backend", secondHost)}}
					for n := range 20 {
						ts := compatSeries("requests_total", mimirpb.Sample{TimestampMs: timestamp, Value: float64(n)})
						ts.Labels = append(ts.Labels, mimirpb.LabelAdapter{Name: "pod", Value: fmt.Sprintf("pod-%02d", n)})
						ts.ResourceRef = uint32(n%2) + 1
						req.Timeseries = append(req.Timeseries, ts)
					}
					write("tenant", req)
				}
				require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingester))
				t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingester)) })
				waitCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				require.NoError(t, ingester.ingestReader.WaitReadConsistencyUntilOffsets(waitCtx, kmeta.NewSingleClusterPartitionOffsets(last())))

				db := ingester.getTSDB("tenant")
				require.NotNil(t, db.nativeResources)
				matchers := [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "requests_total")}}
				result, err := db.nativeResources.nativeResources(context.Background(), matchers, math.MinInt64, math.MaxInt64, 6)
				if !enabled {
					require.ErrorIs(t, err, errNativeResourcesDisabled)
					return
				}
				require.NoError(t, err)
				require.True(t, result.Limited)
				require.Len(t, result.Series, 6)
				frontend := map[string]string{"service.name": "frontend", "service.instance.id": "1"}
				backend := map[string]string{"service.name": "backend", "service.instance.id": "1"}
				backendVersions := []nativeResourceVersion{
					{EffectiveFrom: base, Identifying: backend, Descriptive: map[string]string{"host.name": "node-2", "host.cores": "8"}},
					{EffectiveFrom: base + 60_000, Identifying: backend, Descriptive: map[string]string{"host.name": "node-3", "host.cores": "8"}},
				}
				for n, series := range result.Series {
					require.Equal(t, fmt.Sprintf("pod-%02d", n), series.Labels.Get("pod"))
					if n%2 == 0 {
						require.Equal(t, []nativeResourceVersion{
							{EffectiveFrom: base, Identifying: frontend, Descriptive: map[string]string{"host.name": "node-1", "host.cores": "8"}},
						}, series.Versions)
						continue
					}
					if concurrency > 0 && len(series.Versions) == 1 {
						require.Equal(t, backendVersions[:1], series.Versions)
						continue
					}
					require.Equal(t, backendVersions, series.Versions)
				}
				require.Equal(t, base, result.CoverageStart)
				if concurrency > 0 {
					return
				}

				// A time range picks the versions in force during it: the first resource's, though unchanged since, and
				// the second's, from before the range on.
				result, err = db.nativeResources.nativeResources(context.Background(), matchers, base+30_000, math.MaxInt64, 0)
				require.NoError(t, err)
				require.Len(t, result.Series, 20)
				for n, series := range result.Series {
					require.Len(t, series.Versions, 1+n%2)
				}
				result, err = db.nativeResources.nativeResources(context.Background(), matchers, base+60_000, math.MaxInt64, 0)
				require.NoError(t, err)
				require.Equal(t, backendVersions[1:], result.Series[1].Versions)
			})
		}
	}
}

// The TSDB's resource step builds each resource once per batch, with values as OTLP gives them as strings, and skips
// the head for a series that already has the resource.
func TestTSDBResources(t *testing.T) {
	opts := tsdb.DefaultOptions()
	opts.EnableNativeResources = true
	db, err := tsdb.Open(t.TempDir(), nil, nil, opts, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	entry := &mimirpb.ResourceEntry{Attributes: []mimirpb.ResourceAttribute{
		{Key: "service.name", StringValue: "frontend"},
		{Key: "debug", Type: mimirpb.RESOURCE_ATTRIBUTE_BOOL, BoolValue: true},
		{Key: "ratio", Type: mimirpb.RESOURCE_ATTRIBUTE_DOUBLE, DoubleValue: 1e21},
		{Key: "id", Type: mimirpb.RESOURCE_ATTRIBUTE_BYTES, BytesValue: []byte{1, 2}},
		{Key: "tags", StringValue: `["a"]`},
	}}
	step := newTSDBResources(db)
	resource := step.resource(entry, 0)
	require.Equal(t, &storage.ResourceContext{
		Identifying: map[string]string{"service.name": "frontend"},
		Descriptive: map[string]string{"debug": "true", "ratio": "1e+21", "id": "AQI=", "tags": `["a"]`},
	}, resource)
	require.Same(t, resource, step.resource(entry, 0), "built once per batch")

	app := db.AppenderV2(context.Background())
	ref, err := app.Append(0, labels.FromStrings(model.MetricNameLabel, "requests"), 0, 100, 1, nil, nil, storage.AOptions{Resource: resource})
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	require.Nil(t, newTSDBResources(db).resource(entry, ref), "the series has the resource")
	changed := &mimirpb.ResourceEntry{Attributes: append([]mimirpb.ResourceAttribute{{Key: "host.name", StringValue: "node-1"}}, entry.Attributes...)}
	require.NotNil(t, newTSDBResources(db).resource(changed, ref))
}
