// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/validation"
)

// The Prometheus TSDB stores each series' metadata from records, with the sequential and the parallel pusher, and
// serves it through the engine.
func TestPrometheusEngineServesNativeMetricMetadata(t *testing.T) {
	skipIfSeriesstore(t, "it doesn't keep native metric metadata yet")
	for _, concurrency := range []int{0, 8} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("concurrency=%d enabled=%t", concurrency, enabled), func(t *testing.T) {
				limits := defaultLimitsTestConfig()
				limits.NativeMetricMetadataEnabled = enabled
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

				// Two requests a minute apart, whose help changes, for series of one family.
				base := time.Now().Truncate(time.Minute).Add(-time.Hour).UnixMilli()
				write, last := compatProducer(t, cfg.IngestStorageConfig.KafkaConfig)
				for step, help := range []string{"Requests.", "Requests served."} {
					timestamp := base + int64(step)*60_000
					series := make([]mimirpb.PreallocTimeseries, 0, 20)
					for n := range 20 {
						ts := compatSeries("requests_total", mimirpb.Sample{TimestampMs: timestamp, Value: float64(n)})
						ts.Labels = append(ts.Labels, mimirpb.LabelAdapter{Name: "pod", Value: fmt.Sprintf("pod-%02d", n)})
						ts.SeriesMetadata = mimirpb.SeriesMetadata{Type: mimirpb.COUNTER, Help: help, Unit: "requests"}
						series = append(series, ts)
					}
					write("tenant", &mimirpb.WriteRequest{
						Timeseries: series,
						Metadata:   []*mimirpb.MetricMetadata{{MetricFamilyName: "requests_total", Type: mimirpb.COUNTER, Help: help, Unit: "requests"}},
					})
				}
				require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingester))
				t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingester)) })
				waitCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				require.NoError(t, ingester.ingestReader.WaitReadConsistencyUntilOffsets(waitCtx, kmeta.NewSingleClusterPartitionOffsets(last())))

				db := ingester.getTSDB("tenant")
				require.NotNil(t, db.nativeMetadata)
				result, err := db.nativeMetadata.nativeMetricMetadata(context.Background(), [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "requests_total")}}, 5)
				if !enabled {
					require.ErrorIs(t, err, tsdb.ErrNativeMetadataDisabled)
					return
				}
				require.NoError(t, err)
				require.True(t, result.Limited)
				require.Len(t, result.Series, 5)
				for n, series := range result.Series {
					require.Equal(t, fmt.Sprintf("pod-%02d", n), series.Labels.Get("pod"))
					require.Equal(t, []tsdb.NativeMetricMetadataVersion{
						{EffectiveFrom: base, Metadata: metadata.Metadata{Type: model.MetricTypeCounter, Unit: "requests", Help: "Requests."}},
						{EffectiveFrom: base + 60_000, Metadata: metadata.Metadata{Type: model.MetricTypeCounter, Unit: "requests", Help: "Requests served."}},
					}, series.Versions)
					require.False(t, series.Truncated)
				}
				require.Equal(t, base, result.CoverageStart)
			})
		}
	}
}
