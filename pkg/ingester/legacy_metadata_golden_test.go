// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/validation"
)

var updateGolden = flag.Bool("update-golden", false, "Rewrite the golden files of the tests that have them.")

// An ingester on each engine keeps the same metric families' metadata from a partition's records, whatever metadata
// of their own the series carry.
func TestLegacyMetricMetadataFromRecords(t *testing.T) {
	skipIfSeriesstore(t, "it runs both engines itself")
	limits := defaultLimitsTestConfig()
	limits.MaxGlobalExemplarsPerUser = 100
	overrides := validation.NewOverrides(limits, nil)
	start := func(engine string, kafkaAddress ...string) (*Ingester, ingest.KafkaConfig) {
		cfg := defaultIngesterTestConfig(t)
		cfg.IngestStorageConfig.KafkaConfig.ConsumeFromPositionAtStartup = "start"
		cfg.BlocksStorageConfig.TSDB.Engine = engine
		ingester, _, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, nil, nil, nil, kafkaAddress...)
		return ingester, cfg.IngestStorageConfig.KafkaConfig
	}
	prometheusIngester, kafkaCfg := start(mimir_tsdb.EnginePrometheus)
	seriesstoreIngester, _ := start(mimir_tsdb.EngineSeriesstore, kafkaCfg.Address[0])
	write, last := compatProducer(t, kafkaCfg)
	write("tenant", legacyMetadataRecordFixture(time.Now().UnixMilli()))
	offset := last()

	golden := map[string][]string{}
	for name, ingester := range map[string]*Ingester{"prometheus": prometheusIngester, "seriesstore": seriesstoreIngester} {
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingester))
		t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingester)) })
		waitCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		require.NoError(t, ingester.ingestReader.WaitReadConsistencyUntilOffsets(waitCtx, kmeta.NewSingleClusterPartitionOffsets(offset)))
		cancel()
		response, err := serveIngester(t, ingester).MetricsMetadata(parityContext("tenant", offset), &client.MetricsMetadataRequest{Limit: -1, LimitPerMetric: -1})
		require.NoError(t, err)
		var entries []string
		for _, m := range response.Metadata {
			entries = append(entries, fmt.Sprintf("%s|%s|%s|%s", m.MetricFamilyName, m.Type, m.Help, m.Unit))
		}
		slices.Sort(entries)
		golden[name] = entries
	}
	require.Equal(t, golden["prometheus"], golden["seriesstore"])

	data, err := json.MarshalIndent(golden, "", "  ")
	require.NoError(t, err)
	data = append(data, '\n')
	path := filepath.Join("testdata", "legacy_metric_metadata_from_records.json")
	if *updateGolden {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, data, 0o644))
		return
	}
	expected, err := os.ReadFile(path)
	require.NoError(t, err)
	require.JSONEq(t, string(expected), string(data))
}

// legacyMetadataRecordFixture is a partition's request with what decides which metric families' metadata the
// ingester keeps: series named like their family, two of them, conflicting metadata of a family, an exemplar-only
// series, a series without data named like its family, as the distributor leaves one it strips of its exemplars, a
// family without series, and a classic histogram, whose series aren't named like their family.
func legacyMetadataRecordFixture(now int64) *mimirpb.WriteRequest {
	series := func(name string, extra ...string) mimirpb.PreallocTimeseries {
		labels := []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: name}}
		for i := 0; i < len(extra); i += 2 {
			labels = append(labels, mimirpb.LabelAdapter{Name: extra[i], Value: extra[i+1]})
		}
		return mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels:  labels,
			Samples: []mimirpb.Sample{{TimestampMs: now, Value: 1}},
		}}
	}
	timeseries := []mimirpb.PreallocTimeseries{
		series("requests_total", "pod", "pod-0"),
		series("requests_total", "pod", "pod-1"),
		series("latency_seconds_bucket", "le", "+Inf"),
		series("latency_seconds_count"),
	}
	exemplarOnly := series("exemplars_total", "pod", "pod-0")
	exemplarOnly.Samples = nil
	exemplarOnly.Exemplars = []mimirpb.Exemplar{{TimestampMs: now, Value: 1, Labels: []mimirpb.LabelAdapter{{Name: "trace_id", Value: "abc"}}}}
	withoutData := series("stripped_total", "pod", "pod-0")
	withoutData.Samples = nil
	timeseries = append(timeseries, exemplarOnly, withoutData)

	return &mimirpb.WriteRequest{
		Timeseries: timeseries,
		Metadata: []*mimirpb.MetricMetadata{
			{MetricFamilyName: "requests_total", Type: mimirpb.COUNTER, Help: "Requests.", Unit: "requests"},
			{MetricFamilyName: "requests_total", Type: mimirpb.COUNTER, Help: "Requests served.", Unit: "requests"},
			{MetricFamilyName: "latency_seconds", Type: mimirpb.HISTOGRAM, Help: "Latency.", Unit: "seconds"},
			{MetricFamilyName: "exemplars_total", Type: mimirpb.GAUGE, Help: "With exemplars."},
			{MetricFamilyName: "stripped_total", Type: mimirpb.COUNTER, Help: "Stripped of its exemplars."},
			{MetricFamilyName: "no_series", Type: mimirpb.GAUGE, Help: "No series of its own."},
		},
		Source: mimirpb.API,
	}
}
