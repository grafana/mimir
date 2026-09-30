// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

// An ingester on the seriesstore engine and one on the Prometheus TSDB consume the same records
// and answer every read alike, before and after compacting their heads.
func TestSeriesstoreEngineAnswersLikeThePrometheusTSDB(t *testing.T) {
	skipIfSeriesstore(t, "it runs both engines itself")
	limits := defaultLimitsTestConfig()
	limits.NativeHistogramsIngestionEnabled = true
	limits.OutOfOrderTimeWindow = model.Duration(time.Hour)
	limits.MaxGlobalExemplarsPerUser = 100
	overrides := validation.NewOverrides(limits, nil)

	start := func(engine string, kafkaAddress ...string) (*Ingester, client.IngesterClient, ingest.KafkaConfig) {
		cfg := defaultIngesterTestConfig(t)
		cfg.IngestStorageConfig.KafkaConfig.ConsumeFromPositionAtStartup = "start"
		cfg.BlocksStorageConfig.TSDB.Engine = engine
		ingester, _, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, nil, nil, nil, kafkaAddress...)
		return ingester, nil, cfg.IngestStorageConfig.KafkaConfig
	}
	prometheusIngester, _, kafkaCfg := start(mimir_tsdb.EnginePrometheus)
	seriesstoreIngester, _, _ := start(mimir_tsdb.EngineSeriesstore, kafkaCfg.Address[0])

	// Two tenants, 40 float series a minute apart over 3h (every tenth also 30 minutes out of
	// order), histograms, exemplars, and a label value with a newline.
	now := time.Now().Truncate(time.Minute)
	base := now.Add(-3 * time.Hour).UnixMilli()
	write, last := compatProducer(t, kafkaCfg)
	random := rand.New(rand.NewPCG(3, 4))
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		for minute := int64(0); minute < 180; minute += 10 {
			var series []mimirpb.PreallocTimeseries
			for n := range 40 {
				var samples []mimirpb.Sample
				for step := range int64(10) {
					samples = append(samples, mimirpb.Sample{TimestampMs: base + (minute+step)*60_000, Value: random.Float64() * 100})
				}
				if n%10 == 0 && minute >= 40 {
					samples = append(samples, mimirpb.Sample{TimestampMs: base + (minute-30)*60_000 + 30_000, Value: float64(n)})
				}
				ts := compatSeries("requests_total", samples...)
				ts.Labels = append(ts.Labels,
					mimirpb.LabelAdapter{Name: "job", Value: fmt.Sprintf("job-%d", n%4)},
					mimirpb.LabelAdapter{Name: "pod", Value: fmt.Sprintf("pod-%d", n)})
				if n == 7 {
					ts.Labels = append(ts.Labels, mimirpb.LabelAdapter{Name: "message", Value: "first\nsecond"})
					ts.Exemplars = []mimirpb.Exemplar{{TimestampMs: samples[0].TimestampMs, Value: 1, Labels: []mimirpb.LabelAdapter{{Name: "trace_id", Value: fmt.Sprint(minute)}}}}
				}
				series = append(series, ts)
			}
			var histograms []mimirpb.Histogram
			for step := range int64(10) {
				histograms = append(histograms, mimirpb.FromHistogramToHistogramProto(base+(minute+step)*60_000, test.GenerateTestHistogram(int(minute+step))))
			}
			series = append(series, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
				Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "latency"}}, Histograms: histograms,
			}})
			write(tenant, &mimirpb.WriteRequest{Timeseries: series})
		}
	}
	offset := last()

	apis := map[string]client.IngesterClient{}
	for name, ingester := range map[string]*Ingester{"prometheus": prometheusIngester, "seriesstore": seriesstoreIngester} {
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingester))
		t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingester)) })
		waitCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		require.NoError(t, ingester.ingestReader.WaitReadConsistencyUntilOffsets(waitCtx, kmeta.NewSingleClusterPartitionOffsets(offset)))
		cancel()
		apis[name] = serveIngester(t, ingester)
	}

	windows := [][2]int64{
		{math.MinInt64, math.MaxInt64},
		{base, base + 30*60_000},
		{base + 70*60_000, base + 125*60_000},
		{now.Add(-20 * time.Minute).UnixMilli(), now.UnixMilli()},
	}
	matcherSets := [][]*client.LabelMatcher{
		{parityMatcher(client.REGEX_MATCH, model.MetricNameLabel, ".+")},
		{parityMatcher(client.EQUAL, model.MetricNameLabel, "requests_total"), parityMatcher(client.EQUAL, "job", "job-1")},
		{parityMatcher(client.EQUAL, model.MetricNameLabel, "requests_total"), parityMatcher(client.REGEX_MATCH, "pod", "pod-(1|2|3)0")},
		{parityMatcher(client.EQUAL, model.MetricNameLabel, "requests_total"), parityMatcher(client.NOT_EQUAL, "job", "job-0"), parityMatcher(client.EQUAL, "__query_shard__", "2_of_3")},
		{parityMatcher(client.REGEX_MATCH, "message", "first.second")},
		{parityMatcher(client.EQUAL, model.MetricNameLabel, "latency")},
	}
	compare := func(stage string) {
		for _, tenant := range []string{"tenant-a", "tenant-b"} {
			ctx := parityContext(tenant, offset)
			for _, window := range windows {
				for _, matchers := range matcherSets {
					request := parityRequest(window[0], window[1], matchers...)
					// Samples as a querier merges them: a compacted Prometheus head can return a sample in both
					// a block's chunk and the head's.
					expected := mergedSamples(paritySeries(t, apis["prometheus"], ctx, request))
					actual := mergedSamples(paritySeries(t, apis["seriesstore"], ctx, request))
					require.Equal(t, expected, actual, "%s %s %v %v", stage, tenant, window, matchers)
				}
				names := func(api client.IngesterClient) []string {
					response, err := api.LabelNames(ctx, &client.LabelNamesRequest{StartTimestampMs: window[0], EndTimestampMs: window[1]})
					require.NoError(t, err)
					sort.Strings(response.LabelNames)
					return response.LabelNames
				}
				require.Equal(t, names(apis["prometheus"]), names(apis["seriesstore"]), "%s %s %v label names", stage, tenant, window)
				values := func(api client.IngesterClient) []string {
					response, err := api.LabelValues(ctx, &client.LabelValuesRequest{LabelName: "pod", StartTimestampMs: window[0], EndTimestampMs: window[1]})
					require.NoError(t, err)
					sort.Strings(response.LabelValues)
					return response.LabelValues
				}
				require.Equal(t, values(apis["prometheus"]), values(apis["seriesstore"]), "%s %s %v label values", stage, tenant, window)
			}
			stats := func(api client.IngesterClient) uint64 {
				response, err := api.UserStats(ctx, &client.UserStatsRequest{})
				require.NoError(t, err)
				return response.NumSeries
			}
			require.Equal(t, stats(apis["prometheus"]), stats(apis["seriesstore"]), "%s %s user stats", stage, tenant)
			exemplars := func(api client.IngesterClient) string {
				response, err := api.QueryExemplars(ctx, &client.ExemplarQueryRequest{StartTimestampMs: 0, EndTimestampMs: now.UnixMilli(), Matchers: []*client.LabelMatchers{{Matchers: []*client.LabelMatcher{parityMatcher(client.EQUAL, model.MetricNameLabel, "requests_total")}}}})
				require.NoError(t, err)
				return response.String()
			}
			require.Equal(t, exemplars(apis["prometheus"]), exemplars(apis["seriesstore"]), "%s %s exemplars", stage, tenant)
		}
	}
	compare("head")
	// Then from what head compactions left: the older blocks' data and the rest of the head.
	for _, ingester := range []*Ingester{prometheusIngester, seriesstoreIngester} {
		ingester.compactBlocks(context.Background(), true, now.Add(-time.Hour).UnixMilli(), nil)
	}
	compare("compacted")
}

func serveIngester(t *testing.T, ingester *Ingester) client.IngesterClient {
	server := grpc.NewServer(
		grpc.UnaryInterceptor(middleware.ServerUserHeaderInterceptor),
		grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor),
	)
	client.RegisterIngesterServer(server, ingester)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(listener) }()
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = connection.Close()
		server.Stop()
	})
	return client.NewIngesterClient(connection)
}

// mergedSamples sorts each series' samples by timestamp and drops repeats of the same sample.
func mergedSamples(series map[string][]string) map[string][]string {
	merged := make(map[string][]string, len(series))
	for key, samples := range series {
		samples = slices.Clone(samples)
		slices.Sort(samples)
		merged[key] = slices.Compact(samples)
	}
	return merged
}
