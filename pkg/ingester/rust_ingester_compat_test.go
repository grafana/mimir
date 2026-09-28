// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/value"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/cmd/rust-kafka-ingester/ring-sidecar/handlers"
	"github.com/grafana/mimir/pkg/costattribution"
	"github.com/grafana/mimir/pkg/costattribution/costattributionmodel"
	asmodel "github.com/grafana/mimir/pkg/ingester/activeseries/model"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
	"github.com/grafana/mimir/pkg/util/validation"
)

// The Go/Rust compatibility battery: each test runs the Go ingester and the Rust ingester over the
// same Kafka records, with the same default limits (Go config and Rust flags) and the same
// per-tenant overrides (Go tenant limits and the Rust runtime config), then compares what
// queries return, which requests fail and how, and the Go-named metrics both export.

type compatSetup struct {
	limits       validation.Limits
	tenantLimits map[string]*validation.Limits
	// Rust flags for the non-default limits in `limits`; Rust takes defaults from flags.
	rustArgs []string
	config   func(*Config)
	// Cost attribution needs a manager on the Go side.
	costAttribution bool
}

type compatSide struct {
	api     client.IngesterClient
	offset  int64
	metrics func(tb testing.TB) map[string]*dto.MetricFamily
	usage   func(tb testing.TB) map[string]*dto.MetricFamily
}

type compatIngesters struct {
	goIngester *Ingester
	goSide     compatSide
	rustSide   compatSide
	kafka      ingest.KafkaConfig
}

// produceAndWait writes more records once both ingesters consumed the earlier ones, so each
// appends them in their own flush, and waits until both consumed them too.
func (c *compatIngesters) produceAndWait(tb testing.TB, tenants []string, produce func(write func(string, *mimirpb.WriteRequest))) {
	tb.Helper()
	write, last := compatProducer(tb, c.kafka)
	produce(write)
	offset := last()
	c.goSide.offset = offset
	c.rustSide.offset = offset
	// The test's gRPC server has no read consistency interceptor, so the Go ingester is waited
	// for directly.
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	require.NoError(tb, c.goIngester.ingestReader.WaitReadConsistencyUntilOffsets(waitCtx, kmeta.NewSingleClusterPartitionOffsets(offset)))
	for _, tenant := range tenants {
		_, err := c.rustSide.api.LabelNames(parityContext(tenant, offset), &client.LabelNamesRequest{StartTimestampMs: 0, EndTimestampMs: math.MaxInt64})
		require.NoError(tb, err)
	}
}

func freeAddress(tb testing.TB) string {
	tb.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	address := listener.Addr().String()
	require.NoError(tb, listener.Close())
	return address
}

func gatherFamilies(tb testing.TB, gatherer prometheus.Gatherer) map[string]*dto.MetricFamily {
	tb.Helper()
	families, err := gatherer.Gather()
	require.NoError(tb, err)
	result := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		result[family.GetName()] = family
	}
	return result
}

func scrapeFamilies(tb testing.TB, url string) map[string]*dto.MetricFamily {
	tb.Helper()
	response, err := http.Get(url)
	require.NoError(tb, err)
	defer response.Body.Close()
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(response.Body)
	require.NoError(tb, err)
	return families
}

// servePartitionRing answers the ring sidecar's endpoints from the Go ingester's partition ring.
func servePartitionRing(tb testing.TB, watcher *ring.PartitionRingWatcher, partition int32) string {
	tb.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/active-partitions", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, watcher.PartitionRing().ActivePartitionsCount())
	})
	mux.HandleFunc("/owned-token-ranges", func(w http.ResponseWriter, req *http.Request) {
		var body struct {
			Tenants map[string]int `json:"tenants"`
		}
		require.NoError(tb, json.NewDecoder(req.Body).Decode(&body))
		result := map[string][]uint32{}
		for tenant, shardSize := range body.Tenants {
			subring, err := watcher.PartitionRing().ShuffleShard(tenant, shardSize)
			require.NoError(tb, err)
			ranges, err := subring.GetTokenRangesForPartition(partition)
			if err != nil {
				result[tenant] = nil
				continue
			}
			result[tenant] = ranges
		}
		_ = json.NewEncoder(w).Encode(result)
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	go func() { _ = server.Serve(listener) }()
	tb.Cleanup(func() { _ = server.Close() })
	return "http://" + listener.Addr().String()
}

func startCompatIngesters(tb testing.TB, setup compatSetup, produce func(testing.TB, ingest.KafkaConfig) int64) compatIngesters {
	tb.Helper()
	buildRustIngester(tb)
	ctx := context.Background()
	cfg := defaultIngesterTestConfig(tb)
	cfg.BlocksStorageConfig.TSDB.WALSegmentSizeBytes = -1
	cfg.IngestStorageConfig.KafkaConfig.ConsumeFromPositionAtStartup = "start"
	cfg.OwnedSeriesUpdateInterval = 100 * time.Millisecond
	if setup.config != nil {
		setup.config(&cfg)
	}
	overrides := validation.NewOverrides(setup.limits, validation.NewMockTenantLimits(setup.tenantLimits))
	registry := prometheus.NewRegistry()
	usageRegistry := prometheus.NewRegistry()
	goIngester, _, watcher := createTestIngesterWithIngestStorage(tb, &cfg, overrides, nil, registry, nil)
	if setup.costAttribution {
		manager, err := costattribution.NewManager(3*time.Minute, 20*time.Minute, log.NewNopLogger(), overrides, registry, usageRegistry)
		require.NoError(tb, err)
		require.NoError(tb, services.StartAndAwaitRunning(ctx, manager))
		tb.Cleanup(func() { _ = services.StopAndAwaitTerminated(ctx, manager) })
		goIngester.costAttributionMgr = manager
	}
	offset := produce(tb, cfg.IngestStorageConfig.KafkaConfig)
	require.GreaterOrEqual(tb, offset, int64(0))
	require.NoError(tb, services.StartAndAwaitRunning(ctx, goIngester))
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	require.NoError(tb, goIngester.ingestReader.WaitReadConsistencyUntilOffsets(waitCtx, kmeta.NewSingleClusterPartitionOffsets(offset)))
	server := grpc.NewServer(
		grpc.UnaryInterceptor(middleware.ServerUserHeaderInterceptor),
		grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor),
	)
	client.RegisterIngesterServer(server, goIngester)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	go func() { _ = server.Serve(listener) }()
	goConn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(tb, err)
	tb.Cleanup(func() {
		_ = goConn.Close()
		server.Stop()
		require.NoError(tb, services.StopAndAwaitTerminated(ctx, goIngester))
	})

	// The Rust ingester reads the same tenant limits from a runtime config file written as Go
	// marshals them.
	overridesFile := filepath.Join(tb.TempDir(), "overrides.yaml")
	encoded, err := yaml.Marshal(map[string]any{"overrides": setup.tenantLimits})
	require.NoError(tb, err)
	require.NoError(tb, os.WriteFile(overridesFile, encoded, 0o600))
	ringURL := servePartitionRing(tb, watcher, 0)
	metricsAddress := freeAddress(tb)
	args := append([]string{
		"--runtime-config.file", overridesFile,
		"--active-partitions-url", ringURL + "/active-partitions",
		"--owned-token-ranges-url", ringURL + "/owned-token-ranges",
		"--metrics-listen", metricsAddress,
		"--cost-attribution.registry-path", "/usage-metrics",
		"--ingester.active-series-metrics-update-period", "100ms",
		"--ingester.owned-series-update-interval", "100ms",
	}, setup.rustArgs...)
	process, rustConn := startRustIngesterAndWait(tb, cfg.IngestStorageConfig.KafkaConfig.Address[0], cfg.IngestStorageConfig.KafkaConfig.Topic, tb.TempDir(), offset, args...)
	tb.Cleanup(func() {
		_ = rustConn.Close()
		stopRustIngester(tb, process)
	})
	return compatIngesters{
		kafka:      cfg.IngestStorageConfig.KafkaConfig,
		goIngester: goIngester,
		goSide: compatSide{
			api:     client.NewIngesterClient(goConn),
			offset:  offset,
			metrics: func(tb testing.TB) map[string]*dto.MetricFamily { return gatherFamilies(tb, registry) },
			usage:   func(tb testing.TB) map[string]*dto.MetricFamily { return gatherFamilies(tb, usageRegistry) },
		},
		rustSide: compatSide{
			api:    client.NewIngesterClient(rustConn),
			offset: offset,
			metrics: func(tb testing.TB) map[string]*dto.MetricFamily {
				return scrapeFamilies(tb, "http://"+metricsAddress+"/metrics")
			},
			usage: func(tb testing.TB) map[string]*dto.MetricFamily {
				return scrapeFamilies(tb, "http://"+metricsAddress+"/usage-metrics")
			},
		},
	}
}

// metricValues flattens a family to "labels=value" strings, dropping the labels `ignored`.
func metricValues(family *dto.MetricFamily, ignored ...string) []string {
	if family == nil {
		return nil
	}
	var values []string
	for _, metric := range family.GetMetric() {
		var labels []string
		for _, pair := range metric.GetLabel() {
			skip := false
			for _, name := range ignored {
				skip = skip || pair.GetName() == name
			}
			if !skip {
				labels = append(labels, pair.GetName()+"="+pair.GetValue())
			}
		}
		sort.Strings(labels)
		var value float64
		switch {
		case metric.Counter != nil:
			value = metric.GetCounter().GetValue()
		case metric.Gauge != nil:
			value = metric.GetGauge().GetValue()
		case metric.Histogram != nil:
			value = float64(metric.GetHistogram().GetSampleCount())
		case metric.Untyped != nil:
			value = metric.GetUntyped().GetValue()
		}
		// Zero-valued counters are exported by one side only when it pre-registers them.
		if value == 0 && metric.Counter != nil {
			continue
		}
		formatted := strconv.FormatFloat(value, 'g', -1, 64)
		if metric.Histogram != nil {
			// Observed values matter too, like the number of series each query returned.
			formatted += "/" + strconv.FormatFloat(metric.GetHistogram().GetSampleSum(), 'g', -1, 64)
		}
		values = append(values, strings.Join(labels, ",")+"="+formatted)
	}
	sort.Strings(values)
	return values
}

// requireSameMetrics waits for the named metrics to agree, since each side updates on its own
// schedule, and reports the last difference.
func requireSameMetrics(t *testing.T, ingesters compatIngesters, refresh func(), usage bool, names ...string) {
	t.Helper()
	var last string
	for deadline := time.Now().Add(15 * time.Second); ; {
		if refresh != nil {
			refresh()
		}
		gather := func(side compatSide) map[string]*dto.MetricFamily {
			if usage {
				return side.usage(t)
			}
			return side.metrics(t)
		}
		goFamilies, rustFamilies := gather(ingesters.goSide), gather(ingesters.rustSide)
		last = ""
		for _, name := range names {
			goValues, rustValues := metricValues(goFamilies[name]), metricValues(rustFamilies[name])
			if strings.Join(goValues, "\n") != strings.Join(rustValues, "\n") {
				last += fmt.Sprintf("%s:\n  Go:   %v\n  Rust: %v\n", name, goValues, rustValues)
			}
		}
		if last == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("metrics differ:\n%s", last)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func compatProducer(tb testing.TB, cfg ingest.KafkaConfig) (write func(tenant string, request *mimirpb.WriteRequest), last func() int64) {
	producer, err := ingest.NewKafkaWriterClient(cfg, 20, log.NewNopLogger(), nil)
	require.NoError(tb, err)
	tb.Cleanup(producer.Close)
	var lastOffset int64 = -1
	return func(tenant string, request *mimirpb.WriteRequest) {
			records, _, err := ingest.RecordSerializerFromVersion(2).ToRecords(cfg.Topic, 0, tenant, request, 1<<20)
			require.NoError(tb, err)
			require.NoError(tb, producer.ProduceSync(context.Background(), records...).FirstErr())
			lastOffset = records[len(records)-1].Offset
		}, func() int64 {
			return lastOffset
		}
}

func compatSeries(name string, samples ...mimirpb.Sample) mimirpb.PreallocTimeseries {
	return mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
		Labels:  []mimirpb.LabelAdapter{{Name: "__name__", Value: name}},
		Samples: samples,
	}}
}

func compatQueryAll(t *testing.T, ingesters compatIngesters, tenant string, start, end int64) {
	t.Helper()
	request := parityRequest(start, end, parityMatcher(client.REGEX_MATCH, "__name__", ".+"))
	goSeries := paritySeries(t, ingesters.goSide.api, parityContext(tenant, ingesters.goSide.offset), request)
	rustSeries := paritySeries(t, ingesters.rustSide.api, parityContext(tenant, ingesters.rustSide.offset), request)
	require.Equal(t, goSeries, rustSeries, "tenant %s", tenant)
}

func ms(t time.Time) int64 { return t.UnixMilli() }

// Out-of-order, out-of-bounds, duplicate and grace period handling, with and without an
// out-of-order window, and the native histogram switch.
func TestRustCompatSampleAcceptance(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.NativeHistogramsIngestionEnabled = true
	strict := limits
	window := limits
	window.OutOfOrderTimeWindow = model.Duration(2 * time.Hour)
	noHistograms := limits
	noHistograms.NativeHistogramsIngestionEnabled = false
	pastGrace := limits
	pastGrace.PastGracePeriod = model.Duration(time.Hour)
	now := time.Now().Truncate(time.Second)
	tenants := []string{"strict", "window", "no-histograms", "past-grace"}
	head := ms(now.Add(-10 * time.Minute))
	ingesters := startCompatIngesters(t, compatSetup{
		limits: limits,
		tenantLimits: map[string]*validation.Limits{
			"strict": &strict, "window": &window, "no-histograms": &noHistograms, "past-grace": &pastGrace,
		},
	}, func(tb testing.TB, cfg ingest.KafkaConfig) int64 {
		write, last := compatProducer(tb, cfg)
		for _, tenant := range tenants {
			write(tenant, &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
				compatSeries("a", mimirpb.Sample{TimestampMs: head, Value: 1}),
				compatSeries("b", mimirpb.Sample{TimestampMs: head - 1000, Value: 1}),
				// Within one head append, samples are checked against the committed series, so
				// the repeated and older ones only get dropped when committing.
				compatSeries("i", mimirpb.Sample{TimestampMs: head - 500, Value: 1}, mimirpb.Sample{TimestampMs: head - 500, Value: 2}, mimirpb.Sample{TimestampMs: head - 700, Value: 3}),
			}})
		}
		return last()
	})
	ingesters.produceAndWait(t, tenants, func(write func(string, *mimirpb.WriteRequest)) {
		for _, tenant := range tenants {
			write(tenant, &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
				// Duplicate with the same and another value.
				compatSeries("a", mimirpb.Sample{TimestampMs: head, Value: 1}),
				compatSeries("a", mimirpb.Sample{TimestampMs: head, Value: 2}),
				// Older than the series, within an hour of the head.
				compatSeries("b", mimirpb.Sample{TimestampMs: head - 5000, Value: 2}),
				// New series 30 minutes, 90 minutes and 3 hours behind the head.
				compatSeries("c", mimirpb.Sample{TimestampMs: head - 30*60_000, Value: 3}),
				compatSeries("d", mimirpb.Sample{TimestampMs: head - 90*60_000, Value: 4}),
				compatSeries("e", mimirpb.Sample{TimestampMs: head - 3*60*60_000, Value: 5}),
				// Beyond the creation grace period.
				compatSeries("f", mimirpb.Sample{TimestampMs: ms(now.Add(time.Hour)), Value: 6}),
				// Created timestamps before the first sample.
				{TimeSeries: &mimirpb.TimeSeries{
					Labels:           []mimirpb.LabelAdapter{{Name: "__name__", Value: "g"}},
					Samples:          []mimirpb.Sample{{TimestampMs: head + 1000, Value: 7}},
					CreatedTimestamp: head - 2*60*60_000,
				}},
				{TimeSeries: &mimirpb.TimeSeries{
					Labels:           []mimirpb.LabelAdapter{{Name: "__name__", Value: "h"}},
					Histograms:       []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(head+2000, &histogram.Histogram{Count: 2, Sum: 1, Schema: 0, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{2}})},
					CreatedTimestamp: head + 1500,
				}},
			}})
		}
	})
	for _, tenant := range []string{"strict", "window", "no-histograms", "past-grace"} {
		t.Run(tenant, func(t *testing.T) {
			compatQueryAll(t, ingesters, tenant, 0, ms(now.Add(2*time.Hour)))
		})
	}
	requireSameMetrics(t, ingesters, nil, false,
		"cortex_discarded_samples_total",
		"cortex_ingester_ingested_samples_total",
		"cortex_ingester_ingested_samples_failures_total",
		"cortex_ingester_tsdb_out_of_order_samples_appended_total",
	)
}

// Exemplar storage bounds and ordering, and metadata limits.
func TestRustCompatExemplarsAndMetadata(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.MaxGlobalExemplarsPerUser = 100
	limits.OutOfOrderTimeWindow = model.Duration(time.Hour)
	small := limits
	small.MaxGlobalExemplarsPerUser = 3
	small.MaxGlobalMetricsWithMetadataPerUser = 2
	small.MaxGlobalMetadataPerMetric = 2
	disabled := limits
	disabled.MaxGlobalExemplarsPerUser = 0
	now := ms(time.Now())
	tenants := []string{"default", "small", "disabled"}
	exemplar := func(ts int64, trace string) mimirpb.Exemplar {
		return mimirpb.Exemplar{TimestampMs: ts, Value: 1, Labels: []mimirpb.LabelAdapter{{Name: "trace_id", Value: trace}}}
	}
	// Each round is appended in its own flush: exemplars are validated against the newest one
	// before the flush.
	writeRound := func(write func(string, *mimirpb.WriteRequest), round int) {
		for _, tenant := range tenants {
			{
				base := now - int64(10-round)*10_000
				series := []mimirpb.PreallocTimeseries{}
				for n := range 3 {
					ts := compatSeries(fmt.Sprintf("metric_%d", n), mimirpb.Sample{TimestampMs: base, Value: float64(n)})
					ts.Exemplars = []mimirpb.Exemplar{exemplar(base, fmt.Sprintf("%d-%d", round, n))}
					if round == 2 && n == 0 {
						// Duplicate, out-of-order within the window and too old.
						ts.Exemplars = append(ts.Exemplars, exemplar(base, fmt.Sprintf("%d-%d", round, n)), exemplar(base-5_000, "ooo"), exemplar(base-2*60*60_000, "old"))
					}
					series = append(series, ts)
				}
				// Exemplars without samples for a series that doesn't exist yet.
				missing := compatSeries("missing_metric")
				missing.Exemplars = []mimirpb.Exemplar{exemplar(base, "missing")}
				series = append(series, missing)
				write(tenant, &mimirpb.WriteRequest{Timeseries: series, Metadata: []*mimirpb.MetricMetadata{
					{MetricFamilyName: "metric_0", Type: mimirpb.COUNTER, Help: fmt.Sprintf("help %d", round)},
					{MetricFamilyName: fmt.Sprintf("metric_%d", round), Type: mimirpb.GAUGE, Help: "gauge"},
				}})
			}
		}
	}
	ingesters := startCompatIngesters(t, compatSetup{
		limits:       limits,
		tenantLimits: map[string]*validation.Limits{"small": &small, "disabled": &disabled},
		rustArgs:     []string{"--ingester.max-global-exemplars-per-user", "100", "--ingester.out-of-order-time-window", "1h"},
	}, func(tb testing.TB, cfg ingest.KafkaConfig) int64 {
		write, last := compatProducer(tb, cfg)
		writeRound(write, 0)
		return last()
	})
	for round := 1; round < 3; round++ {
		ingesters.produceAndWait(t, tenants, func(write func(string, *mimirpb.WriteRequest)) { writeRound(write, round) })
	}
	for _, tenant := range tenants {
		t.Run(tenant, func(t *testing.T) {
			exemplarRequest := &client.ExemplarQueryRequest{StartTimestampMs: 0, EndTimestampMs: now + 60_000, Matchers: []*client.LabelMatchers{{Matchers: []*client.LabelMatcher{parityMatcher(client.REGEX_MATCH, "__name__", ".+")}}}}
			goExemplars, err := ingesters.goSide.api.QueryExemplars(parityContext(tenant, ingesters.goSide.offset), exemplarRequest)
			require.NoError(t, err)
			rustExemplars, err := ingesters.rustSide.api.QueryExemplars(parityContext(tenant, ingesters.rustSide.offset), exemplarRequest)
			require.NoError(t, err)
			require.Equal(t, goExemplars.Timeseries, rustExemplars.Timeseries)
			metadata := func(api client.IngesterClient, offset int64) []string {
				response, err := api.MetricsMetadata(parityContext(tenant, offset), &client.MetricsMetadataRequest{Limit: -1, LimitPerMetric: -1})
				require.NoError(t, err)
				var result []string
				for _, item := range response.Metadata {
					result = append(result, item.MetricFamilyName+"/"+item.Help)
				}
				sort.Strings(result)
				return result
			}
			require.Equal(t, metadata(ingesters.goSide.api, ingesters.goSide.offset), metadata(ingesters.rustSide.api, ingesters.rustSide.offset))
		})
	}
	requireSameMetrics(t, ingesters, nil, false,
		"cortex_discarded_metadata_total",
		"cortex_ingester_ingested_exemplars_total",
		"cortex_ingester_ingested_exemplars_failures_total",
		"cortex_ingester_ingested_metadata_total",
		"cortex_ingester_ingested_metadata_failures_total",
		"cortex_ingester_memory_metadata_created_total",
		"cortex_ingester_tsdb_exemplar_exemplars_appended_total",
		"cortex_ingester_tsdb_exemplar_exemplars_in_storage",
		"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
		"cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
	)
}

// accountingSeries writes, for each tenant, series matched by the custom trackers and cost
// attribution trackers in different ways, and one native histogram.
func accountingSeries(write func(string, *mimirpb.WriteRequest), now int64) {
	for _, tenant := range []string{"default", "extra"} {
		var series []mimirpb.PreallocTimeseries
		for n, job := range []string{"api", "api", "web", "db"} {
			for team := range n + 1 {
				series = append(series, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
					Labels:  []mimirpb.LabelAdapter{{Name: "__name__", Value: "up"}, {Name: "job", Value: job}, {Name: "n", Value: strconv.Itoa(n)}, {Name: "team", Value: strconv.Itoa(team)}},
					Samples: []mimirpb.Sample{{TimestampMs: now, Value: 1}},
				}})
			}
		}
		series = append(series, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels:     []mimirpb.LabelAdapter{{Name: "__name__", Value: "latency"}, {Name: "job", Value: "api"}},
			Histograms: []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(now, &histogram.Histogram{Count: 3, Sum: 1, Schema: 0, PositiveSpans: []histogram.Span{{Offset: 0, Length: 3}}, PositiveBuckets: []int64{1, 0, 0}})},
		}})
		write(tenant, &mimirpb.WriteRequest{Timeseries: series})
	}
}

func accountingLimits(t *testing.T) (validation.Limits, validation.Limits, string) {
	// Each tenant's limits are built on their own: copying validation.Limits shares its cached
	// merged tracker configurations.
	newLimits := func() validation.Limits {
		limits := defaultLimitsTestConfig()
		limits.NativeHistogramsIngestionEnabled = true
		limits.MaxGlobalSeriesPerUser = 1000
		trackers, err := asmodel.NewCustomTrackersConfig(map[string]string{"api": `{job="api"}`, "all": `{__name__=~".+"}`})
		require.NoError(t, err)
		limits.ActiveSeriesBaseCustomTrackersConfig = trackers
		limits.CostAttributionBaseTrackers = costattributionmodel.TrackerConfigs{
			"by-team":  {Labels: costattributionmodel.Labels{{Input: "team"}}},
			"internal": {Labels: costattributionmodel.Labels{{Input: "job", Output: "service"}}, Internal: true},
		}
		limits.MaxCostAttributionCardinality = 3
		return limits
	}
	limits := newLimits()
	extra := newLimits()
	additional, err := asmodel.NewCustomTrackersConfig(map[string]string{"web": `{job="web"}`})
	require.NoError(t, err)
	extra.ActiveSeriesAdditionalCustomTrackersConfig = additional
	extra.MaxGlobalSeriesPerUser = 50
	encodedTrackers, err := json.Marshal(limits.CostAttributionBaseTrackers)
	require.NoError(t, err)
	return limits, extra, string(encodedTrackers)
}

func accountingRustArgs(encodedTrackers string) []string {
	return []string{
		"--ingester.max-global-series-per-user", "1000",
		"--ingester.active-series-custom-trackers", `api:{job="api"};all:{__name__=~".+"}`,
		"--validation.cost-attribution-trackers", encodedTrackers,
		"--validation.max-cost-attribution-cardinality", "3",
	}
}

// Active series, custom trackers, cost attribution, head series and limits are reported alike.
func TestRustCompatAccounting(t *testing.T) {
	limits, extra, encodedTrackers := accountingLimits(t)
	now := ms(time.Now())
	ingesters := startCompatIngesters(t, compatSetup{
		limits:          limits,
		tenantLimits:    map[string]*validation.Limits{"extra": &extra},
		costAttribution: true,
		rustArgs:        accountingRustArgs(encodedTrackers),
	}, func(tb testing.TB, cfg ingest.KafkaConfig) int64 {
		write, last := compatProducer(tb, cfg)
		accountingSeries(write, now)
		return last()
	})
	refresh := func() {
		ingesters.goIngester.updateActiveSeries(time.Now())
		ingesters.goIngester.updateLimitMetrics()
	}
	requireSameMetrics(t, ingesters, refresh, false,
		"cortex_ingester_active_series",
		"cortex_ingester_active_series_custom_tracker",
		"cortex_ingester_active_native_histogram_series",
		"cortex_ingester_active_native_histogram_series_custom_tracker",
		"cortex_ingester_active_native_histogram_buckets",
		"cortex_ingester_active_native_histogram_buckets_custom_tracker",
		"cortex_ingester_memory_series",
		"cortex_ingester_memory_users",
		"cortex_ingester_memory_series_created_total",
		"cortex_ingester_owned_series",
		"cortex_ingester_local_limits",
		"cortex_ingester_attributed_active_series",
		"cortex_attributed_series_overflow_labels",
		"cortex_cost_attribution_active_series_tracker_cardinality",
		"cortex_cost_attribution_active_series_tracker_overflown",
	)
	requireSameMetrics(t, ingesters, refresh, true,
		"cortex_ingester_attributed_active_series",
		"cortex_attributed_series_overflow_labels",
	)
	for _, tenant := range []string{"default", "extra"} {
		goStats, err := ingesters.goSide.api.UserStats(parityContext(tenant, ingesters.goSide.offset), &client.UserStatsRequest{})
		require.NoError(t, err)
		rustStats, err := ingesters.rustSide.api.UserStats(parityContext(tenant, ingesters.rustSide.offset), &client.UserStatsRequest{})
		require.NoError(t, err)
		require.Equal(t, goStats.NumSeries, rustStats.NumSeries, tenant)
	}
}

// With owned series tracking, owned series and the local limits derived from them are reported
// alike. Series written before the Go ingester's partition joined its ring were cleared from its
// active series, so those are compared after later samples.
func TestRustCompatOwnedSeries(t *testing.T) {
	limits, extra, encodedTrackers := accountingLimits(t)
	now := ms(time.Now())
	ingesters := startCompatIngesters(t, compatSetup{
		limits:          limits,
		tenantLimits:    map[string]*validation.Limits{"extra": &extra},
		costAttribution: true,
		config: func(cfg *Config) {
			cfg.UpdateIngesterOwnedSeries = true
			cfg.UseIngesterOwnedSeriesForLimits = true
		},
		rustArgs: append(accountingRustArgs(encodedTrackers),
			"--ingester.track-ingester-owned-series", "true",
			"--ingester.use-ingester-owned-series-for-limits", "true",
		),
	}, func(tb testing.TB, cfg ingest.KafkaConfig) int64 {
		write, last := compatProducer(tb, cfg)
		accountingSeries(write, now)
		return last()
	})
	refresh := func() {
		ingesters.goIngester.updateActiveSeries(time.Now())
		ingesters.goIngester.updateLimitMetrics()
	}
	owned := []string{
		"cortex_ingester_memory_series",
		"cortex_ingester_owned_series",
		"cortex_ingester_local_limits",
	}
	requireSameMetrics(t, ingesters, refresh, false, owned...)
	ingesters.produceAndWait(t, []string{"default", "extra"}, func(write func(string, *mimirpb.WriteRequest)) {
		accountingSeries(write, now+1000)
	})
	requireSameMetrics(t, ingesters, refresh, false, append(owned,
		"cortex_ingester_active_series",
		"cortex_ingester_active_series_custom_tracker",
		"cortex_ingester_active_native_histogram_series",
		"cortex_ingester_active_native_histogram_buckets",
	)...)
}

// Queries rejected for resource use fail the same way on both.
func TestRustCompatReadProtection(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the Go ingester's utilization based limiting reads /proc")
	}
	ingesters := startCompatIngesters(t, compatSetup{
		limits: defaultLimitsTestConfig(),
		config: func(cfg *Config) {
			cfg.ReadPathMemoryUtilizationLimit = 1
		},
		rustArgs: []string{"--ingester.read-path-memory-utilization-limit", "1"},
	}, func(tb testing.TB, cfg ingest.KafkaConfig) int64 {
		write, last := compatProducer(tb, cfg)
		write("tenant", &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{compatSeries("a", mimirpb.Sample{TimestampMs: ms(time.Now()), Value: 1})}})
		return last()
	})
	queryError := func(api client.IngesterClient, offset int64) error {
		var err error
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
			_, err = api.LabelNames(parityContext("tenant", offset), &client.LabelNamesRequest{StartTimestampMs: 0, EndTimestampMs: ms(time.Now())})
			if err != nil {
				return err
			}
		}
		return err
	}
	// The Go ingester's check runs in its gRPC tap handle, which the test server doesn't install,
	// so compare with the error it returns from StartReadRequest.
	_, goErr := ingesters.goIngester.StartReadRequest(context.Background())
	for deadline := time.Now().Add(10 * time.Second); goErr == nil && time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		_, goErr = ingesters.goIngester.StartReadRequest(context.Background())
	}
	require.Error(t, goErr)
	rustErr := queryError(ingesters.rustSide.api, ingesters.rustSide.offset)
	require.Error(t, rustErr)
	goStatus, rustStatus := status.Convert(goErr), status.Convert(rustErr)
	// The tap handle wraps the error as Unavailable with its message.
	require.Equal(t, "Unavailable", rustStatus.Code().String())
	require.Equal(t, goStatus.Message(), rustStatus.Message())
}

// compatChunks returns each series' chunks as "min-max:encoding", in stream order.
func compatChunks(tb testing.TB, api client.IngesterClient, ctx context.Context, request *client.QueryRequest) map[string][]string {
	tb.Helper()
	stream, err := api.QueryStream(ctx, request)
	require.NoError(tb, err)
	var series []client.QueryStreamSeries
	result := map[string][]string{}
	for {
		response, err := stream.Recv()
		if err != nil {
			break
		}
		series = append(series, response.StreamingSeries...)
		for _, group := range response.StreamingSeriesChunks {
			key := mimirpb.FromLabelAdaptersToLabels(series[group.SeriesIndex].Labels).String()
			for _, chunk := range group.Chunks {
				result[key] = append(result[key], fmt.Sprintf("%d-%d:%d:%08x", chunk.StartTimestampMs, chunk.EndTimestampMs, chunk.Encoding, crc32.ChecksumIEEE(chunk.Data)))
			}
		}
	}
	return result
}

// Head chunk cutting and histogram recoding give the same chunks: sample-rate based cuts,
// histogram size targets, bucket layout growth, counter resets, gauges and stale markers.
func TestRustCompatChunkLayouts(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.NativeHistogramsIngestionEnabled = true
	limits.OutOfOrderTimeWindow = model.Duration(2 * time.Hour)
	base := ms(time.Now().Add(-90 * time.Minute).Truncate(time.Hour))
	histogramAt := func(t int64, buckets int, scale int64, hint histogram.CounterResetHint) mimirpb.Histogram {
		// `scale` observations in each bucket, so the count matches.
		deltas := make([]int64, buckets)
		deltas[0] = scale
		h := &histogram.Histogram{Count: uint64(buckets) * uint64(scale), Sum: float64(t % 997), Schema: 0, CounterResetHint: hint,
			PositiveSpans: []histogram.Span{{Offset: 0, Length: uint32(buckets)}}, PositiveBuckets: deltas}
		return mimirpb.FromHistogramToHistogramProto(t, h)
	}
	ingesters := startCompatIngesters(t, compatSetup{
		limits:   limits,
		rustArgs: []string{"--ingester.out-of-order-time-window", "2h"},
	}, func(tb testing.TB, cfg ingest.KafkaConfig) int64 {
		write, last := compatProducer(tb, cfg)
		send := func(name string, samples []mimirpb.Sample, histograms []mimirpb.Histogram) {
			write("tenant", &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{
				Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: name}}, Samples: samples, Histograms: histograms,
			}}}})
		}
		var dense, growing, gauge []mimirpb.Histogram
		for i := range 300 {
			dense = append(dense, histogramAt(base+int64(i)*10, 5, 1, histogram.UnknownCounterReset))
			growing = append(growing, histogramAt(base+int64(i)*15_000, 2+i/40, int64(1+i), histogram.UnknownCounterReset))
			gauge = append(gauge, histogramAt(base+int64(i)*15_000, 3+(i/50)%3, int64(1+i%7), histogram.GaugeType))
		}
		send("dense", nil, dense)
		send("dense", nil, []mimirpb.Histogram{histogramAt(base+10_000, 7, 1, histogram.UnknownCounterReset)})
		send("dense", nil, []mimirpb.Histogram{histogramAt(base+11_000, 7, 0, histogram.UnknownCounterReset)})
		send("growing", nil, growing)
		send("gauge", nil, gauge)
		stale := histogramAt(base+300*15_000, 3, 1, histogram.UnknownCounterReset)
		stale.Sum = math.Float64frombits(value.StaleNaN)
		send("gauge", nil, []mimirpb.Histogram{stale})
		return last()
	})
	// Written once the histograms are appended, so the head's max time these move past the
	// out-of-order window doesn't depend on how records were grouped into flushes.
	ingesters.produceAndWait(t, []string{"tenant"}, func(write func(string, *mimirpb.WriteRequest)) {
		var floats []mimirpb.Sample
		for i := range 500 {
			floats = append(floats, mimirpb.Sample{TimestampMs: base + int64(i)*15_000, Value: float64(i * i)})
		}
		write("tenant", &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{
			Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "floats"}}, Samples: floats,
		}}}})
	})
	request := parityRequest(0, base+10*60*60_000, parityMatcher(client.REGEX_MATCH, "__name__", ".+"))
	goChunks := compatChunks(t, ingesters.goSide.api, parityContext("tenant", ingesters.goSide.offset), request)
	rustChunks := compatChunks(t, ingesters.rustSide.api, parityContext("tenant", ingesters.rustSide.offset), request)
	require.Equal(t, goChunks, rustChunks)
	compatQueryAll(t, ingesters, "tenant", 0, base+10*60*60_000)
}

// Head compaction moves the head's min time and removes the series and chunks it no longer
// holds alike, label lookups see the same series, and queries read the same blocks, with the
// postings cache settings of the cell.
func TestRustCompatHeadCompaction(t *testing.T) {
	now := time.Now().Truncate(time.Minute)
	start := ms(now.Add(-5 * time.Hour))
	ingesters := startCompatIngesters(t, compatSetup{
		limits: defaultLimitsTestConfig(),
		config: func(cfg *Config) {
			cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = 100 * time.Millisecond
			cfg.BlocksStorageConfig.TSDB.HeadCompactionIntervalJitterEnabled = false
			cfg.BlocksStorageConfig.TSDB.HeadPostingsForMatchersCacheForce = true
			cfg.BlocksStorageConfig.TSDB.BlockPostingsForMatchersCacheForce = true
			cfg.BlocksStorageConfig.TSDB.SharedPostingsForMatchersCache = true
			cfg.BlocksStorageConfig.TSDB.HeadPostingsForMatchersCacheInvalidation = true
		},
		rustArgs: []string{
			"--blocks-storage.tsdb.head-compaction-interval", "100ms",
			"--blocks-storage.tsdb.head-postings-for-matchers-cache-force", "true",
			"--blocks-storage.tsdb.block-postings-for-matchers-cache-force", "true",
			"--blocks-storage.tsdb.shared-postings-for-matchers-cache", "true",
			"--blocks-storage.tsdb.head-postings-for-matchers-cache-invalidation", "true",
		},
	}, func(tb testing.TB, cfg ingest.KafkaConfig) int64 {
		write, last := compatProducer(tb, cfg)
		var long []mimirpb.Sample
		for at := start; at <= ms(now); at += 60_000 {
			long = append(long, mimirpb.Sample{TimestampMs: at, Value: float64(at)})
		}
		series := []mimirpb.PreallocTimeseries{
			compatSeries("long", long...),
			compatSeries("old", mimirpb.Sample{TimestampMs: start, Value: 1}),
			compatSeries("middle", mimirpb.Sample{TimestampMs: ms(now.Add(-150 * time.Minute)), Value: 1}),
			compatSeries("recent", mimirpb.Sample{TimestampMs: ms(now.Add(-time.Minute)), Value: 1}),
		}
		for n := range 4 {
			sharded := compatSeries("sharded", mimirpb.Sample{TimestampMs: ms(now.Add(-4 * time.Hour)), Value: 1}, mimirpb.Sample{TimestampMs: ms(now.Add(-time.Minute)), Value: 1})
			sharded.Labels = append(sharded.Labels, mimirpb.LabelAdapter{Name: "n", Value: strconv.Itoa(n)})
			series = append(series, sharded)
		}
		write("tenant", &mimirpb.WriteRequest{Timeseries: series})
		return last()
	})
	requireSameMetrics(t, ingesters, nil, false,
		"cortex_ingester_memory_series",
		"cortex_ingester_memory_series_created_total",
		"cortex_ingester_memory_series_removed_total",
		"cortex_ingester_tsdb_head_chunks",
		"cortex_ingester_tsdb_head_chunks_removed_total",
		"cortex_ingester_tsdb_head_min_timestamp_seconds",
		"cortex_ingester_tsdb_head_max_timestamp_seconds",
	)
	windows := [][2]int64{{0, math.MaxInt64}, {start, start + 60*60_000}, {ms(now.Add(-3 * time.Hour)), ms(now.Add(-2 * time.Hour))}, {ms(now.Add(-10 * time.Minute)), ms(now)}}
	for _, window := range windows {
		values := func(side compatSide) []string {
			response, err := side.api.LabelValues(parityContext("tenant", side.offset), &client.LabelValuesRequest{LabelName: "__name__", StartTimestampMs: window[0], EndTimestampMs: window[1]})
			require.NoError(t, err)
			sort.Strings(response.LabelValues)
			return response.LabelValues
		}
		require.Equal(t, values(ingesters.goSide), values(ingesters.rustSide), "window %v", window)
	}
	stats := func(side compatSide) uint64 {
		response, err := side.api.UserStats(parityContext("tenant", side.offset), &client.UserStatsRequest{})
		require.NoError(t, err)
		return response.NumSeries
	}
	require.Equal(t, stats(ingesters.goSide), stats(ingesters.rustSide))
	for _, window := range windows {
		for _, matchers := range [][]*client.LabelMatcher{
			{parityMatcher(client.REGEX_MATCH, "__name__", ".+")},
			{parityMatcher(client.EQUAL, "__name__", "long")},
			{parityMatcher(client.EQUAL, "__name__", "sharded"), parityMatcher(client.EQUAL, "__query_shard__", "1_of_2")},
			{parityMatcher(client.REGEX_MATCH, "__name__", "shard.*"), parityMatcher(client.EQUAL, "__query_shard__", "2_of_2")},
		} {
			request := parityRequest(window[0], window[1], matchers...)
			goSeries := paritySeries(t, ingesters.goSide.api, parityContext("tenant", ingesters.goSide.offset), request)
			rustSeries := paritySeries(t, ingesters.rustSide.api, parityContext("tenant", ingesters.rustSide.offset), request)
			require.Equal(t, goSeries, rustSeries, "window %v matchers %v", window, matchers)
		}
	}
	requireSameMetrics(t, ingesters, nil, false,
		"cortex_ingester_queried_series",
		"cortex_ingester_queried_samples",
		"cortex_ingester_queried_blocks_total",
	)
}

// Queries count the series, samples and exemplars they return alike.
func TestRustCompatQueryMetrics(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.MaxGlobalExemplarsPerUser = 100
	now := ms(time.Now())
	ingesters := startCompatIngesters(t, compatSetup{
		limits:   limits,
		rustArgs: []string{"--ingester.max-global-exemplars-per-user", "100"},
	}, func(tb testing.TB, cfg ingest.KafkaConfig) int64 {
		write, last := compatProducer(tb, cfg)
		var series []mimirpb.PreallocTimeseries
		for n := range 20 {
			ts := compatSeries(fmt.Sprintf("metric_%d", n%3), mimirpb.Sample{TimestampMs: now - 2000, Value: 1}, mimirpb.Sample{TimestampMs: now - 1000, Value: 2})
			ts.Labels = append(ts.Labels, mimirpb.LabelAdapter{Name: "n", Value: strconv.Itoa(n)})
			ts.Exemplars = []mimirpb.Exemplar{{TimestampMs: now - 1000, Value: 1, Labels: []mimirpb.LabelAdapter{{Name: "trace_id", Value: strconv.Itoa(n)}}}}
			series = append(series, ts)
		}
		write("tenant", &mimirpb.WriteRequest{Timeseries: series})
		return last()
	})
	for _, side := range []compatSide{ingesters.goSide, ingesters.rustSide} {
		ctx := parityContext("tenant", side.offset)
		for _, request := range []*client.QueryRequest{
			parityRequest(now-10_000, now, parityMatcher(client.REGEX_MATCH, "__name__", ".+")),
			parityRequest(now-10_000, now, parityMatcher(client.EQUAL, "__name__", "metric_1")),
			parityRequest(now-1500, now, parityMatcher(client.EQUAL, "__name__", "metric_2"), parityMatcher(client.REGEX_MATCH, "n", "1.*")),
			parityRequest(now-10_000, now, parityMatcher(client.EQUAL, "__name__", "missing")),
		} {
			paritySeries(t, side.api, ctx, request)
		}
		// A tenant without data.
		paritySeries(t, side.api, parityContext("other", side.offset), parityRequest(0, now, parityMatcher(client.REGEX_MATCH, "__name__", ".+")))
		_, err := side.api.QueryExemplars(ctx, &client.ExemplarQueryRequest{StartTimestampMs: 0, EndTimestampMs: now, Matchers: []*client.LabelMatchers{{Matchers: []*client.LabelMatcher{parityMatcher(client.EQUAL, "__name__", "metric_0")}}}})
		require.NoError(t, err)
	}
	requireSameMetrics(t, ingesters, nil, false,
		"cortex_ingester_queries_total",
		"cortex_ingester_queried_series",
		"cortex_ingester_queried_samples",
		"cortex_ingester_queried_exemplars",
		"cortex_ingester_queried_blocks_total",
	)
}

// The push circuit breaker counts every head append of records alike once the ingesters run: a
// record of more series than a flush holds is appended in several.
func TestRustCompatPushCircuitBreaker(t *testing.T) {
	ingesters := startCompatIngesters(t, compatSetup{
		limits: defaultLimitsTestConfig(),
		config: func(cfg *Config) {
			cfg.PushCircuitBreaker.Enabled = true
			cfg.PushCircuitBreaker.FailureThresholdPercentage = 20
			cfg.PushCircuitBreaker.RequestTimeout = 2 * time.Second
		},
		rustArgs: []string{
			"--ingester.push-circuit-breaker.enabled", "true",
			"--ingester.push-circuit-breaker.failure-threshold-percentage", "20",
			"--ingester.push-circuit-breaker.request-timeout", "2s",
		},
	}, func(tb testing.TB, cfg ingest.KafkaConfig) int64 {
		write, last := compatProducer(tb, cfg)
		write("tenant", &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{compatSeries("replayed", mimirpb.Sample{TimestampMs: ms(time.Now()), Value: 1})}})
		return last()
	})
	now := ms(time.Now())
	for phase, count := range []int{1, 200, 151} {
		ingesters.produceAndWait(t, []string{"tenant", "other"}, func(write func(string, *mimirpb.WriteRequest)) {
			for _, tenant := range []string{"tenant", "other"} {
				var series []mimirpb.PreallocTimeseries
				for n := range count {
					ts := compatSeries("metric", mimirpb.Sample{TimestampMs: now + int64(phase), Value: 1})
					ts.Labels = append(ts.Labels, mimirpb.LabelAdapter{Name: "n", Value: strconv.Itoa(n)})
					series = append(series, ts)
				}
				write(tenant, &mimirpb.WriteRequest{Timeseries: series})
			}
		})
	}
	requireSameMetrics(t, ingesters, nil, false,
		"cortex_ingester_circuit_breaker_results_total",
		"cortex_ingester_circuit_breaker_request_timeouts_total",
		"cortex_ingester_circuit_breaker_transitions_total",
		"cortex_ingester_circuit_breaker_current_state",
	)
}

// The ring sidecar's lifecycle endpoints, which the rollout operator calls on the Rust ingester,
// answer like the Go ingester's for the same partition states.
func TestRustCompatLifecycleHandlers(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)
	goIngester, _, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, nil, nil, nil)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), goIngester))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), goIngester) })

	store, closer := consul.NewInMemoryClient(ring.GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { _ = closer.Close() })
	partition := ring.NewPartitionInstanceLifecycler(cfg.IngesterPartitionRing.ToLifecyclerConfig(0, "shadow"), "shadow-partitions", "shadow-partitions", store, log.NewNopLogger(), nil)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), partition))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), partition) })
	require.Eventually(t, func() bool {
		goState, _, err := goIngester.ingestPartitionLifecycler.GetPartitionState(context.Background())
		require.NoError(t, err)
		state, _, err := partition.GetPartitionState(context.Background())
		require.NoError(t, err)
		return goState == ring.PartitionActive && state == ring.PartitionActive
	}, 10*time.Second, 10*time.Millisecond)

	timestamp := regexp.MustCompile(`"timestamp":[1-9][0-9]*`)
	respond := func(handler http.HandlerFunc, method string) string {
		recorder := httptest.NewRecorder()
		handler(recorder, httptest.NewRequest(method, "/", nil))
		body := timestamp.ReplaceAllString(recorder.Body.String(), `"timestamp":"set"`)
		return fmt.Sprintf("%d %s %q", recorder.Code, recorder.Header().Get("Content-Type"), body)
	}
	var prepared atomic.Bool
	markerDir := t.TempDir()
	for _, step := range []struct {
		endpoint string
		method   string
	}{
		{"downscale", http.MethodGet},
		{"downscale", http.MethodPost},
		{"downscale", http.MethodGet},
		{"downscale", http.MethodPost},
		{"downscale", http.MethodDelete},
		{"downscale", http.MethodDelete},
		{"downscale", http.MethodPut},
		{"shutdown", http.MethodGet},
		{"shutdown", http.MethodPost},
		{"shutdown", http.MethodGet},
		{"shutdown", http.MethodDelete},
		{"shutdown", http.MethodPut},
	} {
		var goHandler, rustHandler http.HandlerFunc
		if step.endpoint == "downscale" {
			goHandler = goIngester.PreparePartitionDownscaleHandler
			rustHandler = func(w http.ResponseWriter, req *http.Request) {
				handlers.PreparePartitionDownscale(w, req, partition, true, log.NewNopLogger())
			}
		} else {
			goHandler = goIngester.PrepareShutdownHandler
			rustHandler = func(w http.ResponseWriter, req *http.Request) {
				handlers.PrepareShutdown(w, req, markerDir, &prepared, log.NewNopLogger())
			}
		}
		require.Equal(t, respond(goHandler, step.method), respond(rustHandler, step.method), "%s %s", step.method, step.endpoint)
	}
}

// With too many series in memory, both compact the heads whose series went inactive up to the
// idle timeout ago, and then reject older in-order samples.
func TestRustCompatEarlyHeadCompaction(t *testing.T) {
	now := time.Now()
	ingesters := startCompatIngesters(t, compatSetup{
		limits: defaultLimitsTestConfig(),
		config: func(cfg *Config) {
			cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = 100 * time.Millisecond
			cfg.BlocksStorageConfig.TSDB.HeadCompactionIntervalJitterEnabled = false
			cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinInMemorySeries = 10
			cfg.ActiveSeriesMetrics.IdleTimeout = time.Second
		},
		rustArgs: []string{
			"--blocks-storage.tsdb.head-compaction-interval", "100ms",
			"--blocks-storage.tsdb.early-head-compaction-min-in-memory-series", "10",
			"--ingester.active-series-metrics-idle-timeout", "1s",
		},
	}, func(tb testing.TB, cfg ingest.KafkaConfig) int64 {
		write, last := compatProducer(tb, cfg)
		var series []mimirpb.PreallocTimeseries
		for n := range 20 {
			ts := compatSeries("metric", mimirpb.Sample{TimestampMs: ms(now.Add(-20 * time.Minute)), Value: 1}, mimirpb.Sample{TimestampMs: ms(now.Add(-time.Duration(n) * time.Minute)), Value: 2})
			ts.Labels = append(ts.Labels, mimirpb.LabelAdapter{Name: "n", Value: strconv.Itoa(n)})
			series = append(series, ts)
		}
		write("tenant", &mimirpb.WriteRequest{Timeseries: series})
		return last()
	})
	head := []string{
		"cortex_ingester_memory_series",
		"cortex_ingester_memory_series_removed_total",
		"cortex_ingester_tsdb_head_min_timestamp_seconds",
	}
	// The series go inactive after the idle timeout, and the next compaction drops them.
	require.Eventually(t, func() bool {
		return strings.Join(metricValues(ingesters.goSide.metrics(t)["cortex_ingester_memory_series"]), "") == "=0"
	}, 30*time.Second, 100*time.Millisecond)
	requireSameMetrics(t, ingesters, nil, false, head...)
	ingesters.produceAndWait(t, []string{"tenant"}, func(write func(string, *mimirpb.WriteRequest)) {
		write("tenant", &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			compatSeries("late", mimirpb.Sample{TimestampMs: ms(now.Add(-10 * time.Minute)), Value: 1}),
			compatSeries("fresh", mimirpb.Sample{TimestampMs: ms(time.Now()), Value: 1}),
		}})
	})
	requireSameMetrics(t, ingesters, nil, false, append(head, "cortex_discarded_samples_total")...)
	require.Equal(t, []string{"group=,reason=sample-timestamp-too-old,user=tenant=1"}, metricValues(ingesters.goSide.metrics(t)["cortex_discarded_samples_total"]))
}
