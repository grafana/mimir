// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/validation"
)

// The cases mimir-dev-15 can't show, on the three ingester implementations: the Prometheus
// TSDB, the seriesstore engine and the Rust ingester. mimir-dev-15 is one big tenant with a few
// hundred requests a second per ingester; production cells have thousands of tenants, most of a
// few hundred series, and up to thousands of requests a second per ingester.
//
// Each scenario writes one dataset to Kafka, and each implementation consumes it in turn, alone
// in the process, so its memory can be measured. MIMIR_SCENARIO_SCALE scales the datasets.

var scenarioImplementations = []string{mimir_tsdb.EnginePrometheus, mimir_tsdb.EngineSeriesstore, "rust"}

type scenarioQuery struct {
	name     string
	tenant   string
	matchers []*client.LabelMatcher
	series   int
}

type ingesterScenario struct {
	name string
	// tenants is how many tenants the dataset has, series how many each.
	tenants, series int
	// seriesLabels returns the labels of a tenant's series n.
	seriesLabels func(tenant, n int) []mimirpb.LabelAdapter
	queries      []scenarioQuery
	// concurrency runs the queries from this many goroutines at once while samples keep coming.
	concurrency []int
}

func scenarioScale() float64 {
	if scale, err := strconv.ParseFloat(os.Getenv("MIMIR_SCENARIO_SCALE"), 64); err == nil && scale > 0 {
		return scale
	}
	return 1
}

func scaled(n int) int {
	return max(1, int(float64(n)*scenarioScale()))
}

func scenarioTenant(n int) string { return fmt.Sprintf("tenant-%04d", n) }

func ingesterScenarios() []ingesterScenario {
	smallTenants := scaled(1000)
	tinyMetrics := scaled(100_000)
	hugeMetric := scaled(300_000)
	mixed := scaled(200_000)
	return []ingesterScenario{
		{
			// Production's median tenant: a few hundred series, a dozen labels.
			name: "many-small-tenants", tenants: smallTenants, series: 600,
			seriesLabels: func(tenant, n int) []mimirpb.LabelAdapter {
				return scenarioLabels(fmt.Sprintf("metric_%d", n%40), "job", fmt.Sprintf("job-%d", n%5), "instance", fmt.Sprintf("instance-%d", n%30), "pod", fmt.Sprintf("pod-%d", n))
			},
			queries: []scenarioQuery{
				{name: "one-metric", tenant: scenarioTenant(smallTenants / 2), matchers: []*client.LabelMatcher{parityMatcher(client.EQUAL, labels.MetricName, "metric_7")}, series: 15},
				{name: "all-series", tenant: scenarioTenant(smallTenants / 3), matchers: []*client.LabelMatcher{parityMatcher(client.REGEX_MATCH, labels.MetricName, ".+")}, series: 600},
			},
			concurrency: []int{1, 32},
		},
		{
			// Every metric name with two series: a name group, or its postings, per two series.
			name: "tiny-metrics", tenants: 1, series: tinyMetrics,
			seriesLabels: func(_, n int) []mimirpb.LabelAdapter {
				return scenarioLabels(fmt.Sprintf("metric_%07d", n/2), "job", "api", "pod", fmt.Sprintf("pod-%d", n%2))
			},
			queries: []scenarioQuery{
				{name: "one-metric", tenant: scenarioTenant(0), matchers: []*client.LabelMatcher{parityMatcher(client.EQUAL, labels.MetricName, fmt.Sprintf("metric_%07d", tinyMetrics/4))}, series: 2},
				{name: "name-prefix-regex", tenant: scenarioTenant(0), matchers: []*client.LabelMatcher{parityMatcher(client.REGEX_MATCH, labels.MetricName, "metric_00001.*")}, series: scenarioCount(tinyMetrics, func(n int) bool { return strings.HasPrefix(fmt.Sprintf("metric_%07d", n/2), "metric_00001") })},
			},
			concurrency: []int{1, 32},
		},
		{
			// One metric holds every series: its name selects all of them.
			name: "one-huge-metric", tenants: 1, series: hugeMetric,
			seriesLabels: func(_, n int) []mimirpb.LabelAdapter {
				return scenarioLabels("http_requests_total", "job", fmt.Sprintf("job-%d", n%10), "instance", fmt.Sprintf("instance-%d", n%1000), "pod", fmt.Sprintf("pod-%d", n))
			},
			queries: []scenarioQuery{
				{name: "one-pod", tenant: scenarioTenant(0), matchers: []*client.LabelMatcher{parityMatcher(client.EQUAL, labels.MetricName, "http_requests_total"), parityMatcher(client.EQUAL, "pod", fmt.Sprintf("pod-%d", hugeMetric/2))}, series: 1},
				{name: "one-instance", tenant: scenarioTenant(0), matchers: []*client.LabelMatcher{parityMatcher(client.EQUAL, labels.MetricName, "http_requests_total"), parityMatcher(client.EQUAL, "instance", "instance-7")}, series: scenarioCount(hugeMetric, func(n int) bool { return n%1000 == 7 })},
				{name: "pod-regex", tenant: scenarioTenant(0), matchers: []*client.LabelMatcher{parityMatcher(client.EQUAL, labels.MetricName, "http_requests_total"), parityMatcher(client.REGEX_MATCH, "pod", "pod-1.")}, series: scenarioCount(hugeMetric, func(n int) bool { return n >= 10 && n < 20 })},
			},
			concurrency: []int{1, 8, 32, 128},
		},
		{
			// Selectors without a metric name, and name regexes, over many metrics sharing labels.
			name: "nameless-and-name-regex", tenants: 1, series: mixed,
			seriesLabels: func(_, n int) []mimirpb.LabelAdapter {
				return scenarioLabels(fmt.Sprintf("metric_%03d", n%200), "job", fmt.Sprintf("job-%d", n%50), "cluster", fmt.Sprintf("cluster-%d", n%3), "pod", fmt.Sprintf("pod-%d", n))
			},
			queries: []scenarioQuery{
				{name: "nameless-job", tenant: scenarioTenant(0), matchers: []*client.LabelMatcher{parityMatcher(client.EQUAL, "job", "job-17")}, series: scenarioCount(mixed, func(n int) bool { return n%50 == 17 })},
				{name: "nameless-pod", tenant: scenarioTenant(0), matchers: []*client.LabelMatcher{parityMatcher(client.EQUAL, "pod", fmt.Sprintf("pod-%d", mixed/2))}, series: 1},
				{name: "name-regex", tenant: scenarioTenant(0), matchers: []*client.LabelMatcher{parityMatcher(client.REGEX_MATCH, labels.MetricName, "metric_1.."), parityMatcher(client.EQUAL, "job", "job-3")}, series: scenarioCount(mixed, func(n int) bool { return n%200 >= 100 && n%50 == 3 })},
			},
			concurrency: []int{1, 32},
		},
	}
}

func scenarioLabels(name string, pairs ...string) []mimirpb.LabelAdapter {
	out := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: name}}
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, mimirpb.LabelAdapter{Name: pairs[i], Value: pairs[i+1]})
	}
	slices.SortFunc(out, func(a, b mimirpb.LabelAdapter) int { return strings.Compare(a.Name, b.Name) })
	return out
}

func scenarioCount(series int, matches func(n int) bool) int {
	count := 0
	for n := range series {
		if matches(n) {
			count++
		}
	}
	return count
}

// scenarioSamples is how many samples, a minute apart, every series gets up to now.
const scenarioSamples = 4

// writeScenario writes the dataset's samples for the minutes [from, to) before now, a request of
// at most 1000 series at a time.
func writeScenario(tb testing.TB, write func(tenant string, request *mimirpb.WriteRequest), sc ingesterScenario, now time.Time, from, to int) {
	for minute := from; minute < to; minute++ {
		ts := now.Add(time.Duration(minute-to) * time.Minute).UnixMilli()
		for tenant := range sc.tenants {
			for first := 0; first < sc.series; first += 1000 {
				request := &mimirpb.WriteRequest{}
				for n := first; n < min(first+1000, sc.series); n++ {
					request.Timeseries = append(request.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
						Labels:  sc.seriesLabels(tenant, n),
						Samples: []mimirpb.Sample{{TimestampMs: ts, Value: float64(n + minute)}},
					}})
				}
				write(scenarioTenant(tenant), request)
			}
		}
	}
}

func BenchmarkIngesterScenarios(b *testing.B) {
	for _, sc := range ingesterScenarios() {
		b.Run(sc.name, func(b *testing.B) {
			limits := defaultLimitsTestConfig()
			limits.MaxGlobalSeriesPerUser = 0
			limits.MaxGlobalSeriesPerMetric = 0
			overrides := validation.NewOverrides(limits, nil)
			newGoIngester := func(engine string, kafkaAddress ...string) (*Ingester, ingest.KafkaConfig) {
				cfg := defaultIngesterTestConfig(b)
				cfg.IngestStorageConfig.KafkaConfig.ConsumeFromPositionAtStartup = "start"
				cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyMax = 8
				cfg.BlocksStorageConfig.TSDB.Engine = engine
				ingester, _, _ := createTestIngesterWithIngestStorage(b, &cfg, overrides, nil, nil, nil, kafkaAddress...)
				return ingester, cfg.IngestStorageConfig.KafkaConfig
			}
			// The first ingester's cluster holds the dataset for all three.
			first, kafkaCfg := newGoIngester(mimir_tsdb.EnginePrometheus)
			write, last := compatProducer(b, kafkaCfg)
			now := time.Now().Truncate(time.Minute)
			writeScenario(b, write, sc, now, 0, scenarioSamples)
			offset := last()
			totalSeries := sc.tenants * sc.series

			for _, implementation := range scenarioImplementations {
				b.Run(implementation, func(b *testing.B) {
					api, memory, files, stop := startScenarioImplementation(b, implementation, first, newGoIngester, kafkaCfg, offset)
					defer stop()
					// Before the queries, which only allocate transiently.
					usedMemory, openFiles := memory(), files()
					b.Run("memory", func(b *testing.B) {
						for range b.N {
						}
						b.ReportMetric(float64(usedMemory)/float64(totalSeries), "mem-B/series")
						if sc.tenants > 1 {
							b.ReportMetric(float64(usedMemory)/float64(sc.tenants), "mem-B/tenant")
							b.ReportMetric(float64(openFiles)/float64(sc.tenants), "files/tenant")
						}
					})
					for _, query := range sc.queries {
						for _, concurrency := range sc.concurrency {
							b.Run(fmt.Sprintf("%s/concurrency=%d", query.name, concurrency), func(b *testing.B) {
								benchmarkScenarioQuery(b, api, query, offset, concurrency, func(round int) {
									// New samples for every series of the tenant queried, a minute on per round.
									tenant, _ := strconv.Atoi(strings.TrimPrefix(query.tenant, "tenant-"))
									one := sc
									one.tenants = 1
									one.seriesLabels = func(_, n int) []mimirpb.LabelAdapter { return sc.seriesLabels(tenant, n) }
									writeScenario(b, func(_ string, request *mimirpb.WriteRequest) { write(query.tenant, request) }, one, now.Add(time.Duration(round+1)*time.Second), 0, 1)
								})
							})
						}
					}
				})
				first = nil
			}
		})
	}
}

// startScenarioImplementation starts an implementation on the dataset up to offset, and returns
// its API, its memory (Go heap in use, Rust RSS) and open files over what the process had before.
func startScenarioImplementation(
	b *testing.B,
	implementation string,
	first *Ingester,
	newGoIngester func(engine string, kafkaAddress ...string) (*Ingester, ingest.KafkaConfig),
	kafkaCfg ingest.KafkaConfig,
	offset int64,
) (api client.IngesterClient, memory func() uint64, files func() int, stop func()) {
	if implementation == "rust" {
		buildRustIngester(b)
		process, connection := startRustIngesterAndWait(b, kafkaCfg.Address[0], kafkaCfg.Topic, b.TempDir(), offset, "--ingester.max-global-series-per-user=0")
		pid := process.Process.Pid
		return client.NewIngesterClient(connection),
			func() uint64 {
				rss, err := readProcessRSSBytes(pid)
				require.NoError(b, err)
				return rss
			},
			func() int { return processOpenFiles(b, pid) },
			func() {
				_ = connection.Close()
				stopRustIngester(b, process)
			}
	}
	ingester := first
	if ingester == nil || implementation != mimir_tsdb.EnginePrometheus {
		ingester, _ = newGoIngester(implementation, kafkaCfg.Address[0])
	}
	heapBefore := liveHeapBytes()
	filesBefore := processOpenFiles(b, os.Getpid())
	ctx := context.Background()
	require.NoError(b, services.StartAndAwaitRunning(ctx, ingester))
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	require.NoError(b, ingester.ingestReader.WaitReadConsistencyUntilOffsets(waitCtx, kmeta.NewSingleClusterPartitionOffsets(offset)))
	cancel()
	api = serveIngester(b, ingester)
	return api,
		func() uint64 { return liveHeapBytes() - min(heapBefore, liveHeapBytes()) },
		func() int { return processOpenFiles(b, os.Getpid()) - filesBefore },
		func() { require.NoError(b, services.StopAndAwaitTerminated(ctx, ingester)) }
}

func liveHeapBytes() uint64 {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapInuse
}

func processOpenFiles(tb testing.TB, pid int) int {
	output, err := exec.Command("lsof", "-n", "-P", "-p", strconv.Itoa(pid)).Output()
	require.NoError(tb, err)
	// The header line.
	return max(0, strings.Count(string(output), "\n")-1)
}

// benchmarkScenarioQuery runs the query from concurrency goroutines while ingest writes a new
// sample to every series of the tenant each round, and checks every answer's series count.
func benchmarkScenarioQuery(b *testing.B, api client.IngesterClient, query scenarioQuery, offset int64, concurrency int, ingest func(round int)) {
	request := parityRequest(math.MinInt64, math.MaxInt64, query.matchers...)
	ctx := parityContext(query.tenant, offset)
	run := func() time.Duration {
		started := time.Now()
		stream, err := api.QueryStream(ctx, request)
		require.NoError(b, err)
		seen := 0
		for {
			response, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(b, err)
			seen += len(response.StreamingSeries)
		}
		require.Equal(b, query.series, seen, "series of %s", query.name)
		return time.Since(started)
	}
	run()

	stopIngest := make(chan struct{})
	var ingestDone sync.WaitGroup
	ingestDone.Add(1)
	go func() {
		defer ingestDone.Done()
		for round := 0; ; round++ {
			select {
			case <-stopIngest:
				return
			default:
				ingest(round)
			}
		}
	}()

	var (
		latencies   = make([][]time.Duration, concurrency)
		next        atomic.Int64
		workersDone sync.WaitGroup
	)
	b.ResetTimer()
	for worker := range concurrency {
		workersDone.Add(1)
		go func() {
			defer workersDone.Done()
			for next.Add(1) <= int64(b.N) {
				latencies[worker] = append(latencies[worker], run())
			}
		}()
	}
	workersDone.Wait()
	b.StopTimer()
	close(stopIngest)
	ingestDone.Wait()

	all := slices.Concat(latencies...)
	slices.Sort(all)
	quantile := func(q float64) float64 {
		return float64(all[min(len(all)-1, int(q*float64(len(all))))].Microseconds()) / 1000
	}
	b.ReportMetric(quantile(0.5), "p50-ms")
	b.ReportMetric(quantile(0.99), "p99-ms")
}
