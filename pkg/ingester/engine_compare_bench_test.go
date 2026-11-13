// SPDX-License-Identifier: AGPL-3.0-only
//go:build enginecompare

package ingester

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/validation"
)

// BenchmarkEngineCompare drives an ingester the way the Kafka consumer does, with one engine, so that two engines can
// be compared from the same workload: each tenant's records are pushed in order, one at a time, and tenants are
// spread over the workers like the consumer spreads them. It runs once per process, since phases build on each other:
//
//	MIMIR_COMPARE_ENGINE=rust go test -tags enginecompare,rustengine,stringlabels ./pkg/ingester \
//	  -run '^$' -bench BenchmarkEngineCompare -benchtime=1x -timeout=3h
//
// MIMIR_COMPARE_* environment variables size the workload; see compareConfig.
func BenchmarkEngineCompare(b *testing.B) {
	cfg := loadCompareConfig()
	f := newCompareFixture(b, cfg)
	f.run(b)
}

type compareConfig struct {
	engine        string
	series        int // per tenant
	tenants       int
	hours         int
	intervalMs    int64
	workers       int
	recordSeries  int
	oooFraction   float64 // of samples, sent up to lateMs late
	lateMs        int64
	oooWindow     time.Duration
	steadyRounds  int
	profileDir    string
	outPath       string
	queryRepeat   int
	compactEveryM int64
}

func compareEnvInt(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v >= 0 {
		return v
	}
	return fallback
}

func compareEnvFloat(name string, fallback float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(name), 64); err == nil && v >= 0 {
		return v
	}
	return fallback
}

func loadCompareConfig() compareConfig {
	return compareConfig{
		engine:        os.Getenv("MIMIR_COMPARE_ENGINE"),
		series:        compareEnvInt("MIMIR_COMPARE_SERIES", 100_000),
		tenants:       compareEnvInt("MIMIR_COMPARE_TENANTS", 1),
		hours:         compareEnvInt("MIMIR_COMPARE_HOURS", 13),
		intervalMs:    int64(compareEnvInt("MIMIR_COMPARE_INTERVAL_S", 60)) * 1000,
		workers:       compareEnvInt("MIMIR_COMPARE_WORKERS", 4),
		recordSeries:  compareEnvInt("MIMIR_COMPARE_RECORD_SERIES", 1000),
		oooFraction:   compareEnvFloat("MIMIR_COMPARE_OOO_FRACTION", 0.01),
		lateMs:        int64(compareEnvInt("MIMIR_COMPARE_LATE_S", 600)) * 1000,
		oooWindow:     time.Duration(compareEnvInt("MIMIR_COMPARE_OOO_WINDOW_S", 3600)) * time.Second,
		steadyRounds:  compareEnvInt("MIMIR_COMPARE_STEADY_ROUNDS", 10),
		profileDir:    os.Getenv("MIMIR_COMPARE_PROFILE_DIR"),
		outPath:       os.Getenv("MIMIR_COMPARE_OUT"),
		queryRepeat:   compareEnvInt("MIMIR_COMPARE_QUERY_REPEAT", 20),
		compactEveryM: int64(compareEnvInt("MIMIR_COMPARE_COMPACT_EVERY_M", 30)),
	}
}

type compareFixture struct {
	cfg      compareConfig
	ingester *Ingester
	tenants  []string
	lsets    []labels.Labels
	t0       int64
	round    int
	rounds   int
	results  map[string]map[string]float64
	order    []string
	baseRSS  float64
	baseHeap uint64
}

func newCompareFixture(b *testing.B, cfg compareConfig) *compareFixture {
	require.NotEmpty(b, cfg.engine, "MIMIR_COMPARE_ENGINE")
	// Tests instrument every buffer for reference leaks, which costs more than ingesting does; production doesn't.
	mimirpb.CustomCodecConfig{}.RegisterGlobally(prometheus.NewRegistry())
	ingesterCfg := defaultIngesterTestConfig(b)
	ingesterCfg.BlocksStorageConfig.TSDB.Engine = cfg.engine
	ingesterCfg.BlocksStorageConfig.TSDB.ShipInterval = 0
	ingesterCfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
	ingesterCfg.BlocksStorageConfig.TSDB.MemorySnapshotOnShutdown = false
	ingesterCfg.BlocksStorageConfig.TSDB.OffsetCatalogue.Enabled = false
	ingesterCfg.IngestStorageConfig.KafkaConfig.ConsumerGroupOffsetCommitFileEnforced = true
	// The harness compacts on the data's schedule, not the wall clock's.
	ingesterCfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour
	ingesterCfg.BlocksStorageConfig.TSDB.Retention = time.Duration(cfg.hours+1) * time.Hour

	limits := defaultLimitsTestConfig()
	limits.MaxGlobalSeriesPerUser = 0
	limits.MaxGlobalSeriesPerMetric = 0
	limits.OutOfOrderTimeWindow = model.Duration(cfg.oooWindow)
	overrides := validation.NewOverrides(limits, nil)

	reg := prometheus.NewRegistry()
	ingester, _, _ := createTestIngesterWithIngestStorage(b, &ingesterCfg, overrides, nil, reg, nil)
	require.NoError(b, services.StartAndAwaitRunning(context.Background(), ingester))
	b.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), ingester) })

	f := &compareFixture{cfg: cfg, ingester: ingester, results: map[string]map[string]float64{}}
	for t := range cfg.tenants {
		f.tenants = append(f.tenants, fmt.Sprintf("tenant-%d", t))
	}
	f.lsets = make([]labels.Labels, cfg.series)
	for n := range f.lsets {
		f.lsets[n] = compareLabels(n)
	}
	f.rounds = int(int64(cfg.hours) * time.Hour.Milliseconds() / cfg.intervalMs)
	// The history ends now, as a replay of Kafka's retention catching up does.
	now := time.Now().UnixMilli()
	f.t0 = now - int64(f.rounds)*cfg.intervalMs
	f.t0 -= f.t0 % cfg.intervalMs
	return f
}

// compareLabels are the labels of series n: like a tenant's scrape targets, with a twentieth of the series in one
// metric, histograms' buckets, and pods of 20 series each.
func compareLabels(n int) labels.Labels {
	b := labels.NewScratchBuilder(9)
	switch {
	case n%20 == 0:
		b.Add("__name__", "http_requests_total")
	case n%10 == 3:
		b.Add("__name__", "request_duration_seconds_bucket")
		b.Add("le", strconv.Itoa(1<<(n%12)))
	default:
		b.Add("__name__", "metric_"+strconv.Itoa(n%3000))
	}
	b.Add("cluster", "cluster-"+strconv.Itoa(n%12))
	b.Add("container", "c-"+strconv.Itoa(n%20))
	b.Add("instance", fmt.Sprintf("10.%d.%d.%d:9090", (n/20/65536)%256, (n/20/256)%256, (n/20)%256))
	b.Add("job", "job-"+strconv.Itoa(n%150))
	b.Add("namespace", "ns-"+strconv.Itoa(n%60))
	b.Add("pod", "pod-"+strconv.Itoa(n/20))
	b.Sort()
	return b.Labels()
}

func compareMix(n, round int) uint64 {
	x := uint64(n)*0x9E3779B97F4A7C15 + uint64(round)*0xBF58476D1CE4E5B9
	x ^= x >> 31
	x *= 0x94D049BB133111EB
	return x ^ x>>29
}

// records builds the records of a round for a tenant: a few series are late by up to lateMs, so they arrive out of
// order, as when a remote-write client retries.
func (f *compareFixture) records(round int) [][]byte {
	serializer := ingest.RecordSerializerFromVersion(2)
	var out [][]byte
	for start := 0; start < f.cfg.series; start += f.cfg.recordSeries {
		end := min(f.cfg.series, start+f.cfg.recordSeries)
		req := &mimirpb.WriteRequest{Source: mimirpb.API, Timeseries: make([]mimirpb.PreallocTimeseries, 0, end-start)}
		for n := start; n < end; n++ {
			x := compareMix(n, round)
			ts := f.t0 + int64(round)*f.cfg.intervalMs + int64(x%25)
			if f.cfg.oooFraction > 0 && float64(x>>40%1_000_000)/1_000_000 < f.cfg.oooFraction && round > 0 {
				// Late samples land between scrapes, at an offset only this round uses, so they never duplicate one.
				back := 1 + (x>>20)%uint64(max(1, f.cfg.lateMs/f.cfg.intervalMs))
				if int(back) <= round {
					ts = f.t0 + int64(round-int(back))*f.cfg.intervalMs + 25 + int64(round)%(f.cfg.intervalMs-50)
				}
			}
			v := float64(round*(1+n%7)) + float64(x>>40%100)/100
			req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
				Labels:  mimirpb.FromLabelsToLabelAdapters(f.lsets[n]),
				Samples: []mimirpb.Sample{{TimestampMs: ts, Value: v}},
			}})
		}
		records, _, err := serializer.ToRecords("mimir", 0, "", req, 64<<20)
		if err != nil {
			panic(err)
		}
		for _, r := range records {
			out = append(out, r.Value)
		}
	}
	return out
}

func compareTenantWorker(tenant string, workers int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(tenant))
	return int(h.Sum32() % uint32(workers))
}

// pushRounds pushes rounds [from, to) and returns the samples pushed and the CPU and wall seconds spent building
// records, which aren't the engine's.
func (f *compareFixture) pushRounds(b testing.TB, from, to int, onRound func(round int)) (samples int, buildCPU, buildWall float64) {
	for round := from; round < to; round++ {
		cpu, started := processCPUSeconds(), time.Now()
		recs := f.records(round)
		buildCPU += processCPUSeconds() - cpu
		buildWall += time.Since(started).Seconds()
		var wg sync.WaitGroup
		errs := make([]error, f.cfg.workers)
		for w := range f.cfg.workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, tenant := range f.tenants {
					if compareTenantWorker(tenant, f.cfg.workers) != w {
						continue
					}
					ctx := user.InjectOrgID(context.Background(), tenant)
					for _, rec := range recs {
						if err := f.ingester.PushRecord(ctx, rec, 2); err != nil && errs[w] == nil {
							// Late samples beyond the window are rejected softly, as in production.
							if !strings.Contains(err.Error(), "out of order") && !strings.Contains(err.Error(), "too old") {
								errs[w] = err
							}
						}
					}
				}
			}()
		}
		wg.Wait()
		for _, err := range errs {
			require.NoError(b, err)
		}
		samples += f.cfg.series * len(f.tenants)
		if onRound != nil {
			onRound(round)
		}
	}
	return samples, buildCPU, buildWall
}

func (f *compareFixture) phase(b *testing.B, name string, fn func(m map[string]float64)) {
	b.Run(name, func(b *testing.B) {
		m, ok := f.results[name]
		if !ok {
			m = map[string]float64{}
			if f.cfg.profileDir != "" {
				// Deferred, so a failed phase doesn't leave the profiler running for the next ones.
				defer startCompareProfile(b, filepath.Join(f.cfg.profileDir, f.cfg.engine+"-"+strings.ReplaceAll(name, "/", "_")+".cpu.pb.gz"))()
			}
			fn(m)
			f.results[name] = m
			f.order = append(f.order, name)
		}
		for unit, v := range m {
			b.ReportMetric(v, unit)
		}
	})
}

func startCompareProfile(b testing.TB, path string) func() {
	require.NoError(b, os.MkdirAll(filepath.Dir(path), 0o755))
	file, err := os.Create(path)
	require.NoError(b, err)
	require.NoError(b, pprof.StartCPUProfile(file))
	return func() {
		pprof.StopCPUProfile()
		require.NoError(b, file.Close())
	}
}

func (f *compareFixture) compact() {
	f.ingester.compactBlocks(context.Background(), false, 0, nil)
}

func (f *compareFixture) run(b *testing.B) {
	f.baseHeap, f.baseRSS = settledHeap(), currentRSSMB()

	f.phase(b, "replay", func(m map[string]float64) {
		var compactWall, compactCPU float64
		lastCompact := f.t0
		started, cpu := time.Now(), processCPUSeconds()
		samples, buildCPU, buildWall := f.pushRounds(b, 0, f.rounds, func(round int) {
			ts := f.t0 + int64(round)*f.cfg.intervalMs
			if ts-lastCompact >= f.cfg.compactEveryM*60_000 {
				lastCompact = ts
				cs, cc := time.Now(), processCPUSeconds()
				f.compact()
				compactWall += time.Since(cs).Seconds()
				compactCPU += processCPUSeconds() - cc
			}
		})
		wall, totalCPU := time.Since(started).Seconds(), processCPUSeconds()-cpu
		m["samples"] = float64(samples)
		m["ingest-cpu-ns/sample"] = (totalCPU - buildCPU - compactCPU) * 1e9 / float64(samples)
		m["compact-cpu-s"], m["compact-wall-s"] = compactCPU, compactWall
		m["cpu-s"] = totalCPU - buildCPU
		m["wall-s"] = wall - buildWall
	})
	f.round = f.rounds
	f.phase(b, "memory-after-replay", f.memory)

	f.phase(b, "steady-ingest", func(m map[string]float64) {
		started, cpu := time.Now(), processCPUSeconds()
		samples, buildCPU, buildWall := f.pushRounds(b, f.round, f.round+f.cfg.steadyRounds, nil)
		wall, totalCPU := time.Since(started).Seconds(), processCPUSeconds()-cpu
		m["ingest-cpu-ns/sample"] = (totalCPU - buildCPU) * 1e9 / float64(samples)
		m["wall-s"] = wall - buildWall
	})
	f.round += f.cfg.steadyRounds

	f.queryPhases(b)
	f.phase(b, "memory-after-queries", f.memory)

	f.phase(b, "compact-final", func(m map[string]float64) {
		started, cpu := time.Now(), processCPUSeconds()
		f.compact()
		m["cpu-s"], m["wall-s"] = processCPUSeconds()-cpu, time.Since(started).Seconds()
	})
	f.phase(b, "memory-after-compaction", f.memory)

	f.write(b)
}

// compareSettle, when an engine sets it, gives back the memory the engine freed outside Go's heap, as
// debug.FreeOSMemory does for Go's, and returns what the engine reports about its memory.
var compareSettle func(i *Ingester, tenants []string) map[string]float64

func (f *compareFixture) memory(m map[string]float64) {
	if compareSettle != nil {
		for k, v := range compareSettle(f.ingester, f.tenants) {
			m[k] = v / float64(f.cfg.series*len(f.tenants))
		}
	}
	heap := settledHeap()
	rss := currentRSSMB()
	total := float64(f.cfg.series * len(f.tenants))
	m["rss-MB"] = rss
	m["rss-delta-B/series"] = (rss - f.baseRSS) * (1 << 20) / total
	m["go-heap-delta-B/series"] = (float64(heap) - float64(f.baseHeap)) / total
	m["disk-MB"] = compareDirMB(f.ingester.cfg.BlocksStorageConfig.TSDB.Dir)
}

type compareQuery struct {
	name     string
	from, to int64 // offsets back from the newest sample, in ms
	matchers []*labels.Matcher
}

func compareEq(name, value string) *labels.Matcher {
	return labels.MustNewMatcher(labels.MatchEqual, name, value)
}

func compareRe(name, value string) *labels.Matcher {
	return labels.MustNewMatcher(labels.MatchRegexp, name, value)
}

func (f *compareFixture) queries() []compareQuery {
	h := time.Hour.Milliseconds()
	all := int64(f.cfg.hours) * h
	mid := f.cfg.series / 2 / 20 * 20
	pod := "pod-" + strconv.Itoa(mid/20)
	return []compareQuery{
		{"one-pod/1h", h, 0, []*labels.Matcher{compareEq("pod", pod)}},
		{"one-pod/all", all, 0, []*labels.Matcher{compareEq("pod", pod)}},
		{"one-pod/cold-1h", all * 7 / 10, all*7/10 - h, []*labels.Matcher{compareEq("pod", pod)}},
		{"one-metric/1h", h, 0, []*labels.Matcher{compareEq("__name__", "metric_7")}},
		{"one-metric/all", all, 0, []*labels.Matcher{compareEq("__name__", "metric_7")}},
		{"metric-job-regex/1h", h, 0, []*labels.Matcher{compareEq("__name__", "metric_7"), compareRe("job", "job-(1|3|7)")}},
		{"huge-metric-job/1h", h, 0, []*labels.Matcher{compareEq("__name__", "http_requests_total"), compareEq("job", "job-30")}},
		{"huge-metric/10m", 10 * 60_000, 0, []*labels.Matcher{compareEq("__name__", "http_requests_total")}},
		{"huge-metric-pod-regex/1h", h, 0, []*labels.Matcher{compareEq("__name__", "http_requests_total"), compareRe("pod", "pod-1.*")}},
		{"nameless-job/1h", h, 0, []*labels.Matcher{compareEq("job", "job-17")}},
		{"name-regex/1h", h, 0, []*labels.Matcher{compareRe("__name__", "metric_12.."), compareEq("namespace", "ns-5")}},
		{"buckets/1h", h, 0, []*labels.Matcher{compareEq("__name__", "request_duration_seconds_bucket"), compareEq("namespace", "ns-3")}},
		{"entity-cluster-namespace/1h", h, 0, []*labels.Matcher{compareEq("cluster", "cluster-4"), compareEq("namespace", "ns-16")}},
		{"entity-namespace-not-job/1h", h, 0, []*labels.Matcher{compareEq("namespace", "ns-16"), labels.MustNewMatcher(labels.MatchNotEqual, "job", "job-16")}},
	}
}

// compareStream discards what it is sent, after counting it.
type compareStream struct {
	grpc.ServerStream
	ctx            context.Context
	series, chunks int
}

func (s *compareStream) Send(resp *client.QueryStreamResponse) error {
	s.series += len(resp.StreamingSeries)
	for _, c := range resp.StreamingSeriesChunks {
		s.chunks += len(c.Chunks)
	}
	return nil
}

func (s *compareStream) Context() context.Context { return s.ctx }

func (f *compareFixture) queryPhases(b *testing.B) {
	newest := f.t0 + int64(f.round-1)*f.cfg.intervalMs + 25
	for _, q := range f.queries() {
		f.phase(b, "query/"+q.name, func(m map[string]float64) {
			req, err := client.ToQueryRequest(0, 0, q.matchers)
			require.NoError(b, err)
			req.StartTimestampMs, req.EndTimestampMs = newest-q.from, newest-q.to
			req.StreamingChunksBatchSize = 256
			run := func() *compareStream {
				ctx := user.InjectOrgID(context.Background(), f.tenants[0])
				stream := &compareStream{ctx: ctx}
				require.NoError(b, f.ingester.QueryStream(req, stream))
				return stream
			}
			first := run()
			latencies := make([]time.Duration, 0, f.cfg.queryRepeat)
			var cpu float64
			for range f.cfg.queryRepeat {
				c, started := processCPUSeconds(), time.Now()
				run()
				latencies = append(latencies, time.Since(started))
				cpu += processCPUSeconds() - c
			}
			slices.Sort(latencies)
			m["series"], m["chunks"] = float64(first.series), float64(first.chunks)
			m["p50-ms"] = float64(latencies[len(latencies)/2].Microseconds()) / 1000
			m["cpu-ms/query"] = cpu * 1000 / float64(f.cfg.queryRepeat)
		})
	}
	labelQueries := []struct {
		name string
		run  func(ctx context.Context) (int, error)
	}{
		{"label-names/all", func(ctx context.Context) (int, error) {
			resp, err := f.ingester.LabelNames(ctx, &client.LabelNamesRequest{StartTimestampMs: f.t0, EndTimestampMs: newest})
			if err != nil {
				return 0, err
			}
			return len(resp.LabelNames), nil
		}},
		{"label-values-pod/all", func(ctx context.Context) (int, error) {
			resp, err := f.ingester.LabelValues(ctx, &client.LabelValuesRequest{LabelName: "pod", StartTimestampMs: f.t0, EndTimestampMs: newest})
			if err != nil {
				return 0, err
			}
			return len(resp.LabelValues), nil
		}},
		{"label-values-pod-by-metric/1h", func(ctx context.Context) (int, error) {
			matchers, err := client.ToLabelMatchers([]*labels.Matcher{compareEq("__name__", "metric_7")})
			if err != nil {
				return 0, err
			}
			resp, err := f.ingester.LabelValues(ctx, &client.LabelValuesRequest{LabelName: "pod", StartTimestampMs: newest - time.Hour.Milliseconds(), EndTimestampMs: newest, Matchers: &client.LabelMatchers{Matchers: matchers}})
			if err != nil {
				return 0, err
			}
			return len(resp.LabelValues), nil
		}},
	}
	for _, lq := range labelQueries {
		f.phase(b, "query/"+lq.name, func(m map[string]float64) {
			ctx := user.InjectOrgID(context.Background(), f.tenants[0])
			n, err := lq.run(ctx)
			require.NoError(b, err)
			latencies := make([]time.Duration, 0, f.cfg.queryRepeat)
			var cpu float64
			for range f.cfg.queryRepeat {
				c, started := processCPUSeconds(), time.Now()
				_, err := lq.run(ctx)
				require.NoError(b, err)
				latencies = append(latencies, time.Since(started))
				cpu += processCPUSeconds() - c
			}
			slices.Sort(latencies)
			m["values"] = float64(n)
			m["p50-ms"] = float64(latencies[len(latencies)/2].Microseconds()) / 1000
			m["cpu-ms/query"] = cpu * 1000 / float64(f.cfg.queryRepeat)
		})
	}
}

func (f *compareFixture) write(b *testing.B) {
	if f.cfg.outPath == "" {
		return
	}
	type phase struct {
		Name    string             `json:"name"`
		Metrics map[string]float64 `json:"metrics"`
	}
	out := struct {
		Engine string  `json:"engine"`
		Config string  `json:"config"`
		Phases []phase `json:"phases"`
	}{Engine: f.cfg.engine, Config: fmt.Sprintf("%+v", f.cfg)}
	for _, name := range f.order {
		out.Phases = append(out.Phases, phase{name, f.results[name]})
	}
	data, err := json.MarshalIndent(out, "", "  ")
	require.NoError(b, err)
	require.NoError(b, os.WriteFile(f.cfg.outPath, data, 0o644))
}

func processCPUSeconds() float64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
}

func settledHeap() uint64 {
	runtime.GC()
	debug.FreeOSMemory()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapInuse
}

func currentRSSMB() float64 {
	// Freed memory is given back first, so both runtimes are measured settled.
	debug.FreeOSMemory()
	if data, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if fields := strings.Fields(line); len(fields) >= 2 && fields[0] == "VmRSS:" {
				kb, _ := strconv.ParseFloat(fields[1], 64)
				return kb / 1024
			}
		}
	}
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		return 0
	}
	kb, _ := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	return kb / 1024
}

func compareDirMB(dir string) float64 {
	var size int64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if stat, ok := info.Sys().(*syscall.Stat_t); err == nil && ok && !info.IsDir() {
			size += stat.Blocks * 512
		}
		return nil
	})
	return float64(size) / (1 << 20)
}
