// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

// A production-sized tenant on one ingester: 2M owned series and 13h of retention, so that most
// of the data is in compacted blocks on disk and the head holds the last few hours. Run it once
// per engine, with one iteration, since every phase builds on the previous one:
//
//	go test ./pkg/storage/seriesstore/store -run '^$' -bench BenchmarkRealistic -benchtime=1x -timeout=2h
//
// MIMIR_REALISTIC_SERIES (default 2M), MIMIR_REALISTIC_HOURS (13), MIMIR_REALISTIC_INTERVAL_S
// (scrape interval, default 120: production's 15s to 60s would take hours to ingest) and
// MIMIR_REALISTIC_ENGINES (default "prometheus,seriesstore") scale it.

const (
	realisticBatch    = 2000
	realisticCompact  = 30 * 60 * 1000
	realisticHoursMs  = 3_600_000
	realisticStartRaw = 1_700_000_000_000
)

// realisticWorkers is how many goroutines append at once, like the ingester's concurrent consumers.
var realisticWorkers = realisticEnvInt("MIMIR_REALISTIC_WORKERS", 8)

func realisticEnvInt(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}

func realisticLabels(n int) promlabels.Labels {
	b := promlabels.NewScratchBuilder(9)
	switch {
	case n%20 == 0:
		// One metric holds a twentieth of the series: its name selects 100k of them.
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

// realisticSample is a scrape's timestamp, a few ms off the interval, and a counter-like value.
func realisticSample(n, round int, intervalMs int64) (int64, float64) {
	x := uint64(n)*0x9E3779B97F4A7C15 + uint64(round)*0xBF58476D1CE4E5B9
	x ^= x >> 31
	return int64(round)*intervalMs + int64(x%25), float64(round*(1+n%7)) + float64(x>>40%100)/100
}

func processCPU() float64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
}

func rssMB() float64 {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		return 0
	}
	kb, _ := strconv.Atoi(strings.TrimSpace(string(out)))
	return float64(kb) / 1024
}

func heapInUse() uint64 {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapInuse
}

func dirSizeMB(dir string) float64 {
	// Mapped chunk files only take blocks once written back.
	syscall.Sync()
	var size int64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		// Allocated blocks, not the length: chunk files may be preallocated and sparse.
		if stat, ok := info.Sys().(*syscall.Stat_t); err == nil && ok && !info.IsDir() {
			size += stat.Blocks * 512
		}
		return nil
	})
	return float64(size) / (1 << 20)
}

type realisticFixture struct {
	b          *testing.B
	engine     string
	dir        string
	series     int
	rounds     int
	intervalMs int64
	t0         int64
	lsets      []promlabels.Labels
	refs       []storage.SeriesRef
	head       headUnderTest
	baseHeap   uint64
	results    map[string]map[string]float64
	// What every engine answered a query with, to compare them.
	expected *sync.Map
}

func (f *realisticFixture) open() {
	if f.engine == "prometheus" {
		f.head = openPrometheus(f.b, f.dir, differentialOptions{}, nil)
		return
	}
	engine, err := OpenEngine(f.dir, "tenant", EngineOptions{
		Shards:                realisticEnvInt("MIMIR_REALISTIC_SHARDS", 64),
		RetentionMs:           13 * realisticHoursMs,
		SecondaryHashFunction: secondaryHash,
	})
	require.NoError(f.b, err)
	f.head = engine
}

// appendRound appends the round's sample to every series, from workers each owning a slice of the
// series like the ingester's concurrent consumers. It returns the samples appended.
func (f *realisticFixture) appendRound(round int, series int) int {
	var (
		wg     sync.WaitGroup
		failed atomic.Value
	)
	for w := range realisticWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Errors are checked outside the testing package's locks, which would dominate the
			// profile of what is measured.
			fail := func(err error) {
				if err != nil {
					failed.CompareAndSwap(nil, err)
				}
			}
			app := f.head.Appender(context.Background())
			count := 0
			for n := w; n < series; n += realisticWorkers {
				ts, v := realisticSample(n, round, f.intervalMs)
				ref, err := app.Append(f.refs[n], f.lsets[n], f.t0+ts, v)
				if err != nil {
					fail(err)
					return
				}
				f.refs[n] = ref
				if count++; count%realisticBatch == 0 {
					fail(app.Commit())
					app = f.head.Appender(context.Background())
				}
			}
			fail(app.Commit())
		}()
	}
	wg.Wait()
	if err, ok := failed.Load().(error); ok {
		require.NoError(f.b, err)
	}
	return series
}

// phase runs fn once, however often the benchmark framework calls it, and reports its metrics.
func (f *realisticFixture) phase(b *testing.B, name string, fn func(m map[string]float64)) {
	b.Run(name, func(b *testing.B) {
		m, ok := f.results[name]
		if !ok {
			m = map[string]float64{}
			fn(m)
			f.results[name] = m
		}
		for unit, v := range m {
			b.ReportMetric(v, unit)
		}
	})
}

func (f *realisticFixture) memory(m map[string]float64) {
	// MIMIR_REALISTIC_HEAP_PROFILE names a file for the live heap profile of the first call.
	if path := os.Getenv("MIMIR_REALISTIC_HEAP_PROFILE"); path != "" && f.results["memory-after-ingest"] == nil {
		runtime.GC()
		file, err := os.Create(path + "." + f.engine)
		require.NoError(f.b, err)
		require.NoError(f.b, pprof.Lookup("heap").WriteTo(file, 0))
		require.NoError(f.b, file.Close())
	}
	m["heap-B/series"] = float64(heapInUse()-f.baseHeap) / float64(f.series)
	m["rss-MB"] = rssMB()
	m["disk-MB"] = dirSizeMB(f.dir)
}

type realisticQuery struct {
	name     string
	from, to int64 // offsets from t0, in ms
	matchers []*promlabels.Matcher
}

func eq(name, value string) *promlabels.Matcher {
	return promlabels.MustNewMatcher(promlabels.MatchEqual, name, value)
}

func re(name, value string) *promlabels.Matcher {
	return promlabels.MustNewMatcher(promlabels.MatchRegexp, name, value)
}

func (f *realisticFixture) queries() []realisticQuery {
	end := int64(f.rounds-1) * f.intervalMs
	last1h := [2]int64{end - realisticHoursMs, end}
	all := [2]int64{0, end}
	cold1h := [2]int64{end * 3 / 10, end*3/10 + realisticHoursMs}
	mid := f.series / 2 / 20 * 20 // a series with its container 0: an http_requests_total series
	pod := "pod-" + strconv.Itoa(mid/20)
	return []realisticQuery{
		{"one-pod/1h", last1h[0], last1h[1], []*promlabels.Matcher{eq("pod", pod)}},
		{"one-pod/13h", all[0], all[1], []*promlabels.Matcher{eq("pod", pod)}},
		{"one-pod/cold-1h", cold1h[0], cold1h[1], []*promlabels.Matcher{eq("pod", pod)}},
		{"one-metric/1h", last1h[0], last1h[1], []*promlabels.Matcher{eq("__name__", "metric_7")}},
		{"one-metric/13h", all[0], all[1], []*promlabels.Matcher{eq("__name__", "metric_7")}},
		{"metric-job-regex/1h", last1h[0], last1h[1], []*promlabels.Matcher{eq("__name__", "metric_7"), re("job", "job-(1|3|7)")}},
		{"huge-metric-pod-regex/1h", last1h[0], last1h[1], []*promlabels.Matcher{eq("__name__", "http_requests_total"), re("pod", "pod-1.*")}},
		{"huge-metric-job/1h", last1h[0], last1h[1], []*promlabels.Matcher{eq("__name__", "http_requests_total"), eq("job", "job-30")}},
		{"nameless-job/1h", last1h[0], last1h[1], []*promlabels.Matcher{eq("job", "job-17")}},
		{"nameless-job/13h", all[0], all[1], []*promlabels.Matcher{eq("job", "job-17")}},
		{"name-regex/1h", last1h[0], last1h[1], []*promlabels.Matcher{re("__name__", "metric_12.."), eq("namespace", "ns-5")}},
		{"buckets/1h", last1h[0], last1h[1], []*promlabels.Matcher{eq("__name__", "request_duration_seconds_bucket"), eq("namespace", "ns-3")}},
	}
}

func (f *realisticFixture) selectSeries(q realisticQuery) int {
	querier, err := f.head.ChunkQuerier(f.t0+q.from, f.t0+q.to)
	require.NoError(f.b, err)
	defer querier.Close()
	set := querier.Select(context.Background(), true, nil, q.matchers...)
	series := 0
	for set.Next() {
		it := set.At().Iterator(nil)
		for it.Next() {
			_ = it.At().Chunk.Bytes()
		}
		require.NoError(f.b, it.Err())
		series++
	}
	require.NoError(f.b, set.Err())
	return series
}

// timed runs fn iterations times and reports its latency quantiles and CPU per call.
func timed(iterations int, fn func()) (p50, p99, cpuMs float64) {
	latencies := make([]time.Duration, 0, iterations)
	cpuBefore := processCPU()
	for range iterations {
		started := time.Now()
		fn()
		latencies = append(latencies, time.Since(started))
	}
	cpu := processCPU() - cpuBefore
	slices.Sort(latencies)
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	return ms(latencies[len(latencies)/2]), ms(latencies[min(len(latencies)-1, len(latencies)*99/100)]), cpu * 1000 / float64(iterations)
}

func (f *realisticFixture) queryPhases(b *testing.B, prefix string) {
	for _, q := range f.queries() {
		f.phase(b, prefix+"query/"+q.name, func(m map[string]float64) {
			series := f.selectSeries(q)
			if previous, loaded := f.expected.LoadOrStore(q.name, series); loaded {
				require.Equal(b, previous, series, "series of %s differ between engines", q.name)
			}
			p50, p99, cpu := timed(max(5, min(100, 2_000_000/max(1, series*20))), func() { f.selectSeries(q) })
			m["series"], m["p50-ms"], m["p99-ms"], m["cpu-ms/query"] = float64(series), p50, p99, cpu
		})
	}
	end := int64(f.rounds-1) * f.intervalMs
	labelQueries := []struct {
		name     string
		from, to int64
		run      func(querier storage.Querier) (int, error)
	}{
		{"label-names/13h", 0, end, func(q storage.Querier) (int, error) {
			names, _, err := q.LabelNames(context.Background(), nil)
			return len(names), err
		}},
		{"label-values-job/13h", 0, end, func(q storage.Querier) (int, error) {
			values, _, err := q.LabelValues(context.Background(), "job", nil)
			return len(values), err
		}},
		{"label-values-pod-by-metric/13h", 0, end, func(q storage.Querier) (int, error) {
			values, _, err := q.LabelValues(context.Background(), "pod", nil, eq("__name__", "metric_7"))
			return len(values), err
		}},
		{"label-values-pod-by-job/1h", end - realisticHoursMs, end, func(q storage.Querier) (int, error) {
			values, _, err := q.LabelValues(context.Background(), "pod", nil, eq("job", "job-17"))
			return len(values), err
		}},
	}
	for _, lq := range labelQueries {
		f.phase(b, prefix+"query/"+lq.name, func(m map[string]float64) {
			run := func() int {
				querier, err := f.head.Querier(f.t0+lq.from, f.t0+lq.to)
				require.NoError(b, err)
				defer querier.Close()
				n, err := lq.run(querier)
				require.NoError(b, err)
				return n
			}
			values := run()
			if previous, loaded := f.expected.LoadOrStore(lq.name, values); loaded {
				require.Equal(b, previous, values, "values of %s differ between engines", lq.name)
			}
			p50, p99, cpu := timed(10, func() { run() })
			m["values"], m["p50-ms"], m["p99-ms"], m["cpu-ms/query"] = float64(values), p50, p99, cpu
		})
	}
}

func BenchmarkRealistic(b *testing.B) {
	series := realisticEnvInt("MIMIR_REALISTIC_SERIES", 2_000_000)
	hours := realisticEnvInt("MIMIR_REALISTIC_HOURS", 13)
	intervalMs := int64(realisticEnvInt("MIMIR_REALISTIC_INTERVAL_S", 120)) * 1000
	engines := strings.Split(os.Getenv("MIMIR_REALISTIC_ENGINES"), ",")
	if os.Getenv("MIMIR_REALISTIC_ENGINES") == "" {
		engines = benchEngines
	}
	rounds := int(int64(hours) * realisticHoursMs / intervalMs)
	t0 := int64(realisticStartRaw) - int64(realisticStartRaw)%chunkRangeMs

	lsets := make([]promlabels.Labels, series)
	for n := range lsets {
		lsets[n] = realisticLabels(n)
	}
	var expected sync.Map
	for _, engine := range engines {
		b.Run(engine, func(b *testing.B) {
			f := &realisticFixture{
				b: b, engine: engine, dir: b.TempDir(), series: series, rounds: rounds, intervalMs: intervalMs, t0: t0,
				lsets: lsets, refs: make([]storage.SeriesRef, series), results: map[string]map[string]float64{}, expected: &expected,
			}
			f.baseHeap = heapInUse()
			f.open()
			b.Cleanup(func() { _ = f.head.Close() })

			f.phase(b, "ingest-history", func(m map[string]float64) {
				var samples int
				var compactWall, compactCPU float64
				started, cpuStart := time.Now(), processCPU()
				lastCompact := int64(0)
				for round := range rounds {
					samples += f.appendRound(round, series)
					if ts := int64(round) * f.intervalMs; ts-lastCompact >= realisticCompact {
						lastCompact = ts
						cs, cc := time.Now(), processCPU()
						require.NoError(b, f.head.Compact(context.Background()))
						compactWall += time.Since(cs).Seconds()
						compactCPU += processCPU() - cc
					}
				}
				wall, cpu := time.Since(started).Seconds(), processCPU()-cpuStart
				m["samples/s"] = float64(samples) / (wall - compactWall)
				m["cpu-ns/sample"] = (cpu - compactCPU) * 1e9 / float64(samples)
				m["compact-wall-s"], m["compact-cpu-s"] = compactWall, compactCPU
			})
			f.phase(b, "memory-after-ingest", f.memory)

			// Series with a head: Mimir's ingester holds the last 2h to 3h there, and the rest in blocks.
			f.phase(b, "steady-ingest", func(m map[string]float64) {
				started, cpuStart := time.Now(), processCPU()
				samples := 0
				for round := rounds; round < rounds+5; round++ {
					samples += f.appendRound(round, series)
				}
				wall, cpu := time.Since(started).Seconds(), processCPU()-cpuStart
				m["samples/s"], m["cpu-ns/sample"] = float64(samples)/wall, cpu*1e9/float64(samples)
			})
			f.rounds += 5

			f.queryPhases(b, "")

			f.phase(b, "memory-after-queries", f.memory)

			f.phase(b, "mixed-ingest-and-queries", func(m map[string]float64) {
				stop := make(chan struct{})
				var ingested, roundsDone atomic.Int64
				var ingestDone sync.WaitGroup
				ingestDone.Add(1)
				go func() {
					defer ingestDone.Done()
					for round := f.rounds; ; round++ {
						select {
						case <-stop:
							return
						default:
							ingested.Add(int64(f.appendRound(round, series)))
							roundsDone.Add(1)
						}
					}
				}()
				queries := f.queries()
				var (
					mu        sync.Mutex
					latencies []time.Duration
					wg        sync.WaitGroup
				)
				started, cpuStart := time.Now(), processCPU()
				deadline := started.Add(20 * time.Second)
				for w := range 8 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for i := w; time.Now().Before(deadline); i++ {
							q := queries[i%len(queries)]
							begun := time.Now()
							f.selectSeries(q)
							mu.Lock()
							latencies = append(latencies, time.Since(begun))
							mu.Unlock()
						}
					}()
				}
				wg.Wait()
				wall, cpu := time.Since(started).Seconds(), processCPU()-cpuStart
				close(stop)
				ingestDone.Wait()
				f.rounds += int(roundsDone.Load())
				slices.Sort(latencies)
				m["ingest-samples/s"] = float64(ingested.Load()) / wall
				m["queries/s"] = float64(len(latencies)) / wall
				m["p50-ms"] = float64(latencies[len(latencies)/2].Microseconds()) / 1000
				m["p99-ms"] = float64(latencies[len(latencies)*99/100].Microseconds()) / 1000
				m["cpu-cores"] = cpu / wall
			})

			f.phase(b, "series-churn", func(m map[string]float64) {
				// A rollout: a twentieth of the series are replaced by new ones, with a new pod label.
				churn := series / 20
				fresh := make([]promlabels.Labels, churn)
				for n := range fresh {
					builder := promlabels.NewScratchBuilder(8)
					realisticLabels(n * 20).Range(func(l promlabels.Label) {
						if l.Name == "pod" {
							l.Value += "-new"
						}
						builder.Add(l.Name, l.Value)
					})
					builder.Sort()
					fresh[n] = builder.Labels()
				}
				started, cpuStart := time.Now(), processCPU()
				ts := f.t0 + int64(f.rounds+100)*f.intervalMs
				app := f.head.Appender(context.Background())
				for n, lset := range fresh {
					_, err := app.Append(0, lset, ts, float64(n))
					require.NoError(b, err)
					if n%realisticBatch == realisticBatch-1 {
						require.NoError(b, app.Commit())
						app = f.head.Appender(context.Background())
					}
				}
				require.NoError(b, app.Commit())
				wall, cpu := time.Since(started).Seconds(), processCPU()-cpuStart
				m["created-series/s"], m["cpu-us/series"] = float64(churn)/wall, cpu*1e6/float64(churn)
			})

			f.phase(b, "restart", func(m map[string]float64) {
				started := time.Now()
				require.NoError(b, f.head.Close())
				m["close-s"] = time.Since(started).Seconds()
				started = time.Now()
				f.open()
				m["open-s"] = time.Since(started).Seconds()
				// The restarted head's series are found by labels again.
				clear(f.refs)
				m["series-restored"] = float64(f.head.NumSeries())
				f.memory(m)
			})
			if f.head.NumSeries() == 0 {
				b.Fatal("restart lost every series")
			}
		})
	}
}
