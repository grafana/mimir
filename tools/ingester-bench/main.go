// SPDX-License-Identifier: AGPL-3.0-only

// ingester-bench runs two ingesters side by side on the same synthetic, production-shaped data, fed through Kafka as
// distributors would, and measures their CPU and memory under a query load of the same shape while checking that
// they answer every read alike.
//
// It writes a backfill to an in-process Kafka, starts both ingesters, which replay it like after a restart, then
// keeps producing every second as it passes, sends reads to both at the profile's rates, and measures a window of
// steady state:
//
//	go run ./tools/ingester-bench -series=500000 -backfill=2h -warmup=5m -duration=15m
//
// By default A is the Prometheus TSDB and B the seriesstore engine, of a binary built from the working tree.
// -binary-a and -binary-b compare two builds, -engine-a and -engine-b two engines.
package main

import (
	"cmp"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/grafana/dskit/flagext"
)

type config struct {
	profile           string
	series            int
	tenants           int
	seed              uint64
	backfill          time.Duration
	warmup, duration  time.Duration
	retention         time.Duration
	oooWindow         time.Duration
	queryScale        float64
	compareFraction   float64
	concurrency       int
	out               string
	binaryA, binaryB  string
	engineA, engineB  string
	extraA, extraB    flagext.StringSliceCSV
	kafkaDir          string
	cpuProfileSeconds int
	external          string
	kafkaPort         int
	dirA, dirB        string
	hold              time.Duration
	keep              bool
}

func main() {
	if watchMain() {
		return
	}
	var cfg config
	flag.StringVar(&cfg.profile, "profile", "", "Profile of the series and reads, as JSON; fields it has replace the built-in profile's.")
	flag.IntVar(&cfg.series, "series", 300_000, "Series of the ingester, over all tenants.")
	flag.IntVar(&cfg.tenants, "tenants", 0, "Tenants of the ingester (default the profile's): with fewer series than the profile's ingesters, fewer tenants keep the tenants' sizes as they are there.")
	flag.Uint64Var(&cfg.seed, "seed", 1, "Seed of the synthetic data and reads.")
	flag.DurationVar(&cfg.backfill, "backfill", 4*time.Hour, "History written before the ingesters start, which they replay. The head and the active series are as they are after hours of ingestion by then; the retention's worth, such as 13h, adds the cold data only the oldest reads read.")
	flag.DurationVar(&cfg.warmup, "warmup", 5*time.Minute, "Time of live feed and reads before the measurement window.")
	flag.DurationVar(&cfg.duration, "duration", 15*time.Minute, "Measurement window.")
	flag.DurationVar(&cfg.retention, "retention", 13*time.Hour, "TSDB retention of the ingesters, and how far back reads go.")
	flag.DurationVar(&cfg.oooWindow, "out-of-order-window", time.Hour, "Out-of-order window of the tenants.")
	flag.Float64Var(&cfg.queryScale, "query-scale", 1, "Factor of the profile's read rates.")
	flag.Float64Var(&cfg.compareFraction, "compare-fraction", 1, "Share of the reads whose responses are compared; all are sent and timed. Comparing takes the bench more CPU than reading takes the ingesters, so at high read rates comparing them all makes the bench the bottleneck.")
	flag.IntVar(&cfg.concurrency, "concurrency", 256, "Reads in flight at most; reads beyond are dropped and counted.")
	flag.StringVar(&cfg.out, "out", "", "Directory of the run's data, profiles and report (default a new temporary one).")
	flag.StringVar(&cfg.binaryA, "binary-a", "", "Mimir binary of ingester A (default built from the working tree).")
	flag.StringVar(&cfg.binaryB, "binary-b", "", "Mimir binary of ingester B (default the same as A).")
	flag.StringVar(&cfg.engineA, "engine-a", "prometheus", "TSDB engine of ingester A.")
	flag.StringVar(&cfg.engineB, "engine-b", "seriesstore", "TSDB engine of ingester B.")
	flag.Var(&cfg.extraA, "args-a", "Extra flags of ingester A, comma separated.")
	flag.Var(&cfg.extraB, "args-b", "Extra flags of ingester B, comma separated.")
	flag.StringVar(&cfg.kafkaDir, "kafka-dir", "", "Directory the in-process Kafka keeps its log in, instead of memory.")
	flag.IntVar(&cfg.cpuProfileSeconds, "cpu-profile-seconds", 60, "Length of the CPU profiles taken in the middle of the window.")
	flag.BoolVar(&cfg.keep, "keep", false, "Keep the ingesters' data when done.")
	flag.StringVar(&cfg.external, "external", "", "Directory shared with the containers that run the ingesters, with -exec: the bench writes their command lines to it once the backfill is done, and stops them through their PID files, which needs a shared process namespace.")
	flag.IntVar(&cfg.kafkaPort, "kafka-port", 0, "Port of the in-process Kafka (default a free one).")
	flag.StringVar(&cfg.dirA, "dir-a", "", "Data directory of ingester A (default in -out, or /data with -external).")
	flag.StringVar(&cfg.dirB, "dir-b", "", "Data directory of ingester B (default in -out, or /data with -external).")
	flag.DurationVar(&cfg.hold, "hold", 0, "Time to wait after the report before exiting, to copy the run's files.")
	renderPath := flag.String("render", "", "Write the report.html of a report.json, such as one copied from a pod, next to it, instead of running a bench.")
	execSpecPath := flag.String("exec", "", "Run the ingester of the spec file the bench writes with -external, once it's there, instead of a bench.")
	flag.Parse()
	if *execSpecPath != "" {
		log.Fatal(execMain(*execSpecPath))
	}
	if *renderPath != "" {
		if err := renderReport(*renderPath); err != nil {
			log.Fatal(err)
		}
		return
	}
	log.SetFlags(log.Ltime)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, cfg config) error {
	if cfg.out == "" {
		dir, err := os.MkdirTemp("", "ingester-bench-")
		if err != nil {
			return err
		}
		cfg.out = dir
	}
	if err := os.MkdirAll(cfg.out, 0o755); err != nil {
		return err
	}
	log.Printf("run directory %s", cfg.out)
	profile, err := loadProfile(cfg.profile)
	if err != nil {
		return err
	}
	if cfg.tenants > 0 {
		profile.Series.Tenants = cfg.tenants
	}
	if cfg.external != "" {
		// The containers' image has the binaries, and each its volume.
		cfg.binaryA, cfg.binaryB = cmp.Or(cfg.binaryA, "/bin/mimir"), cmp.Or(cfg.binaryB, cfg.binaryA, "/bin/mimir")
		cfg.dirA, cfg.dirB = cmp.Or(cfg.dirA, "/data"), cmp.Or(cfg.dirB, "/data")
		cfg.keep = true
	}
	cfg.dirA, cfg.dirB = cmp.Or(cfg.dirA, filepath.Join(cfg.out, "a")), cmp.Or(cfg.dirB, filepath.Join(cfg.out, "b"))
	if cfg.binaryA == "" {
		cfg.binaryA = filepath.Join(cfg.out, "mimir")
		log.Printf("building %s", cfg.binaryA)
		build := exec.CommandContext(ctx, "go", "build", "-o", cfg.binaryA, "./cmd/mimir")
		build.Stdout, build.Stderr = os.Stdout, os.Stderr
		if err := build.Run(); err != nil {
			return fmt.Errorf("build mimir: %w", err)
		}
	}
	if cfg.binaryB == "" {
		cfg.binaryB = cfg.binaryA
	}

	w := newWorld(profile.Series, cfg.series, cfg.seed)
	f, err := newFeed(w, cfg.kafkaDir, cfg.kafkaPort)
	if err != nil {
		return err
	}
	defer f.close()
	now := time.Now().UnixMilli() / 1000 * 1000
	dataStart := now - cfg.backfill.Milliseconds()
	log.Printf("%d series in %d tenants at %s; backfilling %s", w.totalSeries(), len(w.tenants), f.address(), cfg.backfill)
	backfillStart := time.Now()
	rec := newRecorder(5*time.Second, 10*time.Second)
	if err := f.produceSeconds(ctx, dataStart, now); err != nil {
		return err
	}
	// The seconds that passed while backfilling, as fast as well.
	caughtUp := now
	for target := time.Now().UnixMilli() / 1000 * 1000; caughtUp < target; target = time.Now().UnixMilli() / 1000 * 1000 {
		if err := f.produceSeconds(ctx, caughtUp, target); err != nil {
			return err
		}
		caughtUp = target
	}
	log.Printf("backfilled %d samples in %s, %s", f.samples.Load(), time.Since(backfillStart).Round(time.Second), f)
	rec.phase("backfill", backfillStart, time.Now())

	feedCtx, stopFeed := context.WithCancel(ctx)
	defer stopFeed()
	var feedWG sync.WaitGroup
	feedWG.Add(1)
	go func() {
		defer feedWG.Done()
		if err := f.live(feedCtx, caughtUp); err != nil && feedCtx.Err() == nil {
			log.Printf("feed: %v", err)
		}
	}()

	var ingesters [2]*ingesterProcess
	names := [2]string{"A " + cfg.engineA, "B " + cfg.engineB}
	for i, c := range []ingesterConfig{
		{name: names[0], binary: cfg.binaryA, engine: cfg.engineA, dir: cfg.dirA, httpPort: 18180, grpcPort: 19195, extraArgs: cfg.extraA},
		{name: names[1], binary: cfg.binaryB, engine: cfg.engineB, dir: cfg.dirB, httpPort: 18280, grpcPort: 19295, extraArgs: cfg.extraB},
	} {
		c.kafka, c.retention, c.oooWindow, c.external = f.address(), cfg.retention, cfg.oooWindow, cfg.external
		ingesters[i], err = startIngester(ctx, c)
		if err != nil {
			return err
		}
		defer func(p *ingesterProcess) {
			p.stop()
			if !cfg.keep {
				_ = os.RemoveAll(p.dir)
			}
		}(ingesters[i])
	}
	// From the ingesters' start, so that the replay shows too.
	recordCtx, stopRecording := context.WithCancel(ctx)
	defer stopRecording()
	var recordWG sync.WaitGroup
	recordWG.Add(1)
	go func() { defer recordWG.Done(); rec.sample(recordCtx, ingesters, f, 5*time.Second) }()
	var replay [2]float64
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, ing := range ingesters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			errs[i] = ing.waitReady(ctx, nil)
			replay[i] = time.Since(start).Seconds()
			rec.phase("replay "+ing.name[:1], start, time.Now())
			if errs[i] != nil {
				errs[i] = fmt.Errorf("%s: %w\n%s", ing.name, errs[i], ing.logTail())
				return
			}
			log.Printf("%s replayed the backfill in %.0fs", ing.name, replay[i])
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	gen := newQueryGen(w, profile.Queries, cfg.retention.Milliseconds(), func() int64 { return dataStart }, func() int64 { return time.Now().UnixMilli() })
	rates := gen.rates(cfg.queryScale)
	var rateList []string
	for rpc, rate := range rates {
		rateList = append(rateList, fmt.Sprintf("%s %.2f/s", rpc, rate))
	}
	log.Printf("reads: %s", strings.Join(rateList, ", "))
	retryDelay := time.Duration(max(profile.Series.LateMaxSeconds, 60))*time.Second + 30*time.Second
	d := newDriver(gen, f, ingesters, cfg.concurrency, retryDelay)
	slowLog, err := os.Create(filepath.Join(cfg.out, "slow.log"))
	if err != nil {
		return err
	}
	defer slowLog.Close()
	d.slowLog = slowLog
	d.recorder = rec
	d.compareFraction = cfg.compareFraction
	readCtx, stopReads := context.WithCancel(ctx)
	defer stopReads()
	go d.run(readCtx, rates, cfg.seed)
	// From the first read: the ingesters' memory, and their heap every two minutes, to see what grows even if one
	// runs out of memory before the window.
	go watchIngesters(readCtx, ingesters, cfg.out)

	log.Printf("warming up for %s", cfg.warmup)
	warmupStart := time.Now()
	if !sleep(ctx, cfg.warmup) {
		return ctx.Err()
	}
	rec.phase("warmup", warmupStart, time.Now())
	windowStart := time.Now()
	d.resetLatencies()
	d.measuring.Store(true)
	s := newSampler(ingesters, f)
	sampleCtx, stopSampling := context.WithCancel(ctx)
	defer stopSampling()
	var samplerWG sync.WaitGroup
	samplerWG.Add(1)
	go func() { defer samplerWG.Done(); s.run(sampleCtx, 10*time.Second) }()
	log.Printf("measuring for %s", cfg.duration)
	if !sleep(ctx, cfg.duration/2) {
		return ctx.Err()
	}
	var profileWG sync.WaitGroup
	for i, ing := range ingesters {
		profileWG.Add(1)
		go func() {
			defer profileWG.Done()
			if err := ing.profile("profile", filepath.Join(cfg.out, fmt.Sprintf("cpu-%c.pb.gz", 'a'+i)), cfg.cpuProfileSeconds); err != nil {
				log.Printf("%s CPU profile: %v", ing.name, err)
			}
		}()
	}
	if !sleep(ctx, cfg.duration-cfg.duration/2) {
		return ctx.Err()
	}
	stopSampling()
	samplerWG.Wait()
	d.measuring.Store(false)
	rec.phase("window", windowStart, time.Now())
	for i, ing := range ingesters {
		for _, kind := range []string{"heap", "allocs"} {
			if err := ing.profile(kind, filepath.Join(cfg.out, fmt.Sprintf("%s-%c.pb.gz", kind, 'a'+i)), 0); err != nil {
				log.Printf("%s %s profile: %v", ing.name, kind, err)
			}
		}
	}
	profileWG.Wait()
	stopReads()
	log.Printf("waiting for the retries of the reads that differed")
	retriesStart := time.Now()
	d.pending.Wait()
	rec.phase("retries", retriesStart, time.Now())
	stopRecording()
	recordWG.Wait()
	timeline, phases := rec.finish()
	stopFeed()
	feedWG.Wait()

	report := Report{
		Ingesters: names, Series: w.totalSeries(), Tenants: len(w.tenants), Replay: replay, WindowS: s.endAt.Sub(s.startAt).Seconds(),
		SamplesPerS: samplesPerSecond(w), APIs: d.apiReports(), Mismatches: d.examples,
		Config: runConfig(cfg, profile, rates), Started: backfillStart.Unix(), Phases: phases, Timeline: timeline, SlowReads: d.slowReads(),
	}
	for i := range ingesters {
		report.Resources[i] = s.resources(i)
	}
	report.print(os.Stdout)
	path := filepath.Join(cfg.out, "report.json")
	if err := writeJSON(path, report); err != nil {
		return err
	}
	if err := writeHTML(filepath.Join(cfg.out, "report.html"), report); err != nil {
		return err
	}
	if cfg.external != "" {
		// The pod's logs keep the summary after the pod is gone; the timeline is in report.json.
		summary := report
		summary.Timeline, summary.SlowReads = Timeline{}, nil
		data, _ := json.Marshal(summary)
		fmt.Printf("REPORT %s\n", data)
	}
	log.Printf("report and profiles in %s", cfg.out)
	if cfg.hold > 0 {
		// The image has no tar for kubectl cp: the run's files are served instead, for a port-forward.
		server := &http.Server{Addr: ":8090", Handler: http.FileServer(http.Dir(cfg.out)), ReadHeaderTimeout: 10 * time.Second}
		go func() { _ = server.ListenAndServe() }()
		log.Printf("holding for %s, serving %s on :8090", cfg.hold, cfg.out)
		sleep(ctx, cfg.hold)
		_ = server.Close()
	}
	return nil
}

func samplesPerSecond(w *world) float64 {
	total := 0.0
	for _, t := range w.tenants {
		for _, m := range t.metrics {
			total += float64(m.seriesCount()) / (float64(m.intervalMs) / 1000)
		}
	}
	return total
}

func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// runConfig is what the report says the run was.
func runConfig(cfg config, profile Profile, rates map[string]float64) map[string]string {
	out := map[string]string{
		"series": fmt.Sprint(cfg.series), "tenants": fmt.Sprint(profile.Series.Tenants), "seed": fmt.Sprint(cfg.seed),
		"backfill": cfg.backfill.String(), "warmup": cfg.warmup.String(), "duration": cfg.duration.String(),
		"retention": cfg.retention.String(), "out_of_order_window": cfg.oooWindow.String(), "query_scale": fmt.Sprint(cfg.queryScale),
		"concurrency": fmt.Sprint(cfg.concurrency), "compare_fraction": fmt.Sprint(cfg.compareFraction), "engine_a": cfg.engineA, "engine_b": cfg.engineB,
		"binary_a": filepath.Base(cfg.binaryA), "binary_b": filepath.Base(cfg.binaryB),
		"args_a": strings.Join(cfg.extraA, " "), "args_b": strings.Join(cfg.extraB, " "),
		"profile": cmp.Or(filepath.Base(cfg.profile), "built-in"), "query_shapes": fmt.Sprint(len(profile.Queries.Shapes)),
		"external": fmt.Sprint(cfg.external != ""),
	}
	for rpc, rate := range rates {
		out["rate_"+rpc] = fmt.Sprintf("%.3f/s", rate)
	}
	return out
}

func renderReport(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var report Report
	if err := json.Unmarshal(data, &report); err != nil {
		return err
	}
	out := filepath.Join(filepath.Dir(path), "report.html")
	if err := writeHTML(out, report); err != nil {
		return err
	}
	log.Printf("wrote %s", out)
	return nil
}
