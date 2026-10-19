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
// The ingesters run with the flags of defaults.yaml, as a multi-tenant cell runs them, by default both of a binary
// built from the working tree. -config files override it, with flags of both and of each, and their binaries or images:
//
//	go run ./tools/ingester-bench -config=my-comparison.yaml
//
// The run's directory has report.json, report.html and CPU and heap profiles. -results keeps them in a directory of
// past runs, with an index.html of them all, B against A over runs; -add-result adds a report.json from elsewhere.
// run-in-kubernetes.sh builds the image, runs job.yaml in a cluster with CPUs of their own for each ingester, and adds
// the report to a results directory:
//
//	IMAGE_REPO=... KUBE_CONTEXT=... NAMESPACE=... RESULTS=~/ingester-bench-results tools/ingester-bench/run-in-kubernetes.sh
package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
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
	readScales        string
	compareFraction   float64
	concurrency       int
	out               string
	configPaths       flagext.StringSliceCSV
	kafkaDir          string
	cpuProfileSeconds int
	external          string
	kafkaPort         int
	dirA, dirB        string
	hold              time.Duration
	results           string
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
	flag.DurationVar(&cfg.warmup, "warmup", 21*time.Minute, "Time of live feed and reads before the measurement window. An ingester counts every series a replay ingested as active for its active series idle timeout, 20 minutes by default, so the counts are only as they'd be after hours of ingestion past it.")
	flag.DurationVar(&cfg.duration, "duration", 15*time.Minute, "Measurement window.")
	flag.DurationVar(&cfg.retention, "retention", 13*time.Hour, "TSDB retention of the ingesters, and how far back reads go.")
	flag.DurationVar(&cfg.oooWindow, "out-of-order-window", 2*time.Hour, "Out-of-order window of the tenants.")
	flag.Float64Var(&cfg.queryScale, "query-scale", 1, "Factor of the profile's read rates.")
	flag.StringVar(&cfg.readScales, "read-scales", "", "Factors of some reads' rates on top of -query-scale, such as LabelNamesAndValues=50,QueryStream=0: a short run of a small ingester sends a few of the rare reads otherwise.")
	flag.Float64Var(&cfg.compareFraction, "compare-fraction", 1, "Share of the reads whose responses are compared; all are sent and timed. Comparing takes the bench more CPU than reading takes the ingesters, so at high read rates comparing them all makes the bench the bottleneck.")
	flag.IntVar(&cfg.concurrency, "concurrency", 256, "Reads in flight at most; reads beyond are dropped and counted.")
	flag.StringVar(&cfg.out, "out", "", "Directory of the run's data, profiles and report (default a new temporary one).")
	flag.Var(&cfg.configPaths, "config", "Configuration files of the ingesters, comma separated, each over defaults.yaml and the ones before: flags of both and of each, and their binaries or images.")
	flag.StringVar(&cfg.kafkaDir, "kafka-dir", "", "Directory the in-process Kafka keeps its log in, instead of memory.")
	flag.IntVar(&cfg.cpuProfileSeconds, "cpu-profile-seconds", 60, "Length of the CPU profiles taken in the middle of the window.")
	flag.BoolVar(&cfg.keep, "keep", false, "Keep the ingesters' data when done.")
	flag.StringVar(&cfg.external, "external", "", "Directory shared with the containers that run the ingesters, with -exec: the bench writes their command lines to it once the backfill is done, and stops them through their PID files, which needs a shared process namespace.")
	flag.IntVar(&cfg.kafkaPort, "kafka-port", 0, "Port of the in-process Kafka (default a free one).")
	flag.StringVar(&cfg.dirA, "dir-a", "", "Data directory of ingester A (default in -out, or /data with -external).")
	flag.StringVar(&cfg.dirB, "dir-b", "", "Data directory of ingester B (default in -out, or /data with -external).")
	flag.DurationVar(&cfg.hold, "hold", 0, "Time to wait after the report before exiting, to copy the run's files.")
	resultsDir := flag.String("results", "", "Directory of past runs: the run's report is saved in it, and its index.html of all runs written again.")
	addPath := flag.String("add-result", "", "Add the report.json, such as one copied from a pod, to -results and write its index.html again, instead of running a bench.")
	renderPath := flag.String("render", "", "Write the report.html of a report.json, such as one copied from a pod, next to it, instead of running a bench.")
	printConfig := flag.Bool("print-config", false, "Print the configuration of -config, merged, as JSON, instead of running a bench.")
	installPath := flag.String("install", "", "Copy the bench's binary to this path, for containers of other images to run, instead of running a bench.")
	execSpecPath := flag.String("exec", "", "Run the ingester of the spec file the bench writes with -external, once it's there, instead of a bench.")
	flag.Parse()
	if *execSpecPath != "" {
		log.Fatal(execMain(*execSpecPath))
	}
	if *installPath != "" {
		if err := install(*installPath); err != nil {
			log.Fatal(err)
		}
		return
	}
	if *printConfig {
		c, err := loadConfig(cfg.configPaths)
		if err != nil {
			log.Fatal(err)
		}
		data, _ := json.MarshalIndent(c, "", "  ")
		fmt.Println(string(data))
		return
	}
	cfg.results = *resultsDir
	if *addPath != "" {
		if cfg.results == "" {
			log.Fatal("-add-result needs -results")
		}
		dir, err := addResult(cfg.results, *addPath)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("added %s, index in %s", dir, filepath.Join(cfg.results, "index.html"))
		return
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

func run(parent context.Context, cfg config) (err error) {
	// A run without its feed would measure idle ingesters: the feed failing ends it, with the feed's error.
	ctx, failRun := context.WithCancelCause(parent)
	defer func() {
		if cause := context.Cause(ctx); err != nil && parent.Err() == nil && cause != nil && !errors.Is(cause, context.Canceled) {
			err = cause
		}
		failRun(nil)
	}()
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
	benchCfg, err := loadConfig(cfg.configPaths)
	if err != nil {
		return err
	}
	specs := [2]ingesterSpec{benchCfg.ingester(0), benchCfg.ingester(1)}
	if cfg.tenants > 0 {
		profile.Series.Tenants = cfg.tenants
	}
	if cfg.external != "" {
		// Each container's image has its binary, and each container its volume.
		for i := range specs {
			specs[i].Binary = cmp.Or(specs[i].Binary, "/bin/mimir")
		}
		cfg.dirA, cfg.dirB = cmp.Or(cfg.dirA, "/data"), cmp.Or(cfg.dirB, "/data")
		cfg.keep = true
	}
	cfg.dirA, cfg.dirB = cmp.Or(cfg.dirA, filepath.Join(cfg.out, "a")), cmp.Or(cfg.dirB, filepath.Join(cfg.out, "b"))
	for i := range specs {
		if specs[i].Binary != "" {
			continue
		}
		if specs[i].Image != "" {
			return fmt.Errorf("ingester %c: an image is for run-in-kubernetes.sh; started by the bench, it needs a binary", 'A'+i)
		}
		tree := filepath.Join(cfg.out, "mimir")
		if i == 0 || specs[0].Binary != tree {
			log.Printf("building %s", tree)
			// Stamped with the tree's commit, which the report says it compared.
			build := exec.CommandContext(ctx, "go", "build", "-o", tree, "-ldflags", "-X github.com/grafana/mimir/pkg/util/version.Revision="+benchRevision(), "./cmd/mimir")
			build.Stdout, build.Stderr = os.Stdout, os.Stderr
			if err := build.Run(); err != nil {
				return fmt.Errorf("build mimir: %w", err)
			}
		}
		specs[i].Binary = tree
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
			failRun(fmt.Errorf("feed: %w", err))
		}
	}()

	// Where the ingesters read it: the containers that run them share the external directory.
	overridesDir := cfg.out
	if cfg.external != "" {
		overridesDir = cfg.external
	}
	overridesPath := filepath.Join(overridesDir, "overrides.json")
	if err := writeOverrides(w, overridesPath); err != nil {
		return err
	}
	var ingesters [2]*ingesterProcess
	names := [2]string{strings.TrimSpace("A " + specs[0].Name), strings.TrimSpace("B " + specs[1].Name)}
	for i, c := range []ingesterConfig{
		{name: names[0], dir: cfg.dirA, httpPort: 18180, grpcPort: 19195},
		{name: names[1], dir: cfg.dirB, httpPort: 18280, grpcPort: 19295},
	} {
		c.binary, c.spec = specs[i].Binary, specs[i]
		c.kafka, c.retention, c.oooWindow, c.external, c.runtimeConfig = f.address(), cfg.retention, cfg.oooWindow, cfg.external, overridesPath
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
	for _, item := range strings.Split(cfg.readScales, ",") {
		if item == "" {
			continue
		}
		rpc, factor, ok := strings.Cut(item, "=")
		f, err := strconv.ParseFloat(factor, 64)
		if _, known := rates[rpc]; !ok || err != nil || !known {
			return fmt.Errorf("-read-scales: %q is not a read and its factor", item)
		}
		rates[rpc] *= f
	}
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
	go d.run(readCtx, ctx, rates, cfg.seed)
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
		Config: runConfig(cfg, specs, ingesters, profile, rates), Started: backfillStart.Unix(), Phases: phases, Timeline: timeline, SlowReads: d.slowReads(),
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
	if cfg.results != "" {
		dir, err := saveResult(cfg.results, report, cfg.out)
		if err != nil {
			return err
		}
		log.Printf("saved in %s, index in %s", dir, filepath.Join(cfg.results, "index.html"))
	}
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
func runConfig(cfg config, specs [2]ingesterSpec, ingesters [2]*ingesterProcess, profile Profile, rates map[string]float64) map[string]string {
	out := map[string]string{
		"series": fmt.Sprint(cfg.series), "tenants": fmt.Sprint(profile.Series.Tenants), "seed": fmt.Sprint(cfg.seed),
		"backfill": cfg.backfill.String(), "warmup": cfg.warmup.String(), "duration": cfg.duration.String(),
		"retention": cfg.retention.String(), "out_of_order_window": cfg.oooWindow.String(), "query_scale": fmt.Sprint(cfg.queryScale), "read_scales": cfg.readScales,
		"concurrency": fmt.Sprint(cfg.concurrency), "compare_fraction": fmt.Sprint(cfg.compareFraction),
		"config":  strings.Join(cfg.configPaths, ","),
		"profile": cmp.Or(filepath.Base(cfg.profile), "built-in"), "query_shapes": fmt.Sprint(len(profile.Queries.Shapes)),
		"external":       fmt.Sprint(cfg.external != ""),
		"bench_revision": benchRevision(),
	}
	for i, spec := range specs {
		id := string(rune('a' + i))
		out["name_"+id], out["binary_"+id], out["image_"+id] = spec.Name, filepath.Base(spec.Binary), spec.Image
		out["args_"+id] = strings.Join(spec.withArgs(nil), " ")
		out["version_"+id] = ingesters[i].version()
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
