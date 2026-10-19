// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/ingester/client"
	querierapi "github.com/grafana/mimir/pkg/querier/api"
)

// ingesterProcess is an ingester of its own process, consuming the feed's partition.
type ingesterProcess struct {
	name   string
	engine string
	// The ingester's process when the bench started it, or the file of its PID when another container runs it.
	cmd      *exec.Cmd
	pidFile  string
	httpPort int
	grpcPort int
	dir      string
	conn     *grpc.ClientConn
	client   client.IngesterClient
}

type ingesterConfig struct {
	name, binary, engine, dir string
	httpPort, grpcPort        int
	kafka                     string
	extraArgs                 []string
	oooWindow, retention      time.Duration
	// The runtime configuration of the tenants' limits, and the read path's limits by utilization, 0 to not limit it.
	runtimeConfig  string
	readPathCPU    int
	readPathMemory int64
	// The percentage of the records whose buffers are instrumented for reference leaks, 0 to not instrument them.
	leaksPercentage float64
	// Directory shared with containers that run the ingesters: the bench writes their command lines to it instead of
	// starting them.
	external string
}

func startIngester(ctx context.Context, cfg ingesterConfig) (*ingesterProcess, error) {
	if cfg.external == "" {
		if err := os.MkdirAll(cfg.dir, 0o755); err != nil {
			return nil, err
		}
	}
	args := []string{
		"-target=ingester",
		"-server.http-listen-port=" + strconv.Itoa(cfg.httpPort),
		"-server.grpc-listen-port=" + strconv.Itoa(cfg.grpcPort),
		"-server.grpc-max-recv-msg-size-bytes=104857600",
		"-server.grpc-max-send-msg-size-bytes=104857600",
		"-server.grpc-max-concurrent-streams=1000",
		// The partition is the instance's: both ingesters own partition 0, each in rings of their own.
		"-ingester.ring.instance-id=ingester-zone-a-0",
		"-ingester.ring.instance-addr=127.0.0.1",
		"-ingester.ring.store=inmemory",
		"-ingester.partition-ring.store=inmemory",
		"-ingester.ring.replication-factor=1",
		"-ingester.ring.heartbeat-period=5s",
		"-ingest-storage.enabled=true",
		"-ingest-storage.kafka.address=" + cfg.kafka,
		"-ingest-storage.kafka.topic=" + topic,
		"-ingest-storage.kafka.consume-from-position-at-startup=start",
		"-ingest-storage.kafka.auto-create-topic-enabled=false",
		"-ingest-storage.kafka.consumer-group-offset-commit-file-enforced=true",
		"-ingester.push-grpc-method-enabled=false",
		"-blocks-storage.backend=filesystem",
		"-blocks-storage.filesystem.dir=" + filepath.Join(cfg.dir, "bucket"),
		"-blocks-storage.tsdb.dir=" + filepath.Join(cfg.dir, "tsdb"),
		"-blocks-storage.tsdb.retention-period=" + cfg.retention.String(),
		// Neither ships: the seriesstore engine has no blocks to, and the bench measures ingestion and reads.
		"-blocks-storage.tsdb.ship-interval=0",
		"-blocks-storage.tsdb.flush-blocks-on-shutdown=false",
		"-blocks-storage.tsdb.memory-snapshot-on-shutdown=false",
		"-ingester.out-of-order-time-window=" + cfg.oooWindow.String(),
		"-ingester.max-global-series-per-user=150000",
		"-ingester.max-global-series-per-metric=0",
		"-ingester.max-global-exemplars-per-user=100000",
		"-ingester.native-histograms-ingestion-enabled=true",
		"-validation.max-label-names-per-series=100",
		"-validation.max-length-label-value=4096",
		"-querier.max-fetched-series-per-query=0",
		"-querier.max-fetched-chunks-per-query=0",
		"-ingester.instance-limits.max-series=0",
		"-ingester.instance-limits.max-tenants=0",
		"-ingester.instance-limits.max-inflight-push-requests=0",
		"-ingester.instance-limits.max-ingestion-rate=0",
		"-ingester.read-path-cpu-utilization-limit=" + strconv.Itoa(cfg.readPathCPU),
		"-ingester.read-path-memory-utilization-limit=" + strconv.FormatInt(cfg.readPathMemory, 10),
		// As a multi-tenant cell runs them: the TSDB's caches and planning, timely and early head compactions, the
		// circuit breakers, owned series, the error sampling and the instrumentation of a share of the records for
		// reference leaks, cost attribution and the tenants' limits and trackers.
		"-blocks-storage.tsdb.head-postings-for-matchers-cache-force=true",
		"-blocks-storage.tsdb.head-postings-for-matchers-cache-invalidation=true",
		"-blocks-storage.tsdb.head-postings-for-matchers-cache-max-bytes=26214400",
		"-blocks-storage.tsdb.head-postings-for-matchers-cache-ttl=10m",
		"-blocks-storage.tsdb.block-postings-for-matchers-cache-force=true",
		"-blocks-storage.tsdb.block-postings-for-matchers-cache-max-bytes=26214400",
		"-blocks-storage.tsdb.block-postings-for-matchers-cache-ttl=10m",
		"-blocks-storage.tsdb.shared-postings-for-matchers-cache=true",
		"-blocks-storage.tsdb.index-lookup-planning.enabled=true",
		"-blocks-storage.tsdb.index-lookup-planning.comparison-portion=0.01",
		"-blocks-storage.tsdb.float-chunk-encoding=xor2",
		"-blocks-storage.tsdb.head-chunks-write-buffer-size-bytes=1048576",
		"-blocks-storage.tsdb.head-compaction-interval=15m",
		"-blocks-storage.tsdb.timely-head-compaction-enabled=true",
		"-blocks-storage.tsdb.early-head-compaction-min-in-memory-series=2400000",
		"-blocks-storage.tsdb.early-head-compaction-min-estimated-series-reduction-percentage=15",
		"-blocks-storage.tsdb.series-hash-cache-max-size-bytes=367001600",
		"-ingester.active-series-metrics-enabled=true",
		"-ingester.active-series-metrics-idle-timeout=20m",
		"-ingester.early-compaction-non-owned-series-enabled=true",
		"-ingester.track-ingester-owned-series=true",
		"-ingester.use-ingester-owned-series-for-limits=true",
		"-ingester.error-sample-rate=10",
		"-ingester.push-circuit-breaker.enabled=true",
		"-ingester.push-circuit-breaker.failure-threshold-percentage=20",
		"-ingester.push-circuit-breaker.initial-delay=60s",
		"-ingester.push-circuit-breaker.request-timeout=2s",
		"-ingester.read-circuit-breaker.enabled=true",
		"-ingester.read-circuit-breaker.failure-threshold-percentage=20",
		"-ingester.read-circuit-breaker.request-timeout=30s",
		"-ingester.max-global-metadata-per-metric=10",
		"-ingester.max-global-metadata-per-user=30000",
		"-instrument-reference-leaks.percentage=" + strconv.FormatFloat(cfg.leaksPercentage, 'g', -1, 64),
		"-instrument-reference-leaks.before-reuse-period=2m",
		"-instrument-reference-leaks.max-inflight-instrumented-bytes=100000000",
		"-cost-attribution.registry-path=/usage-metrics",
		"-validation.cost-attribution-trackers=" + baseCostAttributionTrackers(),
		"-validation.max-active-series-additional-custom-trackers=200",
		"-runtime-config.file=" + cfg.runtimeConfig,
		"-memberlist.join=",
		"-usage-stats.enabled=false",
		"-log.level=warn",
	}
	if cfg.engine == "prometheus" {
		args = slicesDelete(args, "-blocks-storage.tsdb.memory-snapshot-on-shutdown=false")
	} else {
		// Only for other engines: the Prometheus TSDB is the default, and released binaries have no such flag.
		args = append(args, "-blocks-storage.tsdb.engine="+cfg.engine)
	}
	args = append(args, cfg.extraArgs...)
	p := &ingesterProcess{name: cfg.name, engine: cfg.engine, httpPort: cfg.httpPort, grpcPort: cfg.grpcPort, dir: cfg.dir}
	if cfg.external != "" {
		id := strings.ToLower(cfg.name[:1])
		p.pidFile = filepath.Join(cfg.external, id+".pid")
		if err := writeSpec(filepath.Join(cfg.external, id+".json"), execSpec{Binary: cfg.binary, Dir: cfg.dir, Args: args}); err != nil {
			return nil, err
		}
	} else {
		// Through a watcher of the bench's own binary, which stops the ingester when the bench is gone, however it ended.
		self, err := os.Executable()
		if err != nil {
			return nil, err
		}
		cmd := exec.CommandContext(ctx, self, append([]string{cfg.binary}, args...)...)
		cmd.Env = append(os.Environ(), watchEnv+"="+strconv.Itoa(os.Getpid()))
		logFile, err := os.Create(filepath.Join(cfg.dir, "ingester.log"))
		if err != nil {
			return nil, err
		}
		cmd.Stdout, cmd.Stderr = logFile, logFile
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		p.cmd = cmd
	}
	var err error
	p.conn, err = grpc.NewClient("127.0.0.1:"+strconv.Itoa(cfg.grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100<<20)),
		grpc.WithUnaryInterceptor(querierapi.ReadConsistencyClientUnaryInterceptor),
		grpc.WithStreamInterceptor(querierapi.ReadConsistencyClientStreamInterceptor),
	)
	if err != nil {
		p.stop()
		return nil, err
	}
	p.client = client.NewIngesterClient(p.conn)
	return p, nil
}

func slicesDelete(args []string, arg string) []string {
	out := args[:0]
	for _, a := range args {
		if a != arg {
			out = append(out, a)
		}
	}
	return out
}

// waitReady waits for the ingester to say it is ready, which it does once it caught up with the partition.
func (p *ingesterProcess) waitReady(ctx context.Context, progress func()) error {
	for {
		if p.cmd != nil && p.cmd.ProcessState != nil {
			return fmt.Errorf("%s exited: %s", p.name, p.cmd.ProcessState)
		}
		resp, err := http.Get(p.url("/ready"))
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		if progress != nil {
			progress()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (p *ingesterProcess) url(path string) string {
	return "http://127.0.0.1:" + strconv.Itoa(p.httpPort) + path
}

// metrics scrapes the ingester's metrics, summing the samples of each metric name.
func (p *ingesterProcess) metrics() (map[string]float64, error) {
	resp, err := http.Get(p.url("/metrics"))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(bufio.NewReader(resp.Body))
	if err != nil && len(families) == 0 {
		return nil, err
	}
	out := map[string]float64{}
	for name, family := range families {
		for _, m := range family.Metric {
			var v float64
			switch {
			case m.Counter != nil:
				v = m.Counter.GetValue()
			case m.Gauge != nil:
				v = m.Gauge.GetValue()
			case m.Untyped != nil:
				v = m.Untyped.GetValue()
			case m.Summary != nil:
				out[name+"_count"] += float64(m.Summary.GetSampleCount())
				continue
			default:
				continue
			}
			key := name
			for _, l := range m.Label {
				// The few labels the report needs.
				if l.GetName() == "cpu_class" || (name == "cortex_ingest_storage_reader_last_consumed_offset" && l.GetName() == "partition") {
					key += "{" + l.GetName() + "=" + l.GetValue() + "}"
				}
			}
			out[key] += v
		}
	}
	return out, nil
}

// lastConsumedOffset is the partition's offset the ingester consumed up to.
func (p *ingesterProcess) lastConsumedOffset() int64 {
	m, err := p.metrics()
	if err != nil {
		return -1
	}
	if v, ok := m["cortex_ingest_storage_reader_last_consumed_offset{partition=0}"]; ok {
		return int64(v)
	}
	return -1
}

// profile saves a pprof profile of the ingester.
func (p *ingesterProcess) profile(kind, path string, seconds int) error {
	url := p.url("/debug/pprof/" + kind)
	if seconds > 0 {
		url += "?seconds=" + strconv.Itoa(seconds)
	}
	httpClient := http.Client{Timeout: time.Duration(seconds+60) * time.Second}
	resp, err := httpClient.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, resp.Body)
	return err
}

func (p *ingesterProcess) stop() {
	if p.conn != nil {
		_ = p.conn.Close()
	}
	if p.pidFile != "" {
		// The pod shares its process namespace: the ingester's container ends with it.
		data, err := os.ReadFile(p.pidFile)
		if err != nil {
			return
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil {
			return
		}
		_ = syscall.Kill(pid, syscall.SIGTERM)
		for deadline := time.Now().Add(2 * time.Minute); time.Now().Before(deadline) && syscall.Kill(pid, 0) == nil; {
			time.Sleep(time.Second)
		}
		return
	}
	if p.cmd != nil && p.cmd.Process != nil {
		_ = p.cmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() { _ = p.cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Minute):
			_ = p.cmd.Process.Kill()
		}
	}
}

// logTail returns the end of the ingester's log, for errors.
func (p *ingesterProcess) logTail() string {
	if p.pidFile != "" {
		return "(the ingester's log is its container's)"
	}
	data, err := os.ReadFile(filepath.Join(p.dir, "ingester.log"))
	if err != nil {
		return ""
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	return strings.Join(lines[max(0, len(lines)-20):], "\n")
}

const watchEnv = "INGESTER_BENCH_WATCH_PARENT"

// watchMain runs the command of its arguments, and stops it when the process of the environment's PID is gone or
// when it is itself told to stop. It returns false when the bench isn't running as a watcher.
func watchMain() bool {
	parent, err := strconv.Atoi(os.Getenv(watchEnv))
	if err != nil || len(os.Args) < 2 {
		return false
	}
	cmd := exec.Command(os.Args[1], os.Args[2:]...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case err := <-done:
			if err != nil {
				os.Exit(1)
			}
			os.Exit(0)
		case sig := <-signals:
			_ = cmd.Process.Signal(sig)
		case <-ticker.C:
			// The bench's process is gone, or it is another's now that the bench's parent adopted its PID.
			if syscall.Kill(parent, 0) != nil || os.Getppid() != parent {
				_ = cmd.Process.Signal(os.Interrupt)
				select {
				case <-done:
				case <-time.After(time.Minute):
					_ = cmd.Process.Kill()
				}
				os.Exit(1)
			}
		}
	}
}

// execSpec is the command line of an ingester another container runs.
type execSpec struct {
	Binary string   `json:"binary"`
	Dir    string   `json:"dir"`
	Args   []string `json:"args"`
}

func writeSpec(path string, spec execSpec) error {
	data, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	// Written whole, then renamed: the container waiting for it never reads half of it.
	if err := os.WriteFile(path+".tmp", data, 0o644); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}

// execMain waits for the spec at path, written once the bench's backfill is done, then runs its command in place of
// itself, so that the ingester is its container's process, with its CPUs and memory limit. The PID file next to the
// spec lets the bench stop it.
func execMain(path string) error {
	var data []byte
	for {
		var err error
		if data, err = os.ReadFile(path); err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	var spec execSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return err
	}
	if err := os.MkdirAll(spec.Dir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(strings.TrimSuffix(path, ".json")+".pid", []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
		return err
	}
	return syscall.Exec(spec.Binary, append([]string{spec.Binary}, spec.Args...), os.Environ())
}
