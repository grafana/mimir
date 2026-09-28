// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/grafana/mimir/cmd/rust-kafka-ingester/ring-sidecar/handlers"
	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/util/shutdownmarker"
)

type config struct {
	instanceID       string
	podIP            string
	zone             string
	memberlistRole   string
	memberlistPort   int
	rustPort         int
	partition        int
	join             string
	clusterLabel     string
	instanceRingKey  string
	partitionRingKey string
	listen           string
	pollInterval     time.Duration
	sharedRing       bool
	coverageFile     string
	minCoverage      time.Duration
	limitsRingKey    string

	unregisterOnShutdown       bool
	shutdownMarkerDir          string
	allowPartitionStateChanges bool
}

func main() {
	var cfg config
	flag.StringVar(&cfg.instanceID, "instance-id", "", "Stable pod name")
	flag.StringVar(&cfg.podIP, "pod-ip", "", "Pod IP")
	flag.StringVar(&cfg.zone, "zone", "zone-c", "Ring availability zone")
	flag.StringVar(&cfg.memberlistRole, "memberlist-role", "bridge", "Memberlist zone-aware routing role")
	flag.IntVar(&cfg.memberlistPort, "memberlist-port", 7946, "Memberlist gossip port")
	flag.IntVar(&cfg.rustPort, "rust-port", 9095, "Rust gRPC port")
	flag.IntVar(&cfg.partition, "partition", -1, "Kafka partition ID")
	flag.StringVar(&cfg.join, "memberlist-join", "", "Comma-separated memberlist seed addresses")
	flag.StringVar(&cfg.clusterLabel, "memberlist-cluster-label", "", "Memberlist cluster label")
	flag.StringVar(&cfg.instanceRingKey, "instance-ring-key", "rust-partition-ingesters/ring", "Isolated ingester ring KV key")
	flag.StringVar(&cfg.partitionRingKey, "partition-ring-key", "rust-ingester-partitions", "Isolated partition ring KV key")
	flag.StringVar(&cfg.listen, "listen", ":8080", "HTTP health and metrics address")
	flag.BoolVar(&cfg.sharedRing, "shared-ring", false, "Join rings owned by Go ingesters: never create or activate partitions, and only own partitions that exist and are not inactive")
	flag.StringVar(&cfg.coverageFile, "coverage-file", "", "File where the Rust ingester records, in Unix milliseconds, the time since which it holds complete data")
	flag.DurationVar(&cfg.minCoverage, "min-coverage", 0, "Minimum age of complete data before registering; set it above -querier.query-ingesters-within")
	flag.StringVar(&cfg.limitsRingKey, "limits-partition-ring-key", "ingester-partitions", "Partition ring KV key whose active partitions divide global limits, read-only")
	flag.BoolVar(&cfg.unregisterOnShutdown, "ingester.ring.unregister-on-shutdown", true, "Unregister from the instance ring on shutdown, like the Go ingester flag")
	flag.StringVar(&cfg.shutdownMarkerDir, "shutdown-marker-dir", "", "Directory of the prepare-shutdown marker, which survives restarts like the Go ingester's in its TSDB directory")
	flag.BoolVar(&cfg.allowPartitionStateChanges, "allow-partition-state-changes", false, "In the shared ring, let prepare-partition-downscale change partition states; they belong to the Go ingesters otherwise")
	flag.Parse()
	if cfg.instanceID == "" || net.ParseIP(cfg.podIP) == nil || cfg.partition < 0 || cfg.join == "" || cfg.clusterLabel == "" {
		fmt.Fprintln(os.Stderr, "instance-id, pod-ip, partition, memberlist-join and memberlist-cluster-label are required")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config) error {
	logger := log.NewLogfmtLogger(os.Stdout)
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	memberlistConfig := newMemberlistConfig(cfg)
	resolver := dns.NewProvider(dns.GolangResolverType, 0, logger, registry)
	memberlistService := memberlist.NewKVInitService(&memberlistConfig, logger, resolver, registry)
	if err := startService(ctx, memberlistService); err != nil {
		return fmt.Errorf("start memberlist: %w", err)
	}
	defer services.StopAndAwaitTerminated(context.Background(), memberlistService)
	store, err := memberlistService.GetMemberlistKV()
	if err != nil {
		return fmt.Errorf("initialize memberlist: %w", err)
	}
	if err := store.AwaitRunning(ctx); err != nil {
		return fmt.Errorf("join memberlist: %w", err)
	}
	instanceClient, err := memberlist.NewClient(store, ring.GetCodec())
	if err != nil {
		return err
	}
	partitionClient, err := memberlist.NewClient(store, ring.GetPartitionRingCodec())
	if err != nil {
		return err
	}

	var ready atomic.Bool
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "rust_ingester_ring_ready", Help: "Whether the Rust ingester is present in both shadow rings and serving reads.",
	}, func() float64 {
		if ready.Load() {
			return 1
		}
		return 0
	}))
	lifecycle := &lifecycleState{}
	if cfg.shutdownMarkerDir != "" {
		exists, err := shutdownmarker.Exists(shutdownmarker.GetPath(cfg.shutdownMarkerDir))
		if err != nil {
			return fmt.Errorf("check prepare-shutdown marker: %w", err)
		}
		lifecycle.prepared.Store(exists)
	}
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_ingester_prepare_shutdown_requested", Help: "If the ingester has been requested to prepare for shutdown via endpoint or marker file.",
	}, func() float64 {
		if lifecycle.prepared.Load() {
			return 1
		}
		return 0
	}))
	mux := http.NewServeMux()
	mux.HandleFunc("/ingester/prepare-shutdown", func(w http.ResponseWriter, req *http.Request) {
		handlers.PrepareShutdown(w, req, cfg.shutdownMarkerDir, &lifecycle.prepared, logger)
	})
	mux.HandleFunc("/ingester/prepare-partition-downscale", func(w http.ResponseWriter, req *http.Request) {
		preparePartitionDownscaleHandler(w, req, cfg, lifecycle, logger)
	})
	mux.HandleFunc("/owned-token-ranges", func(w http.ResponseWriter, req *http.Request) {
		value, err := partitionClient.Get(req.Context(), cfg.limitsRingKey)
		if err != nil || value == nil {
			http.Error(w, "partition ring unavailable", http.StatusServiceUnavailable)
			return
		}
		var body struct {
			Tenants map[string]int `json:"tenants"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ranges, err := ownedTokenRanges(value.(*ring.PartitionRingDesc), int32(cfg.partition), body.Tenants)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ranges)
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "Rust ingester or ring unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.Handle("/memberlist", memberlistService)
	mux.HandleFunc("/ring", func(w http.ResponseWriter, req *http.Request) {
		instance, instanceErr := instanceClient.Get(req.Context(), cfg.instanceRingKey)
		partition, partitionErr := partitionClient.Get(req.Context(), cfg.partitionRingKey)
		if instanceErr != nil || partitionErr != nil {
			http.Error(w, "ring unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ready": ready.Load(), "instance_ring_key": cfg.instanceRingKey,
			"partition_ring_key": cfg.partitionRingKey,
			"instance_ring":      instance, "partition_ring": partition,
		})
	})
	// Global per-tenant limits are divided by the active partitions of the Go ingesters' ring,
	// whichever ring this pod registers in.
	mux.HandleFunc("/active-partitions", func(w http.ResponseWriter, req *http.Request) {
		value, err := partitionClient.Get(req.Context(), cfg.limitsRingKey)
		if err != nil || value == nil {
			http.Error(w, "partition ring unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprintln(w, activePartitions(value.(*ring.PartitionRingDesc)))
	})
	httpServer := &http.Server{Addr: cfg.listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	listener, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return err
	}
	go func() { _ = httpServer.Serve(listener) }()
	defer httpServer.Shutdown(context.Background())

	return manage(ctx, cfg, instanceClient, partitionClient, func() bool { return memberlistService.State() == services.Running }, &ready, lifecycle, logger)
}

func newMemberlistConfig(cfg config) memberlist.KVConfig {
	var memberlistConfig memberlist.KVConfig
	flagext.DefaultValues(&memberlistConfig)
	memberlistConfig.NodeName = cfg.instanceID + "-rust-ring"
	memberlistConfig.ClusterLabel = cfg.clusterLabel
	memberlistConfig.Codecs = []codec.Codec{
		ring.GetCodec(), ring.GetPartitionRingCodec(), distributor.GetReplicaDescCodec(),
		memberlist.GetPropagationDelayTrackerCodec(),
	}
	memberlistConfig.TCPTransport.BindAddrs = []string{cfg.podIP}
	memberlistConfig.TCPTransport.BindPort = cfg.memberlistPort
	memberlistConfig.JoinMembers = strings.Split(cfg.join, ",")
	memberlistConfig.AbortIfFastJoinFails = true
	memberlistConfig.ZoneAwareRouting = memberlist.ZoneAwareRoutingConfig{
		Enabled: true, Zone: cfg.zone, Role: cfg.memberlistRole,
	}
	return memberlistConfig
}

func preparePartitionDownscaleHandler(w http.ResponseWriter, req *http.Request, cfg config, lifecycle *lifecycleState, logger log.Logger) {
	// A typed nil would not compare to nil in the handler.
	var partition handlers.PartitionLifecycler
	if p := lifecycle.partition.Load(); p != nil {
		partition = p
	}
	handlers.PreparePartitionDownscale(w, req, partition, !cfg.sharedRing || cfg.allowPartitionStateChanges, logger)
}

func manage(ctx context.Context, cfg config, instanceClient, partitionClient kv.Client, memberlistRunning func() bool, ready *atomic.Bool, lifecycle *lifecycleState, logger log.Logger) error {
	if lifecycle == nil {
		lifecycle = &lifecycleState{}
	}
	if cfg.pollInterval == 0 {
		cfg.pollInterval = 2 * time.Second
	}
	rustAddress := net.JoinHostPort("127.0.0.1", strconv.Itoa(cfg.rustPort))
	advertiseAddress := net.JoinHostPort(cfg.podIP, strconv.Itoa(cfg.rustPort))
	for ctx.Err() == nil {
		if !memberlistRunning() {
			return errors.New("memberlist stopped")
		}
		if !reachable(rustAddress) || !hasCoverage(cfg, time.Now()) || !mayOwnPartition(ctx, cfg, partitionClient) {
			ready.Store(false)
			wait(ctx, cfg.pollInterval)
			continue
		}
		// The Rust port opens only after replay and cache warming. A fresh lifecycler
		// is needed after each outage because dskit services cannot restart.
		cycleRegistry := prometheus.NewRegistry()
		instance, err := ring.NewBasicLifecycler(ring.BasicLifecyclerConfig{
			ID: cfg.instanceID, Addr: advertiseAddress, Zone: cfg.zone,
			HeartbeatPeriod: 5 * time.Second, HeartbeatTimeout: time.Minute, NumTokens: 0,
		}, "rust-ingester", cfg.instanceRingKey, instanceClient,
			ring.NewLeaveOnStoppingDelegate(ring.NewInstanceRegisterDelegate(ring.ACTIVE, 0), logger), logger, cycleRegistry)
		if err != nil {
			return err
		}
		if err := startService(ctx, instance); err != nil {
			return err
		}
		partition := ring.NewPartitionInstanceLifecycler(ring.PartitionInstanceLifecyclerConfig{
			PartitionID: int32(cfg.partition), InstanceID: cfg.instanceID,
			WaitOwnersCountOnPending: waitOwnersOnPending(cfg), PollingInterval: cfg.pollInterval,
		}, "rust-ingester-partitions", cfg.partitionRingKey, partitionClient, logger, cycleRegistry)
		partition.SetRemoveOwnerOnShutdown(true)
		if cfg.sharedRing || lifecycle.prepared.Load() {
			partition.SetCreatePartitionOnStartup(false)
		}
		if err := startService(ctx, partition); err != nil {
			_ = services.StopAndAwaitTerminated(context.Background(), instance)
			return err
		}
		_ = level.Info(logger).Log("msg", "Rust ingester registered", "partition", cfg.partition, "address", advertiseAddress)
		lifecycle.partition.Store(partition)
		for ctx.Err() == nil && reachable(rustAddress) && memberlistRunning() && mayOwnPartition(ctx, cfg, partitionClient) {
			state, _, err := partition.GetPartitionState(ctx)
			ready.Store(err == nil && state == ring.PartitionActive)
			wait(ctx, cfg.pollInterval)
		}
		lifecycle.partition.Store(nil)
		ready.Store(false)
		// An outage withdraws both registrations so queriers stop asking this pod. A process
		// shutdown keeps them like the Go ingester, unless prepare-shutdown was requested.
		if ctx.Err() != nil && !lifecycle.prepared.Load() {
			instance.SetKeepInstanceInTheRingOnShutdown(!cfg.unregisterOnShutdown)
			partition.SetRemoveOwnerOnShutdown(false)
		}
		_ = services.StopAndAwaitTerminated(context.Background(), partition)
		_ = services.StopAndAwaitTerminated(context.Background(), instance)
		_ = level.Info(logger).Log("msg", "Rust ingester withdrawn from rings", "partition", cfg.partition)
	}
	return ctx.Err()
}

// A shared partition ring belongs to the Go ingesters: an extra owner must never make a partition
// ACTIVE, and must not keep an INACTIVE partition alive after Go's owners leave.
func waitOwnersOnPending(cfg config) int {
	if cfg.sharedRing {
		return math.MaxInt32
	}
	return 1
}

func mayOwnPartition(ctx context.Context, cfg config, partitionClient kv.Client) bool {
	if !cfg.sharedRing {
		return true
	}
	value, err := partitionClient.Get(ctx, cfg.partitionRingKey)
	if err != nil || value == nil {
		return false
	}
	partition, exists := value.(*ring.PartitionRingDesc).Partitions[int32(cfg.partition)]
	return exists && partition.State != ring.PartitionInactive
}

type lifecycleState struct {
	prepared  atomic.Bool
	partition atomic.Pointer[ring.PartitionInstanceLifecycler]
}

// Like the owned series service's partition ring strategy: each tenant's shuffle shard, and this
// partition's token ranges in it, or nil when the shard skips this partition.
func ownedTokenRanges(desc *ring.PartitionRingDesc, partitionID int32, tenants map[string]int) (map[string][]uint32, error) {
	partitionRing, err := ring.NewPartitionRing(*desc)
	if err != nil {
		return nil, err
	}
	result := make(map[string][]uint32, len(tenants))
	for tenant, shardSize := range tenants {
		subring, err := partitionRing.ShuffleShard(tenant, shardSize)
		if err != nil {
			return nil, fmt.Errorf("shuffle shard for %s: %w", tenant, err)
		}
		ranges, err := subring.GetTokenRangesForPartition(partitionID)
		if errors.Is(err, ring.ErrPartitionDoesNotExist) {
			result[tenant] = nil
			continue
		}
		if err != nil {
			return nil, err
		}
		result[tenant] = ranges
	}
	return result, nil
}

func activePartitions(desc *ring.PartitionRingDesc) int {
	count := 0
	for _, partition := range desc.Partitions {
		if partition.State == ring.PartitionActive {
			count++
		}
	}
	return count
}

// Queriers ask ingesters for data up to -querier.query-ingesters-within old, so joining with a
// shorter history would silently drop samples from results.
func hasCoverage(cfg config, now time.Time) bool {
	if cfg.minCoverage == 0 {
		return true
	}
	contents, err := os.ReadFile(cfg.coverageFile)
	if err != nil {
		return false
	}
	since, err := strconv.ParseInt(strings.TrimSpace(string(contents)), 10, 64)
	if err != nil {
		return false
	}
	return now.Sub(time.UnixMilli(since)) >= cfg.minCoverage
}

func reachable(address string) bool {
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func wait(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func startService(ctx context.Context, service services.Service) error {
	if err := service.StartAsync(context.Background()); err != nil {
		return err
	}
	if err := service.AwaitRunning(ctx); err != nil {
		service.StopAsync()
		_ = service.AwaitTerminated(context.Background())
		return err
	}
	return nil
}
