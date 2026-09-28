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

	"github.com/grafana/mimir/pkg/distributor"
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
	mux := http.NewServeMux()
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
	httpServer := &http.Server{Addr: cfg.listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	listener, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return err
	}
	go func() { _ = httpServer.Serve(listener) }()
	defer httpServer.Shutdown(context.Background())

	return manage(ctx, cfg, instanceClient, partitionClient, func() bool { return memberlistService.State() == services.Running }, &ready, logger)
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

func manage(ctx context.Context, cfg config, instanceClient, partitionClient kv.Client, memberlistRunning func() bool, ready *atomic.Bool, logger log.Logger) error {
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
		if cfg.sharedRing {
			partition.SetCreatePartitionOnStartup(false)
		}
		if err := startService(ctx, partition); err != nil {
			_ = services.StopAndAwaitTerminated(context.Background(), instance)
			return err
		}
		_ = level.Info(logger).Log("msg", "Rust ingester registered", "partition", cfg.partition, "address", advertiseAddress)
		for ctx.Err() == nil && reachable(rustAddress) && memberlistRunning() && mayOwnPartition(ctx, cfg, partitionClient) {
			state, _, err := partition.GetPartitionState(ctx)
			ready.Store(err == nil && state == ring.PartitionActive)
			wait(ctx, cfg.pollInterval)
		}
		ready.Store(false)
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
