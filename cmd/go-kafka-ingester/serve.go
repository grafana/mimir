// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	// Accepts gzip-compressed requests, like the Go ingester.
	_ "google.golang.org/grpc/encoding/gzip"

	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/kafka"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/limits"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/metrics"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/protection"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/segment"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/service"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/store"
	"github.com/grafana/mimir/pkg/ingester/client"
)

func formatInt(value int64) string { return strconv.FormatInt(value, 10) }

func parseInt(value string) (int64, error) { return strconv.ParseInt(value, 10, 64) }

func durationOf(value string) (time.Duration, error) {
	ms, err := limits.ParseDurationMs(value)
	return time.Duration(ms) * time.Millisecond, err
}

type source struct {
	topic, brokers string
}

// signals is closed once, when the process must stop.
type signals struct {
	once   sync.Once
	closed chan struct{}
	set    atomic.Bool
}

func newSignals() *signals { return &signals{closed: make(chan struct{})} }

func (s *signals) fire() {
	s.once.Do(func() {
		s.set.Store(true)
		close(s.closed)
	})
}

func (s *signals) fired() bool { return s.set.Load() }

func serve(args serveArgs) error {
	saslUsername, saslPassword := args.saslUsername, args.saslPassword
	if !args.saslUsernameSet {
		saslUsername = os.Getenv("MIMIR_KAFKA_SASL_USERNAME")
	}
	if !args.passwordSet {
		saslPassword = os.Getenv("MIMIR_KAFKA_SASL_PASSWORD")
	}
	if args.segmentSyncIntervalMs == 0 {
		return errors.New("segment sync interval must be greater than zero")
	}
	var configuredStart *kafka.StartOffset
	switch args.startOffset {
	case "":
	case "earliest":
		configuredStart = &kafka.StartOffset{Kind: kafka.StartEarliest}
	case "latest":
		configuredStart = &kafka.StartOffset{Kind: kafka.StartLatest}
	default:
		offset, err := strconv.ParseInt(args.startOffset, 10, 64)
		if err != nil {
			return fmt.Errorf("parse start offset: %w", err)
		}
		configuredStart = &kafka.StartOffset{Kind: kafka.StartAt, Offset: offset}
	}
	bootstrap := bootstrapStart(configuredStart, args.bootstrapLookbackSeconds, args.retentionSeconds)
	sources := []source{{args.topic, args.brokers}}
	for _, additional := range args.additionalKafkaCluster {
		topic, brokers, ok := strings.Cut(additional, "=")
		if !ok {
			return errors.New("additional Kafka cluster must be TOPIC=BROKER[,BROKER...]")
		}
		sources = append(sources, source{topic, brokers})
	}
	activeWindowMs := args.activeWindowSeconds * 1000
	if args.activeSeriesIdleTimeout != "" {
		ms, err := limits.ParseDurationMs(args.activeSeriesIdleTimeout)
		if err != nil {
			return err
		}
		activeWindowMs = ms
	}
	retention := store.Retention{}
	if args.retentionSeconds.set {
		retention = store.Retention{Ms: args.retentionSeconds.value * 1000, Set: true}
	}
	partition := int32(args.partition)
	chunkDir := filepath.Join(args.dataDir, "chunks_head")
	// Installed before the snapshot restore: an unhandled SIGTERM would kill the process after the
	// one-use snapshot is consumed and force a full segment replay on the next start.
	shutdown := newSignals()
	warmup := newSignals()
	go func() {
		received := make(chan os.Signal, 1)
		signal.Notify(received, syscall.SIGINT, syscall.SIGTERM)
		<-received
		log.Printf("phase=shutdown_signal_received")
		shutdown.fire()
	}()
	ctx := context.Background()
	// Like Mimir, the runtime config must load before the ingester starts.
	defaults, err := args.limits.ToLimits()
	if err != nil {
		return err
	}
	overrides := limits.NewOverridesWithInstanceLimits(defaults, args.instanceLimits)
	runtimeConfig, err := limits.NewRuntimeConfig(args.runtimeConfig)
	if err != nil {
		return err
	}
	if !runtimeConfig.IsEmpty() {
		if _, err := runtimeConfig.Load(ctx, overrides); err != nil {
			return fmt.Errorf("load runtime config: %w", err)
		}
		period, err := durationOf(args.runtimeConfig.ReloadPeriod)
		if err != nil {
			return err
		}
		go runtimeConfig.Run(ctx, overrides, period)
	}
	if args.activePartitionsURL != "" {
		store.WaitActivePartitions(ctx, args.activePartitionsURL, overrides, 30*time.Second)
		go store.PollActivePartitions(ctx, args.activePartitionsURL, overrides, 10*time.Second)
	}
	metrics.ActiveSeriesLoading.Set(1)
	if args.metricsListen != "" {
		if _, err := metrics.Serve(args.metricsListen, args.costAttributionRegistry, args.sidecarMetricsURL); err != nil {
			return err
		}
	}
	readProtection := protection.ReadProtection{}
	if args.protection.CPUUtilizationLimit > 0 || args.protection.MemoryUtilizationLimit > 0 {
		limiter := protection.NewUtilizationLimiter(args.protection.CPUUtilizationLimit, args.protection.MemoryUtilizationLimit)
		readProtection.LimitingReason = limiter.Reason()
		go limiter.Run(ctx, protection.NewProcessScanner())
	}
	var circuitBreaker, pushCircuitBreaker *protection.CircuitBreaker
	if config, err := args.protection.CircuitBreaker(); err != nil {
		return err
	} else if config != nil {
		circuitBreaker = protection.NewCircuitBreaker(*config, protection.ReadRequestType)
	}
	if config, err := args.protection.PushCircuitBreaker(); err != nil {
		return err
	} else if config != nil {
		pushCircuitBreaker = protection.NewCircuitBreaker(*config, protection.PushRequestType)
	}
	readProtection.CircuitBreaker = circuitBreaker
	metadataRetainMs, err := limits.ParseDurationMs(args.metadataRetainPeriod)
	if err != nil {
		return err
	}
	activeSeriesUpdate, err := durationOf(args.activeSeriesUpdatePeriod)
	if err != nil {
		return err
	}
	log.Printf("phase=store_config partition=%d ingest_threads=%d store_shards=%d", partition, args.ingestThreads, args.storeShards)
	config, err := storeConfigOf(args, activeWindowMs, retention, overrides)
	if err != nil {
		return err
	}
	startup, err := openStore(args.dataDir, chunkDir, sources, partition, config, shutdown.fired)
	if err != nil {
		return err
	}
	if startup.stopped {
		return nil
	}
	s := startup.store
	headPeriod, err := durationOf(args.ownedSeriesUpdateInterval)
	if err != nil {
		return err
	}
	compactionInterval, err := durationOf(args.headCompactionInterval)
	if err != nil {
		return err
	}
	var serving atomic.Bool
	spawnAccounting(s, accounting{
		activeSeriesUpdate: activeSeriesUpdate,
		metadataRetainMs:   metadataRetainMs,
		headPeriod:         headPeriod,
		compactionInterval: compactionInterval,
		ownedSeries:        args.trackOwnedSeries || args.useOwnedSeriesForLimits,
		serving:            &serving,
	})
	if args.ownedTokenRangesURL != "" {
		go store.PollOwnedRanges(ctx, args.ownedTokenRangesURL, s, headPeriod)
	}
	consistency := kafka.NewConsistency(partition, int32(args.readCompartment), len(sources), time.Duration(args.consistencyTimeoutSeconds)*time.Second)
	fatal := newSignals()
	if args.profileListen != "" {
		if err := startProfiling(args.profileListen); err != nil {
			return err
		}
	}
	var workers []chan store.SnapshotOffset
	var replayReady []chan struct{}
	for cluster, src := range sources {
		if shutdown.fired() {
			stopWorkers(shutdown, workers)
			return nil
		}
		log.Printf("phase=disk_recovery_start cluster=%d topic=%s partition=%d data_dir=%s", cluster, src.topic, partition, args.dataDir)
		recoveredTimestamp := int64(0)
		var segmentLog *segment.Log
		if startup.logs != nil {
			resumed := startup.logs[cluster]
			segmentLog, recoveredTimestamp = resumed.log, resumed.offset.TimestampMs
		} else {
			segmentLog, recoveredTimestamp, err = replaySegments(s, args, cluster, src.topic, partition, retention, shutdown)
			if err != nil {
				if shutdown.fired() {
					stopWorkers(shutdown, workers)
					return nil
				}
				return err
			}
		}
		persisted, hasPersisted := segmentLog.LastOffset()
		log.Printf("phase=disk_recovery_complete cluster=%d partition=%d persisted_offset=%s", cluster, partition, optionalOffset(persisted, hasPersisted))
		kafkaConfig := kafka.Config{
			Brokers: src.brokers, Topic: src.topic, Partition: partition, TLS: args.kafkaTLS,
			SASLUsername: saslUsername, SASLPassword: saslPassword, SASLMechanism: args.saslMechanism,
		}
		partitionClient, err := kafka.Connect(kafkaConfig)
		if err != nil {
			return err
		}
		latestOffset, err := partitionClient.GetOffset(ctx, kafka.Latest)
		if err != nil {
			return fmt.Errorf("fetch latest Kafka offset: %w", err)
		}
		log.Printf("phase=kafka_earliest_lookup_start cluster=%d topic=%s partition=%d latest_offset=%d", cluster, src.topic, partition, latestOffset)
		earliestCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		earliestOffset, err := partitionClient.GetOffset(earliestCtx, kafka.Earliest)
		cancel()
		if err != nil {
			return fmt.Errorf("fetch earliest Kafka offset: %w", err)
		}
		sourceStart := kafka.StartOffset{Kind: kafka.StartAt}
		switch {
		case hasPersisted:
			sourceStart.Offset = persisted + 1
		case bootstrap.lookback:
			since := nowMs() - bootstrap.lookbackSeconds*1000
			offset, found, err := partitionClient.OffsetForTime(ctx, since)
			if err != nil {
				return err
			}
			if !found {
				offset = latestOffset
			}
			sourceStart.Offset = offset
		default:
			sourceStart = bootstrap.offset
		}
		// A fresh start or a gap past Kafka retention restarts the complete-data window at the
		// first consumed record; an older window without a record is replaced conservatively by
		// now.
		coveragePath := filepath.Join(args.dataDir, "coverage")
		coveragePending := !hasPersisted || persisted+1 < earliestOffset
		if !coveragePending {
			if _, err := os.Stat(coveragePath); errors.Is(err, os.ErrNotExist) {
				if err := raiseCoverage(coveragePath, nowMs()); err != nil {
					return err
				}
			}
		}
		consistency.HighWatermark(cluster, latestOffset)
		replayTarget := latestOffset - 1
		replayComplete := initialReplayComplete(sourceStart, earliestOffset, latestOffset)
		log.Printf("phase=kafka_replay_start cluster=%d topic=%s partition=%d start_offset=%s earliest_offset=%d latest_offset=%d replay_target=%d already_caught_up=%t",
			cluster, src.topic, partition, startOffsetLabel(sourceStart), earliestOffset, latestOffset, replayTarget, replayComplete)
		if hasPersisted {
			consistency.Consumed(cluster, persisted, persisted+1, recoveredTimestamp)
		}
		consistency.SetClient(cluster, partitionClient)
		initialOffset := sourceStart.Offset
		switch sourceStart.Kind {
		case kafka.StartEarliest:
			initialOffset = earliestOffset
		case kafka.StartLatest:
			initialOffset = latestOffset
		}
		if err := partitionClient.Assign(initialOffset); err != nil {
			return err
		}
		ready := make(chan struct{})
		replayReady = append(replayReady, ready)
		done := make(chan store.SnapshotOffset, 1)
		workers = append(workers, done)
		consumer := &consumer{
			cluster:        cluster,
			partition:      partition,
			config:         kafkaConfig,
			client:         partitionClient,
			consistency:    consistency,
			initialOffset:  initialOffset,
			replayTarget:   replayTarget,
			replayComplete: replayComplete,
			latestOffset:   latestOffset,
			syncInterval:   time.Duration(args.segmentSyncIntervalMs) * time.Millisecond,
			shutdown:       shutdown,
			warmup:         warmup,
			ready:          ready,
			applier: &applier{
				store:              s,
				consistency:        consistency,
				segmentLog:         segmentLog,
				cluster:            cluster,
				fatal:              fatal,
				coveragePath:       coveragePath,
				coveragePending:    coveragePending,
				lastTimestampMs:    recoveredTimestamp,
				pushCircuitBreaker: pushCircuitBreaker,
				replayTarget:       replayTarget,
			},
		}
		go func() { done <- consumer.run() }()
	}

	for _, ready := range replayReady {
		select {
		case <-ready:
		case <-shutdown.closed:
			return shutdownWithSnapshot(s, shutdown, workers, fatal, partition)
		}
	}
	if err := s.PruneExpired(); err != nil {
		return err
	}
	if shutdown.fired() {
		return shutdownWithSnapshot(s, shutdown, workers, fatal, partition)
	}
	warmup.fire()

	options := []grpc.ServerOption{
		service.ServerCodec(),
		grpc.MaxConcurrentStreams(uint32(args.protection.GRPCMaxConcurrentStreams)),
		grpc.ChainUnaryInterceptor(readProtection.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(readProtection.StreamServerInterceptor()),
	}
	switch {
	case args.grpcTLSCert != "" && args.grpcTLSKey != "":
		certificate, err := tls.LoadX509KeyPair(args.grpcTLSCert, args.grpcTLSKey)
		if err != nil {
			return fmt.Errorf("read gRPC TLS certificate and key: %w", err)
		}
		options = append(options, grpc.Creds(credentials.NewServerTLSFromCert(&certificate)))
	case args.grpcTLSCert != "" || args.grpcTLSKey != "":
		return errors.New("both gRPC TLS certificate and key are required")
	}
	listener, err := net.Listen("tcp", args.listen)
	if err != nil {
		return fmt.Errorf("parse listen address: %w", err)
	}
	server := grpc.NewServer(options...)
	client.RegisterIngesterServer(server, service.WithConsistency(s, consistency))
	log.Printf("phase=grpc_start partition=%d address=%s", partition, listener.Addr())
	serving.Store(true)
	for _, breaker := range []*protection.CircuitBreaker{circuitBreaker, pushCircuitBreaker} {
		if breaker != nil {
			breaker.Activate()
		}
	}
	metrics.ActiveSeriesLoading.Set(0)
	gracefulShutdownTimeout, err := durationOf(args.gracefulShutdownTimeout)
	if err != nil {
		return err
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	var serveErr error
	select {
	case serveErr = <-served:
	case <-shutdown.closed:
	case <-fatal.closed:
	}
	shutdown.fire()
	boundedGracefulStop(server, gracefulShutdownTimeout)
	log.Printf("phase=grpc_stopped partition=%d", partition)
	if err := shutdownWithSnapshot(s, shutdown, workers, fatal, partition); err != nil {
		return err
	}
	if serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
		return serveErr
	}
	if fatal.fired() {
		return errors.New("Kafka ingester stopped after a fatal consumer error")
	}
	log.Printf("phase=shutdown_complete partition=%d", partition)
	return nil
}

// boundedGracefulStop stops the server once its requests finished, or after timeout, dropping the
// requests still in flight.
func boundedGracefulStop(server interface {
	GracefulStop()
	Stop()
}, timeout time.Duration) {
	stopped := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(timeout):
		log.Printf("phase=grpc_shutdown_timeout timeout_ms=%d", timeout.Milliseconds())
		server.Stop()
	}
}

func optionalOffset(offset int64, ok bool) string {
	if !ok {
		return "None"
	}
	return fmt.Sprintf("Some(%d)", offset)
}

func startOffsetLabel(start kafka.StartOffset) string {
	switch start.Kind {
	case kafka.StartEarliest:
		return "earliest"
	case kafka.StartLatest:
		return "latest"
	default:
		return strconv.FormatInt(start.Offset, 10)
	}
}

// replaySegments rebuilds the store from a Kafka cluster's segment log.
func replaySegments(s *store.Store, args serveArgs, cluster int, topic string, partition int32, retention store.Retention, shutdown *signals) (*segment.Log, int64, error) {
	recoveredTimestamp, recoveredCount := int64(0), 0
	lastLog := time.Now()
	batch := make([]store.IngestRecord, 0, applyBatch)
	segmentLog, err := segment.OpenReplaying(args.dataDir, cluster, topic, partition, segment.Retention{Ms: retention.Ms, Set: retention.Set}, args.ingestThreads,
		func(recovered segment.RecoveredRecord) error {
			if shutdown.fired() {
				return errors.New("shutdown requested during segment recovery")
			}
			recoveredCount++
			if time.Since(lastLog) >= 30*time.Second {
				log.Printf("phase=disk_recovery_progress cluster=%d partition=%d records=%d offset=%d", cluster, partition, recoveredCount, recovered.Offset)
				lastLog = time.Now()
			}
			recoveredTimestamp = max(recoveredTimestamp, recovered.KafkaTimestampMs)
			if hasData(&recovered.Request) {
				batch = append(batch, store.IngestRecord{Tenant: recovered.Tenant, Request: recovered.Request, IngestedMs: recovered.IngestedMs})
				if len(batch) >= applyBatch {
					if err := s.IngestBatch(batch); err != nil {
						return fmt.Errorf("restore Kafka cluster %d through offset %d from disk: %w", cluster, recovered.Offset, err)
					}
					batch = make([]store.IngestRecord, 0, applyBatch)
				}
			}
			return nil
		})
	if err != nil {
		return nil, 0, err
	}
	if err := s.IngestBatch(batch); err != nil {
		return nil, 0, fmt.Errorf("restore Kafka cluster %d from disk: %w", cluster, err)
	}
	log.Printf("phase=disk_recovery_records cluster=%d partition=%d records=%d", cluster, partition, recoveredCount)
	return segmentLog, recoveredTimestamp, nil
}

// stopWorkers stops the consumers and returns each one's snapshot offset.
func stopWorkers(shutdown *signals, workers []chan store.SnapshotOffset) []store.SnapshotOffset {
	shutdown.fire()
	offsets := make([]store.SnapshotOffset, 0, len(workers))
	for _, worker := range workers {
		offsets = append(offsets, <-worker)
	}
	return offsets
}

// Called only once every Kafka cluster finished recovery, so the store covers each log's last
// offset.
func shutdownWithSnapshot(s *store.Store, shutdown *signals, workers []chan store.SnapshotOffset, fatal *signals, partition int32) error {
	offsets := stopWorkers(shutdown, workers)
	log.Printf("phase=consumers_stopped partition=%d", partition)
	if fatal.fired() {
		log.Printf("phase=head_snapshot_skipped partition=%d reason=fatal_error", partition)
		return nil
	}
	started := time.Now()
	if err := s.WriteSnapshot(offsets); err != nil {
		return err
	}
	log.Printf("phase=head_snapshot_written partition=%d duration_ms=%d", partition, time.Since(started).Milliseconds())
	return nil
}

// eviction jitter: like Mimir's per-process jitter on the non-owned series grace period, spread
// over twice the min grace period so replicas evict at different times.
func evictionJitterMs(minGraceMs int64) int64 {
	variance := 2 * minGraceMs
	if variance <= 0 {
		return 0
	}
	seed := int64(uint32(time.Now().Nanosecond()) ^ uint32(os.Getpid()))
	return seed % variance
}

func nowMs() int64 { return time.Now().UnixMilli() }
