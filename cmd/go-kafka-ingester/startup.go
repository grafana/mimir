// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/kafka"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/limits"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/segment"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/store"
)

type storeConfig struct {
	activeWindowMs      int64
	retention           store.Retention
	shards              int
	threads             int
	overrides           *limits.Overrides
	costAttribution     [2]int64
	flushSeries         int
	pusherShards        store.PusherShards
	nonOwnedEviction    *store.NonOwnedEviction
	earlyHeadCompaction *store.EarlyHeadCompaction
}

func storeConfigOf(args serveArgs, activeWindowMs int64, retention store.Retention, overrides *limits.Overrides) (storeConfig, error) {
	cleanup, err := limits.ParseDurationMs(args.costAttributionCleanup)
	if err != nil {
		return storeConfig{}, err
	}
	eviction, err := limits.ParseDurationMs(args.costAttributionEviction)
	if err != nil {
		return storeConfig{}, err
	}
	config := storeConfig{
		activeWindowMs:  activeWindowMs,
		retention:       retention,
		shards:          args.storeShards,
		threads:         args.ingestThreads,
		overrides:       overrides,
		costAttribution: [2]int64{cleanup, eviction},
		pusherShards: store.PusherShards{
			Max:            args.ingestionConcurrencyMax,
			BytesPerSample: args.ingestionBytesPerSample,
			TargetFlushes:  args.ingestionTargetFlushes,
		},
	}
	if args.ingestionConcurrencyMax != 0 {
		config.flushSeries = args.ingestionConcurrencyBatch
	}
	if args.earlyCompactionMinSeries > 0 {
		config.earlyHeadCompaction = &store.EarlyHeadCompaction{
			MinInMemorySeries:      args.earlyCompactionMinSeries,
			MinReductionPercentage: args.earlyCompactionMinPercent,
		}
	}
	if args.nonOwnedCompactionEnabled {
		minGrace, err := limits.ParseDurationMs(args.nonOwnedMinGracePeriod)
		if err != nil {
			return storeConfig{}, err
		}
		maxGrace, err := limits.ParseDurationMs(args.nonOwnedMaxGracePeriod)
		if err != nil {
			return storeConfig{}, err
		}
		config.nonOwnedEviction = &store.NonOwnedEviction{MinGraceMs: minGrace, MaxGraceMs: maxGrace, JitterMs: evictionJitterMs(minGrace)}
	}
	return config, nil
}

func (c storeConfig) configure(s *store.Store) *store.Store {
	return s.WithOverrides(c.overrides).
		WithCostAttributionIntervals(c.costAttribution[0], c.costAttribution[1]).
		WithFlushSeries(c.flushSeries).
		WithPusherShards(c.pusherShards).
		WithNonOwnedEviction(c.nonOwnedEviction).
		WithEarlyHeadCompaction(c.earlyHeadCompaction)
}

type resumedLog struct {
	log    *segment.Log
	offset store.SnapshotOffset
}

// startupStore is the store to serve: restored from the head snapshot with each cluster's log
// opened at its checkpoint (logs set), empty to rebuild from the segment logs, or none because
// shutdown arrived during the restore and the snapshot was written back (stopped).
type startupStore struct {
	store   *store.Store
	logs    []resumedLog
	stopped bool
}

func openStore(dataDir, chunkDir string, sources []source, partition int32, config storeConfig, shutdownRequested func() bool) (startupStore, error) {
	started := time.Now()
	rebuild := func() (startupStore, error) {
		s, err := store.NewWithShards(config.activeWindowMs, config.retention, chunkDir, config.shards, config.threads)
		if err != nil {
			return startupStore{}, err
		}
		return startupStore{store: config.configure(s)}, nil
	}
	restored, err := store.Restore(config.activeWindowMs, config.retention, chunkDir, config.threads)
	if err != nil {
		return startupStore{}, err
	}
	if restored == nil {
		return rebuild()
	}
	if len(restored.Offsets) != len(sources) {
		log.Printf("phase=head_snapshot_stale partition=%d reason=kafka_cluster_count", partition)
		_ = restored.Store.Close()
		return rebuild()
	}
	logs := make([]resumedLog, len(sources))
	for cluster, src := range sources {
		offset := restored.Offsets[cluster]
		segmentLog, err := segment.OpenAtCheckpoint(dataDir, cluster, src.topic, partition, segment.Retention{Ms: config.retention.Ms, Set: config.retention.Set}, offset.Offset, offset.HasOffset)
		if err != nil {
			return startupStore{}, err
		}
		if segmentLog == nil {
			log.Printf("phase=head_snapshot_stale partition=%d reason=segment_checkpoint_mismatch", partition)
			_ = restored.Store.Close()
			return rebuild()
		}
		logs[cluster] = resumedLog{log: segmentLog, offset: offset}
	}
	log.Printf("phase=head_snapshot_restored partition=%d duration_ms=%d", partition, time.Since(started).Milliseconds())
	// The snapshot is consumed on read, so a shutdown before ingestion starts must write it back.
	if shutdownRequested() {
		if err := restored.Store.WriteSnapshot(restored.Offsets); err != nil {
			return startupStore{}, err
		}
		log.Printf("phase=head_snapshot_written partition=%d reason=shutdown_during_restore", partition)
		for _, resumed := range logs {
			_ = resumed.log.Close()
		}
		_ = restored.Store.Close()
		return startupStore{stopped: true}, nil
	}
	return startupStore{store: config.configure(restored.Store), logs: logs}, nil
}

type bootstrapPlan struct {
	lookback        bool
	lookbackSeconds int64
	offset          kafka.StartOffset
}

// A pod without a persisted offset (new, or with a lost disk) serves reads for the whole
// retention like the Go ingesters of its partition, so by default it replays that much of Kafka.
func bootstrapStart(start *kafka.StartOffset, lookback, retention optionalInt) bootstrapPlan {
	switch {
	case lookback.set:
		return bootstrapPlan{lookback: true, lookbackSeconds: lookback.value}
	case start != nil:
		return bootstrapPlan{offset: *start}
	case retention.set:
		return bootstrapPlan{lookback: true, lookbackSeconds: retention.value}
	default:
		return bootstrapPlan{offset: kafka.StartOffset{Kind: kafka.StartEarliest}}
	}
}

func initialReplayComplete(start kafka.StartOffset, earliestOffset, latestOffset int64) bool {
	switch start.Kind {
	case kafka.StartLatest:
		return true
	case kafka.StartAt:
		return start.Offset >= latestOffset
	default:
		return earliestOffset >= latestOffset
	}
}

// raiseCoverage records, in Unix milliseconds, the time since which this ingester holds complete
// data. It only ever moves forward so a gap is never hidden by an older value.
func raiseCoverage(path string, sinceMs int64) error {
	if contents, err := os.ReadFile(path); err == nil {
		if current, err := strconv.ParseInt(strings.TrimSpace(string(contents)), 10, 64); err == nil && current >= sinceMs {
			return nil
		}
	}
	temporary := strings.TrimSuffix(path, filepath.Ext(path)) + ".tmp"
	if err := os.WriteFile(temporary, []byte(strconv.FormatInt(sinceMs, 10)), 0o644); err != nil {
		return fmt.Errorf("write coverage %s: %w", temporary, err)
	}
	if err := os.Rename(temporary, path); err != nil {
		return fmt.Errorf("record coverage %s: %w", path, err)
	}
	return nil
}
