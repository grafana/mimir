---
title: "0004: Distributed partition series limit using HyperLogLog"
description: "Distributed partition series limit using HyperLogLog"
draft: true
---

# 0004: Distributed partition series limit using HyperLogLog

**Author:** TBD

**Date:** 10/2025

**Sponsor(s):** TBD

**Type:** Feature

**Status:** Implemented (Phase 1 & Phase 2)

**Related issues/PRs:** TBD

---

## Background

Mimir's ingest storage feature decouples the write and read paths by introducing Kafka as an intermediary between distributors and ingesters. In this architecture:

- **Traditional path**: Distributor → Ingester (direct gRPC) → TSDB HEAD
- **Ingest storage path**: Distributor → Kafka → Ingester → TSDB HEAD

Series are deterministically routed to Kafka partitions based on their hash (computed from tenant ID and labels). Each partition is owned by one or more ingesters that consume from it and write to their TSDB. This provides better resilience, scalability, and decoupling between components.

### Partition Architecture

The partition ring manages Kafka partition ownership:
- Tenants can be shuffle-sharded across N partitions (configured via `-ingest-storage.ingestion-partition-tenant-shard-size`)
- Each partition is consumed by one or more ingesters (replication)
- Series routing is deterministic based on `ShardByAllLabelAdapters(userID, labels)`

### Existing Limits

Mimir enforces several types of limits:

1. **Per-instance limits** (distributor-level):
   - Max inflight push requests
   - Max ingestion rate per distributor instance

2. **Per-tenant limits** (cluster-level):
   - Max global series per user
   - Max global series per metric
   - Ingestion rate and request rate limits

3. **Ingester limits** (per-ingester instance):
   - Max series per user (enforced by each ingester)
   - Max series per metric (enforced by each ingester)

## Problem Statement

When ingest storage is enabled, there is no mechanism to limit the number of series sent to a single Kafka partition within a time window. This creates several risks:

1. **Ingester overload**: A partition receiving too many series can overwhelm the ingesters consuming from it, causing:
   - High memory usage (series in TSDB HEAD)
   - High CPU usage (processing and compaction)
   - Query performance degradation
   - Potential out-of-memory crashes

2. **Unbalanced load**: Even with shuffle-sharding, tenant distribution and cardinality patterns can create hot partitions that receive disproportionate load.

3. **Missing pre-Kafka protection**: Existing ingester instance limits (`max_series_per_user`) only apply after data is consumed from Kafka. By then, data is already in Kafka, and rejecting it wastes resources. We need a pre-Kafka limit similar to ingester instance limits.

4. **Challenge of distributed counting**: With hundreds of distributors independently receiving and routing series, we need an efficient way to track per-partition series cardinality across the cluster without:
   - Centralizing all writes (bottleneck)
   - Requiring expensive coordination (performance impact)
   - Using excessive memory or network bandwidth (scalability)

## Goals

- Provide a per-partition series limit that prevents individual partitions from being overwhelmed
- Implement distributed series cardinality tracking across all distributors
- Minimize performance impact on the hot ingestion path (avoid bottlenecks)
- Use minimal resources (memory, CPU, network) for cardinality tracking
- Support configurable time windows for cardinality measurement (default: 20 minutes)
- Ensure thread-safety across concurrent ingestion requests
- Accept eventual consistency in cardinality estimates (no strong consistency required)
- Provide configurable per-tenant limits via runtime configuration

## Non-Goals

- Exact series counting (approximate counting via HyperLogLog is acceptable)
- Strong consistency guarantees (eventual consistency is sufficient)
- Preventing all possible partition overload scenarios (this is one layer of defense)
- Replacing existing ingester instance limits (this complements them)
- Balancing load across partitions (shuffle-sharding handles this)

## Solution Overview

We will implement a distributed cardinality tracking system using the **HyperLogLog (HLL)** probabilistic data structure with **memberlist gossip** for state synchronization.

## Implementation Status

Both Phase 1 (local tracking) and Phase 2 (distributed synchronization) have been completed.

### Phase 1: Local HLL Tracking (✅ Completed)

The core HLL tracking functionality has been implemented:

- Created `pkg/distributor/hlltracker/` package
- Forked HyperLogLog implementation to `pkg/distributor/hlltracker/hyperloglog/`
- Implemented `Tracker` service with:
  - Per-partition HLL structures (`partitionTracker`)
  - Sliding time window with current + historical HLLs
  - Minute rotation logic with automatic cleanup
  - Thread-safe state management
- Implemented distributor middleware (`preKafkaPartitionLimitMiddleware`):
  - Positioned as the last middleware before Kafka write
  - Calculates partition assignments using partition ring
  - Updates local HLL copies with series hashes
  - Checks limits and rejects requests if exceeded
  - Only commits updates if all partitions are within limits
- Added configuration flags:
  - `-distributor.partition-series-tracker.enabled`
  - `-distributor.partition-series-tracker.time-window-minutes` (default: 20)
  - `-distributor.partition-series-tracker.hll-precision` (default: 11)
  - `-distributor.partition-series-tracker.update-interval-seconds` (default: 1)
- Added runtime limit configuration:
  - `max_series_per_partition` in per-tenant limits
  - Global limit when tracker is enabled
- Comprehensive unit tests:
  - HLL operations and accuracy tests
  - Tracker state management tests
  - Middleware behavior tests
  - Concurrent update tests
  - Time window cleanup tests

### Phase 2: Distributed Synchronization (✅ Completed)

Memberlist gossip integration has been implemented:

- Implemented KV operations in `Tracker`:
  - Binary codec for HLL state (`kv_codec.go`)
  - CAS updates with automatic merge conflict resolution
  - WatchPrefix pattern for receiving remote updates
  - State loading on startup
  - Automatic key cleanup for expired minutes
- Implemented KV state structure (`PartitionHLLState`):
  - Partition ID
  - Unix minute timestamp
  - HLL registers (binary serialized)
  - Updated timestamp for ordering
- Added background workers:
  - KV push ticker (1 second): pushes dirty HLL state via CAS
  - KV watch goroutine: merges remote updates into local state
  - Rotation ticker (1 second): handles minute boundaries
- Implemented merge logic:
  - Current minute updates merge into `currentHLL`
  - Historical minute updates merge into `historicalHLLs[minute]`
  - Automatic rebuild of merged historical cache
  - Time window validation (ignores stale data)
- Wired up KV client in distributor:
  - Uses same KV store config as distributor ring
  - Creates separate KV client with partition HLL codec
  - Automatically enables Phase 2 when distributor ring is enabled
  - Falls back to Phase 1 (local-only) when ring is disabled
- Added Phase 2 tests:
  - Mock KV client for testing CAS and watch operations
  - KV push tests
  - Remote state merge tests
  - Historical merge tests
  - Stale data rejection tests
- Key format: `partition-series/<partition_id>/<unix_minute>`

### Current Behavior

- **With distributor ring enabled**: Phase 2 mode
  - HLL state is synchronized across all distributors via memberlist
  - Provides cluster-wide per-partition cardinality tracking
  - Eventual consistency across distributors (1-2 second lag)

- **Without distributor ring**: Phase 1 mode
  - Each distributor tracks only its own series locally
  - Still provides protection against hot partitions
  - Useful for single-distributor deployments or testing

### Key Components

1. **HyperLogLog Algorithm**: Provides space-efficient cardinality estimation
   - Memory footprint: ~2 KB per partition per minute (2048 registers, precision 11)
   - Error rate: ~5-6% (standard deviation with precision 11)
   - Mergeable: Multiple HLL instances can be combined efficiently
   - Fast operations: O(1) add, O(m) merge where m = number of registers
   - **Note on precision**: The 5-6% error rate is acceptable for this use case. An ingester will not run out of memory if it ingests 3.3M series instead of 3M series. The variation in resource usage due to the nature of series (label cardinality, sample rate) is far greater than the difference caused by a 5% cardinality estimation error. This is not an exact science, and we prioritize low memory overhead over perfect accuracy.

2. **Sliding Time Window**: Track series over a configurable window (default: 20 minutes)
   - Store N HLL structures per partition (one per minute)
   - "Current" HLL: Tracks the current minute's series
   - "Historical" HLLs: Track the previous N-1 minutes
   - Merged HLL: Cached combination of all historical (non-current) HLLs

3. **Memberlist Gossip**: Synchronize HLL state across distributors
   - Use existing `kv.Client` infrastructure (same as HA-Tracker)
   - Keys: `partition-series/<unix-minute>/<partition-id>`
   - Values: HLL registers (byte slice)
   - CAS operations for conflict-free updates
   - Watch pattern for receiving updates from other distributors

4. **Distributor Middleware**: Enforce limits before writing to Kafka
   - Intercept push requests before Kafka write
   - Update local HLL copies with series from the request
   - Check if any partition would exceed its limit
   - Reject entire request if limit would be exceeded
   - Commit updates to tracker only if request is accepted

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         Distributor Instance 1                   │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ Ingestion Middleware Chain                                 │ │
│  │  → Limits → Metrics → HA-Dedupe → ... → Validation        │ │
│  │  → [NEW] Pre-Kafka Partition Limit ← hlltracker.Tracker   │ │
│  │  → Push to Kafka                                           │ │
│  └────────────────────────────────────────────────────────────┘ │
│                            ↕ (CAS updates, Watch)                │
└────────────────────────────────────────────────────────────────┘
                              ↕
                    ┌──────────────────┐
                    │  Memberlist KV   │
                    │  partition-series│
                    │    /<minute>/    │
                    │   /<partition>   │
                    └──────────────────┘
                              ↕
┌─────────────────────────────────────────────────────────────────┐
│                         Distributor Instance 2                   │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ hlltracker.Tracker                                         │ │
│  │  Per-partition HLL structures (current + historical)       │ │
│  │  Watches KV for updates from other distributors            │ │
│  │  Periodically pushes local updates via CAS                 │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### How It Works

**Initialization (distributor startup):**
1. Start `hlltracker.Tracker` as a service
2. Load existing HLL state from memberlist KV
3. Watch KV prefix `partition-series/` for updates
4. Start background ticker (1 second) to:
   - Rotate "current" HLL when minute changes
   - Update cached merged historical HLL
   - Clean up old HLL structures (outside time window)
5. Start background ticker (1 second) to push local updates via CAS

**Ingestion path (per push request):**
1. Request arrives at distributor
2. Passes through middleware chain (including HA-dedupe, relabel, validation)
3. Pre-Kafka partition limit middleware:
   - Get read-only copies of historical merged HLL (cached)
   - Get writable copies of current HLL for affected partitions
   - Calculate partition assignments for series in request (metadata is not counted)
   - Add series hashes to local copies of current HLL
   - For each affected partition:
     - Merge historical HLL + updated current HLL
     - Call `Count()` to get estimated cardinality
     - If count > limit: reject request, discard HLL updates
     - If count ≤ limit: continue
   - If all partitions are within limit:
     - Call `tracker.UpdateCurrent(partitionUpdates)` to commit
     - Continue to next middleware (write to Kafka)

**Background synchronization (continuous):**
1. Every 1 second, tracker performs CAS update to KV
2. Only pushes HLL registers if they changed since last push
3. CAS callback merges with existing value in KV (conflict resolution)
4. Other distributors receive updates via Watch callbacks
5. Upon receiving update: merge remote HLL into local HLL

**Minute rotation (every minute):**
1. Ticker detects minute boundary crossed (from minute M to minute M+1)
2. "Current" HLL is moved to "historical" HLLs with key M (the minute that just ended)
3. New empty HLL created as new "current" for minute M+1
4. Rebuild cached merged historical HLL (merge of all historical HLLs)
5. Delete HLL structures older than time window (minute < M+1 - window_minutes)
6. Delete corresponding KV keys (cleanup for old minutes)

## Implementation Details

### Package Structure

```
pkg/distributor/hlltracker/
├── hlltracker.go          # Main Tracker implementation
├── hlltracker_test.go     # Unit tests
├── hyperloglog/           # Forked HLL implementation (if needed)
│   ├── hyperloglog.go     # HLL data structure and operations
│   └── hyperloglog_test.go
└── config.go              # Configuration structures
```

### Core Data Structures

```go
package hlltracker

import (
    "context"
    "sync"
    "time"

    "github.com/grafana/dskit/kv"
    "github.com/grafana/dskit/services"
)

// Config holds configuration for the HLL-based partition series tracker.
type Config struct {
    Enabled bool `yaml:"enabled"`

    // KVStore is the backend storage for the partition series tracker.
    // Memberlist is the recommended backend.
    KVStore kv.Config `yaml:"kvstore"`

    // TimeWindowMinutes is the number of minutes to track series cardinality.
    // Default: 20
    TimeWindowMinutes int `yaml:"time_window_minutes"`

    // UpdateIntervalSeconds is how often to push local HLL state to KV store.
    // Default: 1
    UpdateIntervalSeconds int `yaml:"update_interval_seconds"`

    // HLLPrecision controls the HLL precision parameter (log2(m) where m is
    // the number of registers). Higher precision = lower error but more memory.
    // Valid range: 4-18. Default: 11 (2048 registers, ~2KB per HLL, ~5-6% error)
    HLLPrecision int `yaml:"hll_precision"`
}

// Tracker tracks per-partition series cardinality using HyperLogLog.
type Tracker struct {
    services.Service

    cfg    Config
    kv     kv.Client
    logger log.Logger

    // partitionsMu protects the partitions map
    partitionsMu sync.RWMutex
    partitions   map[int32]*partitionTracker

    // Current unix minute (minutes since epoch)
    currentMinuteMu sync.RWMutex
    currentMinute   int64

    // Tickers
    rotateTicker *time.Ticker
    updateTicker *time.Ticker

    // Metrics
    partitionSeriesEstimate *prometheus.GaugeVec
    kvUpdates              prometheus.Counter
    kvUpdateErrors         prometheus.Counter
    rejectedRequests       *prometheus.CounterVec
}

// partitionTracker tracks HLL state for a single partition.
type partitionTracker struct {
    partitionID int32

    // mu protects all fields below
    mu sync.RWMutex

    // currentHLL tracks series in the current minute
    // Accessed with RLock for reading copies, Lock for updates
    currentHLL *hyperloglog.HyperLogLog

    // historicalHLLs maps unix-minute → HLL for past minutes
    // Key is unix timestamp / 60
    historicalHLLs map[int64]*hyperloglog.HyperLogLog

    // mergedHistoricalHLL is a cached merge of all historical HLLs
    // Rebuilt when currentHLL is rotated or when KV updates arrive
    mergedHistoricalHLL *hyperloglog.HyperLogLog

    // dirty tracks if currentHLL has been modified since last KV push
    dirty bool
}

// PartitionUpdate represents an update to a partition's current HLL.
type PartitionUpdate struct {
    PartitionID int32
    UpdatedHLL  *hyperloglog.HyperLogLog
}

// CurrentState represents a snapshot of current HLL state for middleware.
type CurrentState struct {
    // MergedHistorical is a read-only copy of merged historical HLLs
    MergedHistorical *hyperloglog.HyperLogLog

    // CurrentCopy is a writable copy of the current HLL
    CurrentCopy *hyperloglog.HyperLogLog
}
```

### Tracker Implementation

#### Constructor

```go
func New(
    cfg Config,
    logger log.Logger,
    reg prometheus.Registerer,
) (*Tracker, error) {
    // Validate config
    if cfg.TimeWindowMinutes <= 0 {
        cfg.TimeWindowMinutes = 20
    }
    if cfg.UpdateIntervalSeconds <= 0 {
        cfg.UpdateIntervalSeconds = 1
    }
    if cfg.HLLPrecision < 4 || cfg.HLLPrecision > 18 {
        cfg.HLLPrecision = 11  // Default to 2048 registers
    }

    t := &Tracker{
        cfg:        cfg,
        logger:     logger,
        partitions: make(map[int32]*partitionTracker),
        // ... initialize metrics
    }

    // Create KV client (similar to HA-Tracker pattern)
    kvClient, err := kv.NewClient(
        cfg.KVStore,
        GetPartitionSeriesCodec(),
        kv.RegistererWithKVName(reg, "partition-series-tracker"),
        logger,
    )
    if err != nil {
        return nil, err
    }
    t.kv = kvClient

    t.Service = services.NewBasicService(t.starting, t.running, t.stopping)
    return t, nil
}
```

#### Lifecycle Methods

```go
func (t *Tracker) starting(ctx context.Context) error {
    // Initialize current minute
    t.currentMinute = time.Now().Unix() / 60

    // Load existing state from KV
    if err := t.loadStateFromKV(ctx); err != nil {
        return err
    }

    // Start watching KV for updates
    go t.watchKV(ctx)

    return nil
}

func (t *Tracker) running(ctx context.Context) error {
    t.rotateTicker = time.NewTicker(1 * time.Second)
    t.updateTicker = time.NewTicker(
        time.Duration(t.cfg.UpdateIntervalSeconds) * time.Second,
    )
    defer t.rotateTicker.Stop()
    defer t.updateTicker.Stop()

    for {
        select {
        case <-ctx.Done():
            return nil
        case <-t.rotateTicker.C:
            t.checkAndRotateMinute()
        case <-t.updateTicker.C:
            t.pushUpdatesToKV(ctx)
        }
    }
}

func (t *Tracker) stopping(_ error) error {
    // Flush any pending updates
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    t.pushUpdatesToKV(ctx)
    return nil
}
```

#### Key Operations

```go
// GetCurrentState returns thread-safe copies of HLL state for a partition.
// This is called by the middleware for each request.
func (t *Tracker) GetCurrentState(partitionID int32) CurrentState {
    pt := t.getOrCreatePartition(partitionID)

    pt.mu.RLock()
    defer pt.mu.RUnlock()

    return CurrentState{
        MergedHistorical: pt.mergedHistoricalHLL.Clone(),
        CurrentCopy:      pt.currentHLL.Clone(),
    }
}

// UpdateCurrent atomically merges updates into the current HLL.
// Called by middleware after successfully validating a request.
func (t *Tracker) UpdateCurrent(updates []PartitionUpdate) {
    for _, update := range updates {
        pt := t.getOrCreatePartition(update.PartitionID)

        pt.mu.Lock()
        pt.currentHLL.Merge(update.UpdatedHLL)
        pt.dirty = true
        pt.mu.Unlock()
    }
}

// checkAndRotateMinute checks if we've crossed a minute boundary.
func (t *Tracker) checkAndRotateMinute() {
    nowMinute := time.Now().Unix() / 60

    t.currentMinuteMu.RLock()
    current := t.currentMinute
    t.currentMinuteMu.RUnlock()

    if nowMinute == current {
        return // No rotation needed
    }

    // Minute changed - rotate all partitions
    t.currentMinuteMu.Lock()
    t.currentMinute = nowMinute
    t.currentMinuteMu.Unlock()

    t.partitionsMu.RLock()
    partitions := make([]*partitionTracker, 0, len(t.partitions))
    for _, pt := range t.partitions {
        partitions = append(partitions, pt)
    }
    t.partitionsMu.RUnlock()

    for _, pt := range partitions {
        pt.rotateMinute(current, nowMinute, t.cfg.TimeWindowMinutes)
    }
}

// rotateMinute moves current HLL to historical and creates new current.
func (pt *partitionTracker) rotateMinute(
    oldMinute, newMinute int64,
    windowMinutes int,
) {
    pt.mu.Lock()
    defer pt.mu.Unlock()

    // Move current to historical
    pt.historicalHLLs[oldMinute] = pt.currentHLL
    pt.currentHLL = hyperloglog.New(/* precision */)
    pt.dirty = false

    // Rebuild merged historical HLL
    pt.rebuildMergedHistorical()

    // Clean up old entries
    cutoff := newMinute - int64(windowMinutes)
    for minute := range pt.historicalHLLs {
        if minute <= cutoff {
            delete(pt.historicalHLLs, minute)
        }
    }
}

// rebuildMergedHistorical merges all historical HLLs.
// Must be called with pt.mu held.
func (pt *partitionTracker) rebuildMergedHistorical() {
    merged := hyperloglog.New(/* precision */)
    for _, hll := range pt.historicalHLLs {
        merged.Merge(hll)
    }
    pt.mergedHistoricalHLL = merged
}
```

#### Memberlist Integration

```go
// kvKey returns the memberlist key for a partition and minute.
func kvKey(minute int64, partitionID int32) string {
    return fmt.Sprintf("partition-series/%d/%d", minute, partitionID)
}

// kvValue is the value stored in memberlist KV (just the HLL registers).
// We use protobuf for encoding (consistent with Mimir conventions).
type kvValue struct {
    Registers []byte `protobuf:"bytes,1,opt,name=registers,proto3" json:"registers,omitempty"`
}

// GetPartitionSeriesCodec returns the codec for encoding/decoding kvValue.
func GetPartitionSeriesCodec() codec.Codec {
    return codec.NewProtoCodec("partitionSeries", func() proto.Message {
        return &kvValue{}
    })
}

// pushUpdatesToKV performs CAS updates for all dirty partitions.
func (t *Tracker) pushUpdatesToKV(ctx context.Context) {
    t.currentMinuteMu.RLock()
    currentMinute := t.currentMinute
    t.currentMinuteMu.RUnlock()

    t.partitionsMu.RLock()
    partitions := make([]*partitionTracker, 0, len(t.partitions))
    for _, pt := range t.partitions {
        partitions = append(partitions, pt)
    }
    t.partitionsMu.RUnlock()

    for _, pt := range partitions {
        pt.mu.RLock()
        dirty := pt.dirty
        partitionID := pt.partitionID

        if !dirty {
            pt.mu.RUnlock()
            continue
        }

        // Clone registers for CAS operation (don't hold lock during CAS)
        registers := make([]byte, len(pt.currentHLL.Registers))
        copy(registers, pt.currentHLL.Registers)
        pt.mu.RUnlock()

        key := kvKey(currentMinute, partitionID)

        err := t.kv.CAS(ctx, key, func(in interface{}) (
            out interface{},
            retry bool,
            err error,
        ) {
            var existingRegisters []byte
            if in != nil {
                if v, ok := in.(*kvValue); ok && v != nil {
                    existingRegisters = v.Registers
                }
            }

            // Merge our registers with existing (conflict resolution)
            merged := mergeRegisters(existingRegisters, registers)

            return &kvValue{Registers: merged}, true, nil
        })

        if err != nil {
            t.kvUpdateErrors.Inc()
            level.Warn(t.logger).Log(
                "msg", "failed to push HLL update to KV",
                "partition", partitionID,
                "err", err,
            )
        } else {
            t.kvUpdates.Inc()

            // Mark as not dirty after successful push
            pt.mu.Lock()
            pt.dirty = false
            pt.mu.Unlock()
        }
    }
}

// mergeRegisters merges two HLL register arrays (max of each position).
func mergeRegisters(a, b []byte) []byte {
    if len(a) == 0 {
        return b
    }
    if len(b) == 0 {
        return a
    }

    merged := make([]byte, len(a))
    for i := range merged {
        if a[i] > b[i] {
            merged[i] = a[i]
        } else {
            merged[i] = b[i]
        }
    }
    return merged
}

// watchKV watches for updates from other distributors.
func (t *Tracker) watchKV(ctx context.Context) {
    t.kv.WatchPrefix(ctx, "partition-series/", func(key string, value interface{}) bool {
        // Parse key: partition-series/<minute>/<partition>
        var minute int64
        var partitionID int32
        _, err := fmt.Sscanf(key, "partition-series/%d/%d", &minute, &partitionID)
        if err != nil {
            level.Warn(t.logger).Log("msg", "invalid key format", "key", key)
            return true // Continue watching
        }

        v, ok := value.(*kvValue)
        if !ok || v == nil {
            return true
        }

        // Merge remote update into our local state
        t.mergeRemoteUpdate(minute, partitionID, v.Registers)

        return true // Continue watching
    })
}

// mergeRemoteUpdate merges a remote HLL update into local state.
func (t *Tracker) mergeRemoteUpdate(minute int64, partitionID int32, registers []byte) {
    t.currentMinuteMu.RLock()
    currentMinute := t.currentMinute
    t.currentMinuteMu.RUnlock()

    pt := t.getOrCreatePartition(partitionID)

    pt.mu.Lock()
    defer pt.mu.Unlock()

    var targetHLL *hyperloglog.HyperLogLog

    if minute == currentMinute {
        // Update current HLL
        targetHLL = pt.currentHLL
    } else if minute < currentMinute && minute > currentMinute-int64(t.cfg.TimeWindowMinutes) {
        // Update historical HLL (if within window)
        if _, exists := pt.historicalHLLs[minute]; !exists {
            pt.historicalHLLs[minute] = hyperloglog.New(/* precision */)
        }
        targetHLL = pt.historicalHLLs[minute]
    } else {
        // Outside window, ignore
        return
    }

    // Merge registers into target HLL
    for i := range targetHLL.Registers {
        if i < len(registers) && registers[i] > targetHLL.Registers[i] {
            targetHLL.Registers[i] = registers[i]
        }
    }

    // Rebuild merged historical if we updated a historical HLL
    if minute != currentMinute {
        pt.rebuildMergedHistorical()
    }
}

// loadStateFromKV loads existing HLL state on startup.
func (t *Tracker) loadStateFromKV(ctx context.Context) error {
    keys, err := t.kv.List(ctx, "partition-series/")
    if err != nil {
        return err
    }

    for _, key := range keys {
        var minute int64
        var partitionID int32
        _, err := fmt.Sscanf(key, "partition-series/%d/%d", &minute, &partitionID)
        if err != nil {
            continue
        }

        value, err := t.kv.Get(ctx, key)
        if err != nil {
            continue
        }

        v, ok := value.(*kvValue)
        if !ok || v == nil {
            continue
        }

        t.mergeRemoteUpdate(minute, partitionID, v.Registers)
    }

    return nil
}

// getOrCreatePartition returns or creates a partition tracker.
func (t *Tracker) getOrCreatePartition(partitionID int32) *partitionTracker {
    t.partitionsMu.RLock()
    pt, exists := t.partitions[partitionID]
    t.partitionsMu.RUnlock()

    if exists {
        return pt
    }

    t.partitionsMu.Lock()
    defer t.partitionsMu.Unlock()

    // Double-check after acquiring write lock
    if pt, exists := t.partitions[partitionID]; exists {
        return pt
    }

    pt = &partitionTracker{
        partitionID:         partitionID,
        currentHLL:          hyperloglog.New(t.cfg.HLLPrecision),
        historicalHLLs:      make(map[int64]*hyperloglog.HyperLogLog),
        mergedHistoricalHLL: hyperloglog.New(t.cfg.HLLPrecision),
        dirty:               false,
    }

    t.partitions[partitionID] = pt
    return pt
}
```

### Distributor Integration

#### Configuration

```go
// In pkg/distributor/distributor.go

type Config struct {
    // ... existing fields ...

    HLLTracker hlltracker.Config `yaml:"partition_series_tracker"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
    // ... existing flags ...

    f.BoolVar(&cfg.HLLTracker.Enabled,
        "distributor.partition-series-tracker.enabled",
        false,
        "Enable distributed partition series tracking using HyperLogLog.")

    // Register KVStore flags with prefix
    cfg.HLLTracker.KVStore.RegisterFlagsWithPrefix("distributor.partition-series-tracker.", "partition series tracker", f)

    f.IntVar(&cfg.HLLTracker.TimeWindowMinutes,
        "distributor.partition-series-tracker.time-window-minutes",
        20,
        "Time window in minutes for tracking partition series cardinality.")

    f.IntVar(&cfg.HLLTracker.UpdateIntervalSeconds,
        "distributor.partition-series-tracker.update-interval-seconds",
        1,
        "How often to push local HLL state to memberlist KV, in seconds.")

    f.IntVar(&cfg.HLLTracker.HLLPrecision,
        "distributor.partition-series-tracker.hll-precision",
        11,
        "HyperLogLog precision (log2 of number of registers). Valid range: 4-18. Default 11 (2048 registers, ~2KB, ~5-6% error). Higher = more accurate but more memory.")
}
```

#### Runtime Limit Configuration

```go
// In pkg/util/validation/limits.go

type Limits struct {
    // ... existing fields ...

    // MaxSeriesPerPartition is the maximum number of series per partition
    // in the configured time window. 0 = disabled.
    MaxSeriesPerPartition int `yaml:"max_series_per_partition" json:"max_series_per_partition"`
}

func (l *Limits) UnmarshalYAML(value *yaml.Node) error {
    // Set defaults
    l.MaxSeriesPerPartition = 3000000 // 3 million
    // ... unmarshal ...
}

// In pkg/util/validation/overrides.go

func (o *Overrides) MaxSeriesPerPartition(userID string) int {
    return o.getOverridesForUser(userID).MaxSeriesPerPartition
}
```

#### Middleware Implementation

```go
// In pkg/distributor/distributor.go

type Distributor struct {
    // ... existing fields ...

    hllTracker *hlltracker.Tracker
}

func New(cfg Config, ...) (*Distributor, error) {
    // ... existing initialization ...

    if cfg.HLLTracker.Enabled && cfg.IngestStorageConfig.Enabled {
        tracker, err := hlltracker.New(
            cfg.HLLTracker,
            log.With(logger, "component", "partition-series-tracker"),
            registerer,
        )
        if err != nil {
            return nil, err
        }
        d.hllTracker = tracker
    }

    // ... rest of initialization ...
}

// Update service dependencies
func (d *Distributor) starting(ctx context.Context) error {
    // ... existing startup ...

    if d.hllTracker != nil {
        if err := services.StartAndAwaitRunning(ctx, d.hllTracker); err != nil {
            return err
        }
    }

    return nil
}

// Update wrapPushWithMiddlewares
func (d *Distributor) wrapPushWithMiddlewares(next PushFunc) PushFunc {
    var middlewares []PushWrapper

    middlewares = append(middlewares, d.limitsMiddleware)
    middlewares = append(middlewares, d.metricsMiddleware)
    middlewares = append(middlewares, d.prePushHaDedupeMiddleware)
    middlewares = append(middlewares, d.prePushRelabelMiddleware)
    middlewares = append(middlewares, d.prePushSortAndFilterMiddleware)
    middlewares = append(middlewares, d.prePushValidationMiddleware)

    // Add partition series limit middleware if enabled
    if d.hllTracker != nil {
        middlewares = append(middlewares, d.preKafkaPartitionLimitMiddleware)
    }

    middlewares = append(middlewares, d.cfg.PushWrappers...)
    middlewares = append(middlewares, d.prePushMaxSeriesLimitMiddleware)

    for ix := len(middlewares) - 1; ix >= 0; ix-- {
        next = middlewares[ix](next)
    }

    return d.outerMaybeDelayMiddleware(next)
}

// preKafkaPartitionLimitMiddleware enforces per-partition series limits.
func (d *Distributor) preKafkaPartitionLimitMiddleware(next PushFunc) PushFunc {
    return func(ctx context.Context, pushReq *Request) error {
        next, maybeCleanup := NextOrCleanup(next, pushReq)
        defer maybeCleanup()

        req, err := pushReq.WriteRequest()
        if err != nil {
            return err
        }

        userID, err := tenant.TenantID(ctx)
        if err != nil {
            return err
        }

        // Get the limit for this tenant
        limit := d.limits.MaxSeriesPerPartition(userID)
        if limit <= 0 {
            // Limit disabled for this tenant
            return next(ctx, pushReq)
        }

        // Get series tokens only (not metadata) - we only limit series per partition
        seriesKeys := getTokensForSeries(userID, req.Timeseries)

        subring, err := d.partitionsRing.ShuffleShard(
            userID,
            d.limits.IngestionPartitionsTenantShardSize(userID),
        )
        if err != nil {
            return err
        }

        partitionRing := ring.NewActivePartitionBatchRing(subring.PartitionRing())

        // Group series by partition
        partitionSeries := make(map[int32][]uint32)

        err = ring.DoBatchWithOptions(ctx, ring.WriteNoExtend, partitionRing, seriesKeys,
            func(partition ring.InstanceDesc, indexes []int) error {
                partitionID, err := strconv.ParseInt(partition.Id, 10, 32)
                if err != nil {
                    return err
                }

                // Collect series hashes for this partition
                for _, idx := range indexes {
                    partitionSeries[int32(partitionID)] = append(
                        partitionSeries[int32(partitionID)],
                        seriesKeys[idx],
                    )
                }

                return nil
            },
            ring.DoBatchOptions{},
        )

        if err != nil {
            return err
        }

        // For each partition, get current state and update with new series
        updates := make([]hlltracker.PartitionUpdate, 0, len(partitionSeries))

        for partitionID, seriesHashes := range partitionSeries {
            state := d.hllTracker.GetCurrentState(partitionID)

            // Add new series to current HLL copy
            for _, hash := range seriesHashes {
                state.CurrentCopy.Add(hash)
            }

            // Merge historical and updated current
            merged := state.MergedHistorical.Clone()
            merged.Merge(state.CurrentCopy)

            // Check if limit would be exceeded
            count := merged.Count()
            if count > uint64(limit) {
                // Limit exceeded - reject request
                return newPartitionSeriesLimitError(
                    userID,
                    int32(partitionID),
                    limit,
                    count,
                )
            }

            // Limit OK - save update for commit
            updates = append(updates, hlltracker.PartitionUpdate{
                PartitionID: partitionID,
                UpdatedHLL:  state.CurrentCopy,
            })
        }

        // All partitions within limit - commit updates
        d.hllTracker.UpdateCurrent(updates)

        // Continue to next middleware
        return next(ctx, pushReq)
    }
}

// newPartitionSeriesLimitError creates an error for partition series limit.
func newPartitionSeriesLimitError(
    userID string,
    partitionID int32,
    limit int,
    estimated uint64,
) error {
    return httpgrpc.Errorf(
        http.StatusTooManyRequests,
        "per-partition series limit of %d exceeded for partition %d (estimated: %d series); "+
            "adjust -distributor.partition-series-tracker.time-window-minutes or "+
            "increase the limit via max_series_per_partition in runtime config",
        limit,
        partitionID,
        estimated,
    )
}
```

### HyperLogLog Implementation

We will use the DataDog HyperLogLog implementation as a reference. If needed, we can fork it to `pkg/distributor/hlltracker/hyperloglog/` with minimal modifications:

```go
// pkg/distributor/hlltracker/hyperloglog/hyperloglog.go

package hyperloglog

// HyperLogLog is a probabilistic cardinality estimator.
type HyperLogLog struct {
    M         uint32  // Number of registers
    B         uint8   // Bits for register indexing
    Alpha     float64 // Bias correction constant
    Registers []byte  // Register values (one byte per register)
}

// New creates a new HyperLogLog with the given precision.
// precision is log2(m) where m is the number of registers.
func New(precision uint8) *HyperLogLog {
    // ... implementation from DataDog ...
}

// Add adds a 32-bit hash to the HyperLogLog.
func (h *HyperLogLog) Add(val uint32) {
    // ... implementation from DataDog ...
}

// Merge merges another HyperLogLog into this one.
func (h *HyperLogLog) Merge(other *HyperLogLog) {
    // ... implementation from DataDog ...
}

// Count returns the estimated cardinality.
func (h *HyperLogLog) Count() uint64 {
    // ... implementation from DataDog ...
}

// Clone creates a deep copy of the HyperLogLog.
func (h *HyperLogLog) Clone() *HyperLogLog {
    clone := &HyperLogLog{
        M:         h.M,
        B:         h.B,
        Alpha:     h.Alpha,
        Registers: make([]byte, len(h.Registers)),
    }
    copy(clone.Registers, h.Registers)
    return clone
}
```

### Testing Strategy

1. **Unit Tests**:
   - HLL operations (add, merge, count, accuracy)
   - Partition tracker operations (rotate, merge, cleanup)
   - Thread-safety tests (concurrent updates)
   - Time window management

2. **Integration Tests**:
   - Multiple distributor instances with memberlist
   - CAS conflict resolution
   - State synchronization across instances
   - Minute rotation with time mocking

3. **Benchmarks**:
   - Middleware overhead per request
   - HLL operations performance
   - Memory usage per partition

4. **E2E Tests**:
   - Actual ingestion with limit enforcement
   - Limit exceeded scenarios
   - Multi-tenant scenarios

## Implementation Plan

The implementation will be done in two phases to reduce complexity and enable incremental testing.

### Phase 1: Local HLL Tracking (No Distribution)

**Goal**: Implement the core HLL tracking functionality without memberlist integration.

**Scope**:
1. Create `pkg/distributor/hlltracker/` package
2. Fork or vendor HyperLogLog implementation
3. Implement `Tracker` with:
   - Per-partition HLL structures
   - Sliding time window (current + historical)
   - Minute rotation logic
   - Local state management (no KV operations)
4. Implement distributor middleware:
   - Calculate partition assignments
   - Update HLL with series hashes
   - Check limits and reject if exceeded
5. Add configuration flags and runtime limits
6. Write comprehensive unit tests
7. Write benchmarks to measure overhead

**Deliverables**:
- Working per-distributor partition series limiting
- All tests passing
- Performance benchmarks showing acceptable overhead

**Limitations**:
- Each distributor tracks only its own series (no cluster-wide view)
- Limits will be less accurate in multi-distributor setups
- Good for initial deployment and testing

### Phase 2: Distributed Synchronization (Memberlist)

**Goal**: Add memberlist gossip to synchronize HLL state across distributors.

**Scope**:
1. Implement KV operations in `Tracker`:
   - CAS updates with merge logic
   - Watch pattern for remote updates
   - Load state on startup
   - Key cleanup for old minutes
2. Implement `kvValue` protobuf message and codec
3. Add background ticker for periodic CAS pushes
4. Add merging logic for remote updates
5. Update tests for distributed scenarios
6. Add integration tests with multiple distributors
7. Add metrics for KV operations

**Deliverables**:
- Full distributed partition series limiting
- Eventual consistency across distributors
- All tests passing (including integration tests)
- Production-ready implementation

**Benefits**:
- Accurate cluster-wide per-partition cardinality
- Protection against hot partitions across distributors
- Resilient to distributor failures and restarts

### Implementation Notes

1. **Proto file**: Create `pkg/distributor/hlltracker/hlltracker.proto` for the kvValue message
2. **Codec registration**: Register the codec similar to HA-Tracker's ReplicaDesc codec
3. **KV key prefix**: Use "partition-series/" to avoid conflicts with other KV data
4. **Cleanup strategy**: Use two-phase deletion (mark, then delete) like HA-Tracker to ensure Watch notifications propagate
5. **Error handling**: If KV operations fail, log warnings but don't fail requests (fail open for availability)
6. **Metrics**: Add gauges for per-partition cardinality estimates, counters for CAS operations, rejected requests
7. **Testing**: Mock time in tests to control minute rotation and window management

### Rollout Plan

1. **Phase 1 deployment**:
   - Deploy with `-distributor.partition-series-tracker.enabled=false` (default)
   - Enable for a subset of tenants via runtime config
   - Monitor performance impact
   - Tune HLL precision and time window as needed

2. **Phase 2 deployment**:
   - Deploy Phase 2 code (memberlist integration)
   - Gradually enable for more tenants
   - Monitor memberlist bandwidth usage
   - Monitor limit enforcement effectiveness

3. **Full rollout**:
   - Enable by default for all tenants with ingest storage
   - Set appropriate default limits
   - Document operator guidelines for tuning

## Alternative Approaches Considered

### 1. Exact Counting with Distributed Hash Table

**Approach**: Store exact series sets in a distributed hash table.

**Rejected because**:
- High memory usage (store all series IDs)
- High network bandwidth (synchronize full sets)
- Expensive merge operations (set unions)
- Doesn't scale to millions of series per partition

### 2. Central Counting Service

**Approach**: Single service tracks all series across distributors.

**Rejected because**:
- Single point of failure
- Bottleneck for all writes
- High latency added to ingestion path
- Doesn't align with Mimir's distributed architecture

### 3. Bloom Filters

**Approach**: Use Bloom filters instead of HyperLogLog.

**Rejected because**:
- Bloom filters track membership, not cardinality
- Cannot get count without iterating all items
- Merge operation more complex
- HyperLogLog is purpose-built for cardinality estimation

### 4. Sampling-Based Estimation

**Approach**: Sample series and extrapolate cardinality.

**Rejected because**:
- Less accurate for sparse data
- Requires careful sampling strategy
- HyperLogLog provides better accuracy guarantees
- Harder to reason about error bounds

## Open Questions

1. **HLL precision tuning**: Default precision of 11 (2KB per HLL, ~5-6% error). Should we make this dynamically tunable per tenant?
   - **Decision**: No, keep it simple with a single global precision setting. The 5-6% error is acceptable for all use cases.

2. **Memory limits**: With 1000 partitions, 20-minute window, precision 11:
   - Per partition: 20 minutes × 2 KB = 40 KB
   - Total: 1000 × 40 KB = 40 MB
   - This is very acceptable. Even with 10,000 partitions, we'd only use 400 MB.

3. **KV bandwidth**: How much bandwidth will CAS updates consume?
   - Updates per second: number of partitions receiving writes
   - Bytes per update: ~2 KB (registers)
   - With 1000 active partitions, 1 update/second: ~2 MB/s per distributor
   - Need to measure in testing, but should be manageable

4. **Limit threshold**: Default of 3M series per partition - is this appropriate?
   - Depends on ingester memory capacity
   - Should be configurable per deployment via runtime config
   - Operators should adjust based on their ingester specs

5. **Error handling**: If memberlist is unavailable, should we:
   - Allow writes (risk exceeding limits) ← **Recommended**: Fail open for availability
   - Reject writes (risk false positives)
   - Fall back to local-only tracking
   - Add a flag to configure this behavior

6. **Series vs Metadata**: Should we only track series or also metadata?
   - **Decision**: Only track series (not metadata). The limit is about ingester memory from series in TSDB HEAD, not metadata.

## Success Criteria

1. **Functional**:
   - Successfully limits series per partition
   - Rejects requests that would exceed limits
   - Synchronizes state across distributors
   - Handles minute rotation correctly

2. **Performance**:
   - Middleware adds <1ms latency to P99
   - Memory usage scales linearly with partitions
   - No significant increase in CPU usage
   - KV bandwidth within acceptable limits (<10 MB/s per distributor)

3. **Operational**:
   - Clear metrics for monitoring
   - Helpful error messages for users
   - Documentation for operators
   - Runbooks for troubleshooting

## Related Work

- [HA-Tracker](https://github.com/grafana/mimir/blob/main/pkg/distributor/ha_tracker.go): Memberlist pattern
- [HyperLogLog++](https://research.google/pubs/hyperloglog-in-practice-algorithmic-engineering-of-a-state-of-the-art-cardinality-estimation-algorithm/): Algorithm reference
- [DataDog HyperLogLog](https://github.com/DataDog/hyperloglog): Implementation reference
- [Tenancy Multiplexing Proposal](./reduce-multitenancy-cost.md): Similar use of HLL for series counting
