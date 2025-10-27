// SPDX-License-Identifier: AGPL-3.0-only

package hlltracker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/distributor/hlltracker/hyperloglog"
)

// Tracker tracks per-partition series cardinality using HyperLogLog.
// Supports both Phase 1 (local-only) and Phase 2 (distributed with memberlist).
type Tracker struct {
	services.Service

	cfg    Config
	logger log.Logger

	// KV client for distributed state synchronization (Phase 2)
	// If nil, tracker operates in local-only mode (Phase 1)
	kvClient kv.Client

	// partitionsMu protects the partitions map
	partitionsMu sync.RWMutex
	partitions   map[int32]*partitionTracker

	// Current unix minute (minutes since epoch)
	currentMinuteMu sync.RWMutex
	currentMinute   int64

	// Ticker for minute rotation
	rotateTicker *time.Ticker

	// Ticker for KV push (Phase 2 only)
	kvPushTicker *time.Ticker

	// Metrics
	partitionSeriesEstimate *prometheus.GaugeVec
	rotations               prometheus.Counter
	kvPushes                *prometheus.CounterVec
	kvPushErrors            *prometheus.CounterVec
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
	// Rebuilt when currentHLL is rotated or when updates arrive
	mergedHistoricalHLL *hyperloglog.HyperLogLog
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

// MaxSeriesPerPartition returns the configured global max series per partition limit.
func (t *Tracker) MaxSeriesPerPartition() int {
	return t.cfg.MaxSeriesPerPartition
}

// New creates a new Tracker.
// kvClient is optional: if nil, operates in local-only mode (Phase 1).
// If provided, enables distributed state synchronization via memberlist (Phase 2).
func New(
	cfg Config,
	logger log.Logger,
	reg prometheus.Registerer,
	kvClient kv.Client,
) (*Tracker, error) {
	// Validate and set defaults
	if cfg.TimeWindowMinutes <= 0 {
		cfg.TimeWindowMinutes = 20
	}
	if cfg.UpdateIntervalSeconds <= 0 {
		cfg.UpdateIntervalSeconds = 1
	}
	if cfg.HLLPrecision < 4 || cfg.HLLPrecision > 18 {
		cfg.HLLPrecision = 11 // Default to 2048 registers
	}

	t := &Tracker{
		cfg:        cfg,
		logger:     log.With(logger, "component", "partition-series-tracker"),
		kvClient:   kvClient,
		partitions: make(map[int32]*partitionTracker),

		partitionSeriesEstimate: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_distributor_partition_series_estimate",
			Help: "Estimated number of series per partition based on HyperLogLog.",
		}, []string{"partition"}),
		rotations: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_distributor_partition_series_tracker_rotations_total",
			Help: "Total number of minute rotations in the partition series tracker.",
		}),
		kvPushes: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_partition_series_tracker_kv_pushes_total",
			Help: "Total number of KV push operations, labeled by status (success/failure).",
		}, []string{"status"}),
		kvPushErrors: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_partition_series_tracker_kv_push_errors_total",
			Help: "Total number of KV push errors, labeled by error type.",
		}, []string{"error"}),
	}

	if kvClient != nil {
		level.Info(t.logger).Log("msg", "partition series tracker running in distributed mode (Phase 2)")
	} else {
		level.Info(t.logger).Log("msg", "partition series tracker running in local-only mode (Phase 1)")
	}

	t.Service = services.NewBasicService(t.starting, t.running, t.stopping)
	return t, nil
}

func (t *Tracker) starting(ctx context.Context) error {
	// Initialize current minute
	t.currentMinute = time.Now().Unix() / 60
	level.Info(t.logger).Log("msg", "partition series tracker starting", "current_minute", t.currentMinute)

	// If KV client is enabled, start watching for remote updates (Phase 2)
	if t.kvClient != nil {
		level.Info(t.logger).Log("msg", "starting KV watch for remote HLL updates")
	}

	return nil
}

func (t *Tracker) running(ctx context.Context) error {
	// Check every second for minute rotation
	t.rotateTicker = time.NewTicker(1 * time.Second)
	defer t.rotateTicker.Stop()

	// If KV client is enabled, push state periodically (Phase 2)
	var kvPushChan <-chan time.Time
	if t.kvClient != nil {
		t.kvPushTicker = time.NewTicker(time.Duration(t.cfg.UpdateIntervalSeconds) * time.Second)
		defer t.kvPushTicker.Stop()
		kvPushChan = t.kvPushTicker.C

		// Start KV watch in a separate goroutine
		go t.watchKV(ctx)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.rotateTicker.C:
			t.checkAndRotateMinute()
		case <-kvPushChan:
			// This case is skipped if kvPushChan is nil
			t.pushToKV(ctx)
		}
	}
}

func (t *Tracker) stopping(_ error) error {
	level.Info(t.logger).Log("msg", "partition series tracker stopping")
	return nil
}

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
	level.Debug(t.logger).Log("msg", "rotating minute", "old_minute", current, "new_minute", nowMinute)

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
		pt.rotateMinute(current, nowMinute, t.cfg.TimeWindowMinutes, t.cfg.HLLPrecision)

		// Update metrics
		pt.mu.RLock()
		merged := pt.mergedHistoricalHLL.Clone()
		merged.Merge(pt.currentHLL)
		count := merged.Count()
		pt.mu.RUnlock()

		t.partitionSeriesEstimate.WithLabelValues(string(pt.partitionID)).Set(float64(count))
	}

	t.rotations.Inc()
}

// rotateMinute moves current HLL to historical and creates new current.
func (pt *partitionTracker) rotateMinute(
	oldMinute, newMinute int64,
	windowMinutes int,
	precision int,
) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	// Move current to historical
	pt.historicalHLLs[oldMinute] = pt.currentHLL
	pt.currentHLL = hyperloglog.New(uint8(precision))

	// Rebuild merged historical HLL
	pt.rebuildMergedHistorical(uint8(precision))

	// Clean up old entries outside the time window
	cutoff := newMinute - int64(windowMinutes)
	for minute := range pt.historicalHLLs {
		if minute <= cutoff {
			delete(pt.historicalHLLs, minute)
		}
	}
}

// rebuildMergedHistorical merges all historical HLLs.
// Must be called with pt.mu held (Lock, not RLock).
func (pt *partitionTracker) rebuildMergedHistorical(precision uint8) {
	merged := hyperloglog.New(precision)
	for _, hll := range pt.historicalHLLs {
		merged.Merge(hll)
	}
	pt.mergedHistoricalHLL = merged
}

// pushToKV pushes current HLL state for all partitions to the KV store.
// This is called periodically in Phase 2 to synchronize state across distributors.
func (t *Tracker) pushToKV(ctx context.Context) {
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
		hll := pt.currentHLL.Clone()
		pt.mu.RUnlock()

		// Create state object
		state := &PartitionHLLState{
			PartitionID: pt.partitionID,
			UnixMinute:  currentMinute,
			HLL:         hll,
			UpdatedAtMs: time.Now().UnixMilli(),
		}

		// Generate KV key
		key := makePartitionHLLKey(pt.partitionID, currentMinute)

		// Push to KV using CAS with merge
		err := t.kvClient.CAS(ctx, key, func(in interface{}) (out interface{}, retry bool, err error) {
			if in == nil {
				// No existing value, just write ours
				return state, false, nil
			}

			// Merge with existing value
			existing, ok := in.(*PartitionHLLState)
			if !ok {
				level.Error(t.logger).Log("msg", "unexpected type in KV", "key", key, "type", fmt.Sprintf("%T", in))
				t.kvPushErrors.WithLabelValues("type_error").Inc()
				return nil, false, fmt.Errorf("unexpected type: %T", in)
			}

			// Merge HLLs
			merged := existing.HLL.Clone()
			merged.Merge(hll)

			// Return merged state
			return &PartitionHLLState{
				PartitionID: pt.partitionID,
				UnixMinute:  currentMinute,
				HLL:         merged,
				UpdatedAtMs: time.Now().UnixMilli(),
			}, false, nil
		})

		if err != nil {
			level.Warn(t.logger).Log("msg", "failed to push HLL state to KV", "partition", pt.partitionID, "err", err)
			t.kvPushes.WithLabelValues("failure").Inc()
			t.kvPushErrors.WithLabelValues("cas_error").Inc()
		} else {
			t.kvPushes.WithLabelValues("success").Inc()
		}
	}
}

// makePartitionHLLKey generates a KV key for a partition's HLL state at a specific minute.
// Format: "partition-series/<partition_id>/<unix_minute>"
func makePartitionHLLKey(partitionID int32, unixMinute int64) string {
	return fmt.Sprintf("partition-series/%d/%d", partitionID, unixMinute)
}

// watchKV watches the KV store for remote HLL updates and merges them into local state.
// This runs in a separate goroutine and blocks until context is cancelled.
func (t *Tracker) watchKV(ctx context.Context) {
	// Watch all keys with "partition-series/" prefix
	// The KV client should have been created with the appropriate prefix/codec
	t.kvClient.WatchPrefix(ctx, "partition-series/", func(key string, value interface{}) bool {
		if value == nil {
			// Key was deleted, ignore (cleanup happens during rotation)
			return true
		}

		state, ok := value.(*PartitionHLLState)
		if !ok {
			level.Error(t.logger).Log("msg", "unexpected type in KV watch", "key", key, "type", fmt.Sprintf("%T", value))
			return true // Continue watching
		}

		// Merge remote state into local
		t.mergeRemoteState(state)

		return true // Continue watching
	})

	level.Info(t.logger).Log("msg", "KV watch terminated")
}

// mergeRemoteState merges a remote HLL state into the local partition tracker.
// This is called when we receive updates from other distributors via KV watch.
func (t *Tracker) mergeRemoteState(remoteState *PartitionHLLState) {
	if remoteState == nil || remoteState.HLL == nil {
		return
	}

	pt := t.getOrCreatePartition(remoteState.PartitionID)

	pt.mu.Lock()
	defer pt.mu.Unlock()

	// Get current minute to determine where to merge
	t.currentMinuteMu.RLock()
	currentMinute := t.currentMinute
	t.currentMinuteMu.RUnlock()

	if remoteState.UnixMinute == currentMinute {
		// Merge into current HLL
		pt.currentHLL.Merge(remoteState.HLL)
		level.Debug(t.logger).Log(
			"msg", "merged remote state into current HLL",
			"partition", remoteState.PartitionID,
			"minute", remoteState.UnixMinute,
		)
	} else if remoteState.UnixMinute < currentMinute && remoteState.UnixMinute >= currentMinute-int64(t.cfg.TimeWindowMinutes) {
		// Merge into historical HLL for this minute
		existingHLL, exists := pt.historicalHLLs[remoteState.UnixMinute]
		if !exists {
			// Create new historical HLL
			pt.historicalHLLs[remoteState.UnixMinute] = remoteState.HLL.Clone()
		} else {
			// Merge with existing
			existingHLL.Merge(remoteState.HLL)
		}

		// Rebuild merged historical cache
		pt.rebuildMergedHistorical(uint8(t.cfg.HLLPrecision))

		level.Debug(t.logger).Log(
			"msg", "merged remote state into historical HLL",
			"partition", remoteState.PartitionID,
			"minute", remoteState.UnixMinute,
		)
	} else {
		// State is too old or too far in future, ignore
		level.Debug(t.logger).Log(
			"msg", "ignoring remote state outside time window",
			"partition", remoteState.PartitionID,
			"minute", remoteState.UnixMinute,
			"current_minute", currentMinute,
		)
	}
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
		currentHLL:          hyperloglog.New(uint8(t.cfg.HLLPrecision)),
		historicalHLLs:      make(map[int64]*hyperloglog.HyperLogLog),
		mergedHistoricalHLL: hyperloglog.New(uint8(t.cfg.HLLPrecision)),
	}

	t.partitions[partitionID] = pt
	level.Debug(t.logger).Log("msg", "created partition tracker", "partition", partitionID)
	return pt
}
