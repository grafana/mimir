// SPDX-License-Identifier: AGPL-3.0-only

package hlltracker

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/distributor/hlltracker/hyperloglog"
)

// Tracker tracks per-partition series cardinality using HyperLogLog.
// Phase 1: Local-only tracking without memberlist synchronization.
type Tracker struct {
	services.Service

	cfg    Config
	logger log.Logger

	// partitionsMu protects the partitions map
	partitionsMu sync.RWMutex
	partitions   map[int32]*partitionTracker

	// Current unix minute (minutes since epoch)
	currentMinuteMu sync.RWMutex
	currentMinute   int64

	// Ticker for minute rotation
	rotateTicker *time.Ticker

	// Metrics
	partitionSeriesEstimate *prometheus.GaugeVec
	rotations               prometheus.Counter
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
// Phase 1: No KV client needed yet.
func New(
	cfg Config,
	logger log.Logger,
	reg prometheus.Registerer,
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
		partitions: make(map[int32]*partitionTracker),

		partitionSeriesEstimate: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_distributor_partition_series_estimate",
			Help: "Estimated number of series per partition based on HyperLogLog.",
		}, []string{"partition"}),
		rotations: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_distributor_partition_series_tracker_rotations_total",
			Help: "Total number of minute rotations in the partition series tracker.",
		}),
	}

	t.Service = services.NewBasicService(t.starting, t.running, t.stopping)
	return t, nil
}

func (t *Tracker) starting(ctx context.Context) error {
	// Initialize current minute
	t.currentMinute = time.Now().Unix() / 60
	level.Info(t.logger).Log("msg", "partition series tracker starting", "current_minute", t.currentMinute)
	return nil
}

func (t *Tracker) running(ctx context.Context) error {
	// Check every second for minute rotation
	t.rotateTicker = time.NewTicker(1 * time.Second)
	defer t.rotateTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.rotateTicker.C:
			t.checkAndRotateMinute()
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
