// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/nautilus/loadstats"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/validation"
)

var tracer = otel.Tracer("pkg/readcache")

// Readcache is the read-path counterpart to the ingester. Unlike the
// ingester it does not handle writes — Push, partition lifecycler and
// owned-series machinery are deliberately absent. It consumes Kafka in
// parallel with the ingester (or, in the dev cell, from a dedicated
// experimental topic — see the readcache phase 2 plan) and serves the
// read RPCs against per-(tenant, partition) heads.
//
// The 1:1 partition-to-instance stipulation that constrains the
// ingester is lifted here: a single readcache pod may own many
// partitions, and the rebalancer is free to shuffle ownership across
// instances at runtime (Phase 2B).
type Readcache struct {
	services.Service

	cfg    Config
	limits *validation.Overrides
	logger log.Logger
	reg    prometheus.Registerer

	partitionMu sync.RWMutex
	// partitions is the set of partitions currently owned by this
	// instance, keyed by partition ID. Populated by the static
	// -readcache.owned-partitions flag in Phase 2A, and by the
	// rebalancer log subscription in Phase 2B.
	partitions map[int32]*partitionState

	// queryLoad and rangeSeries are the per-partition query-load and
	// per-hash-range active-series signals that the rebalancer pulls
	// via HashRangeStats. They live on readcache (not ingester) per
	// the plan.
	queryLoad   *loadstats.Tracker
	rangeSeries *loadstats.RangeSeries
}

// partitionState bundles the per-partition Kafka reader and the
// per-tenant heads keyed by tenant ID.
type partitionState struct {
	partitionID int32

	// reader is the Kafka consumer driving samples into this
	// partition. Nil until startKafkaReader is called.
	reader *ingest.PartitionReader

	// warm flips to true once the Kafka reader has caught up to the
	// starting fetch offset enough that read RPCs can be served
	// without misleadingly partial results. Until then, the read
	// handlers return errStillWarming so the distributor can fall
	// back to the previous lease owner. Phase 2A flips warm to true
	// immediately after addPartition returns; a future patch hooks
	// this into the reader's first-fetch-completed signal.
	warm atomic.Bool

	tenantsMu sync.RWMutex
	tenants   map[string]*partitionTSDB
}

func newPartitionState(partitionID int32) *partitionState {
	return &partitionState{
		partitionID: partitionID,
		tenants:     make(map[string]*partitionTSDB),
	}
}

// New constructs a Readcache. Wiring (Kafka readers, ring registration,
// rebalancer subscription) happens in starting().
func New(
	cfg Config,
	limits *validation.Overrides,
	logger log.Logger,
	reg prometheus.Registerer,
) (*Readcache, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating readcache config: %w", err)
	}

	r := &Readcache{
		cfg:         cfg,
		limits:      limits,
		logger:      logger,
		reg:         reg,
		partitions:  make(map[int32]*partitionState),
		queryLoad:   loadstats.NewTracker("cortex_readcache"),
		rangeSeries: loadstats.NewRangeSeries(),
	}

	if reg != nil {
		reg.MustRegister(r.queryLoad)
	}

	r.Service = services.NewBasicService(r.starting, r.running, r.stopping)
	return r, nil
}

func (r *Readcache) starting(ctx context.Context) error {
	if err := os.MkdirAll(r.cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("creating readcache data-dir %q: %w", r.cfg.DataDir, err)
	}

	// Static partition assignment for Phase 2A. Phase 2B replaces this
	// with a subscription to WatchReadcacheAssignments on the
	// rebalancer.
	pids, err := r.cfg.ParseOwnedPartitions()
	if err != nil {
		return fmt.Errorf("parsing -readcache.owned-partitions: %w", err)
	}
	for _, pid := range pids {
		if err := r.addPartition(ctx, pid); err != nil {
			return fmt.Errorf("starting partition %d: %w", pid, err)
		}
	}

	level.Info(r.logger).Log(
		"msg", "readcache started",
		"instance_id", r.cfg.InstanceID,
		"owned_partitions", len(pids),
		"kafka_topic", r.cfg.KafkaTopic,
	)
	return nil
}

func (r *Readcache) running(ctx context.Context) error {
	// Per-partition tickers for head compaction. Aggregated to one
	// shared goroutine to avoid one goroutine per partition; the
	// granularity (an hour by default) makes shared scheduling fine.
	compactT := time.NewTicker(r.cfg.HeadCompactionInterval)
	defer compactT.Stop()

	loadStatsTickT := time.NewTicker(loadstats.TickInterval)
	defer loadStatsTickT.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-compactT.C:
			r.compactHeads()
		case <-loadStatsTickT.C:
			r.queryLoad.Tick()
		}
	}
}

func (r *Readcache) stopping(_ error) error {
	r.partitionMu.Lock()
	defer r.partitionMu.Unlock()

	var firstErr error
	for pid, p := range r.partitions {
		if err := r.stopPartitionLocked(pid, p); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	r.partitions = map[int32]*partitionState{}
	return firstErr
}

// addPartition begins owning a partition: opens the Kafka reader and
// initializes the per-tenant TSDB map. Idempotent: a second call for
// the same partition ID is a no-op.
//
// startKafkaReader uses services.StartAndAwaitRunning which blocks
// until the reader has caught up to its target lag. During catch-up
// the reader calls back into partitionPusher.PushToStorageAndReleaseRequest,
// which calls getOrOpenTSDB and therefore RLocks partitionMu. So we
// must NOT hold partitionMu while the reader catches up, or we
// deadlock the very first time a non-empty partition is added.
//
// The fix is to publish the partitionState into r.partitions under
// the write lock, drop the lock, then start the reader. Concurrent
// addPartition calls for the same ID are coordinated via the once-
// only insert: any caller that loses the race observes the existing
// entry and bails out.
func (r *Readcache) addPartition(ctx context.Context, partitionID int32) error {
	r.partitionMu.Lock()
	if _, ok := r.partitions[partitionID]; ok {
		r.partitionMu.Unlock()
		return nil
	}
	if err := os.MkdirAll(r.cfg.DataDir, 0o755); err != nil {
		r.partitionMu.Unlock()
		return fmt.Errorf("creating readcache data-dir for partition %d: %w", partitionID, err)
	}
	p := newPartitionState(partitionID)
	r.partitions[partitionID] = p
	r.partitionMu.Unlock()

	if err := r.startKafkaReader(ctx, p); err != nil {
		r.partitionMu.Lock()
		delete(r.partitions, partitionID)
		r.partitionMu.Unlock()
		return err
	}

	// Phase 2A: the reader does not yet expose a "caught up to the
	// starting fetch offset" signal, so we mark the partition warm
	// immediately. A follow-up patch wires p.warm into the reader's
	// initial-fetch-completed event so distributors see the "still
	// warming" gRPC error during the genuine cold-start window
	// after a partition move.
	p.warm.Store(true)

	level.Info(r.logger).Log("msg", "readcache: partition added", "partition", partitionID)
	return nil
}

// removePartition stops owning a partition: shuts down the Kafka
// reader and closes per-tenant TSDBs. Idempotent.
func (r *Readcache) removePartition(partitionID int32) error {
	r.partitionMu.Lock()
	defer r.partitionMu.Unlock()
	p, ok := r.partitions[partitionID]
	if !ok {
		return nil
	}
	delete(r.partitions, partitionID)
	return r.stopPartitionLocked(partitionID, p)
}

// stopPartitionLocked closes a partition's resources. Caller must hold
// partitionMu.
//
// Tear-down order: stop the Kafka reader first so no more samples are
// arriving, then close the per-tenant TSDBs.
func (r *Readcache) stopPartitionLocked(partitionID int32, p *partitionState) error {
	var firstErr error
	if err := r.stopKafkaReaderLocked(p); err != nil {
		firstErr = err
		level.Warn(r.logger).Log("msg", "stopping partition reader", "partition", partitionID, "err", err)
	}

	p.tenantsMu.Lock()
	defer p.tenantsMu.Unlock()

	for tenant, db := range p.tenants {
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
			level.Warn(r.logger).Log("msg", "closing partition TSDB",
				"user", tenant, "partition", partitionID, "err", err)
		}
	}
	p.tenants = map[string]*partitionTSDB{}
	level.Info(r.logger).Log("msg", "readcache: partition removed", "partition", partitionID)
	return firstErr
}

// getOrOpenTSDB returns the TSDB for (tenantID, partitionID),
// creating one if needed. Returns nil if the readcache does not own
// the partition.
func (r *Readcache) getOrOpenTSDB(tenantID string, partitionID int32) (*partitionTSDB, error) {
	r.partitionMu.RLock()
	p, ok := r.partitions[partitionID]
	r.partitionMu.RUnlock()
	if !ok {
		return nil, nil
	}

	p.tenantsMu.RLock()
	db := p.tenants[tenantID]
	p.tenantsMu.RUnlock()
	if db != nil {
		return db, nil
	}

	// Open under the per-partition tenant map's lock so two concurrent
	// callers can't race to open the same TSDB.
	p.tenantsMu.Lock()
	defer p.tenantsMu.Unlock()
	if db := p.tenants[tenantID]; db != nil {
		return db, nil
	}

	opened, err := openPartitionTSDB(
		tenantID,
		partitionID,
		r.cfg.DataDir,
		r.cfg.BlocksStorage.TSDB,
		r.logger,
		r.reg,
	)
	if err != nil {
		return nil, err
	}
	p.tenants[tenantID] = opened
	return opened, nil
}

// listTSDBsForTenant returns the partition TSDBs this readcache owns
// for the given tenant, across all owned partitions. If hint is non-
// nil, only the TSDB for the matching partition is returned (or empty
// if this readcache does not own that partition).
//
// Returns an errStillWarming gRPC status if any of the partitions
// the query would scan is still warming its head — callers must
// surface this error verbatim so the distributor's fallback path
// can react. Returning the partial set of warm partitions would
// silently produce incomplete results, which is strictly worse than
// failing fast.
func (r *Readcache) listTSDBsForTenant(tenantID string, hint *client.QueryAttributionHint) ([]*partitionTSDB, error) {
	r.partitionMu.RLock()
	defer r.partitionMu.RUnlock()

	if hint != nil {
		p, ok := r.partitions[hint.PartitionId]
		if !ok {
			return nil, nil
		}
		if !p.warm.Load() {
			return nil, errStillWarming(hint.PartitionId)
		}
		p.tenantsMu.RLock()
		db := p.tenants[tenantID]
		p.tenantsMu.RUnlock()
		if db == nil {
			return nil, nil
		}
		return []*partitionTSDB{db}, nil
	}

	out := make([]*partitionTSDB, 0, len(r.partitions))
	for _, p := range r.partitions {
		if !p.warm.Load() {
			return nil, errStillWarming(p.partitionID)
		}
		p.tenantsMu.RLock()
		if db := p.tenants[tenantID]; db != nil {
			out = append(out, db)
		}
		p.tenantsMu.RUnlock()
	}
	return out, nil
}

func (r *Readcache) compactHeads() {
	r.partitionMu.RLock()
	parts := make([]*partitionState, 0, len(r.partitions))
	for _, p := range r.partitions {
		parts = append(parts, p)
	}
	r.partitionMu.RUnlock()

	for _, p := range parts {
		p.tenantsMu.RLock()
		dbs := make([]*partitionTSDB, 0, len(p.tenants))
		for _, db := range p.tenants {
			dbs = append(dbs, db)
		}
		p.tenantsMu.RUnlock()
		for _, db := range dbs {
			if err := db.CompactHead(); err != nil && !errors.Is(err, context.Canceled) {
				level.Warn(r.logger).Log("msg", "compact head failed",
					"user", db.tenantID, "partition", db.partitionID, "err", err)
			}
		}
	}
}

// OwnedPartitions returns a snapshot of currently-owned partition IDs.
// Useful for the static-assignment dev path and for tests.
func (r *Readcache) OwnedPartitions() []int32 {
	r.partitionMu.RLock()
	defer r.partitionMu.RUnlock()
	out := make([]int32, 0, len(r.partitions))
	for pid := range r.partitions {
		out = append(out, pid)
	}
	return out
}
