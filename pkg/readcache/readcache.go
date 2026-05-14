// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/nautilus/loadstats"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
	"github.com/grafana/mimir/pkg/nautilus/rebalancer"
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
	// instance, keyed by partition ID. Populated by the rebalancer's
	// WatchReadcacheAssignments stream in production, or by the
	// legacy static -readcache.owned-partitions flag when the
	// rebalancer address is unset (tests / degraded mode).
	partitions map[int32]*partitionState

	// rebalancerConn is the gRPC connection used by the
	// WatchReadcacheAssignments subscriber. Nil when
	// RebalancerAddress is empty.
	rebalancerConn *grpc.ClientConn

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

	if r.cfg.RebalancerAddress != "" {
		conn, err := r.dialRebalancer(ctx)
		if err != nil {
			return fmt.Errorf("dialing nautilus rebalancer %q: %w", r.cfg.RebalancerAddress, err)
		}
		r.rebalancerConn = conn
		level.Info(r.logger).Log(
			"msg", "readcache started; awaiting first assignment snapshot",
			"instance_id", r.cfg.InstanceID,
			"rebalancer", r.cfg.RebalancerAddress,
			"kafka_topic", r.cfg.KafkaTopic,
		)
		// Don't seed from OwnedPartitions: the rebalancer is
		// authoritative. The watch goroutine (started by running)
		// will issue addPartition/removePartition calls as snapshots
		// arrive.
		return nil
	}

	// Legacy fallback: no rebalancer address configured (tests or
	// degraded-mode bring-up). Honour the static OwnedPartitions
	// flag so existing single-node setups keep working.
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
		"msg", "readcache started with static partition assignment",
		"instance_id", r.cfg.InstanceID,
		"owned_partitions", len(pids),
		"kafka_topic", r.cfg.KafkaTopic,
	)
	return nil
}

// dialRebalancer opens a gRPC connection to the nautilus rebalancer.
// Mirrors the distributor's dial behaviour
// (insecure, blocking, X-Scope-OrgID interceptor injected via
// per-RPC metadata in the watch goroutine).
func (r *Readcache) dialRebalancer(ctx context.Context) (*grpc.ClientConn, error) {
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}
	// nolint:staticcheck // grpc.DialContext() has been deprecated; we'll address it before upgrading to gRPC 2.
	return grpc.DialContext(ctx, r.cfg.RebalancerAddress, dialOpts...)
}

func (r *Readcache) running(ctx context.Context) error {
	// Per-partition tickers for head compaction. Aggregated to one
	// shared goroutine to avoid one goroutine per partition; the
	// granularity (an hour by default) makes shared scheduling fine.
	compactT := time.NewTicker(r.cfg.HeadCompactionInterval)
	defer compactT.Stop()

	loadStatsTickT := time.NewTicker(loadstats.TickInterval)
	defer loadStatsTickT.Stop()

	// When configured, subscribe to the rebalancer's
	// WatchReadcacheAssignments stream and react to ownership
	// changes by add/removing partitions. The goroutine returns
	// when ctx is cancelled.
	if r.rebalancerConn != nil {
		go r.watchReadcacheAssignments(ctx)
	}

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
	var firstErr error
	for pid, p := range r.partitions {
		if err := r.stopPartitionLocked(pid, p); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	r.partitions = map[int32]*partitionState{}
	r.partitionMu.Unlock()

	if r.rebalancerConn != nil {
		if err := r.rebalancerConn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		r.rebalancerConn = nil
	}
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
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// watchReadcacheAssignments runs a long-lived
// WatchReadcacheAssignments subscription against the rebalancer.
// Snapshots are passed to applyAssignment, which converts them into
// add/removePartition calls. Mirrors the distributor-side
// watchReadcacheAssignments backoff loop so a rebalancer outage is
// transparently survived.
func (r *Readcache) watchReadcacheAssignments(ctx context.Context) {
	const (
		minBackoff = 250 * time.Millisecond
		maxBackoff = 8 * time.Second
	)
	backoff := minBackoff

	cli := rebalancer.NewNautilusRebalancerClient(r.rebalancerConn)

	for ctx.Err() == nil {
		stream, err := cli.WatchReadcacheAssignments(ctx, &rebalancer.WatchReadcacheAssignmentsRequest{})
		if err != nil {
			level.Warn(r.logger).Log("msg", "failed to open WatchReadcacheAssignments stream", "err", err, "backoff", backoff)
			sleepWithCtx(ctx, backoff)
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}
		backoff = minBackoff

		if err := r.consumeAssignmentStream(ctx, stream); err != nil && ctx.Err() == nil {
			if !errors.Is(err, io.EOF) {
				level.Warn(r.logger).Log("msg", "WatchReadcacheAssignments stream ended", "err", err, "backoff", backoff)
			}
		}
		if ctx.Err() != nil {
			return
		}
		sleepWithCtx(ctx, backoff)
		backoff = nextBackoff(backoff, maxBackoff)
	}
}

func (r *Readcache) consumeAssignmentStream(ctx context.Context, stream rebalancer.NautilusRebalancer_WatchReadcacheAssignmentsClient) error {
	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		entries := rebalancer.ReadcacheEntriesFromProto(resp.Entries)
		if err := r.applyAssignment(ctx, entries, time.Now()); err != nil {
			level.Warn(r.logger).Log("msg", "applying readcache assignment", "err", err)
			// Keep consuming: a single failed add/remove must not
			// take down the whole subscription. The next snapshot
			// will reconcile.
		}
	}
}

// applyAssignment reconciles the local owned-partition set with the
// authoritative snapshot from the rebalancer. Partitions whose
// active lease names this instance are added if missing; partitions
// not in the snapshot are removed.
//
// `at` is the wall-clock instant used to determine which leases are
// currently active. Pre-issued future leases (From > at) are not
// considered ours yet; expired leases (To <= at) are not considered
// ours anymore. This mirrors the readcacheassignment.Log.Lookup
// semantics the distributor uses on the read path, so add/remove
// transitions happen at the same instant on both sides.
func (r *Readcache) applyAssignment(ctx context.Context, entries []readcacheassignment.LogEntry, at time.Time) error {
	wanted := map[int32]struct{}{}
	for _, e := range entries {
		if e.InstanceID != r.cfg.InstanceID {
			continue
		}
		if !e.ActiveAt(at) {
			continue
		}
		wanted[e.PartitionID] = struct{}{}
	}

	r.partitionMu.RLock()
	current := make(map[int32]struct{}, len(r.partitions))
	for pid := range r.partitions {
		current[pid] = struct{}{}
	}
	r.partitionMu.RUnlock()

	var toAdd, toRemove []int32
	for pid := range wanted {
		if _, ok := current[pid]; !ok {
			toAdd = append(toAdd, pid)
		}
	}
	for pid := range current {
		if _, ok := wanted[pid]; !ok {
			toRemove = append(toRemove, pid)
		}
	}
	sort.Slice(toAdd, func(i, j int) bool { return toAdd[i] < toAdd[j] })
	sort.Slice(toRemove, func(i, j int) bool { return toRemove[i] < toRemove[j] })

	var firstErr error
	for _, pid := range toAdd {
		if err := r.addPartition(ctx, pid); err != nil {
			level.Warn(r.logger).Log("msg", "failed to add partition from rebalancer assignment",
				"partition", pid, "err", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
	}
	for _, pid := range toRemove {
		if err := r.removePartition(pid); err != nil {
			level.Warn(r.logger).Log("msg", "failed to remove partition from rebalancer assignment",
				"partition", pid, "err", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
	}
	if len(toAdd) > 0 || len(toRemove) > 0 {
		level.Info(r.logger).Log("msg", "readcache reconciled partition assignment",
			"added", len(toAdd), "removed", len(toRemove), "owned", len(wanted))
	}
	return firstErr
}

// sleepWithCtx sleeps for d unless ctx is cancelled first.
func sleepWithCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// nextBackoff doubles d up to maxBackoff.
func nextBackoff(d, max time.Duration) time.Duration {
	d *= 2
	if d > max {
		return max
	}
	return d
}
