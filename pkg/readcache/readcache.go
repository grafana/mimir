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
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/ingester/lookupplan"
	"github.com/grafana/mimir/pkg/nautilus/loadstats"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
	"github.com/grafana/mimir/pkg/nautilus/rebalancer"
	"github.com/grafana/mimir/pkg/storage/ingest"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
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

	// spotlights is the readcache's local cache of the rebalancer's
	// spotlighted hash ranges. Populated by a background poller
	// (runSpotlightLoop) and read on the spotlight emission tick.
	// Always non-nil after New.
	spotlights *readcacheSpotlightTracker

	// instanceLifecycler registers this readcache pod in the
	// service-discovery ring (KV key "readcache"). Nil when the
	// caller of New didn't pass one (e.g. unit tests that don't
	// care about ring registration).
	instanceLifecycler *ring.BasicLifecycler

	// queryLoad and partitionSeries are the per-partition query-load
	// and per-partition active-series signals that the rebalancer
	// pulls via HashRangeStats. Per-hash-range counts live on each
	// partitionState (see partitionState.ranges) so residue stays
	// attributed to the partition that has it. They live on readcache
	// (not ingester) per the plan.
	queryLoad       *loadstats.Tracker
	partitionSeries *loadstats.PartitionSeries

	// samplesIngestedTotal counts float and native-histogram samples
	// pulled from the Kafka topic and successfully appended to a
	// partition's TSDB head. Labelled by Kafka partition ID. Use
	// rate() over this counter to get samples/sec per partition; the
	// readcache counterpart to
	// cortex_distributor_nautilus_partition_samples_written_total on
	// the producer side.
	samplesIngestedTotal *prometheus.CounterVec

	// seriesWalkMu prevents overlapping background series walks when a
	// single tick takes longer than loadstats.TickInterval.
	seriesWalkMu sync.Mutex

	// seriesHashCache and postings-for-matchers cache factories mirror
	// the ingester: shared across partition TSDBs on this process.
	seriesHashCache                      *hashcache.SeriesHashCache
	headPostingsForMatchersCacheFactory  tsdb.PostingsForMatchersCacheFactory
	blockPostingsForMatchersCacheFactory tsdb.PostingsForMatchersCacheFactory

	// pushErrSamplers rate-limits logging of soft append errors on the Kafka path,
	// matching the ingester push pipeline (ingester.PushWriteRequestTimeseries).
	pushErrSamplers ingester.IngesterErrSamplers

	// tsdbMetrics aggregates per-(tenant, partition) TSDB registry metrics
	// (e.g. cortex_readcache_memory_series), mirroring the ingester's TSDBMetrics.
	tsdbMetrics *mimir_tsdb.TSDBMetrics

	// stopPartitionHook is a test-only seam fired at the top of
	// stopPartition. It lets tests assert the shutdown fan-out
	// invariants (no partitionMu held while stopPartition runs;
	// per-partition stops actually overlap). Always nil in
	// production; a non-nil setter would be a code smell. Tests
	// install hooks before calling services.StopAndAwaitTerminated.
	stopPartitionHook func(partitionID int32)

	// frozen holds read-only epoch TSDBs for partitions this pod no
	// longer actively owns. After a partition->readcache move the
	// previous owner stops consuming but keeps the slice it ingested
	// queryable (a "frozen epoch") so a query whose interval spans
	// the move still finds the pre-move data on the previous owner.
	// The absolute-wallclock reaper drops an epoch once its newest
	// sample is older than LocalBlockRetention. Keyed by partition
	// ID; a partition may have several frozen epochs if it churned
	// on and off this pod.
	frozenMu sync.RWMutex
	frozen   map[int32][]*frozenEpoch

	// epochSeq tracks the next epoch number to hand out per partition
	// (0 on first acquisition). Mutated only in addPartition under
	// partitionMu.
	epochSeq map[int32]int
}

// partitionState bundles the per-partition Kafka reader and the
// per-tenant heads keyed by tenant ID.
type partitionState struct {
	partitionID int32

	// epoch is the per-partition acquisition generation on this pod.
	// It is assigned when the pod gains the partition (addPartition)
	// and is used as the on-disk TSDB directory suffix so a fresh
	// live TSDB never collides with a still-open frozen epoch of the
	// same partition after it leaves and returns. Epoch 0 is the
	// first acquisition and uses the legacy directory layout.
	epoch int

	// reader is the Kafka consumer driving samples into this
	// partition. Nil until startKafkaReader is called.
	reader *ingest.PartitionReader

	// readerMetrics is the Collector actually registered on the main
	// Mimir registry: a *frozenDescCollector wrapping the per-partition
	// prometheus.Registry that the PartitionReader writes its own
	// counters/histograms into. We keep it on partitionState so that
	// on partition removal we can unregister it from the main Mimir
	// registry; this is what lets the rebalancer move a partition off
	// and back onto the same readcache pod without panicking on
	// duplicate metric registrations. See newFrozenDescCollector and
	// startKafkaReader for the rationale behind wrapping the inner
	// Registry instead of registering it directly.
	readerMetrics prometheus.Collector

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

	// ranges is the per-partition hash-range bookkeeping that feeds
	// the rebalancer's load signal. SetHashRanges updates
	// ranges.currentRanges and migrates dropped ranges into
	// ranges.historicalRanges so the walker keeps counting head
	// residue until compaction clears it. Each walker tick updates
	// ranges.rangeCounts from this partition's TSDB heads.
	ranges *partitionRanges
}

func newPartitionState(partitionID int32) *partitionState {
	return &partitionState{
		partitionID: partitionID,
		tenants:     make(map[string]*partitionTSDB),
		ranges:      newPartitionRanges(),
	}
}

// New constructs a Readcache. Wiring (Kafka readers, ring registration,
// rebalancer subscription) happens in starting().
//
// instanceLifecycler may be nil; when supplied, the readcache starts
// it as a subservice so the pod joins the readcache ring (KV key
// "readcache") and the rebalancer / distributor can discover this
// instance without static config.
func New(
	cfg Config,
	limits *validation.Overrides,
	instanceLifecycler *ring.BasicLifecycler,
	logger log.Logger,
	reg prometheus.Registerer,
) (*Readcache, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating readcache config: %w", err)
	}

	tsdbCfg := cfg.BlocksStorage.TSDB
	r := &Readcache{
		cfg:                cfg,
		limits:             limits,
		logger:             logger,
		reg:                reg,
		partitions:         make(map[int32]*partitionState),
		frozen:             make(map[int32][]*frozenEpoch),
		epochSeq:           make(map[int32]int),
		instanceLifecycler: instanceLifecycler,
		queryLoad:          loadstats.NewTracker("cortex_readcache"),
		partitionSeries:    loadstats.NewPartitionSeries(),
		spotlights:         newReadcacheSpotlightTracker(),
	}

	r.seriesHashCache = hashcache.NewSeriesHashCache(tsdbCfg.SeriesHashCacheMaxBytes)

	var headPostingsMetrics *tsdb.PostingsForMatchersCacheMetrics
	var blockPostingsMetrics *tsdb.PostingsForMatchersCacheMetrics
	if reg != nil {
		headPostingsMetrics = tsdb.NewPostingsForMatchersCacheMetrics(prometheus.WrapRegistererWithPrefix("cortex_readcache_tsdb_head_", reg))
		blockPostingsMetrics = tsdb.NewPostingsForMatchersCacheMetrics(prometheus.WrapRegistererWithPrefix("cortex_readcache_tsdb_block_", reg))
	} else {
		headPostingsMetrics = tsdb.NewPostingsForMatchersCacheMetrics(nil)
		blockPostingsMetrics = tsdb.NewPostingsForMatchersCacheMetrics(nil)
	}
	r.headPostingsForMatchersCacheFactory = tsdb.NewPostingsForMatchersCacheFactory(tsdb.PostingsForMatchersCacheConfig{
		Shared:                tsdbCfg.SharedPostingsForMatchersCache,
		KeyFunc:               tenant.TenantID,
		Invalidation:          tsdbCfg.HeadPostingsForMatchersCacheInvalidation,
		CacheVersions:         tsdbCfg.HeadPostingsForMatchersCacheVersions,
		TTL:                   tsdbCfg.HeadPostingsForMatchersCacheTTL,
		MaxItems:              tsdbCfg.HeadPostingsForMatchersCacheMaxItems,
		MaxBytes:              tsdbCfg.HeadPostingsForMatchersCacheMaxBytes,
		Force:                 tsdbCfg.HeadPostingsForMatchersCacheForce,
		Metrics:               headPostingsMetrics,
		PostingsClonerFactory: lookupplan.ActualSelectedPostingsClonerFactory{},
	})
	r.blockPostingsForMatchersCacheFactory = tsdb.NewPostingsForMatchersCacheFactory(tsdb.PostingsForMatchersCacheConfig{
		Shared:                tsdbCfg.SharedPostingsForMatchersCache,
		KeyFunc:               tenant.TenantID,
		Invalidation:          false,
		CacheVersions:         0,
		TTL:                   tsdbCfg.BlockPostingsForMatchersCacheTTL,
		MaxItems:              tsdbCfg.BlockPostingsForMatchersCacheMaxItems,
		MaxBytes:              tsdbCfg.BlockPostingsForMatchersCacheMaxBytes,
		Force:                 tsdbCfg.BlockPostingsForMatchersCacheForce,
		Metrics:               blockPostingsMetrics,
		PostingsClonerFactory: lookupplan.ActualSelectedPostingsClonerFactory{},
	})

	r.pushErrSamplers = ingester.NewIngesterErrSamplers(10)

	r.samplesIngestedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_readcache_samples_ingested_total",
		Help: "Total float and native-histogram samples successfully appended to a partition's TSDB head from the Kafka ingest topic, labelled by Kafka partition ID. Use rate() to get samples/sec per partition. Readcache counterpart to cortex_distributor_nautilus_partition_samples_written_total.",
	}, []string{"partition"})

	if reg != nil {
		r.tsdbMetrics = mimir_tsdb.NewTSDBMetrics(prometheus.WrapRegistererWithPrefix("cortex_readcache_", reg), logger)
		reg.MustRegister(r.queryLoad, r.samplesIngestedTotal)
	}

	r.Service = services.NewBasicService(r.starting, r.running, r.stopping)
	return r, nil
}

func (r *Readcache) starting(ctx context.Context) error {
	if r.cfg.WipeTSDBDirOnStartup {
		level.Warn(r.logger).Log("msg", "wiping readcache data directory on startup as configured", "dir", r.cfg.DataDir)
		if err := os.RemoveAll(r.cfg.DataDir); err != nil {
			return fmt.Errorf("wiping readcache data directory on startup: %w", err)
		}
	}

	if err := os.MkdirAll(r.cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("creating readcache data-dir %q: %w", r.cfg.DataDir, err)
	}

	// Register in the readcache ring BEFORE subscribing to the
	// rebalancer: the rebalancer's slicer enumerates the ring for
	// eligible instances, so any partition this pod is meant to own
	// must wait until our ring entry is visible to the slicer.
	// Conversely, the WatchReadcacheAssignments stream we open below
	// is the channel through which the slicer's choice reaches us,
	// so it must come after the lifecycler is running.
	if r.instanceLifecycler != nil {
		if err := services.StartAndAwaitRunning(ctx, r.instanceLifecycler); err != nil {
			return fmt.Errorf("starting readcache instance lifecycler: %w", err)
		}
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

	tsdbUpdateT := time.NewTicker(r.cfg.TSDBConfigUpdatePeriod)
	defer tsdbUpdateT.Stop()

	loadStatsTickT := time.NewTicker(loadstats.TickInterval)
	defer loadStatsTickT.Stop()

	reapT := time.NewTicker(frozenEpochReapInterval)
	defer reapT.Stop()

	// When configured, subscribe to the rebalancer's
	// WatchReadcacheAssignments stream and react to ownership
	// changes by add/removing partitions. The goroutine returns
	// when ctx is cancelled.
	if r.rebalancerConn != nil {
		go r.watchReadcacheAssignments(ctx)
		go r.runSpotlightLoop(ctx)
	}

	// Prime partition/hash-range snapshots so the first HashRangeStats
	// RPC does not return zeros while waiting for the first tick.
	go r.refreshSeriesStats(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-compactT.C:
			r.compactHeads()
		case <-tsdbUpdateT.C:
			r.applyPartitionTSDBTenantSettings()
		case <-loadStatsTickT.C:
			r.queryLoad.Tick()
			r.tickSampleRates()
			go r.refreshSeriesStats(ctx)
		case <-reapT.C:
			r.reapFrozenEpochs(time.Now())
		}
	}
}

// shutdownStopConcurrency caps the fan-out used to tear down owned
// partitions during stopping(). The work per partition is mostly
// IO-bound (Kafka client close, in-flight fetch flush, per-tenant
// TSDB Close), so a moderate fan-out is enough to overlap most of
// the wait time without overwhelming the Kafka client or disk. The
// value is deliberately a constant (rather than NumCPU or a config
// flag) because the bottleneck is wall-clock on external IO, not
// goroutine budget; raising it further has diminishing returns.
const shutdownStopConcurrency = 16

func (r *Readcache) stopping(_ error) error {
	// Snapshot the owned partitions under the write lock and clear
	// the map BEFORE doing any blocking teardown. Two reasons:
	//
	//  1. The Kafka reader's stop path waits for in-flight
	//     partitionPusher.PushToStorageAndReleaseRequest calls to
	//     finish. Those calls call getOrOpenTSDB, which RLocks
	//     partitionMu. Holding the write lock across stop deadlocks
	//     the pusher against ourselves, with no upper bound — the
	//     reader's consume loop uses context.WithoutCancel so even
	//     ctx cancellation does not unwedge it. Dropping the lock
	//     before stop lets the in-flight pushers observe the empty
	//     map, short-circuit via the `db == nil` branch in
	//     PushToStorageAndReleaseRequest, and unblock the reader.
	//
	//  2. Clearing the map first means new pushers (e.g. those that
	//     arrived between the moment we entered stopping() and the
	//     moment we got the lock) will see the partition gone and
	//     drop the batch gracefully via that same nil-DB branch.
	r.partitionMu.Lock()
	parts := make([]partitionEntry, 0, len(r.partitions))
	for pid, p := range r.partitions {
		parts = append(parts, partitionEntry{partitionID: pid, state: p})
	}
	r.partitions = map[int32]*partitionState{}
	r.partitionMu.Unlock()

	// Fan out the per-partition stop. Each partition's teardown is
	// independent: it owns its Kafka reader and its per-tenant TSDB
	// map, neither shared with peers. Using errgroup.SetLimit caps
	// concurrency to shutdownStopConcurrency so a pod owning N
	// partitions doesn't try to open N concurrent Kafka client
	// shutdowns at once.
	var firstErr error
	var firstErrMu sync.Mutex
	g := new(errgroup.Group)
	g.SetLimit(shutdownStopConcurrency)
	for _, entry := range parts {
		entry := entry
		g.Go(func() error {
			if err := r.stopPartition(entry.partitionID, entry.state); err != nil {
				firstErrMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				firstErrMu.Unlock()
			}
			return nil
		})
	}
	_ = g.Wait()

	// Close any frozen epoch TSDBs we were still serving. They have no
	// reader to stop, so a plain Close suffices.
	r.frozenMu.Lock()
	frozenByPartition := r.frozen
	r.frozen = map[int32][]*frozenEpoch{}
	r.frozenMu.Unlock()
	for _, eps := range frozenByPartition {
		for _, ep := range eps {
			for tenant, db := range ep.tenants {
				if r.tsdbMetrics != nil {
					r.tsdbMetrics.RemoveRegistryForTenant(tsdbMetricsTenantID(tenant, ep.partitionID))
				}
				if err := db.Close(); err != nil && firstErr == nil {
					firstErr = err
				}
			}
		}
	}

	if r.rebalancerConn != nil {
		if err := r.rebalancerConn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		r.rebalancerConn = nil
	}

	// Tear the lifecycler down last so the rebalancer's slicer can
	// still see us as ACTIVE while we drain partitions above. The
	// LeaveOnStopping delegate inserts the UNREGISTER state into the
	// ring so a clean shutdown removes the entry rather than waiting
	// for heartbeat timeout.
	if r.instanceLifecycler != nil {
		if err := services.StopAndAwaitTerminated(context.Background(), r.instanceLifecycler); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// partitionEntry pairs a partition ID with its state for the
// shutdown snapshot. Kept as a local type so the snapshot slice is
// allocation-cheap and self-documenting; we can't iterate the live
// r.partitions map outside the lock.
type partitionEntry struct {
	partitionID int32
	state       *partitionState
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
	// Assign this acquisition's epoch (0 on the first time this pod
	// owns the partition; incremented on each re-acquisition) so a
	// fresh live TSDB never collides on disk with a frozen epoch of
	// the same partition still being served.
	p.epoch = r.epochSeq[partitionID]
	r.epochSeq[partitionID]++
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

// removePartition stops actively owning a partition: it shuts down
// the Kafka reader but, instead of closing the per-tenant TSDBs,
// freezes them as a read-only epoch (see freezePartition) so the
// slice this pod ingested while it owned the partition stays
// queryable until the reaper drops it. Idempotent.
//
// The partition is deleted from r.partitions BEFORE the reader stop
// blocks for in-flight pushes. Holding partitionMu across the reader
// stop would deadlock against the pusher: the reader's consume loop
// (in pkg/storage/ingest/reader.go) uses context.WithoutCancel so the
// in-flight call cannot be interrupted, and that call routes through
// getOrOpenTSDB which RLocks partitionMu. Once the partition is gone
// from the map, in-flight pushers observe `db == nil` from
// getOrOpenTSDB and short-circuit, unblocking the reader stop.
func (r *Readcache) removePartition(partitionID int32) error {
	r.partitionMu.Lock()
	p, ok := r.partitions[partitionID]
	if ok {
		delete(r.partitions, partitionID)
	}
	r.partitionMu.Unlock()
	if !ok {
		return nil
	}
	return r.freezePartition(partitionID, p)
}

// stopPartition closes a partition's resources. The caller must
// guarantee p is no longer reachable through r.partitions (so no
// other goroutine will start a fresh reader or open a new TSDB on
// it) before invoking this. removePartition and stopping() satisfy
// that by deleting from r.partitions under partitionMu first; this
// function then runs WITHOUT holding partitionMu, which is required
// to avoid deadlocking the Kafka reader's stop path against in-flight
// pusher goroutines (see removePartition for details).
//
// Tear-down order: stop the Kafka reader first so no more samples are
// arriving, then close the per-tenant TSDBs. Once the reader has
// returned from StopAndAwaitTerminated, no goroutine is calling
// PushToStorageAndReleaseRequest for this partition, so taking
// p.tenantsMu and Closing the TSDBs is safe.
func (r *Readcache) stopPartition(partitionID int32, p *partitionState) error {
	if hook := r.stopPartitionHook; hook != nil {
		hook(partitionID)
	}
	var firstErr error
	if err := r.stopKafkaReaderLocked(p); err != nil {
		firstErr = err
		level.Warn(r.logger).Log("msg", "stopping partition reader", "partition", partitionID, "err", err)
	}

	p.tenantsMu.Lock()
	defer p.tenantsMu.Unlock()

	for tenant, db := range p.tenants {
		if r.tsdbMetrics != nil {
			r.tsdbMetrics.RemoveRegistryForTenant(tsdbMetricsTenantID(tenant, partitionID))
		}
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

	tsdbPromReg := prometheus.NewRegistry()
	opened, err := openPartitionTSDB(
		tenantID,
		partitionID,
		p.epoch,
		r.cfg.DataDir,
		r.cfg.BlocksStorage.TSDB,
		r.cfg.LocalBlockRetention,
		r.limits,
		r.seriesHashCache,
		r.headPostingsForMatchersCacheFactory,
		r.blockPostingsForMatchersCacheFactory,
		tsdbPromReg,
		r.logger,
	)
	if err != nil {
		return nil, err
	}
	if r.tsdbMetrics != nil {
		r.tsdbMetrics.SetRegistryForTenant(tsdbMetricsTenantID(tenantID, partitionID), tsdbPromReg)
	}
	p.tenants[tenantID] = opened
	return opened, nil
}

// tsdbMetricsTenantID is the key used in TSDBMetrics for a (tenant, partition) head.
func tsdbMetricsTenantID(tenantID string, partitionID int32) string {
	return fmt.Sprintf("%s/%d", tenantID, partitionID)
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
	var out []*partitionTSDB

	// Live partitions this pod currently owns.
	r.partitionMu.RLock()
	if hint != nil {
		if p, ok := r.partitions[hint.PartitionId]; ok {
			if !p.warm.Load() {
				r.partitionMu.RUnlock()
				return nil, errStillWarming(hint.PartitionId)
			}
			p.tenantsMu.RLock()
			if db := p.tenants[tenantID]; db != nil {
				out = append(out, db)
			}
			p.tenantsMu.RUnlock()
		}
	} else {
		for _, p := range r.partitions {
			if !p.warm.Load() {
				r.partitionMu.RUnlock()
				return nil, errStillWarming(p.partitionID)
			}
			p.tenantsMu.RLock()
			if db := p.tenants[tenantID]; db != nil {
				out = append(out, db)
			}
			p.tenantsMu.RUnlock()
		}
	}
	r.partitionMu.RUnlock()

	// Frozen epochs from prior ownership stints of these partitions.
	// They are read-only and disjoint in identity from the live TSDBs
	// (distinct on-disk directories), so adding them can never
	// duplicate a live head. The per-querier [from,through] range
	// applied downstream scopes each frozen epoch to the sample-time
	// window it actually overlaps, so non-overlapping epochs return
	// no series. Acquired after partitionMu is released to keep a
	// strict lock order (partitionMu -> frozenMu) free of nesting.
	r.frozenMu.RLock()
	addFrozen := func(partitionID int32) {
		for _, ep := range r.frozen[partitionID] {
			if db := ep.tenants[tenantID]; db != nil {
				out = append(out, db)
			}
		}
	}
	if hint != nil {
		addFrozen(hint.PartitionId)
	} else {
		for partitionID := range r.frozen {
			addFrozen(partitionID)
		}
	}
	r.frozenMu.RUnlock()

	return out, nil
}

func (r *Readcache) applyPartitionTSDBTenantSettings() {
	if r.limits == nil {
		return
	}
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
			_ = db.applyTenantTSDBSettings(r.limits, r.logger)
		}
	}
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
	var snapshotForInstance int
	for _, e := range entries {
		if e.InstanceID != r.cfg.InstanceID {
			continue
		}
		snapshotForInstance++
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

	level.Info(r.logger).Log(
		"msg", "readcache assignment snapshot received",
		"instance_id", r.cfg.InstanceID,
		"snapshot_entries", len(entries),
		"entries_for_instance", snapshotForInstance,
		"wanted_partitions", len(wanted),
		"current_partitions", len(current),
		"to_add", len(toAdd),
		"to_remove", len(toRemove),
		"wanted_partition_ids", formatPartitionIDs(partitionIDsFromSet(wanted), 20),
		"add_partition_ids", formatPartitionIDs(toAdd, 20),
		"remove_partition_ids", formatPartitionIDs(toRemove, 20),
	)

	var firstErr error
	var addFailed, removeFailed []int32
	for _, pid := range toAdd {
		if err := r.addPartition(ctx, pid); err != nil {
			level.Warn(r.logger).Log("msg", "failed to add partition from rebalancer assignment",
				"partition", pid, "err", err)
			addFailed = append(addFailed, pid)
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
			removeFailed = append(removeFailed, pid)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
	}
	if len(toAdd) > 0 || len(toRemove) > 0 {
		// owned reports len(r.partitions) after the add/remove fan-out
		// rather than len(wanted) so the log doesn't lie when an add
		// fails (e.g. the metric-registry collision the partition
		// lifecycle used to leak): wanted is the goal, owned is the
		// reality. add_failed / remove_failed surface the gap so an
		// operator can see at a glance whether the reconcile achieved
		// what the rebalancer asked for.
		r.partitionMu.RLock()
		ownedNow := len(r.partitions)
		r.partitionMu.RUnlock()
		level.Info(r.logger).Log("msg", "readcache reconciled partition assignment",
			"added", len(toAdd)-len(addFailed),
			"removed", len(toRemove)-len(removeFailed),
			"add_failed", len(addFailed),
			"remove_failed", len(removeFailed),
			"wanted", len(wanted),
			"owned", ownedNow,
			"added_partition_ids", formatPartitionIDs(toAdd, 20),
			"removed_partition_ids", formatPartitionIDs(toRemove, 20),
			"add_failed_partition_ids", formatPartitionIDs(addFailed, 20),
			"remove_failed_partition_ids", formatPartitionIDs(removeFailed, 20),
		)
	}
	return firstErr
}

func partitionIDsFromSet(set map[int32]struct{}) []int32 {
	out := make([]int32, 0, len(set))
	for pid := range set {
		out = append(out, pid)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
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
