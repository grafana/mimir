// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/hashcache"

	"github.com/grafana/mimir/pkg/ingester/lookupplan"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

// partitionTSDB is the readcache equivalent of pkg/ingester.userTSDB,
// scoped to a single (tenant, partition) pair.
//
// Structural differences vs the ingester's userTSDB:
//
//   - **Compaction stays on.** The normal head→block compaction loop
//     runs so the resident head doesn't grow unbounded.
//   - **Shipping is off.** No Shipper is configured; readcache never
//     uploads blocks to object storage. Blockbuilder is the canonical
//     long-term home for blocks on the experimental Kafka topic.
//   - **No active-series tracker / ownedSeries / cost attribution.**
//     Those are write-path concerns that don't apply to readcache.
//   - **TSDB head mutations are serialized** (append, runtime ApplyConfig,
//     CompactHead) so Kafka parallel ingestion cannot interleave commits
//     on the same head; the ingester achieves the same with acquireAppendLock.
type partitionTSDB struct {
	tenantID    string
	partitionID int32
	dir         string

	db *tsdb.DB

	mu     sync.RWMutex
	closed bool

	// tsdbMut serializes TSDB head mutations. The Kafka ingest path uses
	// ingest.PusherConsumer with parallel shards when
	// ingestion-concurrency-max > 0, which can call the readcache pusher
	// concurrently; Prometheus TSDB expects a single writer at a time
	// for appends (same contract as the ingester's acquireAppendLock).
	// We also hold this for CompactHead and ApplyConfig so those never
	// race with appends.
	tsdbMut sync.Mutex
}

// openPartitionTSDB opens (or creates) the on-disk TSDB at
// <data-dir>/<tenant>/partition-<id>/, with compaction enabled and
// shipping disabled. TSDB options mirror pkg/ingester.createTSDB for
// fields that affect ingest and query semantics (OOO window, exemplars,
// postings caches, isolation), using per-tenant limits from Overrides
// the same way the ingester does.
//
// localBlockRetention is the readcache-scoped time-retention applied
// to persisted blocks. It is plumbed into tsdb.Options.RetentionDuration,
// so Prometheus's standard time-retention (BeyondTimeRetention) deletes
// blocks whose MaxTime is more than localBlockRetention older than the
// newest block's MaxTime. Deletion only runs on reloadBlocks(), which
// is triggered by CompactHead — so the *effective* retention upper bound
// is roughly localBlockRetention + one HeadCompactionInterval. Pass 0
// to disable time-retention entirely (the pre-wired behavior, useful
// for tests where data should never age out).
//
// We deliberately do not pass cfg.Retention here: readcache and the
// ingester serve different lifetimes (blockbuilder is the canonical
// long-term home), and shoehorning them through the same -blocks-
// storage.tsdb.retention-period flag conflates two distinct retention
// budgets. The readcache-local knob makes the override explicit.
func openPartitionTSDB(
	tenantID string,
	partitionID int32,
	epoch int,
	rootDir string,
	cfg mimir_tsdb.TSDBConfig,
	localBlockRetention time.Duration,
	limits *validation.Overrides,
	maxExemplarsCap int,
	seriesHashCache *hashcache.SeriesHashCache,
	headPostingsForMatchersCacheFactory, blockPostingsForMatchersCacheFactory tsdb.PostingsForMatchersCacheFactory,
	tsdbPromReg prometheus.Registerer,
	logger log.Logger,
) (*partitionTSDB, error) {
	dir := partitionEpochDir(rootDir, tenantID, partitionID, epoch)

	userLogger := log.With(logger, "user", tenantID, "partition", partitionID)

	blockRanges := cfg.BlockRanges.ToMilliseconds()
	if len(blockRanges) == 0 {
		// Match the Prometheus default if the config didn't specify any.
		blockRanges = []int64{int64(2 * time.Hour / time.Millisecond)}
	}

	var oooTW time.Duration
	if limits != nil {
		oooTW = limits.OutOfOrderTimeWindow(tenantID)
		if oooTW < 0 {
			oooTW = 0
		}
	}
	maxExemplars := effectiveMaxExemplars(limits, tenantID, maxExemplarsCap)

	opts := &tsdb.Options{
		// RetentionDuration uses the readcache-local retention rather
		// than cfg.Retention (the shared ingester knob). See the
		// doc comment on openPartitionTSDB for rationale.
		RetentionDuration:                    localBlockRetention.Milliseconds(),
		MinBlockDuration:                     blockRanges[0],
		MaxBlockDuration:                     blockRanges[len(blockRanges)-1],
		NoLockfile:                           true,
		StripeSize:                           cfg.StripeSize,
		HeadChunksWriteBufferSize:            cfg.HeadChunksWriteBufferSize,
		HeadChunksEndTimeVariance:            cfg.HeadChunksEndTimeVariance,
		WALCompression:                       cfg.WALCompressionType(),
		WALSegmentSize:                       cfg.WALSegmentSizeBytes,
		WALReplayConcurrency:                 cfg.WALReplayConcurrency,
		EnableExemplarStorage:                true,
		MaxExemplars:                         maxExemplars,
		SeriesHashCache:                      seriesHashCache,
		EnableMemorySnapshotOnShutdown:       cfg.MemorySnapshotOnShutdown,
		EnableBiggerOOOBlockForOldSamples:    cfg.BiggerOutOfOrderBlocksForOldSamples,
		IsolationDisabled:                    true,
		HeadChunksWriteQueueSize:             cfg.HeadChunksWriteQueueSize,
		EnableOverlappingCompaction:          false,
		EnableSharding:                       true,
		OutOfOrderTimeWindow:                 oooTW.Milliseconds(),
		OutOfOrderCapMax:                     int64(cfg.OutOfOrderCapacityMax),
		TimelyCompaction:                     cfg.TimelyHeadCompaction,
		SharedPostingsForMatchersCache:       cfg.SharedPostingsForMatchersCache,
		PostingsForMatchersCacheKeyFunc:      tenant.TenantID,
		HeadPostingsForMatchersCacheFactory:  headPostingsForMatchersCacheFactory,
		BlockPostingsForMatchersCacheFactory: blockPostingsForMatchersCacheFactory,
		PostingsClonerFactory:                lookupplan.ActualSelectedPostingsClonerFactory{},
	}

	db, err := tsdb.Open(dir, util_log.SlogFromGoKit(userLogger), tsdbPromReg, opts, nil)
	if err != nil {
		return nil, fmt.Errorf("opening partition TSDB %q: %w", dir, err)
	}
	// Use our own compaction schedule (no automatic background
	// compactions kicked off by Prometheus). The readcache Service
	// calls CompactHead on its own ticker.
	db.DisableCompactions()

	return &partitionTSDB{
		tenantID:    tenantID,
		partitionID: partitionID,
		dir:         dir,
		db:          db,
	}, nil
}

// partitionEpochDir is the on-disk directory for a (tenant, partition,
// epoch) TSDB. Epoch 0 — the first time this pod owns the partition —
// uses the legacy "partition-<id>" path so the common single-epoch
// case is unchanged; later epochs (after a partition leaves and
// returns to the same pod) get an "-epoch-<n>" suffix so a fresh live
// TSDB never collides on disk with a still-open frozen epoch.
func partitionEpochDir(rootDir, tenantID string, partitionID int32, epoch int) string {
	name := fmt.Sprintf("partition-%d", partitionID)
	if epoch > 0 {
		name = fmt.Sprintf("partition-%d-epoch-%d", partitionID, epoch)
	}
	return filepath.Join(rootDir, tenantID, name)
}

// sampleBounds returns the inclusive [minT, maxT] sample-time span this
// TSDB currently holds across its head and persisted blocks. When the
// TSDB has no data it returns (0, -1) so that maxT < minT and any
// overlap test against a real query range is false. Used to scope
// frozen epochs at query time and to drive the absolute-wallclock
// reaper.
func (p *partitionTSDB) sampleBounds() (minT, maxT int64) {
	minT, maxT = int64(math.MaxInt64), int64(math.MinInt64)
	if h := p.db.Head(); h != nil && h.MinTime() <= h.MaxTime() {
		minT = min(minT, h.MinTime())
		maxT = max(maxT, h.MaxTime())
	}
	for _, b := range p.db.Blocks() {
		m := b.Meta()
		minT = min(minT, m.MinTime)
		maxT = max(maxT, m.MaxTime)
	}
	if minT > maxT {
		return 0, -1
	}
	return minT, maxT
}

// effectiveMaxExemplars returns the exemplar-storage capacity for one
// per-(tenant, partition) TSDB: the tenant's global limit clamped to
// maxExemplarsCap (cap <= 0 means uncapped). The global limit can't be
// used directly because Prometheus's circular exemplar storage
// preallocates its entire ring buffer at the configured capacity, and
// readcache instantiates one storage per (tenant, partition) TSDB — a
// large global limit would be preallocated in full by every open TSDB
// (live and frozen), multiplying it by the open-TSDB count. The
// resulting buffers are permanently-live, pointer-bearing heap that
// every GC mark cycle must scan; on dev-15 this alone pushed the heap
// goal past GOMEMLIMIT and locked two pods into ~30 cores of
// continuous GC. The ingester avoids the same trap by dividing the
// global limit by the ring's shard count (Limiter.maxExemplarsPerUser);
// readcache has no equivalent divisor, so it caps instead.
func effectiveMaxExemplars(limits *validation.Overrides, tenantID string, maxExemplarsCap int) int64 {
	if limits == nil {
		return 0
	}
	maxExemplars := int64(limits.MaxGlobalExemplarsPerUser(tenantID))
	if maxExemplars < 0 {
		maxExemplars = 0
	}
	if maxExemplarsCap > 0 && maxExemplars > int64(maxExemplarsCap) {
		maxExemplars = int64(maxExemplarsCap)
	}
	return maxExemplars
}

// applyTenantTSDBSettings reapplies per-tenant TSDB settings from runtime
// limits (mirrors ingester.applyTSDBSettings). maxExemplarsCap must be
// the same cap passed to openPartitionTSDB, or the periodic reapply
// would resize the exemplar ring back up to the tenant's global limit.
func (p *partitionTSDB) applyTenantTSDBSettings(limits *validation.Overrides, maxExemplarsCap int, logger log.Logger) error {
	if limits == nil || p.db == nil {
		return nil
	}
	p.tsdbMut.Lock()
	defer p.tsdbMut.Unlock()

	oooTW := limits.OutOfOrderTimeWindow(p.tenantID)
	if oooTW < 0 {
		oooTW = 0
	}
	cfg := config.Config{
		StorageConfig: config.StorageConfig{
			ExemplarsConfig: &config.ExemplarsConfig{
				MaxExemplars: effectiveMaxExemplars(limits, p.tenantID, maxExemplarsCap),
			},
			TSDBConfig: &config.TSDBConfig{
				OutOfOrderTimeWindow: oooTW.Milliseconds(),
			},
		},
	}
	if err := p.db.ApplyConfig(&cfg); err != nil {
		level.Error(logger).Log("msg", "failed to apply config to readcache partition TSDB", "user", p.tenantID, "partition", p.partitionID, "err", err)
		return err
	}
	return nil
}

// Appender returns a fresh Appender for ingesting samples into this
// partition's head. The caller is responsible for Commit / Rollback.
func (p *partitionTSDB) Appender(ctx context.Context) storage.Appender {
	return p.db.Appender(ctx)
}

// Querier returns a Querier covering [mint, maxt].
func (p *partitionTSDB) Querier(mint, maxt int64) (storage.Querier, error) {
	return p.db.Querier(mint, maxt)
}

// ChunkQuerier returns a ChunkQuerier covering [mint, maxt].
func (p *partitionTSDB) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return p.db.ChunkQuerier(mint, maxt)
}

// UnorderedChunkQuerier returns an unordered ChunkQuerier.
func (p *partitionTSDB) UnorderedChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return p.db.UnorderedChunkQuerier(mint, maxt)
}

// ExemplarQuerier returns an ExemplarQuerier.
func (p *partitionTSDB) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return p.db.ExemplarQuerier(ctx)
}

// Head returns the in-memory head.
func (p *partitionTSDB) Head() *tsdb.Head {
	return p.db.Head()
}

// Blocks returns the currently-loaded persisted blocks.
func (p *partitionTSDB) Blocks() []*tsdb.Block {
	return p.db.Blocks()
}

// CompactHead compacts the in-memory head into a block on disk.
// Unlike the ingester, blocks stay local; no shipper picks them up.
func (p *partitionTSDB) CompactHead() error {
	p.tsdbMut.Lock()
	defer p.tsdbMut.Unlock()

	h := p.db.Head()
	return p.db.CompactHead(tsdb.NewRangeHead(h, h.MinTime(), h.MaxTime()))
}

// Close shuts down the TSDB. Idempotent.
func (p *partitionTSDB) Close() error {
	// Wait for in-flight appends / compaction / ApplyConfig before closing.
	p.tsdbMut.Lock()
	defer p.tsdbMut.Unlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	if err := p.db.Close(); err != nil {
		level.Warn(util_log.Logger).Log("msg", "error closing partition TSDB",
			"user", p.tenantID, "partition", p.partitionID, "err", err)
		return err
	}
	return nil
}

// IsClosed reports whether Close has been called.
func (p *partitionTSDB) IsClosed() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.closed
}

// Dir returns the on-disk directory for this partition TSDB.
func (p *partitionTSDB) Dir() string {
	return p.dir
}
