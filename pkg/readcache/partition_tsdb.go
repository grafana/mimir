// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
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
type partitionTSDB struct {
	tenantID    string
	partitionID int32
	dir         string

	db *tsdb.DB

	mu     sync.RWMutex
	closed bool
}

// openPartitionTSDB opens (or creates) the on-disk TSDB at
// <data-dir>/<tenant>/partition-<id>/, with compaction enabled and
// shipping disabled. TSDB options mirror pkg/ingester.createTSDB for
// fields that affect ingest and query semantics (OOO window, exemplars,
// postings caches, isolation), using per-tenant limits from Overrides
// the same way the ingester does.
func openPartitionTSDB(
	tenantID string,
	partitionID int32,
	rootDir string,
	cfg mimir_tsdb.TSDBConfig,
	limits *validation.Overrides,
	seriesHashCache *hashcache.SeriesHashCache,
	headPostingsForMatchersCacheFactory, blockPostingsForMatchersCacheFactory tsdb.PostingsForMatchersCacheFactory,
	logger log.Logger,
) (*partitionTSDB, error) {
	dir := filepath.Join(rootDir, tenantID, fmt.Sprintf("partition-%d", partitionID))

	userLogger := log.With(logger, "user", tenantID, "partition", partitionID)

	blockRanges := cfg.BlockRanges.ToMilliseconds()
	if len(blockRanges) == 0 {
		// Match the Prometheus default if the config didn't specify any.
		blockRanges = []int64{int64(2 * time.Hour / time.Millisecond)}
	}

	var oooTW time.Duration
	var maxExemplars int64
	if limits != nil {
		oooTW = limits.OutOfOrderTimeWindow(tenantID)
		if oooTW < 0 {
			oooTW = 0
		}
		maxExemplars = int64(limits.MaxGlobalExemplarsPerUser(tenantID))
		if maxExemplars < 0 {
			maxExemplars = 0
		}
	}

	opts := &tsdb.Options{
		RetentionDuration:                    cfg.Retention.Milliseconds(),
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

	db, err := tsdb.Open(dir, util_log.SlogFromGoKit(userLogger), nil, opts, nil)
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

// applyTenantTSDBSettings reapplies per-tenant TSDB settings from runtime
// limits (mirrors ingester.applyTSDBSettings).
func (p *partitionTSDB) applyTenantTSDBSettings(limits *validation.Overrides, logger log.Logger) error {
	if limits == nil || p.db == nil {
		return nil
	}
	oooTW := limits.OutOfOrderTimeWindow(p.tenantID)
	if oooTW < 0 {
		oooTW = 0
	}
	maxExemplars := int64(limits.MaxGlobalExemplarsPerUser(p.tenantID))
	if maxExemplars < 0 {
		maxExemplars = 0
	}
	cfg := config.Config{
		StorageConfig: config.StorageConfig{
			ExemplarsConfig: &config.ExemplarsConfig{
				MaxExemplars: maxExemplars,
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
	h := p.db.Head()
	return p.db.CompactHead(tsdb.NewRangeHead(h, h.MinTime(), h.MaxTime()))
}

// Close shuts down the TSDB. Idempotent.
func (p *partitionTSDB) Close() error {
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
