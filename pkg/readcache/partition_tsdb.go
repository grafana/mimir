// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/ingester/lookupplan"
	"github.com/grafana/mimir/pkg/mimirpb"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

// partitionTSDB is the readcache equivalent of pkg/ingester.userTSDB,
// scoped to a single (tenant, partition) pair.
//
// Structural differences vs the ingester's userTSDB:
//
//   - **Compaction stays owned by readcache.** Prometheus's background
//     loop is disabled. The service calls DB.Compact on the ingester's
//     zone-staggered schedule, and forces a head flush when the TSDB
//     is idle. Blocks stay local.
//   - **Shipping is off.** No Shipper is configured; readcache never
//     uploads blocks to object storage. Blockbuilder is the canonical
//     long-term home for blocks on the experimental Kafka topic.
//   - **No active-series tracker / ownedSeries / cost attribution.**
//     Those are write-path concerns that don't apply to readcache.
//   - **TSDB head mutations are serialized** (append, runtime ApplyConfig,
//     CompactHead) so Kafka parallel ingestion cannot interleave commits
//     on the same head; the ingester achieves the same with acquireAppendLock.
type partitionTSDB struct {
	tenantID         string
	partitionID      int32
	dir              string
	postingsCacheKey string

	db              *tsdb.DB
	plannerProvider *lookupplan.PlannerProvider

	mu     sync.RWMutex
	closed bool

	// tsdbMut serializes TSDB head mutations. The Kafka ingest path uses
	// ingest.PusherConsumer with parallel shards when
	// ingestion-concurrency-max > 0, which can call the readcache pusher
	// concurrently; Prometheus TSDB expects a single writer at a time
	// for appends (same contract as the ingester's acquireAppendLock).
	// Regular DB.Compact does not take it: Prometheus serializes that
	// itself, and holding it across the call stalls the Kafka fetcher.
	// Forced compaction uses appendState instead of this lock.
	tsdbMut sync.Mutex

	// compactMu serializes regular compaction, forced compaction, and
	// close. Appends do not take it.
	compactMu sync.Mutex

	// fenceMu guards appendState and forcedMaxTime. Appends take it
	// for read; forced compaction and close take it for write, then
	// wait for appends that already started.
	fenceMu            sync.RWMutex
	appendState        tsdbAppendState
	forcedMaxTime      int64
	appendsBeforeFence sync.WaitGroup

	// lastAppend is the wall time of the last successful push, or of
	// open when nothing has been pushed yet.
	lastAppend atomic.Int64

	// closeErrHook, when set, is returned from closeDBLocked after the
	// Prometheus DB has been closed. Tests use it to simulate a Close
	// error: DB.Close stops the database before it reports an error.
	closeErrHook func() error

	mutationObservers [tsdbMutationOperationCount]tsdbMutationObservers
}

type tsdbAppendState uint8

const (
	tsdbAppendActive tsdbAppendState = iota
	tsdbAppendForced
	tsdbAppendClosing
)

var (
	errTSDBCompactionOverlap = fmt.Errorf("readcache TSDB head compaction in progress for this time range")
	errTSDBClosed            = fmt.Errorf("readcache TSDB is closed")
)

type tsdbMutationOperation uint8

const (
	tsdbMutationAppend tsdbMutationOperation = iota
	tsdbMutationApplyConfig
	tsdbMutationCompact
	tsdbMutationClose
	tsdbMutationOperationCount
)

type tsdbMutationObservers struct {
	wait prometheus.Observer
	hold prometheus.Observer
}

func (o tsdbMutationOperation) String() string {
	switch o {
	case tsdbMutationAppend:
		return "append"
	case tsdbMutationApplyConfig:
		return "apply_config"
	case tsdbMutationCompact:
		return "compact"
	case tsdbMutationClose:
		return "close"
	default:
		panic(fmt.Sprintf("unknown TSDB mutation operation %d", o))
	}
}

type postingsCacheKeyContextKey struct{}

func newPostingsCacheKey(tenantID string, partitionID int32, epoch int) string {
	return fmt.Sprintf("%d:%s:%d:%d", len(tenantID), tenantID, partitionID, epoch)
}

func withPostingsCacheKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, postingsCacheKeyContextKey{}, key)
}

func postingsCacheKeyFromContext(ctx context.Context) (string, error) {
	key, ok := ctx.Value(postingsCacheKeyContextKey{}).(string)
	if !ok {
		return "", fmt.Errorf("readcache postings cache key is missing from context")
	}
	return key, nil
}

type partitionSeriesLifecycleCallback struct {
	postingsCache *tsdb.PostingsForMatchersCache
	key           string
}

func (c *partitionSeriesLifecycleCallback) PreCreation(labels.Labels) error {
	return nil
}

func (c *partitionSeriesLifecycleCallback) PostCreation(metric labels.Labels) {
	if c.postingsCache == nil {
		return
	}
	metricName := metric.Get(labels.MetricName)
	if metricName != "" {
		c.postingsCache.InvalidateMetric(c.key, metricName)
	}
}

func (c *partitionSeriesLifecycleCallback) PostDeletion(map[chunks.HeadSeriesRef]labels.Labels) {}

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
// a block only when it is a full retention behind the newest block.
// db.run reloads blocks every BlockReloadInterval whether or not a
// compaction ran. The newest block is never removed that way; idle
// close deletes the directory. Pass 0 to disable time-retention
// (useful for tests where data should never age out).
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
	lookupPlanMetrics lookupplan.Metrics,
	tsdbPromReg prometheus.Registerer,
	logger log.Logger,
) (*partitionTSDB, error) {
	dir := partitionEpochDir(rootDir, tenantID, partitionID, epoch)
	postingsKey := newPostingsCacheKey(tenantID, partitionID, epoch)
	seriesLifecycleCallback := &partitionSeriesLifecycleCallback{key: postingsKey}

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

	partitionDB := &partitionTSDB{
		tenantID:         tenantID,
		partitionID:      partitionID,
		dir:              dir,
		postingsCacheKey: postingsKey,
	}
	if cfg.IndexLookupPlanning.Enabled {
		plannerFactory := lookupplan.NewPlannerFactory(
			lookupPlanMetrics.ForUser(tenantID),
			userLogger,
			lookupplan.NewStatisticsGenerator(userLogger),
			cfg.IndexLookupPlanning.CostConfig,
		)
		partitionDB.plannerProvider = lookupplan.NewPlannerProvider(plannerFactory)
	}

	// BlockReloadInterval is explicit because Prometheus clamps its zero value to one second.
	opts := &tsdb.Options{
		// RetentionDuration uses the readcache-local retention rather
		// than cfg.Retention (the shared ingester knob). See the
		// doc comment on openPartitionTSDB for rationale.
		RetentionDuration:                    localBlockRetention.Milliseconds(),
		MinBlockDuration:                     blockRanges[0],
		MaxBlockDuration:                     blockRanges[len(blockRanges)-1],
		BlockReloadInterval:                  time.Minute,
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
		SeriesLifecycleCallback:              seriesLifecycleCallback,
		SharedPostingsForMatchersCache:       cfg.SharedPostingsForMatchersCache,
		PostingsForMatchersCacheKeyFunc:      postingsCacheKeyFromContext,
		HeadPostingsForMatchersCacheFactory:  headPostingsForMatchersCacheFactory,
		BlockPostingsForMatchersCacheFactory: blockPostingsForMatchersCacheFactory,
		PostingsClonerFactory:                lookupplan.ActualSelectedPostingsClonerFactory{},
		SecondaryHashFunction:                mimirpb.ShardByMetricNameLocalityLabelsFunc(tenantID),
		IndexLookupPlannerFunc:               partitionDB.getIndexLookupPlannerFunc(),
	}

	db, err := tsdb.Open(dir, util_log.SlogFromGoKit(userLogger), tsdbPromReg, opts, nil)
	if err != nil {
		return nil, fmt.Errorf("opening partition TSDB %q: %w", dir, err)
	}
	// Prometheus's own loop is unstaggered. Readcache compacts from
	// its service ticker instead.
	db.DisableCompactions()
	partitionDB.db = db
	partitionDB.touchLastAppend(time.Now())
	if cfg.SharedPostingsForMatchersCache && cfg.HeadPostingsForMatchersCacheInvalidation {
		seriesLifecycleCallback.postingsCache = db.Head().PostingsForMatchersCache()
	}

	if cfg.IndexLookupPlanning.Enabled {
		// Generate initial statistics only after the TSDB has been opened and initialized.
		if err := partitionDB.generateHeadStatistics(); err != nil {
			level.Error(userLogger).Log("msg", "failed to generate initial TSDB head statistics", "err", err)
		}
	}

	return partitionDB, nil
}

// getIndexLookupPlannerFunc returns the configured lookup planner for a block.
func (p *partitionTSDB) getIndexLookupPlannerFunc() tsdb.IndexLookupPlannerFunc {
	return func(blockMeta tsdb.BlockMeta, indexReader tsdb.IndexReader) index.LookupPlanner {
		if p.plannerProvider == nil {
			return lookupplan.NoopPlanner{}
		}
		return p.plannerProvider.GetPlanner(blockMeta, indexReader)
	}
}

// generateHeadStatistics refreshes the lookup planner for this TSDB's mutable Head.
func (p *partitionTSDB) generateHeadStatistics() error {
	if p.plannerProvider == nil {
		return nil
	}

	head := p.db.Head()
	indexReader, err := head.Index()
	if err != nil {
		return fmt.Errorf("failed to open TSDB head index reader: %w", err)
	}
	defer indexReader.Close()

	p.plannerProvider.GenerateAndStorePlanner(head.Meta(), indexReader)
	return nil
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
	defer p.lockForMutation(tsdbMutationApplyConfig)()

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
	q, err := p.db.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return &partitionQuerier{Querier: q, postingsCacheKey: p.postingsCacheKey}, nil
}

// ChunkQuerier returns a ChunkQuerier covering [mint, maxt].
func (p *partitionTSDB) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	q, err := p.db.ChunkQuerier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return &partitionChunkQuerier{ChunkQuerier: q, postingsCacheKey: p.postingsCacheKey}, nil
}

// UnorderedChunkQuerier returns an unordered ChunkQuerier.
func (p *partitionTSDB) UnorderedChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	q, err := p.db.UnorderedChunkQuerier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return &partitionChunkQuerier{ChunkQuerier: q, postingsCacheKey: p.postingsCacheKey}, nil
}

type partitionQuerier struct {
	storage.Querier
	postingsCacheKey string
}

func (q *partitionQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return q.Querier.Select(withPostingsCacheKey(ctx, q.postingsCacheKey), sortSeries, hints, matchers...)
}

func (q *partitionQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.Querier.LabelValues(withPostingsCacheKey(ctx, q.postingsCacheKey), name, hints, matchers...)
}

func (q *partitionQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.Querier.LabelNames(withPostingsCacheKey(ctx, q.postingsCacheKey), hints, matchers...)
}

type partitionChunkQuerier struct {
	storage.ChunkQuerier
	postingsCacheKey string
}

func (q *partitionChunkQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	return q.ChunkQuerier.Select(withPostingsCacheKey(ctx, q.postingsCacheKey), sortSeries, hints, matchers...)
}

func (q *partitionChunkQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.ChunkQuerier.LabelValues(withPostingsCacheKey(ctx, q.postingsCacheKey), name, hints, matchers...)
}

func (q *partitionChunkQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.ChunkQuerier.LabelNames(withPostingsCacheKey(ctx, q.postingsCacheKey), hints, matchers...)
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

// CompactHead flushes the whole head, including the live tip, while
// holding tsdbMut. Production compaction does not call this: tests
// use it to cut a block without waiting for the 1.5× block-range span.
func (p *partitionTSDB) CompactHead() error {
	defer p.lockForMutation(tsdbMutationCompact)()

	h := p.db.Head()
	if err := p.db.CompactHead(tsdb.NewRangeHead(h, h.MinTime(), h.MaxTime())); err != nil {
		return err
	}
	return p.db.CompactOOOHead(context.Background())
}

// compactRegular persists compactable head blocks the way the ingester's
// regular compact does. Appends continue. The live window stays in the head.
func (p *partitionTSDB) compactRegular() error {
	p.compactMu.Lock()
	defer p.compactMu.Unlock()
	if p.IsClosed() {
		return nil
	}
	return p.db.Compact(context.Background())
}

// compactHeadForced flushes the head up to forcedMaxTime in block-range
// slices, then compacts out-of-order data. Samples at or below the cut
// are rejected until the flush finishes. Samples newer than the cut
// are appended. tsdbMut is not held across the flush.
func (p *partitionTSDB) compactHeadForced(blockDuration, forcedMaxTime int64) error {
	p.compactMu.Lock()
	defer p.compactMu.Unlock()
	if p.IsClosed() {
		return nil
	}

	p.fenceMu.Lock()
	if p.appendState != tsdbAppendActive {
		state := p.appendState
		p.fenceMu.Unlock()
		return fmt.Errorf("readcache TSDB head cannot be force-compacted from state %d", state)
	}
	p.appendState = tsdbAppendForced
	p.forcedMaxTime = forcedMaxTime
	p.fenceMu.Unlock()

	defer func() {
		p.fenceMu.Lock()
		if p.appendState == tsdbAppendForced {
			p.appendState = tsdbAppendActive
		}
		p.fenceMu.Unlock()
	}()

	// Appends that observed the active state have already incremented
	// the WaitGroup. Appends that observe the forced state do not.
	p.appendsBeforeFence.Wait()

	h := p.db.Head()
	for {
		blockMinTime, blockMaxTime, isValid, isLast := nextForcedHeadCompactionRange(blockDuration, h.MinTime(), h.MaxTime(), forcedMaxTime)
		if !isValid {
			break
		}
		if err := p.db.CompactHead(tsdb.NewRangeHead(h, blockMinTime, blockMaxTime)); err != nil {
			return err
		}
		if isLast {
			break
		}
	}
	return p.db.CompactOOOHead(context.Background())
}

// beginAppend admits an append, or rejects it when a forced compaction
// or close owns the sample range. tracked is true when the caller must
// call endAppend: the compaction or close is waiting for this append.
func (p *partitionTSDB) beginAppend(minTimestamp int64) (tracked bool, err error) {
	p.fenceMu.RLock()
	defer p.fenceMu.RUnlock()

	switch p.appendState {
	case tsdbAppendActive:
		p.appendsBeforeFence.Add(1)
		return true, nil
	case tsdbAppendForced:
		if minTimestamp <= p.forcedMaxTime {
			return false, errTSDBCompactionOverlap
		}
		return false, nil
	default:
		return false, errTSDBClosed
	}
}

func (p *partitionTSDB) endAppend(tracked bool) {
	if tracked {
		p.appendsBeforeFence.Done()
	}
}

func (p *partitionTSDB) touchLastAppend(t time.Time) {
	p.lastAppend.Store(t.UnixMilli())
}

func (p *partitionTSDB) lastAppendTime() time.Time {
	return time.UnixMilli(p.lastAppend.Load())
}

// beginIdleClose rejects new appends and waits out appends that already
// started. The caller must follow with finishIdleClose or abortIdleClose.
func (p *partitionTSDB) beginIdleClose() bool {
	p.compactMu.Lock()
	p.fenceMu.Lock()
	if p.closed || p.appendState != tsdbAppendActive {
		p.fenceMu.Unlock()
		p.compactMu.Unlock()
		return false
	}
	p.appendState = tsdbAppendClosing
	p.fenceMu.Unlock()
	p.appendsBeforeFence.Wait()
	return true
}

func (p *partitionTSDB) abortIdleClose() {
	p.fenceMu.Lock()
	if p.appendState == tsdbAppendClosing && !p.closed {
		p.appendState = tsdbAppendActive
	}
	p.fenceMu.Unlock()
	p.compactMu.Unlock()
}

func (p *partitionTSDB) finishIdleClose() error {
	defer p.compactMu.Unlock()
	return p.closeDBLocked()
}

// idleCloseClaimed reports whether closeIdleTSDB owns this TSDB.
// The answer is stable only while the caller holds compactMu: that is
// the lock beginIdleClose holds until the map entry is deleted or the
// close is aborted.
func (p *partitionTSDB) idleCloseClaimed() bool {
	if p.IsClosed() {
		return true
	}
	p.fenceMu.RLock()
	closing := p.appendState == tsdbAppendClosing
	p.fenceMu.RUnlock()
	return closing
}

// Close shuts down the TSDB. Idempotent.
func (p *partitionTSDB) Close() error {
	p.compactMu.Lock()
	defer p.compactMu.Unlock()
	return p.closeDBLocked()
}

// closeDBLocked closes the Prometheus DB. compactMu is held by the caller.
func (p *partitionTSDB) closeDBLocked() error {
	p.fenceMu.Lock()
	if p.closed {
		p.fenceMu.Unlock()
		return nil
	}
	alreadyClosing := p.appendState == tsdbAppendClosing
	p.appendState = tsdbAppendClosing
	p.fenceMu.Unlock()
	if !alreadyClosing {
		p.appendsBeforeFence.Wait()
	}

	// Wait for in-flight appends / ApplyConfig before closing.
	defer p.lockForMutation(tsdbMutationClose)()

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	// DB.Close stops the database before it returns an error, so this
	// object is unusable either way. Mark it closed first; the caller
	// drops it from the tenant map.
	p.closed = true
	err := p.db.Close()
	if p.closeErrHook != nil {
		err = p.closeErrHook()
	}
	if err != nil {
		level.Warn(util_log.Logger).Log("msg", "error closing partition TSDB",
			"user", p.tenantID, "partition", p.partitionID, "err", err)
		return err
	}
	return nil
}

// nextForcedHeadCompactionRange computes the next TSDB head range to compact
// when a forced compaction is triggered. If isValid is false, the returned
// range should not be compacted. Copied from the ingester.
func nextForcedHeadCompactionRange(blockDuration, headMinTime, headMaxTime, forcedMaxTime int64) (minTime, maxTime int64, isValid, isLast bool) {
	if headMinTime == math.MaxInt64 || headMaxTime == math.MinInt64 {
		return 0, 0, false, true
	}

	minTime = headMinTime
	maxTime = min(headMaxTime, forcedMaxTime)
	if maxTime < minTime {
		return 0, 0, false, true
	}

	if (minTime/blockDuration)*blockDuration != (maxTime/blockDuration)*blockDuration {
		// Block max time is exclusive, so subtract one millisecond.
		maxTime = ((minTime/blockDuration)+1)*blockDuration - 1
		return minTime, maxTime, true, false
	}
	return minTime, maxTime, true, true
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

func (r *Readcache) instrumentTSDB(db *partitionTSDB) {
	if r.tsdbMutationLockWait == nil || r.tsdbMutationDuration == nil {
		return
	}
	for operation := tsdbMutationOperation(0); operation < tsdbMutationOperationCount; operation++ {
		db.mutationObservers[operation] = tsdbMutationObservers{
			wait: r.tsdbMutationLockWait.WithLabelValues(operation.String()),
			hold: r.tsdbMutationDuration.WithLabelValues(operation.String()),
		}
	}
}

// lockForMutation acquires the TSDB mutation lock and returns its unlock
// function. The observer pointers are nil in low-level TSDB tests that open a
// partitionTSDB without constructing a Readcache.
func (p *partitionTSDB) lockForMutation(operation tsdbMutationOperation) func() {
	waitStarted := time.Now()
	p.tsdbMut.Lock()
	lockedAt := time.Now()
	waitSeconds := lockedAt.Sub(waitStarted).Seconds()
	observers := p.mutationObservers[operation]

	return func() {
		holdSeconds := time.Since(lockedAt).Seconds()
		p.tsdbMut.Unlock()
		if observers.wait != nil {
			observers.wait.Observe(waitSeconds)
		}
		if observers.hold != nil {
			observers.hold.Observe(holdSeconds)
		}
	}
}

func (p *partitionTSDB) storageSnapshot() (tsdbStorageSnapshot, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return tsdbStorageSnapshot{}, false
	}

	blocks := p.Blocks()
	out := tsdbStorageSnapshot{
		tsdbs:      1,
		headSeries: p.Head().NumSeries(),
		blocks:     len(blocks),
	}
	for _, block := range blocks {
		out.blockBytes += block.Size()
	}
	return out, true
}
