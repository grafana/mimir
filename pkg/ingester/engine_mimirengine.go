// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/ingester/activeseries"
	asmodel "github.com/grafana/mimir/pkg/ingester/activeseries/model"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/mimirengine/store"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

// mimirengineEngine is pkg/storage/mimirengine as a tenantEngine. It writes no Prometheus blocks:
// series leaving the head go to its own cold blocks, which it queries and expires itself.
type mimirengineEngine struct {
	*store.Engine
	// The time range of a block, which a flush compacts the head by.
	blockDuration int64
	// Head compactions that moved the head's min time, which is when Prometheus writes a block.
	compactions prometheus.Counter
}

// mimirengineShards is how many store shards a tenant with up to a few hundred thousand series
// spreads them over: appends to a tenant come from the pusher's parallel shards, and each store
// shard has its own lock.
const mimirengineShards = 16

// mimirengineShardsFor is how many store shards a tenant with a limit of maxSeries series gets. The
// Spread larger tenant heads across more shards to reduce write lock contention.
func mimirengineShardsFor(maxSeries int) int {
	switch {
	case maxSeries >= 1_000_000:
		return 4 * mimirengineShards
	case maxSeries >= 300_000:
		return 2 * mimirengineShards
	default:
		return mimirengineShards
	}
}

// mimirengineIdleEvictionMargin is how much longer than the active series' timeout the mimirengine keeps series that
// stopped in memory: they leave the head at a compaction, instead of hours later, with their data still queryable. The
// engine counts active series on the series it holds, so it can't evict one sooner, and it compares the series' newest
// sample time while the active series timeout starts at ingest time.
const mimirengineIdleEvictionMargin = 5 * time.Minute

// testNoIdleEviction makes tests keep the series they expect in memory; it's always false outside tests.
var testNoIdleEviction bool

func idleEvictionMs(activeIdleTimeout time.Duration) int64 {
	if testNoIdleEviction {
		return 0
	}
	return (activeIdleTimeout + mimirengineIdleEvictionMargin).Milliseconds()
}

func openMimirEngine(dir, userID string, reg prometheus.Registerer, opts *tsdb.Options, shards int, checkpoints bool, initialCheckpoint func() ([]byte, error), activeIdleTimeout time.Duration) (tenantEngine, error) {
	// The ingester's memory series metrics are the TSDB head's, which the engine has to provide.
	callback := &countingSeriesCallback{
		SeriesLifecycleCallback: opts.SeriesLifecycleCallback,
		created: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_series_created_total",
			Help: "Total number of series created in the head",
		}),
		removed: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_series_removed_total",
			Help: "Total number of series removed in the head",
		}),
	}
	engine, err := store.OpenEngine(dir, userID, store.EngineOptions{
		Shards:                  shards,
		RetentionMs:             opts.RetentionDuration,
		OutOfOrderTimeWindowMs:  opts.OutOfOrderTimeWindow,
		MaxExemplars:            opts.MaxExemplars,
		TimelyCompaction:        opts.TimelyCompaction,
		IdleEvictionMs:          idleEvictionMs(activeIdleTimeout),
		Checkpoints:             checkpoints,
		InitialCheckpoint:       initialCheckpoint,
		SeriesLifecycleCallback: callback,
		SecondaryHashFunction:   opts.SecondaryHashFunction,
	})
	if err != nil {
		return nil, err
	}
	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_head_series",
		Help: "Total number of series in the head block.",
	}, func() float64 { return float64(engine.NumSeries()) })
	compactions := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_compactions_total",
		Help: "Total number of compactions that were executed for the partition.",
	})
	return mimirengineEngine{Engine: engine, blockDuration: opts.MinBlockDuration, compactions: compactions}, nil
}

// countingSeriesCallback counts the series the engine creates and removes, like the TSDB head's metrics.
type countingSeriesCallback struct {
	tsdb.SeriesLifecycleCallback
	created, removed prometheus.Counter
}

func (c *countingSeriesCallback) PostCreation(lset labels.Labels) {
	c.created.Inc()
	c.SeriesLifecycleCallback.PostCreation(lset)
}

func (c *countingSeriesCallback) PostDeletion(deleted map[chunks.HeadSeriesRef]labels.Labels) {
	c.removed.Add(float64(len(deleted)))
	c.SeriesLifecycleCallback.PostDeletion(deleted)
}

func (e mimirengineEngine) ChunkQuerier(mint, maxt int64, unordered bool) (storage.ChunkQuerier, error) {
	var q storage.ChunkQuerier
	var err error
	if unordered {
		q, err = e.UnorderedChunkQuerier(mint, maxt)
	} else {
		q, err = e.Engine.ChunkQuerier(mint, maxt)
	}
	if err != nil {
		return nil, err
	}
	return wrapStreaming(q), nil
}

// MimirEngine persists its own cold blocks, which the Prometheus block shipper cannot upload.
func (e mimirengineEngine) BlockShippingEnabled() bool { return false }

func (e mimirengineEngine) ShipBlocks(context.Context) {}

func (e mimirengineEngine) OldestUnshippedBlockTime() uint64 { return 0 }

func (e mimirengineEngine) RenderDiagnosticsPage(w http.ResponseWriter, r *http.Request, _ string, _ bool) {
	http.NotFound(w, r)
}

func (e mimirengineEngine) Compact(ctx context.Context) error {
	defer e.countCompaction(e.MinTime())
	return e.Engine.Compact(ctx)
}

// Flush compacts the head block by block up to upTo, then the out-of-order head.
func (e mimirengineEngine) Flush(ctx context.Context, upTo int64) error {
	for {
		blockMinTime, blockMaxTime, isValid, isLast := nextForcedHeadCompactionRange(e.blockDuration, e.MinTime(), e.MaxTime(), upTo)
		if !isValid {
			break
		}

		minTimeBefore := e.MinTime()
		err := e.CompactHead(blockMinTime, blockMaxTime)
		e.countCompaction(minTimeBefore)
		if err != nil {
			return err
		}

		// Do not check again if it was the last range.
		if isLast {
			break
		}
	}

	return e.CompactOOOHead(ctx)
}

// Evict compacts the out-of-order head first, which persists the out-of-order data of the series before they
// leave, then the series themselves. The first failure doesn't stop the second.
func (e mimirengineEngine) Evict(ctx context.Context, refs []storage.SeriesRef) error {
	oooErr := e.CompactOOOHead(ctx)
	if oooErr != nil {
		oooErr = fmt.Errorf("compact the out-of-order head: %w", oooErr)
	}
	selectedErr := e.CompactSelectedSeries(refs)
	if selectedErr != nil {
		selectedErr = fmt.Errorf("compact the selected series: %w", selectedErr)
	}
	return errors.Join(oooErr, selectedErr)
}

func (e mimirengineEngine) countCompaction(minTimeBefore int64) {
	if e.MinTime() != minTimeBefore {
		e.compactions.Inc()
	}
}

func (e mimirengineEngine) Configure(settings engineSettings) error {
	e.SetMaxExemplars(settings.MaxExemplars)
	e.SetOutOfOrderTimeWindow(settings.OutOfOrderTimeWindow)
	return nil
}

func (e mimirengineEngine) NewestDataTime() int64 {
	return max(e.MaxTime(), e.MaxOOOTime())
}

func (e mimirengineEngine) Head() engineHead {
	return mimirengineHead{e.Engine}
}

type mimirengineHead struct {
	*store.Engine
}

var _ activeSeriesHead = mimirengineHead{}

func (h mimirengineHead) ActiveSeries(cutoff time.Time) activeCounts {
	counts := h.Engine.ActiveSeries(cutoff.UnixMilli())
	out := activeCounts{Total: int(counts.Total), OTLP: int(counts.OTLP), NativeHistograms: int(counts.NativeHistograms), NativeHistogramBuckets: int(counts.NativeHistogramBuckets), Trackers: make([]trackerCounts, len(counts.Trackers))}
	for i, tracker := range counts.Trackers {
		out.Trackers[i] = trackerCounts{Total: int(tracker.Total), NativeHistograms: int(tracker.NativeHistograms), NativeHistogramBuckets: int(tracker.NativeHistogramBuckets)}
	}
	return out
}

func (h mimirengineHead) SetActiveTrackers(matchers *asmodel.Matchers) {
	names := matchers.MatcherNames()
	h.Engine.SetActiveTrackers(store.ActiveTrackers{Count: len(names), Match: func(lset labels.Labels) []uint16 {
		matches := matchers.Matches(lset)
		out := make([]uint16, matches.Len())
		for i := range out {
			out[i] = matches.Get(i)
		}
		return out
	}})
}

func (h mimirengineHead) SetCostAttribution(sink costAttributionSink) {
	h.Engine.SetCostAttribution(sink)
}

func (h mimirengineHead) DeactivateSeries(refs []storage.SeriesRef) { h.Engine.DeactivateSeries(refs) }
func (h mimirengineHead) DeactivateAll()                            { h.Engine.DeactivateAll() }

func (h mimirengineHead) ActiveRefs(cutoff time.Time) activeseries.ActiveRefs {
	return activeRefs{h.Engine, cutoff.UnixMilli()}
}

// activeRefs answers for the series of an engine that were ingested at or after a time.
type activeRefs struct {
	engine   *store.Engine
	cutoffMs int64
}

func (a activeRefs) ContainsRef(ref storage.SeriesRef) bool {
	active, _, _ := a.engine.IsActive(ref, a.cutoffMs)
	return active
}

func (a activeRefs) NativeHistogramBuckets(ref storage.SeriesRef) (int, bool) {
	active, buckets, histogram := a.engine.IsActive(ref, a.cutoffMs)
	return buckets, active && histogram
}

func (h mimirengineHead) TimeBounds() timeBounds {
	return timeBounds{MinTime: h.MinTime(), MaxTime: h.MaxTime(), MinOOOTime: h.MinOOOTime(), MaxOOOTime: h.MaxOOOTime()}
}

func (h mimirengineHead) MinInOrderAppendTime() (int64, bool) {
	return h.AppendableMinValidTime()
}

func (h mimirengineHead) Ownership(owned ring.TokenRanges, onNonOwned func(storage.SeriesRef)) int {
	count := 0
	h.ForEachSecondaryHash(func(refs []chunks.HeadSeriesRef, secondaryHashes []uint32) {
		for i, hash := range secondaryHashes {
			if owned.IncludesKey(hash) {
				count++
			} else {
				onNonOwned(storage.SeriesRef(refs[i]))
			}
		}
	})
	return count
}

func (h mimirengineHead) ShardSeries(shardIndex, shardCount uint64) index.Postings {
	out := make([]storage.SeriesRef, 0, 128)
	h.ForEachShardHash(func(refs []storage.SeriesRef, shardHashes []uint64) {
		for i := range refs {
			if shardHashes[i]%shardCount == shardIndex {
				out = append(out, refs[i])
			}
		}
	})
	slices.Sort(out) // The postings of an index are sorted.
	return index.NewListPostings(out)
}

// LabelValuesBytes walks the index, which keeps no such totals: the ingester only asks when a tenant has a threshold
// to report against.
func (h mimirengineHead) LabelValuesBytes() map[string]uint64 {
	idx, err := h.Index()
	if err != nil {
		return nil
	}
	defer idx.Close()
	ctx := context.Background()
	names, err := idx.LabelNames(ctx)
	if err != nil {
		return nil
	}
	out := make(map[string]uint64, len(names))
	for _, name := range names {
		values, err := idx.LabelValues(ctx, name, nil)
		if err != nil {
			continue
		}
		var bytes uint64
		for _, v := range values {
			// Like Prometheus's postings, which only count values long enough to matter.
			if len(v) > index.LabelValueBytesMinLength {
				bytes += uint64(len(v))
			}
		}
		if bytes > 0 {
			out[name] = bytes
		}
	}
	return out
}

// Sync is the engine's durability point, which is nothing: it has no log, and what a crash loses comes back from Kafka.
func (h mimirengineHead) Sync() error {
	return h.FsyncWLSegments()
}

// ValidateMimirEngine refuses configurations the MimirEngine can't serve as the
// Prometheus TSDB would. It only runs in ingesters that select the engine.
func ValidateMimirEngine(tsdbCfg mimir_tsdb.TSDBConfig, ingesterCfg Config, ingestCfg ingest.Config) error {
	if tsdbCfg.Engine != mimir_tsdb.EngineMimirEngine {
		return nil
	}
	if !store.Supported {
		return fmt.Errorf("MimirEngine on this platform: %w", errors.ErrUnsupported)
	}
	var errs []error
	// The engine has no WAL: after an unclean shutdown only Kafka has the data, from the file-stored
	// offset the ingester removes, and a pushed sample would be lost.
	if !ingestCfg.Enabled {
		errs = append(errs, errors.New("requires ingest storage (-ingest-storage.enabled=true)"))
	}
	if ingesterCfg.PushGrpcMethodEnabled {
		errs = append(errs, errors.New("requires the gRPC Push method disabled (-ingester.push-grpc-method-enabled=false)"))
	}
	if !ingestCfg.KafkaConfig.ConsumerGroupOffsetCommitFileEnforced {
		errs = append(errs, errors.New("requires the file-stored Kafka offset (-ingest-storage.kafka.consumer-group-offset-commit-file-enforced=true), which it resets after an unclean shutdown"))
	}
	// It writes no Prometheus blocks.
	if tsdbCfg.IsBlocksShippingEnabled() {
		errs = append(errs, errors.New("doesn't write blocks to ship (-blocks-storage.tsdb.ship-interval=0)"))
	}
	if tsdbCfg.FlushBlocksOnShutdown {
		errs = append(errs, errors.New("doesn't write blocks to flush (-blocks-storage.tsdb.flush-blocks-on-shutdown=false)"))
	}
	if tsdbCfg.OffsetCatalogue.Enabled {
		errs = append(errs, errors.New("has no blocks to catalogue Kafka offsets for (-blocks-storage.tsdb.offset-catalogue.enabled=false)"))
	}
	// It always snapshots its head on shutdown; the Prometheus option would suggest a choice.
	if tsdbCfg.MemorySnapshotOnShutdown {
		errs = append(errs, errors.New("always snapshots its head on shutdown (-blocks-storage.tsdb.memory-snapshot-on-shutdown=false)"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("the %s TSDB engine %w", mimir_tsdb.EngineMimirEngine, errors.Join(errs...))
	}
	return nil
}

// resetKafkaOffsetsAfterUncleanShutdown removes the file-stored Kafka offsets when a mimirengine
// tenant wasn't restored from a snapshot: its head would miss what was consumed since, so the
// ingester replays Kafka from the retention period instead of resuming from the stored offset.
func (i *Ingester) resetKafkaOffsetsAfterUncleanShutdown(unclean []string) error {
	if len(unclean) == 0 {
		return nil
	}
	files, err := filepath.Glob(filepath.Join(i.cfg.BlocksStorageConfig.TSDB.Dir, "kafka-offset*.json"))
	if err != nil {
		return err
	}
	for _, file := range files {
		if err := os.Remove(file); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	level.Warn(i.logger).Log("msg", "tenants weren't restored from a snapshot, replaying Kafka from the retention period", "tenants", len(unclean), "example_tenant", unclean[0], "removed_offset_files", len(files))
	return nil
}

// uncleanlyRestored reports whether db is a MimirEngine that opened without a snapshot.
func uncleanlyRestored(db tenantEngine) bool {
	engine, ok := db.(mimirengineEngine)
	return ok && !engine.Restored()
}

// restoredCheckpoint is the checkpoint db's MimirEngine restored its head from, nil when it restored a clean
// shutdown's snapshot or isn't one.
func restoredCheckpoint(db tenantEngine) []byte {
	if engine, ok := db.(mimirengineEngine); ok {
		return engine.RestoredCheckpoint()
	}
	return nil
}

// mimirengineSnapshotDuty is the share of a head snapshot's time it works between its batches of series: it takes ten
// times as long as it could, and leaves the CPU and disk to queries.
const mimirengineSnapshotDuty = 0.1

const (
	snapshotOutcomeSuccess = "success"
	snapshotOutcomeFailure = "failure"
)

// mimirengineSnapshotsRunning snapshots the head of every mimirengine tenant each interval, one tenant at a time.
func (i *Ingester) mimirengineSnapshotsRunning(ctx context.Context) error {
	ticker := time.NewTicker(i.cfg.BlocksStorageConfig.TSDB.MimirEngineSnapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// The next round retries failed snapshots without stopping ingestion.
			_ = i.snapshotMimirEngineHeads(ctx, mimirengineSnapshotDuty)
		}
	}
}

// snapshotMimirEngineHeads checkpoints each tenant's head with the Kafka offsets committed before it started: the
// head has every record up to them, and an unclean restart replays from the oldest checkpoint.
func (i *Ingester) snapshotMimirEngineHeads(ctx context.Context, duty float64) error {
	started := time.Now()
	snapshots, failures := 0, 0
	var snapshotErr error
	for _, userID := range i.getTSDBUsers() {
		if err := ctx.Err(); err != nil {
			return err
		}
		db := i.getTSDB(userID)
		if db == nil {
			continue
		}
		engine, ok := db.db.(mimirengineEngine)
		if !ok {
			continue
		}
		checkpoint, err := i.committedKafkaOffsets()
		if err == nil {
			err = engine.Checkpoint(ctx, checkpoint, duty)
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			failures++
			i.metrics.mimirengineSnapshots.WithLabelValues(snapshotOutcomeFailure).Inc()
			level.Warn(i.logger).Log("msg", "failed to snapshot the head", "user", userID, "err", err)
			snapshotErr = errors.Join(snapshotErr, fmt.Errorf("snapshot head for tenant %s: %w", userID, err))
			continue
		}
		snapshots++
		i.metrics.mimirengineSnapshots.WithLabelValues(snapshotOutcomeSuccess).Inc()
	}
	i.metrics.mimirengineSnapshotRoundDuration.Observe(time.Since(started).Seconds())
	i.metrics.mimirengineLastSnapshotRound.SetToCurrentTime()
	level.Info(i.logger).Log("msg", "snapshotted the heads", "tenants", snapshots, "failures", failures, "duration", time.Since(started))
	return snapshotErr
}

// committedKafkaOffsets is the offset in each Kafka offset file, by file name.
func (i *Ingester) committedKafkaOffsets() ([]byte, error) {
	files, err := filepath.Glob(filepath.Join(i.cfg.BlocksStorageConfig.TSDB.Dir, "kafka-offset*.json"))
	if err != nil {
		return nil, err
	}
	offsets := map[string]int64{}
	for _, file := range files {
		if offset, ok := ingest.ReadOffsetFile(file, i.ingestPartitionID, i.logger); ok {
			offsets[filepath.Base(file)] = offset
		}
	}
	return json.Marshal(offsets)
}

// rewindKafkaOffsetsToCheckpoints moves each Kafka offset file back to the oldest offset the checkpoints have for it,
// which every restored head has the records up to. A file a checkpoint has no offset for may have records no head has,
// so it is removed, and the ingester replays that Kafka cluster from the retention period.
func (i *Ingester) rewindKafkaOffsetsToCheckpoints(checkpoints [][]byte) error {
	if len(checkpoints) == 0 {
		return nil
	}
	files, err := filepath.Glob(filepath.Join(i.cfg.BlocksStorageConfig.TSDB.Dir, "kafka-offset*.json"))
	if err != nil {
		return err
	}
	decoded := make([]map[string]int64, len(checkpoints))
	for index, checkpoint := range checkpoints {
		if err := json.Unmarshal(checkpoint, &decoded[index]); err != nil {
			return fmt.Errorf("decode a mimirengine checkpoint: %w", err)
		}
	}
	for _, file := range files {
		current, ok := ingest.ReadOffsetFile(file, i.ingestPartitionID, i.logger)
		if !ok {
			// The reader replays from the retention period without it already.
			continue
		}
		oldest, covered := current, true
		for _, offsets := range decoded {
			offset, ok := offsets[filepath.Base(file)]
			if !ok {
				covered = false
				break
			}
			oldest = min(oldest, offset)
		}
		if !covered {
			if err := os.Remove(file); err != nil && !os.IsNotExist(err) {
				return err
			}
			level.Warn(i.logger).Log("msg", "a mimirengine snapshot has no offset for a Kafka offset file, replaying it from the retention period", "file", file)
			continue
		}
		if oldest == current {
			continue
		}
		if err := ingest.WriteOffsetFile(file, i.ingestPartitionID, oldest, i.logger); err != nil {
			return err
		}
		level.Warn(i.logger).Log("msg", "tenants were restored from snapshots taken while ingesting, replaying Kafka from the oldest", "file", file, "offset", oldest, "committed_offset", current, "snapshots", len(checkpoints))
	}
	return nil
}

type mimirengineLockObserver struct {
	wait, held *prometheus.HistogramVec
}

func (o mimirengineLockObserver) ObserveWrite(phase string, waited, held time.Duration) {
	o.wait.WithLabelValues(phase).Observe(waited.Seconds())
	o.held.WithLabelValues(phase, "write").Observe(held.Seconds())
}

func (o mimirengineLockObserver) ObserveRead(phase string, held time.Duration) {
	o.held.WithLabelValues(phase, "read").Observe(held.Seconds())
}

var (
	mimirengineLockWriteHolds = prometheus.NewDesc("cortex_ingester_mimirengine_shard_lock_write_holds_total",
		"How many times the MimirEngine held a store shard's write lock, by phase.", []string{"phase"}, nil)
	mimirengineLockWriteHeld = prometheus.NewDesc("cortex_ingester_mimirengine_shard_lock_write_held_seconds_total",
		"Total time the MimirEngine held store shards' write locks, by phase.", []string{"phase"}, nil)
	mimirengineLockWriteWait = prometheus.NewDesc("cortex_ingester_mimirengine_shard_lock_write_wait_seconds_total",
		"Total time the MimirEngine's writers waited for store shards' write locks, by what then held them.", []string{"phase"}, nil)
)

// mimirengineLockTotals exports every write hold of the store's shard locks, which the lock histograms only sample: the
// store keeps the totals, as observing each hold was a few percent of a busy ingester's CPU.
type mimirengineLockTotals struct{}

func (mimirengineLockTotals) Describe(ch chan<- *prometheus.Desc) {
	ch <- mimirengineLockWriteHolds
	ch <- mimirengineLockWriteHeld
	ch <- mimirengineLockWriteWait
}

func (mimirengineLockTotals) Collect(ch chan<- prometheus.Metric) {
	store.LockTotals(func(phase string, holds int64, held, waited time.Duration) {
		ch <- prometheus.MustNewConstMetric(mimirengineLockWriteHolds, prometheus.CounterValue, float64(holds), phase)
		ch <- prometheus.MustNewConstMetric(mimirengineLockWriteHeld, prometheus.CounterValue, held.Seconds(), phase)
		ch <- prometheus.MustNewConstMetric(mimirengineLockWriteWait, prometheus.CounterValue, waited.Seconds(), phase)
	})
}

var (
	mimirengineColdCandidatesHits = prometheus.NewDesc("cortex_ingester_mimirengine_cold_candidates_cache_hits_total",
		"How many times a selector's candidate series in a cold block were found kept.", nil, nil)
	mimirengineColdCandidatesMisses = prometheus.NewDesc("cortex_ingester_mimirengine_cold_candidates_cache_misses_total",
		"How many times a selector's candidate series in a cold block were looked up in its postings.", nil, nil)
)

// mimirengineColdCandidates exports how often the cold blocks' kept candidates answer, which the store counts.
type mimirengineColdCandidates struct{}

func (mimirengineColdCandidates) Describe(ch chan<- *prometheus.Desc) {
	ch <- mimirengineColdCandidatesHits
	ch <- mimirengineColdCandidatesMisses
}

func (mimirengineColdCandidates) Collect(ch chan<- prometheus.Metric) {
	hits, misses := store.ColdCandidatesCacheCounts()
	ch <- prometheus.MustNewConstMetric(mimirengineColdCandidatesHits, prometheus.CounterValue, float64(hits))
	ch <- prometheus.MustNewConstMetric(mimirengineColdCandidatesMisses, prometheus.CounterValue, float64(misses))
}

// observeMimirEngineLocks has the store's shard locks observed by the ingester's metrics: the store's lock observer is
// the process's, as its shards' are.
func observeMimirEngineLocks(m *ingesterMetrics) {
	store.SetLockObserver(mimirengineLockObserver{wait: m.mimirengineShardLockWait, held: m.mimirengineShardLockHeld})
}
