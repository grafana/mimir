// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/grafana/mimir/pkg/storage/seriesstore/store"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

// seriesstoreEngine is pkg/storage/seriesstore as a tenantEngine. It writes no Prometheus blocks:
// series leaving the head go to its own cold blocks, which it queries and expires itself.
type seriesstoreEngine struct {
	*store.Engine
	// The time range of a block, which a flush compacts the head by.
	blockDuration int64
	// Head compactions that moved the head's min time, which is when Prometheus writes a block.
	compactions prometheus.Counter
}

// seriesstoreShards is how many store shards a tenant with up to a few hundred thousand series
// spreads them over: appends to a tenant come from the pusher's parallel shards, and each store
// shard has its own lock.
const seriesstoreShards = 16

// seriesstoreShardsFor is how many store shards a tenant with a limit of maxSeries series gets. The
// pusher's parallel shards wait on each other's commits of a store shard, which costs a large
// tenant's appends a third of their throughput at 16 shards, while a small tenant's few series
// would only pay for the extra shards' state. A shard count is kept by the tenant's snapshot.
func seriesstoreShardsFor(maxSeries int) int {
	switch {
	case maxSeries >= 1_000_000:
		return 4 * seriesstoreShards
	case maxSeries >= 300_000:
		return 2 * seriesstoreShards
	default:
		return seriesstoreShards
	}
}

func openSeriesstoreEngine(dir, userID string, reg prometheus.Registerer, opts *tsdb.Options, shards int) (tenantEngine, error) {
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
	return seriesstoreEngine{Engine: engine, blockDuration: opts.MinBlockDuration, compactions: compactions}, nil
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

// Ingest appends the floats of series the engine has already by shard, taking each shard's lock once, and the
// rest of the batch with the engine's Prometheus-style appender: new series, histograms, exemplars, created
// timestamps and staleness markers.
func (e seriesstoreEngine) Ingest(ctx context.Context, batch ingestBatch, sink ingestSink) (ingestOutcome, error) {
	var fast, other []int
	for i := range batch.Series {
		if sink.Skip(i) {
			continue
		}
		ts := &batch.Series[i]
		if len(ts.Samples) > 0 && ts.CreatedTimestamp == 0 && (!batch.NativeHistograms || len(ts.Histograms) == 0) && (!batch.Exemplars || len(ts.Exemplars) == 0) {
			fast = append(fast, i)
		} else {
			other = append(other, i)
		}
	}

	var outcome ingestOutcome
	atMs := batch.IngestedAt.UnixMilli()
	leftover, ingested, err := e.Engine.AppendFloats(batch.Series, fast, batch.MinTimestampMs, batch.MaxTimestampMs, atMs, batch.OTLP, floatSink{sink})
	outcome.Samples = ingested
	if err != nil {
		return outcome, err
	}
	// In the order of the request: a series may be created by one that comes before it, such as for its exemplars.
	other = append(other, leftover...)
	slices.Sort(other)
	if len(other) == 0 {
		return outcome, nil
	}

	// The series the appender ingests are marked ingested once it has, for the active series.
	marker := &ingestedMarker{ingestSink: sink}
	rest, err := ingestSubsetThroughAppender(e.Engine.Appender(ctx), batch, marker, other)
	e.Engine.MarkIngested(marker.series, atMs, batch.OTLP)
	outcome.Samples += rest.Samples
	outcome.Exemplars += rest.Exemplars
	outcome.CommitDuration = rest.CommitDuration
	return outcome, err
}

// ingestedMarker collects the series an ingest got samples for.
type ingestedMarker struct {
	ingestSink
	series []store.IngestedSeries
}

func (m *ingestedMarker) Ingested(series int, lbls labels.Labels, ref storage.SeriesRef, histogramBuckets int) {
	m.series = append(m.series, store.IngestedSeries{Ref: ref, HistogramBuckets: histogramBuckets})
	m.ingestSink.Ingested(series, lbls, ref, histogramBuckets)
}

// floatSink tells an ingest sink what AppendFloats does: the series it ingests have no histograms.
type floatSink struct {
	ingestSink
}

// NeedsLabels is whether the active series tracker is on, which is the only one to want a series' labels.
func (s floatSink) NeedsLabels() bool {
	return s.ingestSink.NeedsLabels()
}

func (s floatSink) Ingested(series int, lbls labels.Labels, ref storage.SeriesRef) {
	s.ingestSink.Ingested(series, lbls, ref, -1)
}

func (e seriesstoreEngine) ChunkQuerier(mint, maxt int64, unordered bool) (storage.ChunkQuerier, error) {
	if unordered {
		return e.Engine.UnorderedChunkQuerier(mint, maxt)
	}
	return e.Engine.ChunkQuerier(mint, maxt)
}

func (e seriesstoreEngine) Compact(ctx context.Context) error {
	defer e.countCompaction(e.MinTime())
	return e.Engine.Compact(ctx)
}

// Flush compacts the head block by block up to upTo, then the out-of-order head.
func (e seriesstoreEngine) Flush(ctx context.Context, upTo int64) error {
	for {
		blockMinTime, blockMaxTime, isValid, isLast := nextForcedHeadCompactionRange(e.blockDuration, e.MinTime(), e.MaxTime(), upTo)
		if !isValid {
			break
		}

		minTimeBefore := e.MinTime()
		err := e.Engine.CompactHead(blockMinTime, blockMaxTime)
		e.countCompaction(minTimeBefore)
		if err != nil {
			return err
		}

		// Do not check again if it was the last range.
		if isLast {
			break
		}
	}

	return e.Engine.CompactOOOHead(ctx)
}

// Evict compacts the out-of-order head first, which persists the out-of-order data of the series before they
// leave, then the series themselves. The first failure doesn't stop the second.
func (e seriesstoreEngine) Evict(ctx context.Context, refs []storage.SeriesRef) error {
	oooErr := e.Engine.CompactOOOHead(ctx)
	if oooErr != nil {
		oooErr = fmt.Errorf("compact the out-of-order head: %w", oooErr)
	}
	selectedErr := e.Engine.CompactSelectedSeries(refs)
	if selectedErr != nil {
		selectedErr = fmt.Errorf("compact the selected series: %w", selectedErr)
	}
	return errors.Join(oooErr, selectedErr)
}

func (e seriesstoreEngine) countCompaction(minTimeBefore int64) {
	if e.MinTime() != minTimeBefore {
		e.compactions.Inc()
	}
}

func (e seriesstoreEngine) Configure(settings engineSettings) error {
	e.SetMaxExemplars(settings.MaxExemplars)
	e.SetOutOfOrderTimeWindow(settings.OutOfOrderTimeWindow)
	return nil
}

func (e seriesstoreEngine) Head() engineHead {
	return seriesstoreHead{e.Engine}
}

type seriesstoreHead struct {
	*store.Engine
}

var _ activeSeriesHead = seriesstoreHead{}

func (h seriesstoreHead) ActiveSeries(cutoff time.Time) activeCounts {
	counts := h.Engine.ActiveSeries(cutoff.UnixMilli())
	out := activeCounts{Total: int(counts.Total), OTLP: int(counts.OTLP), NativeHistograms: int(counts.NativeHistograms), NativeHistogramBuckets: int(counts.NativeHistogramBuckets), Trackers: make([]trackerCounts, len(counts.Trackers))}
	for i, tracker := range counts.Trackers {
		out.Trackers[i] = trackerCounts{Total: int(tracker.Total), NativeHistograms: int(tracker.NativeHistograms), NativeHistogramBuckets: int(tracker.NativeHistogramBuckets)}
	}
	return out
}

func (h seriesstoreHead) SetActiveTrackers(matchers *asmodel.Matchers) {
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

func (h seriesstoreHead) DeactivateSeries(refs []storage.SeriesRef) { h.Engine.DeactivateSeries(refs) }
func (h seriesstoreHead) DeactivateAll()                            { h.Engine.DeactivateAll() }

func (h seriesstoreHead) ActiveRefs(cutoff time.Time) activeseries.ActiveRefs {
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

func (h seriesstoreHead) TimeBounds() timeBounds {
	return timeBounds{MinTime: h.MinTime(), MaxTime: h.MaxTime(), MinOOOTime: h.MinOOOTime(), MaxOOOTime: h.MaxOOOTime()}
}

func (h seriesstoreHead) OldestAppendableTime() (int64, bool) {
	return h.AppendableMinValidTime()
}

func (h seriesstoreHead) Ownership(owned ring.TokenRanges) (int, []storage.SeriesRef) {
	var (
		count    int
		nonOwned []storage.SeriesRef
	)
	h.ForEachSecondaryHash(func(refs []chunks.HeadSeriesRef, secondaryHashes []uint32) {
		for i, hash := range secondaryHashes {
			if owned.IncludesKey(hash) {
				count++
			} else {
				nonOwned = append(nonOwned, storage.SeriesRef(refs[i]))
			}
		}
	})
	return count, nonOwned
}

func (h seriesstoreHead) ShardSeries(shardIndex, shardCount uint64) index.Postings {
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

// Sync is the engine's durability point, which is nothing: it has no log, and what a crash loses comes back from Kafka.
func (h seriesstoreHead) Sync() error {
	return h.FsyncWLSegments()
}

// ValidateSeriesstoreEngine refuses configurations the seriesstore engine can't serve as the
// Prometheus TSDB would. It only runs in ingesters that select the engine.
func ValidateSeriesstoreEngine(tsdbCfg mimir_tsdb.TSDBConfig, ingesterCfg Config, ingestCfg ingest.Config) error {
	if tsdbCfg.Engine != mimir_tsdb.EngineSeriesstore {
		return nil
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
		return fmt.Errorf("the %s TSDB engine %w", mimir_tsdb.EngineSeriesstore, errors.Join(errs...))
	}
	return nil
}

// resetKafkaOffsetsAfterUncleanShutdown removes the file-stored Kafka offsets when a seriesstore
// tenant wasn't restored from a clean shutdown: its head would miss what was consumed since, so the
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
	level.Warn(i.logger).Log("msg", "tenants weren't restored from a clean shutdown, replaying Kafka from the retention period", "tenants", len(unclean), "example_tenant", unclean[0], "removed_offset_files", len(files))
	return nil
}

// uncleanlyRestored reports whether db is a seriesstore engine that opened without a clean
// shutdown's snapshot.
func uncleanlyRestored(db tenantEngine) bool {
	engine, ok := db.(seriesstoreEngine)
	return ok && !engine.Restored()
}
