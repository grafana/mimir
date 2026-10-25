// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
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
	"github.com/grafana/mimir/pkg/mimirpb"
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

// testNoIdleEviction makes tests keep the series they expect in memory; it's always false outside tests.
var testNoIdleEviction bool

// testNoCompactionJitter makes tests compact when they expect to, not a random time later; it's always false outside
// tests.
var testNoCompactionJitter bool

// openSeriesstoreEngine opens the tenant's engine, which evicts series idle for idleEviction, with 0 for never.
func openSeriesstoreEngine(dir, userID string, reg prometheus.Registerer, opts *tsdb.Options, shards int, checkpoints, jitter bool, idleEviction time.Duration, onCoalesced func(), onResourceLimited func(store.ResourceLimit)) (tenantEngine, error) {
	if testNoIdleEviction {
		idleEviction = 0
	}
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
		Shards:                 shards,
		RetentionMs:            opts.RetentionDuration,
		OutOfOrderTimeWindowMs: opts.OutOfOrderTimeWindow,
		MaxExemplars:           opts.MaxExemplars,
		TimelyCompaction:       opts.TimelyCompaction,
		JitterCompaction:       jitter,
		IdleEvictionMs:         idleEviction.Milliseconds(),
		// The engine's chunk ranges are the TSDB's block ranges.
		ChunkRangeMs:            opts.MinBlockDuration,
		Checkpoints:             checkpoints,
		SeriesLifecycleCallback: callback,
		SecondaryHashFunction:   opts.SecondaryHashFunction,
		// The changes of the series' metadata over the change limit, which the engine coalesces, are discarded metadata.
		OnMetricMetadataCoalesced: onCoalesced,
		IndexedResourceAttributes: slices.Collect(maps.Keys(opts.IndexedResourceAttrs)),
		// So are the changes of the series' resources over the limits.
		OnResourceLimited: onResourceLimited,
	})
	if err != nil {
		return nil, err
	}
	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_head_series",
		Help: "Total number of series in the head block.",
	}, func() float64 { return float64(engine.NumSeries()) })
	registerSeriesstoreMetricMetadata(reg, engine)
	registerSeriesstoreResources(reg, engine)
	compactions := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_compactions_total",
		Help: "Total number of compactions that were executed for the partition.",
	})
	return seriesstoreEngine{Engine: engine, blockDuration: opts.MinBlockDuration, compactions: compactions}, nil
}

// registerSeriesstoreMetricMetadata registers the engine's native metric metadata metrics on the tenant's registry:
// the Prometheus TSDB's, by the same names, which the ingester's TSDB metrics collect for both engines, and what only
// the engine's keeping of it has.
func registerSeriesstoreMetricMetadata(reg prometheus.Registerer, engine *store.Engine) {
	stats := func(of func(store.MetricMetadataStats) float64) func() float64 {
		return func() float64 { return of(engine.MetricMetadataStats()) }
	}
	for _, gauge := range []struct {
		name, help string
		of         func(store.MetricMetadataStats) float64
	}{
		{"prometheus_tsdb_head_native_metric_metadata_series", "Number of head series with native metric metadata.", func(s store.MetricMetadataStats) float64 { return float64(s.Series) }},
		{"prometheus_tsdb_head_native_metric_metadata_versions", "Number of native metric metadata versions in the head.", func(s store.MetricMetadataStats) float64 { return float64(s.Versions) }},
		{"seriesstore_native_metric_metadata_dictionary_values", "Number of distinct native metric metadata values the engine keeps.", func(s store.MetricMetadataStats) float64 { return float64(s.Values) }},
		{"seriesstore_native_metric_metadata_dictionary_bytes", "Bytes of the type, unit and help of the native metric metadata values the engine keeps.", func(s store.MetricMetadataStats) float64 { return float64(s.ValueBytes) }},
		{"seriesstore_series_entry_capacity", "Number of series entries the engine's name groups have room for.", func(s store.MetricMetadataStats) float64 { return float64(s.EntryCapacity) }},
	} {
		promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{Name: gauge.name, Help: gauge.help}, stats(gauge.of))
	}
	for _, counter := range []struct {
		name, help string
		of         func(store.MetricMetadataStats) float64
	}{
		{"prometheus_tsdb_head_native_metric_metadata_oversized_total", "Total number of appended samples whose native metric metadata exceeded the size limit and was skipped.", func(s store.MetricMetadataStats) float64 { return float64(s.Oversized) }},
		{"prometheus_tsdb_head_native_metric_metadata_version_evictions_total", "Total number of native metric metadata versions evicted by the per-series limit.", func(s store.MetricMetadataStats) float64 { return float64(s.Evictions) }},
		{"seriesstore_native_metric_metadata_clamped_total", "Total number of native metric metadata runs whose start was clamped to their shard's base.", func(s store.MetricMetadataStats) float64 { return float64(s.Clamped) }},
		{"seriesstore_native_metric_metadata_memo_hits_total", "Total number of native metric metadata values found in a batch's memo.", func(s store.MetricMetadataStats) float64 { return float64(s.MemoHits) }},
		{"seriesstore_native_metric_metadata_memo_misses_total", "Total number of native metric metadata values a batch's memo didn't have yet.", func(s store.MetricMetadataStats) float64 { return float64(s.MemoMisses) }},
	} {
		promauto.With(reg).NewCounterFunc(prometheus.CounterOpts{Name: counter.name, Help: counter.help}, stats(counter.of))
	}
}

// registerSeriesstoreResources registers the engine's native resources metrics on the tenant's registry.
func registerSeriesstoreResources(reg prometheus.Registerer, engine *store.Engine) {
	stats := func(of func(store.ResourceStats) float64) func() float64 {
		return func() float64 { return of(engine.ResourceStats()) }
	}
	for _, gauge := range []struct {
		name, help string
		of         func(store.ResourceStats) float64
	}{
		{"seriesstore_native_resources_series", "Number of head series with a native resource.", func(s store.ResourceStats) float64 { return float64(s.Series) }},
		{"seriesstore_native_resources_runs", "Number of runs of the head series' resource identities.", func(s store.ResourceStats) float64 { return float64(s.Runs) }},
		{"seriesstore_native_resources_identities", "Number of distinct resource identities the engine keeps.", func(s store.ResourceStats) float64 { return float64(s.Identities) }},
		{"seriesstore_native_resources_descriptions", "Number of distinct resource descriptions the engine keeps.", func(s store.ResourceStats) float64 { return float64(s.Descriptions) }},
		{"seriesstore_native_resources_versions", "Number of versions of the resource identities' descriptions.", func(s store.ResourceStats) float64 { return float64(s.Versions) }},
		{"seriesstore_native_resources_dictionary_bytes", "Bytes of the resource attributes the engine keeps.", func(s store.ResourceStats) float64 { return float64(s.Bytes) }},
	} {
		promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{Name: gauge.name, Help: gauge.help}, stats(gauge.of))
	}
	for _, counter := range []struct {
		name, help string
		of         func(store.ResourceStats) float64
	}{
		{"seriesstore_native_resources_coalesced_identity_changes_total", "Total number of ranges of unknown resource, for identity changes over the limit.", func(s store.ResourceStats) float64 { return float64(s.CoalescedIdentityChanges) }},
		{"seriesstore_native_resources_coalesced_versions_total", "Total number of coalesced versions, for description changes over the limit.", func(s store.ResourceStats) float64 { return float64(s.CoalescedVersions) }},
		{"seriesstore_native_resources_refused_total", "Total number of resource values the limits on new identities and bytes refused.", func(s store.ResourceStats) float64 { return float64(s.Refused) }},
		{"seriesstore_native_resources_clamped_total", "Total number of resource runs whose start was clamped to their shard's base.", func(s store.ResourceStats) float64 { return float64(s.Clamped) }},
		{"seriesstore_native_resources_memo_hits_total", "Total number of resources found in a batch's memo.", func(s store.ResourceStats) float64 { return float64(s.MemoHits) }},
		{"seriesstore_native_resources_memo_misses_total", "Total number of resources a batch's memo didn't have yet.", func(s store.ResourceStats) float64 { return float64(s.MemoMisses) }},
		{"seriesstore_native_resources_evictions_total", "Total number of resource runs and versions evicted by the per-series cap.", func(s store.ResourceStats) float64 { return float64(s.Evictions) }},
	} {
		promauto.With(reg).NewCounterFunc(prometheus.CounterOpts{Name: counter.name, Help: counter.help}, stats(counter.of))
	}
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
	// The appender gets the series' metadata with their samples when it's kept, and the store's own paths this way.
	e.Engine.SetNativeMetricMetadata(batch.NativeMetricMetadata)
	e.Engine.SetMetricMetadataChangeLimit(batch.NativeMetricMetadataMaxChanges, batch.NativeMetricMetadataWindowMs)
	e.Engine.SetNativeResources(batch.NativeResources)
	e.Engine.SetResourceLimits(batch.NativeResourceLimits)
	if batch.Record != nil {
		return e.ingestRecord(ctx, batch, sink)
	}
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
			ingestPaths.count(ts, batch)
		}
	}
	ingestPaths.series.Add(uint64(len(batch.Series)))
	ingestPaths.fast.Add(uint64(len(fast)))
	ingestPaths.maybeLog(e.Engine)

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

	return e.ingestThroughAppender(ctx, batch, sink, other, outcome)
}

// ingestThroughAppender ingests the series at indices with the engine's appender, adding to outcome.
func (e seriesstoreEngine) ingestThroughAppender(ctx context.Context, batch ingestBatch, sink ingestSink, indices []int, outcome ingestOutcome) (ingestOutcome, error) {
	// The series the appender ingests are marked ingested once it has, for the active series.
	marker := &ingestedMarker{ingestSink: sink}
	rest, err := ingestSubsetThroughAppender(e.Engine.AppenderV2(ctx), batch, marker, indices)
	e.Engine.MarkIngested(marker.series, batch.IngestedAt.UnixMilli(), batch.OTLP)
	outcome.Samples += rest.Samples
	outcome.Exemplars += rest.Exemplars
	outcome.CommitDuration = rest.CommitDuration
	return outcome, err
}

// ReadRecord reads a version 2 record in place.
func (e seriesstoreEngine) ReadRecord(content []byte, version int) (ingestRecord, bool) {
	if version != 2 {
		return nil, false
	}
	record, err := store.ReadRW2(content, ingest.V2CommonSymbols.GetSlice(), ingest.V2RecordSymbolOffset)
	if err != nil {
		// Decoding it tells what is wrong with it.
		return nil, false
	}
	return record, true
}

// ingestRecord appends the floats of the series of the batch's record the engine has without decoding it, and decodes
// it for the others: series it doesn't have yet, and series with more than floats.
func (e seriesstoreEngine) ingestRecord(ctx context.Context, batch ingestBatch, sink ingestSink) (ingestOutcome, error) {
	var outcome ingestOutcome
	record := batch.Record.(*store.RW2Record)
	leftover, ingested, err := e.Engine.AppendRW2(record, batch.MinTimestampMs, batch.MaxTimestampMs, batch.IngestedAt.UnixMilli(), batch.OTLP, floatSink{sink})
	outcome.Samples = ingested
	ingestPaths.series.Add(uint64(record.Len()))
	ingestPaths.fast.Add(uint64(record.Len() - len(leftover)))
	ingestPaths.maybeLog(e.Engine)
	if err != nil || len(leftover) == 0 {
		return outcome, err
	}
	batch.Series = batch.Decoded(leftover)
	other := leftover[:0]
	for _, i := range leftover {
		if !sink.Skip(i) {
			other = append(other, i)
			ingestPaths.count(&batch.Series[i], batch)
		}
	}
	if len(other) == 0 {
		return outcome, nil
	}
	return e.ingestThroughAppender(ctx, batch, sink, other, outcome)
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

// refIngestSink is an ingestSink that may know an ingested series by its ref, without its labels.
type refIngestSink interface {
	IngestedRef(series int, ref storage.SeriesRef) bool
}

// IngestedRef has the engine build a series' labels only for a sink that doesn't know the series and wants them.
func (s floatSink) IngestedRef(series int, ref storage.SeriesRef) bool {
	if known, ok := s.ingestSink.(refIngestSink); ok {
		return known.IngestedRef(series, ref)
	}
	if s.ingestSink.NeedsLabels() {
		return false
	}
	s.ingestSink.Ingested(series, labels.EmptyLabels(), ref, -1)
	return true
}

func (e seriesstoreEngine) ChunkQuerier(mint, maxt int64, unordered bool) (storage.ChunkQuerier, error) {
	var q storage.ChunkQuerier
	var err error
	if unordered {
		q, err = e.Engine.UnorderedChunkQuerier(mint, maxt)
	} else {
		q, err = e.Engine.ChunkQuerier(mint, maxt)
	}
	if err != nil {
		return nil, err
	}
	return wrapStreaming(q), nil
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

func (h seriesstoreHead) SetCostAttribution(sink costAttributionSink) {
	h.Engine.SetCostAttribution(sink)
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

// LabelValuesBytes returns nil: the engine doesn't track the bytes of label values.
func (h seriesstoreHead) LabelValuesBytes() map[string]uint64 {
	return nil
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

// uncleanlyRestored reports whether db is a seriesstore engine that opened without a snapshot.
func uncleanlyRestored(db tenantEngine) bool {
	engine, ok := db.(seriesstoreEngine)
	return ok && !engine.Restored()
}

// restoredCheckpoint is the checkpoint db's seriesstore engine restored its head from, nil when it restored a clean
// shutdown's snapshot or isn't one.
func restoredCheckpoint(db tenantEngine) []byte {
	if engine, ok := db.(seriesstoreEngine); ok {
		return engine.RestoredCheckpoint()
	}
	return nil
}

// seriesstoreSnapshotDuty is the share of a head snapshot's time it works between its batches of series: it takes ten
// times as long as it could, and leaves the CPU and disk to queries.
const seriesstoreSnapshotDuty = 0.1

const (
	snapshotOutcomeSuccess = "success"
	snapshotOutcomeFailure = "failure"
)

// seriesstoreSnapshotsRunning snapshots the head of every seriesstore tenant each interval, one tenant at a time.
func (i *Ingester) seriesstoreSnapshotsRunning(ctx context.Context) error {
	ticker := time.NewTicker(i.cfg.BlocksStorageConfig.TSDB.SeriesstoreSnapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			i.snapshotSeriesstoreHeads(ctx)
		}
	}
}

// snapshotSeriesstoreHeads checkpoints each tenant's head with the Kafka offsets committed before it started: the
// head has every record up to them, and an unclean restart replays from the oldest checkpoint.
func (i *Ingester) snapshotSeriesstoreHeads(ctx context.Context) {
	started := time.Now()
	snapshots, failures := 0, 0
	for _, userID := range i.getTSDBUsers() {
		if ctx.Err() != nil {
			return
		}
		db := i.getTSDB(userID)
		if db == nil {
			continue
		}
		engine, ok := db.db.(seriesstoreEngine)
		if !ok {
			continue
		}
		checkpoint, err := i.committedKafkaOffsets()
		if err == nil {
			err = engine.Checkpoint(ctx, checkpoint, seriesstoreSnapshotDuty)
		}
		if err != nil {
			if ctx.Err() == nil {
				failures++
				i.metrics.seriesstoreSnapshots.WithLabelValues(snapshotOutcomeFailure).Inc()
				level.Warn(i.logger).Log("msg", "failed to snapshot the head", "user", userID, "err", err)
			}
			continue
		}
		snapshots++
		i.metrics.seriesstoreSnapshots.WithLabelValues(snapshotOutcomeSuccess).Inc()
	}
	i.metrics.seriesstoreSnapshotRoundDuration.Observe(time.Since(started).Seconds())
	i.metrics.seriesstoreLastSnapshotRound.SetToCurrentTime()
	level.Info(i.logger).Log("msg", "snapshotted the heads", "tenants", snapshots, "failures", failures, "duration", time.Since(started))
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
			return fmt.Errorf("decode a seriesstore checkpoint: %w", err)
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
			level.Warn(i.logger).Log("msg", "a seriesstore snapshot has no offset for a Kafka offset file, replaying it from the retention period", "file", file)
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

// ingestSubsetThroughAppender is ingestThroughAppender for the series at the indices only, which the sink has been
// asked to skip already.
func ingestSubsetThroughAppender(app extendedAppender, batch ingestBatch, sink ingestSink, subset []int) (ingestOutcome, error) {
	var resources resourceStep
	if batch.NativeResources {
		resources = newSeriesstoreResources()
	}
	outcome, err := appendBatch(app, batch, sink, subset, true, resources)
	if err != nil {
		if rollbackErr := app.Rollback(); rollbackErr != nil {
			err = errors.Join(err, fmt.Errorf("roll back the appender: %w", rollbackErr))
		}
		return outcome, err
	}

	startCommit := time.Now()
	if err := app.Commit(); err != nil {
		return outcome, err
	}
	outcome.CommitDuration = time.Since(startCommit)
	return outcome, nil
}

// ingestPathStats counts why series take the appender path, to find what it is for. Temporary.
type ingestPathStats struct {
	series, fast                                               atomic.Uint64
	createdTimestamp, histograms, exemplars, noSamples, others atomic.Uint64
	histogramSamples                                           atomic.Uint64
	last                                                       atomic.Int64
}

var ingestPaths ingestPathStats

func (s *ingestPathStats) count(ts *mimirpb.PreallocTimeseries, batch ingestBatch) {
	switch {
	case len(ts.Histograms) > 0 && batch.NativeHistograms:
		s.histograms.Add(1)
		s.histogramSamples.Add(uint64(len(ts.Histograms)))
	case len(ts.Exemplars) > 0 && batch.Exemplars:
		s.exemplars.Add(1)
	case ts.CreatedTimestamp != 0:
		s.createdTimestamp.Add(1)
	case len(ts.Samples) == 0:
		s.noSamples.Add(1)
	default:
		s.others.Add(1)
	}
}

func (s *ingestPathStats) maybeLog(engine *store.Engine) {
	now := time.Now().UnixNano()
	last := s.last.Load()
	if now-last < int64(time.Minute) || !s.last.CompareAndSwap(last, now) {
		return
	}
	leftovers := engine.FloatLeftovers()
	fmt.Fprintf(os.Stderr, "ingestlog series=%d fast=%d histograms=%d (samples %d) exemplars=%d createdTimestamp=%d noSamples=%d other=%d leftover: unknown=%d noRef=%d stale=%d records=%d read=%d whole=%d subsets=%d\n",
		s.series.Load(), s.fast.Load(), s.histograms.Load(), s.histogramSamples.Load(), s.exemplars.Load(), s.createdTimestamp.Load(), s.noSamples.Load(), s.others.Load(), leftovers[0], leftovers[1], leftovers[2],
		recordStats.records.Load(), recordStats.direct.Load(), recordStats.whole.Load(), recordStats.subsets.Load())
}
