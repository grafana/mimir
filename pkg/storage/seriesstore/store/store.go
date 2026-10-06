// SPDX-License-Identifier: AGPL-3.0-only

// Package store keeps the ingester's series: labels in compact shared buffers grouped by metric
// name with postings for the other labels, open chunks on the heap, completed chunks in
// memory-mapped chunk files, and series that left the emulated Go head in immutable cold blocks.
// Series are sharded by label hash so records apply to several shards in parallel, the way Go
// ingests with concurrent appenders; a series always lives in one shard, which keeps its samples
// in order.
package store

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
	"github.com/grafana/mimir/pkg/storage/seriesstore/exemplars"
	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
	"github.com/grafana/mimir/pkg/storage/seriesstore/limits"
	"github.com/grafana/mimir/pkg/storage/seriesstore/metrics"
	"github.com/grafana/mimir/pkg/storage/seriesstore/record"
	"github.com/grafana/mimir/pkg/storage/seriesstore/trackers"
)

// DefaultShards is how many store shards a store has, each with its own lock.
const DefaultShards = 16

// Below this many series, a batch is applied on the calling goroutine: once caught up, a batch is
// a few small records, and handing them to other threads cost more than the work.
const parallelMinSeries = 1024

// Mimir's head appender rejects in-order samples more than half a block range behind the head.
const minValidTimeWindowMs = chunkRangeMs / 2

// Retention is how long samples are kept, when Set.
type Retention struct {
	Ms  int64
	Set bool
}

// metadataKey identifies a metric family's metadata entry: type, help and unit.
type metadataKey struct {
	metricType int32
	help, unit string
}

type metadataEntry struct {
	metadata mimirpb.MetricMetadata
	seenMs   int64
}

type ingestedSamples struct {
	at      time.Time
	source  int32
	samples uint64
}

type tenant struct {
	series *seriesByName
	// Tenant metadata, ingestion rates and the head's max time live only in shard 0.
	metadata map[string]map[metadataKey]metadataEntry
	ingested []ingestedSamples
	maxTime  int64
	// The oldest accepted sample, like the Go head's min time before its first compaction.
	minTime int64
	// The min time of the emulated Go head, which head compaction moves in block-range steps.
	headMin int64
	// The ranges the owned series were last recomputed with, and whether a head compaction asks
	// for another recompute, which are Mimir's reasons to recompute owned series.
	ownedRangesSeen *TenantRanges
	ownedRecompute  bool
	// The emulated head's series count at the last head tick.
	headSeries uint64
	// Where the last compaction truncated the head, Prometheus's minValidTime.
	truncatedTo int64
	// The custom trackers the series' cached matches are for, and their generation. Like Go, only
	// a change to the tenant's trackers invalidates them: the runtime config reloads whenever any
	// tenant's overrides change, and matching every series again was a tenth of an ingester's CPU.
	trackers          *trackers.CustomTrackers
	trackerGeneration uint64
	// An engine's series of this shard by reference.
	byRef refIndex
	// An engine's out-of-order head bounds, and the start of its oldest emulated block, in the
	// home shard.
	minOOOTime, maxOOOTime int64
	// The blocks an engine's compactions would have written, oldest first; replaced, never
	// changed, so lookups can keep reading a copy of the slice.
	blocks []emulatedBlock
}

func newTenant() *tenant {
	return &tenant{
		series:      newSeriesByName(),
		metadata:    map[string]map[metadataKey]metadataEntry{},
		maxTime:     math.MinInt64,
		minTime:     math.MaxInt64,
		headMin:     math.MinInt64,
		truncatedTo: math.MinInt64,
		minOOOTime:  math.MaxInt64,
		maxOOOTime:  math.MinInt64,
		// Series start at generation 0, so they match the trackers on the first report.
		trackerGeneration: 1,
	}
}

type shardState struct {
	sync.RWMutex
	tenants map[string]*tenant
	disk    *chunks.DiskMapper
	cold    *coldState
}

func tenantOf(tenants map[string]*tenant, id string) *tenant {
	t, ok := tenants[id]
	if !ok {
		t = newTenant()
		tenants[id] = t
	}
	return t
}

// TenantRanges are a tenant's token ranges in this partition, as the ring sidecar reports them:
// sorted pairs of inclusive bounds, or none when the tenant's shuffle shard skips the partition.
type TenantRanges struct {
	InShard bool
	Ranges  []uint32
}

func (r *TenantRanges) equal(other *TenantRanges) bool {
	return r.InShard == other.InShard && slices.Equal(r.Ranges, other.Ranges)
}

// EarlyHeadCompaction is Mimir's `-blocks-storage.tsdb.early-head-compaction-min-in-memory-series`
// and `-...-min-estimated-series-reduction-percentage`: with that many series in memory, the
// tenants whose inactive series would get the count back under it, or that would drop at least the
// percentage, compact their head up to the active series idle timeout ago.
type EarlyHeadCompaction struct {
	MinInMemorySeries      uint64
	MinReductionPercentage uint64
}

// NonOwnedEviction is Mimir's `-ingester.early-compaction-non-owned-series-*`: series found
// non-owned by an owned series recompute leave the head at the next compaction once non-owned for
// the min grace period (the tenant's head holding its local
// `early_head_compaction_owned_series_threshold`) or the max grace period (any tenant with a
// threshold), plus a per-process jitter.
type NonOwnedEviction struct {
	MinGraceMs int64
	MaxGraceMs int64
	JitterMs   int64
}

// PusherShards is how Mimir's ingest pusher sizes a tenant's shards for the records of one fetch:
// `-ingest-storage.kafka.ingestion-concurrency-max`, `-...-estimated-bytes-per-sample` and
// `-...-target-flushes-per-shard`.
type PusherShards struct {
	Max            int
	BytesPerSample int
	TargetFlushes  int
}

// DefaultPusherShards are Mimir's defaults.
func DefaultPusherShards() PusherShards {
	return PusherShards{Max: 8, BytesPerSample: 200, TargetFlushes: 40}
}

func (p PusherShards) count(bytes, flushSeries int) int {
	if flushSeries == 0 {
		return 1
	}
	count := uint64(bytes) / uint64(max(p.BytesPerSample, 1)) / uint64(flushSeries) / uint64(max(p.TargetFlushes, 1))
	return int(max(min(count, uint64(p.Max)), 1))
}

type costAttributionKey struct{ tenant, tracker string }

type costAttributionState struct {
	overflowSince int64
	overflowing   bool
}

// Store holds every tenant's series.
type Store struct {
	// Set by the engine, which keeps only head series in memory: see headView.memoryIsHead.
	memoryIsHead bool
	// Bumped when a series is evicted from the head, which invalidates what the label lookups
	// cached about the series in it.
	evictEpoch     atomic.Uint64
	shards         []*shardState
	threads        int
	activeWindowMs int64
	retention      Retention
	overrides      *limits.Overrides

	exemplarsLock sync.Mutex
	exemplars     map[string]*exemplars.TenantExemplars[labels.Labels]

	costAttributionLock        sync.Mutex
	costAttribution            map[costAttributionKey]*costAttributionState
	costAttributionLastCleanup int64
	// `-cost-attribution.cleanup-interval` and `-cost-attribution.eviction-interval`.
	costAttributionCleanupMs, costAttributionEvictionMs int64

	// Per tenant, the token ranges of this partition in the tenant's shuffle shard; nil until the
	// ring sidecar answered.
	ownedRanges atomic.Pointer[map[string]TenantRanges]

	// Samples ingested since startup, for the ingestion rate EWMA.
	ingestedSamples atomic.Uint64
	// Series per ingest pusher flush, `-ingest-storage.kafka.ingestion-concurrency-batch-size`, or
	// 0 when `-ingest-storage.kafka.ingestion-concurrency-max` is 0 and records push alone.
	flushSeries         int
	pusherShards        PusherShards
	nonOwnedEviction    *NonOwnedEviction
	earlyHeadCompaction *EarlyHeadCompaction
	// Set when the emulated head's min time moved or series were evicted, so the next head tick
	// moves the series no longer in the head to cold blocks.
	freezePending atomic.Bool
	// Series whose oldest sample a head tick looked up, for tests.
	oldestScans atomic.Uint64
	// Retention's last cutoff: cold blocks keep references to chunks it removed.
	prunedBefore atomic.Int64
	// Whether the last head tick compacted, so this one checks for an early compaction with counts
	// taken after it, like Mimir's check right after its regular compaction.
	compactedLastTick atomic.Bool
}

// New creates a store whose completed chunks go to chunkDir, which is cleared first; without one
// they use anonymous memory.
func New(activeWindowMs int64, retention Retention, chunkDir string) (*Store, error) {
	return NewWithShards(activeWindowMs, retention, chunkDir, DefaultShards, runtime.GOMAXPROCS(0))
}

// Default is an in-memory store with a 20-minute active series window.
func Default() *Store {
	s, err := NewWithShards(20*60*1000, Retention{}, "", 4, 2)
	if err != nil {
		panic(err)
	}
	return s
}

// NewWithShards creates a store with shards store shards whose parallel work uses up to threads
// goroutines.
func NewWithShards(activeWindowMs int64, retention Retention, chunkDir string, shardCount, threads int) (*Store, error) {
	if chunkDir != "" {
		if err := os.RemoveAll(chunkDir); err != nil {
			return nil, fmt.Errorf("remove chunk directory %s: %w", chunkDir, err)
		}
	}
	shardCount = max(shardCount, 1)
	shards := make([]*shardState, shardCount)
	for index := range shards {
		disk, err := chunks.OpenDiskMapper(shardDir(chunkDir, index))
		if err != nil {
			return nil, err
		}
		cold, err := newColdState(coldDir(chunkDir, index))
		if err != nil {
			return nil, err
		}
		shards[index] = &shardState{tenants: map[string]*tenant{}, disk: disk, cold: cold}
	}
	return fromShards(shards, threads, activeWindowMs, retention), nil
}

func fromShards(shards []*shardState, threads int, activeWindowMs int64, retention Retention) *Store {
	s := &Store{
		shards:                     shards,
		threads:                    max(threads, 1),
		activeWindowMs:             activeWindowMs,
		retention:                  retention,
		overrides:                  limits.DefaultOverrides(),
		exemplars:                  map[string]*exemplars.TenantExemplars[labels.Labels]{},
		costAttribution:            map[costAttributionKey]*costAttributionState{},
		costAttributionLastCleanup: nowMs(),
		costAttributionCleanupMs:   3 * 60_000,
		costAttributionEvictionMs:  20 * 60_000,
		flushSeries:                150,
		pusherShards:               DefaultPusherShards(),
	}
	s.prunedBefore.Store(math.MinInt64)
	return s
}

func shardDir(directory string, shard int) string {
	if directory == "" {
		return ""
	}
	return filepath.Join(directory, fmt.Sprintf("shard-%03d", shard))
}

func coldDir(directory string, shard int) string {
	if directory == "" {
		return ""
	}
	return filepath.Join(directory, "cold", fmt.Sprintf("shard-%03d", shard))
}

// WithOverrides takes limits from overrides; without them the store uses Mimir's defaults.
func (s *Store) WithOverrides(overrides *limits.Overrides) *Store {
	s.overrides = overrides
	return s
}

func (s *Store) WithFlushSeries(flushSeries int) *Store {
	s.flushSeries = flushSeries
	return s
}

func (s *Store) WithEarlyHeadCompaction(config *EarlyHeadCompaction) *Store {
	s.earlyHeadCompaction = config
	return s
}

func (s *Store) WithNonOwnedEviction(eviction *NonOwnedEviction) *Store {
	s.nonOwnedEviction = eviction
	return s
}

func (s *Store) WithPusherShards(pusherShards PusherShards) *Store {
	s.pusherShards = pusherShards
	return s
}

func (s *Store) WithCostAttributionIntervals(cleanupMs, evictionMs int64) *Store {
	s.costAttributionCleanupMs, s.costAttributionEvictionMs = cleanupMs, evictionMs
	return s
}

func (s *Store) Overrides() *limits.Overrides {
	return s.overrides
}

// IngestedSamples returns the samples ingested since startup.
func (s *Store) IngestedSamples() uint64 {
	return s.ingestedSamples.Load()
}

// Close unmaps the chunk files and cold blocks.
func (s *Store) Close() error {
	var errs []error
	for _, shard := range s.shards {
		shard.Lock()
		errs = append(errs, shard.disk.Close(), shard.cold.close())
		shard.Unlock()
	}
	return errors.Join(errs...)
}

// parallel runs work for every index on up to s.threads goroutines, returning the first error.
func (s *Store) parallel(count int, work func(index int) error) error {
	if count <= 1 || s.threads <= 1 {
		for index := range count {
			if err := work(index); err != nil {
				return err
			}
		}
		return nil
	}
	var (
		wg    sync.WaitGroup
		next  atomic.Int64
		errs  = make([]error, count)
		limit = min(s.threads, count)
	)
	for range limit {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				index := int(next.Add(1) - 1)
				if index >= count {
					return
				}
				errs[index] = work(index)
			}
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// Parallel runs work, for work done in parallel alongside ingestion.
func (s *Store) Parallel(work func()) {
	work()
}

// OptionalHash is a series' label hash, or none when it repeats a label name, which the store
// rejects.
type OptionalHash struct {
	Hash uint64
	OK   bool
}

// IngestRecord is a decoded Kafka record to apply to the store.
type IngestRecord struct {
	Tenant     string
	Request    record.DecodedRequest
	IngestedMs int64
	TrackRate  bool
	// The Kafka record's value size, from which Mimir's ingest pusher sizes a tenant's shards.
	Bytes int
	// SeriesHashes(&request), when computed while decoding.
	SeriesHashes []OptionalHash
}

// SeriesHashes sorts each series' labels and returns its hash. Done while decoding, it runs where
// records are decoded in parallel rather than in its own pass over a batch.
func SeriesHashes(request *record.DecodedRequest) []OptionalHash {
	hashes := make([]OptionalHash, len(request.Series))
	for index := range request.Series {
		hashes[index] = sortAndHash(&request.Series[index])
	}
	return hashes
}

func sortAndHash(series *record.DecodedSeries) OptionalHash {
	pairs := series.Labels
	if !slices.IsSortedFunc(pairs, comparePairs) {
		slices.SortFunc(pairs, comparePairs)
	}
	for index := 1; index < len(pairs); index++ {
		if pairs[index-1][0] == pairs[index][0] {
			return OptionalHash{}
		}
	}
	return OptionalHash{Hash: labels.HashPairs(pairs), OK: true}
}

func comparePairs(a, b [2]string) int {
	if a[0] != b[0] {
		if a[0] < b[0] {
			return -1
		}
		return 1
	}
	if a[1] != b[1] {
		if a[1] < b[1] {
			return -1
		}
		return 1
	}
	return 0
}

func (s *Store) Ingest(tenantID string, request record.DecodedRequest) error {
	return s.IngestBatch([]IngestRecord{{Tenant: tenantID, Request: request, IngestedMs: nowMs(), TrackRate: true}})
}

func (s *Store) IngestRecovered(tenantID string, request record.DecodedRequest, ingestedMs int64) error {
	return s.IngestBatch([]IngestRecord{{Tenant: tenantID, Request: request, IngestedMs: ingestedMs}})
}

// IngestBatch applies records in order: each shard receives its series in record order, and
// shards run in parallel.
func (s *Store) IngestBatch(records []IngestRecord) error {
	_, err := s.IngestFlushes(records)
	return err
}

// DiscardReason is why a sample was discarded, the `reason` of `cortex_discarded_samples_total`.
type DiscardReason uint8

const (
	DiscardOutOfOrder DiscardReason = iota
	DiscardOutOfBounds
	DiscardTooOld
	DiscardNewValueForTimestamp
	DiscardTooFarInFuture
	DiscardTooFarInPast
	DiscardInvalidNativeHistogram
)

func (r DiscardReason) Label() string {
	switch r {
	case DiscardOutOfOrder:
		return "sample-out-of-order"
	case DiscardOutOfBounds:
		return "sample-timestamp-too-old"
	case DiscardTooOld:
		return "sample-too-old"
	case DiscardNewValueForTimestamp:
		return "new-value-for-timestamp"
	case DiscardTooFarInFuture:
		return "sample-too-far-in-future"
	case DiscardTooFarInPast:
		return "sample-too-far-in-past"
	default:
		return "invalid-native-histogram"
	}
}

// flushState is the head appender of one of Mimir's ingest pusher flushes.
type flushState struct {
	id       uint64
	series   int
	rules    appendRules
	hasRules bool
}

type flushKey struct {
	tenant string
	source int32
}

type discardKey struct {
	index  int
	reason DiscardReason
}

// batchSeries is a series of an IngestFlushes batch, with the record it came from and its
// position in the batch.
// pendingSeries is a series of a batch, before it's hashed and routed to its store shard.
type pendingSeries struct {
	index      int
	series     *record.DecodedSeries
	ingestedMs int64
	rules      appendRules
	flush      uint64
	hash       OptionalHash
	// Whether the hash was computed while decoding.
	hashed bool
}

// ingestScratch is what a batch routes its series with. Batches arrive continuously and each
// held a slice per series and per store shard, which was most of what ingestion left for the GC.
type ingestScratch struct {
	all     []pendingSeries
	buckets [][]batchSeries
	// Per store shard: cleared maps keep their capacity for the next batch.
	committed []map[committedKey]committedHead
}

var ingestScratches = sync.Pool{New: func() any { return &ingestScratch{} }}

// release returns the scratch to the pool without the series it pointed to, which belong to
// records the caller may drop.
func (scratch *ingestScratch) release() {
	clear(scratch.all)
	scratch.all = scratch.all[:0]
	for shard := range scratch.buckets {
		clear(scratch.buckets[shard])
		scratch.buckets[shard] = scratch.buckets[shard][:0]
	}
	for _, committed := range scratch.committed {
		clear(committed)
	}
	ingestScratches.Put(scratch)
}

type batchSeries struct {
	index      int
	position   int
	hash       uint64
	series     *record.DecodedSeries
	ingestedMs int64
	rules      appendRules
	flush      uint64
}

// pendingExemplars are a series' exemplars, to add after its samples in batch order, which is
// the order Mimir appends exemplars in.
type pendingExemplars struct {
	index     int
	position  int
	flush     uint64
	hash      uint64
	labels    labels.Labels
	exemplars []mimirpb.Exemplar
}

// shardOutcome is what a shard applied and rejected, and exemplars to store once the shards
// finish.
type shardOutcome struct {
	accepted          map[int]uint64
	outOfOrder        map[int]uint64
	chunksCreated     map[int]uint64
	discarded         map[discardKey]uint64
	exemplars         []pendingExemplars
	exemplarsNoSeries uint64
}

func newShardOutcome() *shardOutcome {
	return &shardOutcome{
		accepted:      map[int]uint64{},
		outOfOrder:    map[int]uint64{},
		chunksCreated: map[int]uint64{},
		discarded:     map[discardKey]uint64{},
	}
}

// committedKey is a series in a flush: the series hash already spreads the keys.
type committedKey struct {
	flush uint64
	hash  uint64
}

// IngestFlushes is IngestBatch, returning how many head appends the Go ingester's pusher would
// make.
func (s *Store) IngestFlushes(records []IngestRecord) (uint64, error) {
	shardCount := len(s.shards)
	scratch := ingestScratches.Get().(*ingestScratch)
	defer scratch.release()
	if len(scratch.buckets) != shardCount {
		scratch.buckets = make([][]batchSeries, shardCount)
		scratch.committed = make([]map[committedKey]committedHead, shardCount)
	}
	buckets := scratch.buckets
	tenantIDs := make([]string, 0, len(records))
	keepExemplars := make([]bool, 0, len(records))
	total := 0
	for index := range records {
		total += len(records[index].Request.Series)
	}
	all := slices.Grow(scratch.all[:0], total)
	defer func() { scratch.all = all }()
	// Like `idealShardsFor`, a tenant's records spread over shards by their expected series.
	tenantBytes := map[string]int{}
	for index := range records {
		tenantBytes[records[index].Tenant] += records[index].Bytes
	}
	tenantShards := make(map[string]int, len(tenantBytes))
	for tenantID, bytes := range tenantBytes {
		tenantShards[tenantID] = s.pusherShards.count(bytes, s.flushSeries)
	}
	flushes := map[flushKey][]*flushState{}
	var nextFlush uint64
	earlyDiscards := map[discardKey]uint64{}
	var exemplarFailures uint64
	wallNow := nowMs()
	home := s.shards[0]
	home.Lock()
	for index := range records {
		rec := &records[index]
		request := &rec.Request
		hashes := rec.SeriesHashes
		if len(hashes) != len(request.Series) {
			hashes = nil
		}
		tenantLimits := s.overrides.Tenant(rec.Tenant)
		lim := &tenantLimits.Limits
		homeTenant := tenantOf(home.tenants, rec.Tenant)
		s.addMetadata(homeTenant, rec.Tenant, lim, request.Metadata, wallNow)
		if rec.TrackRate {
			var samples uint64
			for series := range request.Series {
				samples += uint64(len(request.Series[series].Samples) + len(request.Series[series].Histograms))
			}
			now := time.Now()
			homeTenant.ingested = append(homeTenant.ingested, ingestedSamples{at: now, source: request.Source, samples: samples})
			drop := 0
			for drop < len(homeTenant.ingested) && now.Sub(homeTenant.ingested[drop].at) > time.Minute {
				drop++
			}
			if drop > 0 {
				homeTenant.ingested = append(homeTenant.ingested[:0], homeTenant.ingested[drop:]...)
			}
		}
		// Like the push path, check wall-clock grace periods before appending.
		maxTimestamp := satAdd(wallNow, lim.CreationGracePeriodMs)
		minTimestamp := int64(math.MinInt64)
		if lim.PastGracePeriodMs > 0 {
			minTimestamp = satSub(satSub(wallNow, lim.PastGracePeriodMs), lim.OutOfOrderTimeWindowMs)
		}
		classify := func(timestamp int64) (DiscardReason, bool) {
			switch {
			case timestamp > maxTimestamp:
				return DiscardTooFarInFuture, true
			case timestamp < minTimestamp:
				return DiscardTooFarInPast, true
			default:
				return 0, false
			}
		}
		keep := lim.MaxGlobalExemplarsPerUser > 0
		window := lim.OutOfOrderTimeWindowMs
		key := flushKey{rec.Tenant, request.Source}
		shards := tenantShards[rec.Tenant]
		if shards == 0 {
			shards = 1
		}
		for seriesIndex := range request.Series {
			series := &request.Series[seriesIndex]
			// Mimir's pusher routes each series to a shard by the hash of its labels.
			shard := 0
			if shards > 1 {
				shard = int(labels.StableHashPairs(series.Labels) % uint64(shards))
			}
			// Mimir's fast path: without an out-of-order window, a series whose samples are all
			// behind the head's min valid time is rejected as a whole, before the grace period
			// checks.
			minAppend, hasMinAppend := int64(0), false
			if states, ok := flushes[key]; ok && states[shard] != nil && states[shard].hasRules {
				minAppend, hasMinAppend = states[shard].rules.minValidTime, true
			} else if homeTenant.maxTime != math.MinInt64 {
				minAppend, hasMinAppend = max(satSub(homeTenant.maxTime, minValidTimeWindowMs), homeTenant.truncatedTo), true
			}
			histogramsCount := lim.NativeHistogramsIngestionEnabled
			if window <= 0 && hasMinAppend && len(series.Exemplars) == 0 &&
				(len(series.Samples) > 0 || (histogramsCount && len(series.Histograms) > 0)) &&
				allSamplesBefore(series, minAppend, histogramsCount) {
				rejected := len(series.Samples)
				if histogramsCount {
					rejected += len(series.Histograms)
				}
				earlyDiscards[discardKey{index, DiscardOutOfBounds}] += uint64(rejected)
				series.Samples = series.Samples[:0]
				series.Histograms = series.Histograms[:0]
				series.CreatedTimestamp = 0
			}
			series.Samples = slices.DeleteFunc(series.Samples, func(sample mimirpb.Sample) bool {
				reason, discard := classify(sample.TimestampMs)
				if discard {
					countEarlyDiscard(earlyDiscards, index, reason)
				}
				return discard
			})
			if lim.NativeHistogramsIngestionEnabled {
				series.Histograms = slices.DeleteFunc(series.Histograms, func(h mimirpb.Histogram) bool {
					reason, discard := classify(h.Timestamp)
					if discard {
						countEarlyDiscard(earlyDiscards, index, reason)
					}
					return discard
				})
			} else {
				// Ignored without an error, like Mimir.
				series.Histograms = series.Histograms[:0]
			}
			if keep {
				before := len(series.Exemplars)
				series.Exemplars = slices.DeleteFunc(series.Exemplars, func(e mimirpb.Exemplar) bool {
					_, discard := classify(e.TimestampMs)
					return discard
				})
				exemplarFailures += uint64(before - len(series.Exemplars))
			} else {
				series.Exemplars = series.Exemplars[:0]
			}
			firstSample, hasFirst := int64(0), false
			if len(series.Samples) > 0 {
				firstSample, hasFirst = series.Samples[0].TimestampMs, true
			} else if len(series.Histograms) > 0 {
				firstSample, hasFirst = series.Histograms[0].Timestamp, true
			}
			// Like Mimir's ingest pusher, each flush of up to flushSeries series of a tenant's shard
			// gets its own head appender, which takes the head's max time when created, or the
			// first sample's for a head that has none yet.
			states, ok := flushes[key]
			if !ok {
				states = make([]*flushState, shards)
				flushes[key] = states
			}
			if states[shard] == nil {
				nextFlush++
				states[shard] = &flushState{id: nextFlush}
			}
			flush := states[shard]
			if s.flushSeries > 0 && flush.series >= s.flushSeries {
				nextFlush++
				*flush = flushState{id: nextFlush}
			}
			if !flush.hasRules && hasFirst {
				headMax := homeTenant.maxTime
				if headMax == math.MinInt64 {
					headMax = firstSample
				}
				flush.rules, flush.hasRules = newAppendRules(headMax, homeTenant.truncatedTo, window), true
			}
			flush.series++
			rules := flush.rules
			if !flush.hasRules {
				rules = newAppendRules(homeTenant.maxTime, homeTenant.truncatedTo, window)
			}
			// Older samples are rejected whatever the series holds.
			acceptableFrom := rules.minValidTime
			if window > 0 {
				acceptableFrom = min(rules.minValidTime, satSub(rules.headMaxTime, window))
			}
			if created := series.CreatedTimestamp; created > 0 && created >= acceptableFrom {
				homeTenant.minTime = min(homeTenant.minTime, created)
			}
			for sample := range series.Samples {
				timestamp := series.Samples[sample].TimestampMs
				// Every sample above the head's max time is accepted.
				homeTenant.maxTime = max(homeTenant.maxTime, timestamp)
				if timestamp >= acceptableFrom {
					homeTenant.minTime = min(homeTenant.minTime, timestamp)
				}
			}
			for h := range series.Histograms {
				timestamp := series.Histograms[h].Timestamp
				homeTenant.maxTime = max(homeTenant.maxTime, timestamp)
				if timestamp >= acceptableFrom {
					homeTenant.minTime = min(homeTenant.minTime, timestamp)
				}
			}
			p := pendingSeries{index: index, series: series, ingestedMs: rec.IngestedMs, rules: rules, flush: flush.id}
			if hashes != nil {
				p.hash, p.hashed = hashes[seriesIndex], true
			}
			all = append(all, p)
		}
		if s.flushSeries == 0 {
			// Without concurrency, Mimir pushes every record on its own.
			delete(flushes, key)
		}
		keepExemplars = append(keepExemplars, keep)
		tenantIDs = append(tenantIDs, rec.Tenant)
	}
	home.Unlock()
	parallel := len(all) >= parallelMinSeries
	// Sorting and hashing labels is most of the per-series cost outside the shards.
	unhashed := 0
	for index := range all {
		if !all[index].hashed {
			unhashed++
		}
	}
	hashAll := func(from, to int) {
		for index := from; index < to; index++ {
			if !all[index].hashed {
				all[index].hash = sortAndHash(all[index].series)
			}
		}
	}
	if unhashed >= parallelMinSeries {
		parts := s.threads
		size := (len(all) + parts - 1) / parts
		_ = s.parallel(parts, func(part int) error {
			hashAll(min(part*size, len(all)), min((part+1)*size, len(all)))
			return nil
		})
	} else {
		hashAll(0, len(all))
	}
	for position := range all {
		p := &all[position]
		if p.hash.OK {
			shard := shardFor(p.hash.Hash, shardCount)
			buckets[shard] = append(buckets[shard], batchSeries{
				index: p.index, position: position, hash: p.hash.Hash, series: p.series,
				ingestedMs: p.ingestedMs, rules: p.rules, flush: p.flush,
			})
		}
	}
	type work struct {
		shard  int
		bucket []batchSeries
	}
	var works []work
	for shard, bucket := range buckets {
		if len(bucket) > 0 {
			works = append(works, work{shard, bucket})
		}
	}
	outcomes := make([]*shardOutcome, len(works))
	apply := func(index int) error {
		w := works[index]
		state := s.shards[w.shard]
		state.Lock()
		defer state.Unlock()
		outcome := newShardOutcome()
		committed := scratch.committed[w.shard]
		if committed == nil {
			committed = make(map[committedKey]committedHead, len(w.bucket))
			scratch.committed[w.shard] = committed
		}
		// A record's series are consecutive, so the tenant rarely changes.
		currentIndex := -1
		var current *tenant
		for position := range w.bucket {
			batch := &w.bucket[position]
			if current == nil || (currentIndex != batch.index && tenantIDs[currentIndex] != tenantIDs[batch.index]) {
				currentIndex = batch.index
				current = tenantOf(state.tenants, tenantIDs[batch.index])
			}
			err := ingestSeries(current, state.disk, batch.series, batch.hash, seriesContext{
				tenantID:      tenantIDs[batch.index],
				rules:         batch.rules,
				keepExemplars: keepExemplars[batch.index],
				index:         batch.index,
				position:      batch.position,
				flush:         batch.flush,
				ingestedMs:    batch.ingestedMs,
			}, committed, outcome)
			if err != nil {
				return err
			}
		}
		outcomes[index] = outcome
		return nil
	}
	var err error
	if len(works) <= 1 || !parallel {
		for index := range works {
			if err = apply(index); err != nil {
				break
			}
		}
	} else {
		err = s.parallel(len(works), apply)
	}
	if err != nil {
		return 0, err
	}
	s.recordOutcomes(tenantIDs, earlyDiscards, exemplarFailures, outcomes)
	return nextFlush, nil
}

func allSamplesBefore(series *record.DecodedSeries, minAppend int64, histograms bool) bool {
	for index := range series.Samples {
		if series.Samples[index].TimestampMs >= minAppend {
			return false
		}
	}
	if histograms {
		for index := range series.Histograms {
			if series.Histograms[index].Timestamp >= minAppend {
				return false
			}
		}
	}
	return true
}

// Mimir's soft error processor doesn't know `too-far-in-past`, so those samples are dropped
// without being counted.
func countEarlyDiscard(discards map[discardKey]uint64, index int, reason DiscardReason) {
	if reason != DiscardTooFarInPast {
		discards[discardKey{index, reason}]++
	}
}

// The hash table inside each shard indexes on the low bits, so shard on the high bits.
func shardFor(hash uint64, shards int) int {
	return int((hash >> 32) % uint64(shards))
}

func (s *Store) recordOutcomes(tenantIDs []string, earlyDiscards map[discardKey]uint64, exemplarFailures uint64, outcomes []*shardOutcome) {
	accepted := map[string]uint64{}
	type tenantReason struct {
		tenant string
		reason DiscardReason
	}
	discarded := map[tenantReason]uint64{}
	for key, count := range earlyDiscards {
		discarded[tenantReason{tenantIDs[key.index], key.reason}] += count
	}
	var pending []pendingExemplars
	for _, outcome := range outcomes {
		if outcome == nil {
			continue
		}
		for index, count := range outcome.accepted {
			accepted[tenantIDs[index]] += count
		}
		for index, count := range outcome.outOfOrder {
			metrics.OutOfOrderSamplesAppended.WithLabelValues(tenantIDs[index]).Add(float64(count))
		}
		for index, count := range outcome.chunksCreated {
			metrics.HeadChunksCreated.WithLabelValues(tenantIDs[index]).Add(float64(count))
		}
		for key, count := range outcome.discarded {
			discarded[tenantReason{tenantIDs[key.index], key.reason}] += count
		}
		pending = append(pending, outcome.exemplars...)
		exemplarFailures += outcome.exemplarsNoSeries
	}
	sort.SliceStable(pending, func(a, b int) bool { return pending[a].position < pending[b].position })
	for tenantID, count := range accepted {
		metrics.IngestedSamples.WithLabelValues(tenantID).Add(float64(count))
		s.ingestedSamples.Add(count)
	}
	failures := map[string]uint64{}
	for key, count := range discarded {
		metrics.DiscardedSamples.WithLabelValues(key.reason.Label(), key.tenant, "").Add(float64(count))
		failures[key.tenant] += count
	}
	for tenantID, count := range failures {
		metrics.IngestedSamplesFailures.WithLabelValues(tenantID).Add(float64(count))
	}
	if len(pending) == 0 {
		if exemplarFailures > 0 {
			metrics.IngestedExemplarsFailures.Add(float64(exemplarFailures))
		}
		return
	}
	var ingested uint64
	s.exemplarsLock.Lock()
	defer s.exemplarsLock.Unlock()
	// The newest exemplar of each series when its flush started, which Mimir's head appender
	// validates against; the storage itself only changes when the flush commits.
	type flushSeries struct{ flush, hash uint64 }
	newestAtFlush := map[flushSeries]*exemplars.Exemplar{}
	for _, series := range pending {
		tenantID := tenantIDs[series.index]
		tenantLimits := s.overrides.Tenant(tenantID)
		capacity := s.overrides.MaxExemplars(&tenantLimits.Limits)
		window := tenantLimits.Limits.OutOfOrderTimeWindowMs
		storage, ok := s.exemplars[tenantID]
		if !ok {
			storage = exemplars.New[labels.Labels](capacity)
			s.exemplars[tenantID] = storage
		}
		if storage.Capacity() != capacity {
			storage.Resize(capacity)
		}
		key := flushSeries{series.flush, series.hash}
		newest, seen := newestAtFlush[key]
		if !seen {
			if stored := storage.Newest(series.hash); stored != nil {
				copied := *stored
				newest = &copied
			}
			newestAtFlush[key] = newest
		}
		var appended uint64
		for index := range series.exemplars {
			e := toExemplar(&series.exemplars[index])
			if exemplars.Validate(capacity, newest, &e, window) != exemplars.Accepted {
				exemplarFailures++
				continue
			}
			ingested++
			stored, rejection := storage.Add(series.hash, func() labels.Labels { return series.labels }, e, window)
			switch {
			case stored:
				appended++
			case rejection == exemplars.OutOfOrder:
				// Rejected when committing, which Mimir doesn't report.
				metrics.OutOfOrderExemplars.Inc()
			}
		}
		if appended > 0 {
			metrics.ExemplarsAppended.WithLabelValues(tenantID).Add(float64(appended))
		}
	}
	metrics.IngestedExemplars.Add(float64(ingested))
	metrics.IngestedExemplarsFailures.Add(float64(exemplarFailures))
}

func toExemplar(e *mimirpb.Exemplar) exemplars.Exemplar {
	out := exemplars.Exemplar{Value: e.Value, TimestampMs: e.TimestampMs}
	if len(e.Labels) > 0 {
		out.Labels = make([]exemplars.Label, len(e.Labels))
		for index, label := range e.Labels {
			out.Labels[index] = exemplars.Label{Name: label.Name, Value: label.Value}
		}
	}
	return out
}

func fromExemplar(e *exemplars.Exemplar) mimirpb.Exemplar {
	out := mimirpb.Exemplar{Value: e.Value, TimestampMs: e.TimestampMs}
	if len(e.Labels) > 0 {
		out.Labels = make([]mimirpb.LabelAdapter, len(e.Labels))
		for index, label := range e.Labels {
			out.Labels[index] = mimirpb.LabelAdapter{Name: label.Name, Value: label.Value}
		}
	}
	return out
}

// addMetadata is Mimir's `userMetricsMetadata.add`: a new metric needs room under the per-user
// limit, and any entry, even a known one, needs room under the per-metric limit.
func (s *Store) addMetadata(t *tenant, tenantID string, lim *limits.Limits, entries []mimirpb.MetricMetadata, now int64) {
	if len(entries) == 0 {
		return
	}
	perUser := s.overrides.MaxMetadataPerUser(lim)
	perMetric := s.overrides.MaxMetadataPerMetric(lim)
	for _, entry := range entries {
		set, known := t.metadata[entry.MetricFamilyName]
		if !known && len(t.metadata) >= perUser {
			metrics.DiscardedMetadata.WithLabelValues("per_user_metadata_limit", tenantID).Inc()
			metrics.IngestedMetadataFailures.Inc()
			continue
		}
		if !known {
			set = map[metadataKey]metadataEntry{}
			t.metadata[entry.MetricFamilyName] = set
		}
		if len(set) >= perMetric {
			metrics.DiscardedMetadata.WithLabelValues("per_metric_metadata_limit", tenantID).Inc()
			metrics.IngestedMetadataFailures.Inc()
			continue
		}
		key := metadataKey{int32(entry.Type), entry.Help, entry.Unit}
		if _, exists := set[key]; !exists {
			metrics.MemoryMetadataCreated.WithLabelValues(tenantID).Inc()
		}
		set[key] = metadataEntry{metadata: detachedMetadata(entry), seenMs: now}
		metrics.IngestedMetadata.Inc()
	}
}

// detachedMetadata copies the strings: decoded metadata shares its Kafka record's buffer.
func detachedMetadata(m mimirpb.MetricMetadata) mimirpb.MetricMetadata {
	m.MetricFamilyName = string([]byte(m.MetricFamilyName))
	m.Help = string([]byte(m.Help))
	m.Unit = string([]byte(m.Unit))
	return m
}

// PurgeMetadata drops metadata not seen for retainMs, like `-ingester.metadata-retain-period`.
func (s *Store) PurgeMetadata(retainMs int64) {
	cutoff := satSub(nowMs(), retainMs)
	home := s.shards[0]
	home.Lock()
	defer home.Unlock()
	for tenantID, t := range home.tenants {
		removed := 0
		for name, set := range t.metadata {
			for key, entry := range set {
				if entry.seenMs < cutoff {
					delete(set, key)
					removed++
				}
			}
			if len(set) == 0 {
				delete(t.metadata, name)
			}
		}
		if removed > 0 {
			metrics.MemoryMetadataRemoved.WithLabelValues(tenantID).Add(float64(removed))
		}
	}
}

// PruneExpired removes what retention no longer keeps.
func (s *Store) PruneExpired() error {
	if !s.retention.Set {
		return nil
	}
	return s.pruneBefore(satSub(nowMs(), s.retention.Ms))
}

func (s *Store) pruneBefore(cutoff int64) error {
	for {
		current := s.prunedBefore.Load()
		if cutoff <= current || s.prunedBefore.CompareAndSwap(current, cutoff) {
			break
		}
	}
	return s.parallel(len(s.shards), func(index int) error {
		state := s.shards[index]
		state.Lock()
		defer state.Unlock()
		for _, t := range state.tenants {
			t.series.retain(func(entry *seriesEntry) bool { return pruneSeries(&entry.series, cutoff) })
		}
		// Like Go's block retention, a cold block goes once all of it is older.
		state.cold.pruneBefore(cutoff)
		return state.disk.TruncateBefore(cutoff)
	})
}

// Like head truncation, retention drops whole chunks, so a chunk that straddles the cutoff stays.
func pruneSeries(series *Series, cutoff int64) bool {
	series.chunks.retain(func(chunk *ChunkMeta) bool { return chunk.MaxTime >= cutoff })
	if series.floatHead != nil && series.floatHead.lastTimestamp() < cutoff {
		series.floatHead = nil
	}
	if series.histogramHead != nil && series.histogramHead.Last().Timestamp < cutoff {
		series.histogramHead = nil
	}
	series.outOfOrder = slices.DeleteFunc(series.outOfOrder, func(sample oooSample) bool { return sample.T < cutoff })
	return series.hasSamples()
}

func nowMs() int64 {
	return time.Now().UnixMilli()
}

func satAdd(a, b int64) int64 {
	sum := a + b
	if (b > 0 && sum < a) || (b < 0 && sum > a) {
		if b > 0 {
			return math.MaxInt64
		}
		return math.MinInt64
	}
	return sum
}

func satSub(a, b int64) int64 {
	difference := a - b
	if (b > 0 && difference > a) || (b < 0 && difference < a) {
		if b > 0 {
			return math.MinInt64
		}
		return math.MaxInt64
	}
	return difference
}

// rangeStart saturates, since lookups ask for ranges up to the ends of int64.
func rangeStart(timestamp int64) int64 {
	rem := timestamp % chunkRangeMs
	if rem < 0 {
		rem += chunkRangeMs
	}
	return satSub(timestamp, rem)
}

func rangeEnd(timestamp int64) int64 {
	return satAdd(rangeStart(timestamp), chunkRangeMs)
}

// Port of Prometheus computeChunkEndTime: spread the remaining range evenly over chunks of the
// observed sample rate so chunk boundaries align with block ranges.
func computeChunkEndTime(start, current, maxTime int64, ratioToFull float64) int64 {
	n := float64(maxTime-start) / (float64(current-start+1) * ratioToFull)
	if n <= 1 {
		return maxTime
	}
	return int64(float64(start) + float64(maxTime-start)/math.Floor(n))
}

// ShardByAllLabels is Mimir's `ShardByAllLabels`: 32-bit FNV-1 over the tenant ID and every label
// name and value.
func ShardByAllLabels(tenantID string, stored labels.Labels) uint32 {
	hash := uint32(2_166_136_261)
	add := func(s string) {
		for index := 0; index < len(s); index++ {
			hash *= 16_777_619
			hash ^= uint32(s[index])
		}
	}
	add(tenantID)
	it := stored.Iter()
	for name, value, ok := it.Next(); ok; name, value, ok = it.Next() {
		add(name)
		add(value)
	}
	return hash
}

// rangesInclude is dskit's `TokenRanges.IncludesKey`: sorted pairs of inclusive range bounds.
func rangesInclude(ranges []uint32, key uint32) bool {
	index, found := slices.BinarySearch(ranges, key)
	return found || index%2 == 1
}
