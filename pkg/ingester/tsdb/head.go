// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb"
	"go.uber.org/atomic"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
)

const (
	defaultPostingsForMatchersCacheTTL  = 10 * time.Second
	defaultPostingsForMatchersCacheSize = 100
)

var (
	// ErrInvalidSample is returned if an appended sample is not valid and can't
	// be ingested.
	ErrInvalidSample = errors.New("invalid sample")
	// ErrAppenderClosed is returned if an appender has already be successfully
	// rolled back or committed.
	ErrAppenderClosed = errors.New("appender closed")

	// defaultIsolationDisabled is true if isolation is disabled by default.
	defaultIsolationDisabled = false
)

// chunkDiskMapper is a temporary interface while we transition from
// 0 size queue to queue based chunk disk mapper.
type chunkDiskMapper interface {
	CutNewFile() (returnErr error)
	IterateAllChunks(f func(seriesRef chunks.HeadSeriesRef, chunkRef chunks.ChunkDiskMapperRef, mint, maxt int64, numSamples uint16, encoding chunkenc.Encoding) error) (err error)
	Truncate(fileNo uint32) error
	DeleteCorrupted(originalErr error) error
	Size() (int64, error)
	Close() error
	Chunk(ref chunks.ChunkDiskMapperRef) (chunkenc.Chunk, error)
	WriteChunk(seriesRef chunks.HeadSeriesRef, mint, maxt int64, chk chunkenc.Chunk, callback func(err error)) (chkRef chunks.ChunkDiskMapperRef)
	IsQueueEmpty() bool
}

// Head handles reads and writes of time series data within a time window.
type Head struct {
	chunkRange               atomic.Int64
	numSeries                atomic.Uint64
	minTime, maxTime         atomic.Int64 // Current min and max of the samples included in the head. TODO(jesusvazquez) Ensure these are properly tracked.
	minValidTime             atomic.Int64 // Mint allowed to be added to the head. It shouldn't be lower than the maxt of the last persisted block.
	lastMemoryTruncationTime atomic.Int64
	lastSeriesID             atomic.Uint64

	metrics      *headMetrics
	opts         *HeadOptions
	logger       log.Logger
	appendPool   sync.Pool
	seriesPool   sync.Pool
	bytesPool    sync.Pool
	memChunkPool sync.Pool

	// All series addressable by their ID or hash.
	series *stripeSeries

	// TODO(codesome): Extend MemPostings to return only OOOPostings, Set OOOStatus, ... Like an additional map of ooo postings.
	postings *index.MemPostings // Postings lists for terms.
	pfmc     *tsdb.PostingsForMatchersCache

	iso *isolation

	cardinalityMutex      sync.Mutex
	cardinalityCache      *index.PostingsStats // Posting stats cache which will expire after 30sec.
	lastPostingsStatsCall time.Duration        // Last posting stats call (PostingsCardinalityStats()) time for caching.

	chunkSnapshotMtx sync.Mutex

	closedMtx sync.Mutex
	closed    bool

	stats *HeadStats
	reg   prometheus.Registerer

	memTruncationInProcess atomic.Bool
}

// HeadOptions are parameters for the Head block.
type HeadOptions struct {
	ChunkRange int64
	// ChunkDirRoot is the parent directory of the chunks directory.
	ChunkPool chunkenc.Pool

	// StripeSize sets the number of entries in the hash map, it must be a power of 2.
	// A larger StripeSize will allocate more memory up-front, but will increase performance when handling a large number of series.
	// A smaller StripeSize reduces the memory allocated, but can decrease performance with large number of series.
	StripeSize     int
	SeriesCallback SeriesLifecycleCallback
}

func DefaultHeadOptions() *HeadOptions {
	ho := &HeadOptions{
		ChunkRange:     2 * time.Hour.Milliseconds(),
		ChunkPool:      chunkenc.NewPool(),
		StripeSize:     DefaultStripeSize,
		SeriesCallback: &noopSeriesLifecycleCallback{},
	}
	return ho
}

// SeriesLifecycleCallback specifies a list of callbacks that will be called during a lifecycle of a series.
// It is always a no-op in Prometheus and mainly meant for external users who import TSDB.
// All the callbacks should be safe to be called concurrently.
// It is up to the user to implement soft or hard consistency by making the callbacks
// atomic or non-atomic. Atomic callbacks can cause degradation performance.
type SeriesLifecycleCallback interface {
	// PreCreation is called before creating a series to indicate if the series can be created.
	// A non nil error means the series should not be created.
	PreCreation(labels.Labels) error
	// PostCreation is called after creating a series to indicate a creation of series.
	PostCreation(labels.Labels)
	// PostDeletion is called after deletion of series.
	PostDeletion(...labels.Labels)
}

// NewHead opens the head block in dir.
func NewHead(r prometheus.Registerer, l log.Logger, opts *HeadOptions, stats *HeadStats) (*Head, error) {
	if l == nil {
		l = log.NewNopLogger()
	}

	if opts.ChunkRange < 1 {
		return nil, errors.Errorf("invalid chunk range %d", opts.ChunkRange)
	}
	if opts.SeriesCallback == nil {
		opts.SeriesCallback = &noopSeriesLifecycleCallback{}
	}

	if stats == nil {
		stats = NewHeadStats()
	}

	h := &Head{
		logger: l,
		opts:   opts,
		memChunkPool: sync.Pool{
			New: func() interface{} {
				return &memChunk{}
			},
		},
		stats: stats,
		reg:   r,

		pfmc: tsdb.NewPostingsForMatchersCache(defaultPostingsForMatchersCacheTTL, defaultPostingsForMatchersCacheSize),
	}
	if err := h.resetInMemoryState(); err != nil {
		return nil, err
	}
	h.metrics = newHeadMetrics(h, r)

	if opts.ChunkPool == nil {
		opts.ChunkPool = chunkenc.NewPool()
	}
	return h, nil
}

func (h *Head) resetInMemoryState() error {
	h.iso = newIsolation(true)
	h.series = newStripeSeries(h.opts.StripeSize, h.opts.SeriesCallback)
	h.postings = index.NewUnorderedMemPostings()
	h.chunkRange.Store(h.opts.ChunkRange)
	h.minTime.Store(math.MaxInt64)
	h.maxTime.Store(math.MinInt64)
	h.lastMemoryTruncationTime.Store(math.MinInt64)
	return nil
}

type headMetrics struct {
	activeAppenders           prometheus.Gauge
	series                    prometheus.GaugeFunc
	seriesCreated             prometheus.Counter
	seriesRemoved             prometheus.Counter
	seriesNotFound            prometheus.Counter
	chunks                    prometheus.Gauge
	chunksCreated             prometheus.Counter
	chunksRemoved             prometheus.Counter
	gcDuration                prometheus.Summary
	samplesAppended           prometheus.Counter
	outOfOrderSamplesAppended prometheus.Counter
	outOfBoundSamples         prometheus.Counter
	outOfOrderSamples         prometheus.Counter
	tooOldSamples             prometheus.Counter
	walTruncateDuration       prometheus.Summary
	walCorruptionsTotal       prometheus.Counter
	dataTotalReplayDuration   prometheus.Gauge
	headTruncateFail          prometheus.Counter
	headTruncateTotal         prometheus.Counter
	checkpointDeleteFail      prometheus.Counter
	checkpointDeleteTotal     prometheus.Counter
	checkpointCreationFail    prometheus.Counter
	checkpointCreationTotal   prometheus.Counter
	mmapChunkCorruptionTotal  prometheus.Counter
	snapshotReplayErrorTotal  prometheus.Counter // Will be either 0 or 1.
	oooHistogram              prometheus.Histogram
}

func newHeadMetrics(h *Head, r prometheus.Registerer) *headMetrics {
	m := &headMetrics{
		activeAppenders: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "prometheus_tsdb_head_active_appenders",
			Help: "Number of currently active appender transactions",
		}),
		series: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "prometheus_tsdb_head_series",
			Help: "Total number of series in the head block.",
		}, func() float64 {
			return float64(h.NumSeries())
		}),
		seriesCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_series_created_total",
			Help: "Total number of series created in the head",
		}),
		seriesRemoved: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_series_removed_total",
			Help: "Total number of series removed in the head",
		}),
		seriesNotFound: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_series_not_found_total",
			Help: "Total number of requests for series that were not found.",
		}),
		chunks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "prometheus_tsdb_head_chunks",
			Help: "Total number of chunks in the head block.",
		}),
		chunksCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_chunks_created_total",
			Help: "Total number of chunks created in the head",
		}),
		chunksRemoved: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_chunks_removed_total",
			Help: "Total number of chunks removed in the head",
		}),
		gcDuration: prometheus.NewSummary(prometheus.SummaryOpts{
			Name: "prometheus_tsdb_head_gc_duration_seconds",
			Help: "Runtime of garbage collection in the head block.",
		}),
		walTruncateDuration: prometheus.NewSummary(prometheus.SummaryOpts{
			Name: "prometheus_tsdb_wal_truncate_duration_seconds",
			Help: "Duration of WAL truncation.",
		}),
		walCorruptionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_wal_corruptions_total",
			Help: "Total number of WAL corruptions.",
		}),
		dataTotalReplayDuration: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "prometheus_tsdb_data_replay_duration_seconds",
			Help: "Time taken to replay the data on disk.",
		}),
		samplesAppended: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_samples_appended_total",
			Help: "Total number of appended samples.",
		}),
		outOfOrderSamplesAppended: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_out_of_order_samples_appended_total",
			Help: "Total number of appended out of order samples.",
		}),
		outOfBoundSamples: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_out_of_bound_samples_total",
			Help: "Total number of out of bound samples ingestion failed attempts with out of order support disabled.",
		}),
		outOfOrderSamples: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_out_of_order_samples_total",
			Help: "Total number of out of order samples ingestion failed attempts due to out of order being disabled.",
		}),
		tooOldSamples: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_too_old_samples_total",
			Help: "Total number of out of order samples ingestion failed attempts with out of support enabled, but sample outside of time window.",
		}),
		headTruncateFail: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_truncations_failed_total",
			Help: "Total number of head truncations that failed.",
		}),
		headTruncateTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_truncations_total",
			Help: "Total number of head truncations attempted.",
		}),
		checkpointDeleteFail: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_checkpoint_deletions_failed_total",
			Help: "Total number of checkpoint deletions that failed.",
		}),
		checkpointDeleteTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_checkpoint_deletions_total",
			Help: "Total number of checkpoint deletions attempted.",
		}),
		checkpointCreationFail: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_checkpoint_creations_failed_total",
			Help: "Total number of checkpoint creations that failed.",
		}),
		checkpointCreationTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_checkpoint_creations_total",
			Help: "Total number of checkpoint creations attempted.",
		}),
		mmapChunkCorruptionTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_mmap_chunk_corruptions_total",
			Help: "Total number of memory-mapped chunk corruptions.",
		}),
		snapshotReplayErrorTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_snapshot_replay_error_total",
			Help: "Total number snapshot replays that failed.",
		}),
		oooHistogram: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "prometheus_tsdb_sample_ooo_delta",
			Help: "Delta in seconds by which a sample is considered out of order (reported regardless of OOO time window and whether sample is accepted or not).",
			Buckets: []float64{
				60 * 10,      // 10 min
				60 * 30,      // 30 min
				60 * 60,      // 60 min
				60 * 60 * 2,  // 2h
				60 * 60 * 3,  // 3h
				60 * 60 * 6,  // 6h
				60 * 60 * 12, // 12h
			},
		}),
	}

	if r != nil {
		r.MustRegister(
			m.activeAppenders,
			m.series,
			m.chunks,
			m.chunksCreated,
			m.chunksRemoved,
			m.seriesCreated,
			m.seriesRemoved,
			m.seriesNotFound,
			m.gcDuration,
			m.walTruncateDuration,
			m.walCorruptionsTotal,
			m.dataTotalReplayDuration,
			m.samplesAppended,
			m.outOfOrderSamplesAppended,
			m.outOfBoundSamples,
			m.outOfOrderSamples,
			m.tooOldSamples,
			m.headTruncateFail,
			m.headTruncateTotal,
			m.checkpointDeleteFail,
			m.checkpointDeleteTotal,
			m.checkpointCreationFail,
			m.checkpointCreationTotal,
			m.mmapChunkCorruptionTotal,
			m.snapshotReplayErrorTotal,
			m.oooHistogram,
			// Metrics bound to functions and not needed in tests
			// can be created and registered on the spot.
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "prometheus_tsdb_head_max_time",
				Help: "Maximum timestamp of the head block. The unit is decided by the library consumer.",
			}, func() float64 {
				return float64(h.MaxTime())
			}),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "prometheus_tsdb_head_min_time",
				Help: "Minimum time bound of the head block. The unit is decided by the library consumer.",
			}, func() float64 {
				return float64(h.MinTime())
			}),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "prometheus_tsdb_isolation_low_watermark",
				Help: "The lowest TSDB append ID that is still referenced.",
			}, func() float64 {
				return float64(h.iso.lowWatermark())
			}),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "prometheus_tsdb_isolation_high_watermark",
				Help: "The highest TSDB append ID that has been given out.",
			}, func() float64 {
				return float64(h.iso.lastAppendID())
			}),
		)
	}
	return m
}
func mmappedChunksDir(dir string) string { return filepath.Join(dir, "chunks_head") }

// HeadStats are the statistics for the head component of the DB.
type HeadStats struct {
	WALReplayStatus *WALReplayStatus
}

// NewHeadStats returns a new HeadStats object.
func NewHeadStats() *HeadStats {
	return &HeadStats{
		WALReplayStatus: &WALReplayStatus{},
	}
}

// WALReplayStatus contains status information about the WAL replay.
type WALReplayStatus struct {
	sync.RWMutex
	Min     int
	Max     int
	Current int
}

// GetWALReplayStatus returns the WAL replay status information.
func (s *WALReplayStatus) GetWALReplayStatus() WALReplayStatus {
	s.RLock()
	defer s.RUnlock()

	return WALReplayStatus{
		Min:     s.Min,
		Max:     s.Max,
		Current: s.Current,
	}
}

const cardinalityCacheExpirationTime = time.Duration(30) * time.Second

// Init loads data from the write ahead log and prepares the head for writes.
// It should be called before using an appender so that it
// limits the ingested samples to the head min valid time.
func (h *Head) Init(minValidTime int64) error {
	h.minValidTime.Store(minValidTime)
	defer func() {
		h.postings.EnsureOrder()
	}()
	defer h.gc() // After loading the wal remove the obsolete data from the head.
	defer func() {
		// Loading of m-mapped chunks and snapshot can make the mint of the Head
		// to go below minValidTime.
		if h.MinTime() < h.minValidTime.Load() {
			h.minTime.Store(h.minValidTime.Load())
		}
	}()

	return nil
}

// PostingsCardinalityStats returns top 10 highest cardinality stats By label and value names.
func (h *Head) PostingsCardinalityStats(statsByLabelName string) *index.PostingsStats {
	h.cardinalityMutex.Lock()
	defer h.cardinalityMutex.Unlock()
	currentTime := time.Duration(time.Now().Unix()) * time.Second
	seconds := currentTime - h.lastPostingsStatsCall
	if seconds > cardinalityCacheExpirationTime {
		h.cardinalityCache = nil
	}
	if h.cardinalityCache != nil {
		return h.cardinalityCache
	}
	h.cardinalityCache = h.postings.Stats(statsByLabelName)
	h.lastPostingsStatsCall = time.Duration(time.Now().Unix()) * time.Second

	return h.cardinalityCache
}

func (h *Head) updateMinMaxTime(mint, maxt int64) {
	for {
		lt := h.MinTime()
		if mint >= lt {
			break
		}
		if h.minTime.CompareAndSwap(lt, mint) {
			break
		}
	}
	for {
		ht := h.MaxTime()
		if maxt <= ht {
			break
		}
		if h.maxTime.CompareAndSwap(ht, maxt) {
			break
		}
	}
}

// SetMinValidTime sets the minimum timestamp the head can ingest.
func (h *Head) SetMinValidTime(minValidTime int64) {
	h.minValidTime.Store(minValidTime)
}

// Truncate removes old data before mint from the head and WAL.
func (h *Head) Truncate(mint int64) (err error) {
	return h.truncateMemory(mint)
}

// OverlapsClosedInterval returns true if the head overlaps [mint, maxt].
func (h *Head) OverlapsClosedInterval(mint, maxt int64) bool {
	return h.MinTime() <= maxt && mint <= h.MaxTime()
}

// truncateMemory removes old data before mint from the head.
func (h *Head) truncateMemory(mint int64) (err error) {
	h.chunkSnapshotMtx.Lock()
	defer h.chunkSnapshotMtx.Unlock()

	initialize := h.MinTime() == math.MaxInt64

	if h.MinTime() >= mint && !initialize {
		return nil
	}

	// The order of these two Store() should not be changed,
	// i.e. truncation time is set before in-process boolean.
	h.lastMemoryTruncationTime.Store(mint)
	h.memTruncationInProcess.Store(true)
	defer h.memTruncationInProcess.Store(false)

	// We wait for pending queries to end that overlap with this truncation.
	if !initialize {
		h.WaitForPendingReadersInTimeRange(h.MinTime(), mint)
	}

	h.minTime.Store(mint)
	h.minValidTime.Store(mint)

	// Ensure that max time is at least as high as min time.
	for h.MaxTime() < mint {
		h.maxTime.CompareAndSwap(h.MaxTime(), mint)
	}

	// This was an initial call to Truncate after loading blocks on startup.
	// We haven't read back the WAL yet, so do not attempt to truncate it.
	if initialize {
		return nil
	}

	h.metrics.headTruncateTotal.Inc()
	return nil
}

// WaitForPendingReadersInTimeRange waits for queries overlapping with given range to finish querying.
// The query timeout limits the max wait time of this function implicitly.
// The mint is inclusive and maxt is the truncation time hence exclusive.
func (h *Head) WaitForPendingReadersInTimeRange(mint, maxt int64) {
	maxt-- // Making it inclusive before checking overlaps.
	overlaps := func() bool {
		o := false
		h.iso.TraverseOpenReads(func(s *isolationState) bool {
			if s.mint <= maxt && mint <= s.maxt {
				// Overlaps with the truncation range.
				o = true
				return false
			}
			return true
		})
		return o
	}
	for overlaps() {
		time.Sleep(500 * time.Millisecond)
	}
}

// WaitForAppendersOverlapping waits for appends overlapping maxt to finish.
func (h *Head) WaitForAppendersOverlapping(maxt int64) {
	for maxt >= h.iso.lowestAppendTime() {
		time.Sleep(500 * time.Millisecond)
	}
}

// IsQuerierCollidingWithTruncation returns if the current querier needs to be closed and if a new querier
// has to be created. In the latter case, the method also returns the new mint to be used for creating the
// new range head and the new querier. This methods helps preventing races with the truncation of in-memory data.
//
// NOTE: The querier should already be taken before calling this.
func (h *Head) IsQuerierCollidingWithTruncation(querierMint, querierMaxt int64) (shouldClose, getNew bool, newMint int64) {
	if !h.memTruncationInProcess.Load() {
		return false, false, 0
	}
	// Head truncation is in process. It also means that the block that was
	// created for this truncation range is also available.
	// Check if we took a querier that overlaps with this truncation.
	memTruncTime := h.lastMemoryTruncationTime.Load()
	if querierMaxt < memTruncTime {
		// Head compaction has happened and this time range is being truncated.
		// This query doesn't overlap with the Head any longer.
		// We should close this querier to avoid races and the data would be
		// available with the blocks below.
		// Cases:
		// 1.     |------truncation------|
		//   |---query---|
		// 2.     |------truncation------|
		//              |---query---|
		return true, false, 0
	}
	if querierMint < memTruncTime {
		// The truncation time is not same as head mint that we saw above but the
		// query still overlaps with the Head.
		// The truncation started after we got the querier. So it is not safe
		// to use this querier and/or might block truncation. We should get
		// a new querier for the new Head range while remaining will be available
		// in the blocks below.
		// Case:
		//      |------truncation------|
		//                        |----query----|
		// Turns into
		//      |------truncation------|
		//                             |---qu---|
		return true, true, memTruncTime
	}

	// Other case is this, which is a no-op
	//      |------truncation------|
	//                              |---query---|
	return false, false, 0
}

type Stats struct {
	NumSeries         uint64
	MinTime, MaxTime  int64
	IndexPostingStats *index.PostingsStats
}

// Stats returns important current HEAD statistics. Note that it is expensive to
// calculate these.
func (h *Head) Stats(statsByLabelName string) *Stats {
	return &Stats{
		NumSeries:         h.NumSeries(),
		MaxTime:           h.MaxTime(),
		MinTime:           h.MinTime(),
		IndexPostingStats: h.PostingsCardinalityStats(statsByLabelName),
	}
}

// RangeHead allows querying Head via an IndexReader, ChunkReader and tombstones.Reader
// but only within a restricted range.  Used for queries and compactions.
type RangeHead struct {
	head       *Head
	mint, maxt int64

	isolationOff bool
}

// NewRangeHead returns a *RangeHead.
// There are no restrictions on mint/maxt.
func NewRangeHead(head *Head, mint, maxt int64) *RangeHead {
	return &RangeHead{
		head: head,
		mint: mint,
		maxt: maxt,
	}
}

// NewRangeHeadWithIsolationDisabled returns a *RangeHead that does not create an isolationState.
func NewRangeHeadWithIsolationDisabled(head *Head, mint, maxt int64) *RangeHead {
	rh := NewRangeHead(head, mint, maxt)
	rh.isolationOff = true
	return rh
}

func (h *RangeHead) Index() (tsdb.IndexReader, error) {
	return h.head.indexRange(h.mint, h.maxt), nil
}

func (h *RangeHead) Chunks() (tsdb.ChunkReader, error) {
	var isoState *isolationState
	if !h.isolationOff {
		isoState = h.head.iso.State(h.mint, h.maxt)
	}
	return h.head.chunksRange(h.mint, h.maxt, isoState)
}

func (h *RangeHead) Tombstones() (tombstones.Reader, error) {
	return nil, nil
}

func (h *RangeHead) MinTime() int64 {
	return h.mint
}

// MaxTime returns the max time of actual data fetch-able from the head.
// This controls the chunks time range which is closed [b.MinTime, b.MaxTime].
func (h *RangeHead) MaxTime() int64 {
	return h.maxt
}

// BlockMaxTime returns the max time of the potential block created from this head.
// It's different to MaxTime as we need to add +1 millisecond to block maxt because block
// intervals are half-open: [b.MinTime, b.MaxTime). Block intervals are always +1 than the total samples it includes.
func (h *RangeHead) BlockMaxTime() int64 {
	return h.MaxTime() + 1
}

func (h *RangeHead) NumSeries() uint64 {
	return h.head.NumSeries()
}

func (h *RangeHead) Meta() tsdb.BlockMeta {
	return tsdb.BlockMeta{
		MinTime: h.MinTime(),
		MaxTime: h.MaxTime(),
		ULID:    h.head.Meta().ULID,
		Stats: tsdb.BlockStats{
			NumSeries: h.NumSeries(),
		},
	}
}

// String returns an human readable representation of the range head. It's important to
// keep this function in order to avoid the struct dump when the head is stringified in
// errors or logs.
func (h *RangeHead) String() string {
	return fmt.Sprintf("range head (mint: %d, maxt: %d)", h.MinTime(), h.MaxTime())
}

// gc removes data before the minimum timestamp from the head.
// It returns
// * The actual min times of the chunks present in the Head.
// * The min OOO time seen during the GC.
// * Min mmap file number seen in the series (in-order and out-of-order) after gc'ing the series.
func (h *Head) gc() (actualInOrderMint int64) {
	// Only data strictly lower than this timestamp must be deleted.
	mint := h.MinTime()

	// Drop old chunks and remember series IDs and hashes if they can be
	// deleted entirely.
	deleted, chunksRemoved, actualInOrderMint := h.series.gc(mint)
	seriesRemoved := len(deleted)

	h.metrics.seriesRemoved.Add(float64(seriesRemoved))
	h.metrics.chunksRemoved.Add(float64(chunksRemoved))
	h.metrics.chunks.Sub(float64(chunksRemoved))
	h.numSeries.Sub(uint64(seriesRemoved))

	// Remove deleted series IDs from the postings lists.
	h.postings.Delete(deleted)

	return actualInOrderMint
}

// NumSeries returns the number of active series in the head.
func (h *Head) NumSeries() uint64 {
	return h.numSeries.Load()
}

// Meta returns meta information about the head.
// The head is dynamic so will return dynamic results.
func (h *Head) Meta() tsdb.BlockMeta {
	var id [16]byte
	copy(id[:], "______head______")
	return tsdb.BlockMeta{
		MinTime: h.MinTime(),
		MaxTime: h.MaxTime(),
		ULID:    ulid.ULID(id),
		Stats: tsdb.BlockStats{
			NumSeries: h.NumSeries(),
		},
	}
}

// MinTime returns the lowest time bound on visible data in the head.
func (h *Head) MinTime() int64 {
	return h.minTime.Load()
}

// MaxTime returns the highest timestamp seen in data of the head.
func (h *Head) MaxTime() int64 {
	return h.maxTime.Load()
}

// compactable returns whether the head has a compactable range.
// The head has a compactable range when the head time range is 1.5 times the chunk range.
// The 0.5 acts as a buffer of the appendable window.
func (h *Head) compactable() bool {
	return h.MaxTime()-h.MinTime() > h.chunkRange.Load()/2*3
}

// Close flushes the WAL and closes the head.
// It also takes a snapshot of in-memory chunks if enabled.
func (h *Head) Close() error {
	h.closedMtx.Lock()
	defer h.closedMtx.Unlock()
	h.closed = true
	return nil
}

// String returns an human readable representation of the TSDB head. It's important to
// keep this function in order to avoid the struct dump when the head is stringified in
// errors or logs.
func (h *Head) String() string {
	return "head"
}

func (h *Head) getOrCreate(hash uint64, lset labels.Labels) (*memSeries, bool, error) {
	// Just using `getOrCreateWithID` below would be semantically sufficient, but we'd create
	// a new series on every sample inserted via Add(), which causes allocations
	// and makes our series IDs rather random and harder to compress in postings.
	s := h.series.getByHash(hash, lset)
	if s != nil {
		return s, false, nil
	}

	// Optimistically assume that we are the first one to create the series.
	id := chunks.HeadSeriesRef(h.lastSeriesID.Inc())

	return h.getOrCreateWithID(id, hash, lset)
}

func (h *Head) getOrCreateWithID(id chunks.HeadSeriesRef, hash uint64, lset labels.Labels) (*memSeries, bool, error) {
	s, created, err := h.series.getOrSet(hash, lset, func() *memSeries {
		return newMemSeries(lset, id, hash, 0, true)
	})
	if err != nil {
		return nil, false, err
	}
	if !created {
		return s, false, nil
	}

	h.metrics.seriesCreated.Inc()
	h.numSeries.Inc()

	h.postings.Add(storage.SeriesRef(id), lset)
	return s, true, nil
}

// seriesHashmap is a simple hashmap for memSeries by their label set. It is built
// on top of a regular hashmap and holds a slice of series to resolve hash collisions.
// Its methods require the hash to be submitted with it to avoid re-computations throughout
// the code.
type seriesHashmap map[uint64][]*memSeries

func (m seriesHashmap) get(hash uint64, lset labels.Labels) *memSeries {
	for _, s := range m[hash] {
		if labels.Equal(s.lset, lset) {
			return s
		}
	}
	return nil
}

func (m seriesHashmap) set(hash uint64, s *memSeries) {
	l := m[hash]
	for i, prev := range l {
		if labels.Equal(prev.lset, s.lset) {
			l[i] = s
			return
		}
	}
	m[hash] = append(l, s)
}

func (m seriesHashmap) del(hash uint64, lset labels.Labels) {
	var rem []*memSeries
	for _, s := range m[hash] {
		if !labels.Equal(s.lset, lset) {
			rem = append(rem, s)
		}
	}
	if len(rem) == 0 {
		delete(m, hash)
	} else {
		m[hash] = rem
	}
}

const (
	// DefaultStripeSize is the default number of entries to allocate in the stripeSeries hash map.
	DefaultStripeSize = 1 << 14
)

// stripeSeries holds series by HeadSeriesRef ("ID") and also by hash of their labels.
// ID-based lookups via getByID() are preferred over getByHash() for performance reasons.
// It locks modulo ranges of IDs and hashes to reduce lock contention.
// The locks are padded to not be on the same cache line. Filling the padded space
// with the maps was profiled to be slower – likely due to the additional pointer
// dereferences.
type stripeSeries struct {
	size                    int
	series                  []map[chunks.HeadSeriesRef]*memSeries // Sharded by ref. A series ref is the value of `size` when the series was being newly added.
	hashes                  []seriesHashmap                       // Sharded by label hash.
	locks                   []stripeLock                          // Sharded by ref for series access, by label hash for hashes access.
	seriesLifecycleCallback SeriesLifecycleCallback
}

type stripeLock struct {
	sync.RWMutex
	// Padding to avoid multiple locks being on the same cache line.
	_ [40]byte
}

func newStripeSeries(stripeSize int, seriesCallback SeriesLifecycleCallback) *stripeSeries {
	s := &stripeSeries{
		size:                    stripeSize,
		series:                  make([]map[chunks.HeadSeriesRef]*memSeries, stripeSize),
		hashes:                  make([]seriesHashmap, stripeSize),
		locks:                   make([]stripeLock, stripeSize),
		seriesLifecycleCallback: seriesCallback,
	}

	for i := range s.series {
		s.series[i] = map[chunks.HeadSeriesRef]*memSeries{}
	}
	for i := range s.hashes {
		s.hashes[i] = seriesHashmap{}
	}
	return s
}

// gc garbage collects old chunks that are strictly before mint and removes
// series entirely that have no chunks left.
// note: returning map[chunks.HeadSeriesRef]struct{} would be more accurate,
// but the returned map goes into postings.Delete() which expects a map[storage.SeriesRef]struct
// and there's no easy way to cast maps.
func (s *stripeSeries) gc(mint int64) (_ map[storage.SeriesRef]struct{}, _ int, _ int64) {
	var (
		deleted                  = map[storage.SeriesRef]struct{}{}
		deletedForCallback       = []labels.Labels{}
		rmChunks                 = 0
		actualMint         int64 = math.MaxInt64
	)
	// Run through all series and truncate old chunks. Mark those with no
	// chunks left as deleted and store their ID.
	for i := 0; i < s.size; i++ {
		s.locks[i].Lock()

		for hash, all := range s.hashes[i] {
			for _, series := range all {
				series.Lock()
				rmChunks += series.truncateChunksBefore(mint)

				if len(series.finishedChunks) > 0 || series.headChunk != nil || series.pendingCommit {
					seriesMint := series.minTime()
					if seriesMint < actualMint {
						actualMint = seriesMint
					}
					series.Unlock()
					continue
				}

				// The series is gone entirely. We need to keep the series lock
				// and make sure we have acquired the stripe locks for hash and ID of the
				// series alike.
				// If we don't hold them all, there's a very small chance that a series receives
				// samples again while we are half-way into deleting it.
				j := int(series.ref) & (s.size - 1)

				if i != j {
					s.locks[j].Lock()
				}

				deleted[storage.SeriesRef(series.ref)] = struct{}{}
				s.hashes[i].del(hash, series.lset)
				delete(s.series[j], series.ref)
				deletedForCallback = append(deletedForCallback, series.lset)

				if i != j {
					s.locks[j].Unlock()
				}

				series.Unlock()
			}
		}

		s.locks[i].Unlock()

		s.seriesLifecycleCallback.PostDeletion(deletedForCallback...)
		deletedForCallback = deletedForCallback[:0]
	}

	if actualMint == math.MaxInt64 {
		actualMint = mint
	}

	return deleted, rmChunks, actualMint
}

func (s *stripeSeries) getByID(id chunks.HeadSeriesRef) *memSeries {
	i := uint64(id) & uint64(s.size-1)

	s.locks[i].RLock()
	series := s.series[i][id]
	s.locks[i].RUnlock()

	return series
}

func (s *stripeSeries) getByHash(hash uint64, lset labels.Labels) *memSeries {
	i := hash & uint64(s.size-1)

	s.locks[i].RLock()
	series := s.hashes[i].get(hash, lset)
	s.locks[i].RUnlock()

	return series
}

func (s *stripeSeries) getOrSet(hash uint64, lset labels.Labels, createSeries func() *memSeries) (*memSeries, bool, error) {
	// PreCreation is called here to avoid calling it inside the lock.
	// It is not necessary to call it just before creating a series,
	// rather it gives a 'hint' whether to create a series or not.
	preCreationErr := s.seriesLifecycleCallback.PreCreation(lset)

	// Create the series, unless the PreCreation() callback as failed.
	// If failed, we'll not allow to create a new series anyway.
	var series *memSeries
	if preCreationErr == nil {
		series = createSeries()
	}

	i := hash & uint64(s.size-1)
	s.locks[i].Lock()

	if prev := s.hashes[i].get(hash, lset); prev != nil {
		s.locks[i].Unlock()
		return prev, false, nil
	}
	if preCreationErr == nil {
		s.hashes[i].set(hash, series)
	}
	s.locks[i].Unlock()

	if preCreationErr != nil {
		// The callback prevented creation of series.
		return nil, false, preCreationErr
	}
	// Setting the series in the s.hashes marks the creation of series
	// as any further calls to this methods would return that series.
	s.seriesLifecycleCallback.PostCreation(series.lset)

	i = uint64(series.ref) & uint64(s.size-1)

	s.locks[i].Lock()
	s.series[i][series.ref] = series
	s.locks[i].Unlock()

	return series, true, nil
}

type sample struct {
	t int64
	v float64
}

func newSample(t int64, v float64) tsdbutil.Sample { return sample{t, v} }
func (s sample) T() int64                          { return s.t }
func (s sample) V() float64                        { return s.v }

// memSeries is the in-memory representation of a series. None of its methods
// are goroutine safe and it is the caller's responsibility to lock it.
type memSeries struct {
	sync.RWMutex

	ref  chunks.HeadSeriesRef
	lset labels.Labels
	hash uint64
	meta *metadata.Metadata

	finishedChunks []*memChunk        // Chunks that are finished.
	headChunk      *memChunk          // Most recent chunk in memory that's still being built.
	firstChunkID   chunks.HeadChunkID // HeadChunkID for mmappedChunks[0]

	firstOOOChunkID chunks.HeadChunkID // HeadOOOChunkID for oooMmappedChunks[0]

	mmMaxTime int64 // Max time of any mmapped chunk, only used during WAL replay.

	// chunkEndTimeVariance is how much variance (between 0 and 1) should be applied to the chunk end time,
	// to spread chunks writing across time. Doesn't apply to the last chunk of the chunk range. 0 to disable variance.
	chunkEndTimeVariance float64

	nextAt int64 // Timestamp at which to cut the next chunk.

	// We keep the last value here (in addition to appending it to the chunk) so we can check for duplicates.
	lastValue float64

	// Current appender for the head chunk. Set when a new head chunk is cut.
	// It is nil only if headChunk is nil. E.g. if there was an appender that created a new series, but rolled back the commit
	// (the first sample would create a headChunk, hence appender, but rollback skipped it while the Append() call would create a series).
	app chunkenc.Appender

	// txs is nil if isolation is disabled.
	txs *txRing

	pendingCommit bool // Whether there are samples waiting to be committed to this series.
}

func newMemSeries(lset labels.Labels, id chunks.HeadSeriesRef, hash uint64, chunkEndTimeVariance float64, isolationDisabled bool) *memSeries {
	s := &memSeries{
		lset:                 lset,
		ref:                  id,
		nextAt:               math.MinInt64,
		chunkEndTimeVariance: chunkEndTimeVariance,
		hash:                 hash,
	}
	if !isolationDisabled {
		s.txs = newTxRing(4)
	}
	return s
}

func (s *memSeries) minTime() int64 {
	if len(s.finishedChunks) > 0 {
		return s.finishedChunks[0].minTime
	}
	if s.headChunk != nil {
		return s.headChunk.minTime
	}
	return math.MinInt64
}

func (s *memSeries) maxTime() int64 {
	// The highest timestamps will always be in the regular (non-OOO) chunks, even if OOO is enabled.
	c := s.head()
	if c != nil {
		return c.maxTime
	}
	if len(s.finishedChunks) > 0 {
		return s.finishedChunks[len(s.finishedChunks)-1].maxTime
	}
	return math.MinInt64
}

// truncateChunksBefore removes all chunks from the series that
// have no timestamp at or after mint.
// Chunk IDs remain unchanged.
func (s *memSeries) truncateChunksBefore(mint int64) int {
	var removedInOrder int
	if s.headChunk != nil && s.headChunk.maxTime < mint {
		// If head chunk is truncated, we can truncate all mmapped chunks.
		removedInOrder = 1 + len(s.finishedChunks)
		s.firstChunkID += chunks.HeadChunkID(removedInOrder)
		s.headChunk = nil
		s.finishedChunks = nil
	}
	if len(s.finishedChunks) > 0 {
		for i, c := range s.finishedChunks {
			if c.maxTime >= mint {
				break
			}
			removedInOrder = i + 1
		}
		s.finishedChunks = append(s.finishedChunks[:0], s.finishedChunks[removedInOrder:]...)
		s.firstChunkID += chunks.HeadChunkID(removedInOrder)
	}

	return removedInOrder
}

// cleanupAppendIDsBelow cleans up older appendIDs. Has to be called after
// acquiring lock.
func (s *memSeries) cleanupAppendIDsBelow(bound uint64) {
	if s.txs != nil {
		s.txs.cleanupAppendIDsBelow(bound)
	}
}

func (s *memSeries) head() *memChunk {
	return s.headChunk
}

type memChunk struct {
	chunk            chunkenc.Chunk
	minTime, maxTime int64
}

// OverlapsClosedInterval returns true if the chunk overlaps [mint, maxt].
func (mc *memChunk) OverlapsClosedInterval(mint, maxt int64) bool {
	return overlapsClosedInterval(mc.minTime, mc.maxTime, mint, maxt)
}

func overlapsClosedInterval(mint1, maxt1, mint2, maxt2 int64) bool {
	return mint1 <= maxt2 && mint2 <= maxt1
}

// mappedChunks describes a head chunk on disk that has been mmapped
type mmappedChunk struct {
	ref              chunks.ChunkDiskMapperRef
	numSamples       uint16
	minTime, maxTime int64
}

// Returns true if the chunk overlaps [mint, maxt].
func (mc *mmappedChunk) OverlapsClosedInterval(mint, maxt int64) bool {
	return overlapsClosedInterval(mc.minTime, mc.maxTime, mint, maxt)
}

type noopSeriesLifecycleCallback struct{}

func (noopSeriesLifecycleCallback) PreCreation(labels.Labels) error { return nil }
func (noopSeriesLifecycleCallback) PostCreation(labels.Labels)      {}
func (noopSeriesLifecycleCallback) PostDeletion(...labels.Labels)   {}

func (h *Head) Size() int64 {
	return 0
}

func (h *RangeHead) Size() int64 {
	return h.head.Size()
}
