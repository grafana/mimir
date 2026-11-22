// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/ingester/activeseries"
	"github.com/grafana/mimir/pkg/mimirpb"
	mimir_storage "github.com/grafana/mimir/pkg/storage"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

// tenantEngine owns per-tenant storage. The ingester schedules compaction and shipping.
type tenantEngine interface {
	Head() engineHead

	// NewestDataTime returns the latest head or persisted-data time in milliseconds,
	// or math.MinInt64 if the engine has no data.
	NewestDataTime() int64

	Querier(mint, maxt int64) (storage.Querier, error)
	// ChunkQuerier may return overlapping chunks in any order when unordered is true.
	ChunkQuerier(mint, maxt int64, unordered bool) (storage.ChunkQuerier, error)
	ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error)

	// Ingest stores the series of a write request, and makes them visible. Soft failures are reported through ctx as
	// they happen, and a hard error undoes the whole batch.
	Ingest(ctx *ingestContext, batch ingestBatch) (ingestOutcome, error)

	// Configure applies tenant settings while the ingester is running.
	Configure(engineSettings) error

	// Compact lets the engine apply its normal compaction criteria.
	Compact(ctx context.Context) error

	// Close releases the store's resources while preserving its on-disk data for reopening.
	Close() error

	// Flush moves in-order samples through upTo (inclusive, in milliseconds) and all
	// out-of-order samples out of memory, along with series that have nothing newer.
	Flush(ctx context.Context, upTo int64) error

	BlockShippingEnabled() bool
	// The caller must prevent idle close while ShipBlocks uses the local data.
	ShipBlocks(ctx context.Context)

	// Evict persists the selected series before removing them from memory.
	Evict(ctx context.Context, refs []storage.SeriesRef) error
}

// ingestContext must not be retained after Ingest returns because its bookkeeping and data belong to the request.
type ingestContext struct {
	context.Context

	i                  *Ingester
	userID             string
	startAppend        time.Time
	request            *pushRequest
	stats              *pushStats
	errProcessor       *mimir_storage.SoftAppendErrorProcessor
	updateFirstPartial func(sampler *util_log.Sampler, errFn softErrorFunction)
	activeSeries       *activeseries.ActiveSeries
	// idx is used to decrease active series count in case of error for cost attribution.
	idx tsdb.IndexReader

	outOfOrderWindow               time.Duration
	minAppendTime                  int64
	maxTimestampMs, minTimestampMs int64

	// The out-of-order exemplars seen in the series being ingested, since only all being out of order is reported.
	oooExemplarsSeries, oooExemplars int

	minAppendTimeAvailable           bool
	nativeHistogramsIngestionEnabled bool
	isOTLP                           bool
}

type engineSettings struct {
	// OutOfOrderTimeWindow is in milliseconds.
	OutOfOrderTimeWindow int64
	MaxExemplars         int64
	FloatChunkEncoding   string
}

type prometheusEngine struct {
	db            *tsdb.DB
	blockDuration int64

	shippingLogger log.Logger
	shipper        BlocksUploader

	shippedBlocksMtx sync.Mutex
	shippedBlocks    map[ulid.ULID]time.Time
}

// timeBounds are the times of the samples in the head, in milliseconds.
type timeBounds struct {
	MinTime, MaxTime       int64
	MinOOOTime, MaxOOOTime int64
}

// engineHead is the in-memory part of a tenantEngine, which the ingester reads for series counts, time bounds,
// the head index, and series ownership.
type engineHead interface {
	NumSeries() uint64
	TimeBounds() timeBounds
	// MinInOrderAppendTime returns the minimum timestamp for in-order appends,
	// in milliseconds, when known. Older out-of-order samples may still be accepted.
	MinInOrderAppendTime() (int64, bool)

	Index() (tsdb.IndexReader, error)
	// Ownership returns how many of the series fall in the token ranges, and the refs of those that don't.
	Ownership(owned ring.TokenRanges) (ownedCount int, nonOwned []storage.SeriesRef)
	// ShardSeries returns the series in shard shardIndex of shardCount, by ref.
	ShardSeries(shardIndex, shardCount uint64) index.Postings

	// LabelValuesBytes returns the bytes of the distinct values of each label name.
	LabelValuesBytes() map[string]uint64

	// Sync makes what was appended so far durable, before the ingester commits its Kafka offset.
	Sync() error
}

// mustIndex returns the head's index, for callers that can't continue without it.
func mustIndex(head engineHead) tsdb.IndexReader {
	idx, err := head.Index()
	if err != nil {
		panic(err)
	}
	return idx
}

// testEngine lets tests run the whole package on another engine (MIMIR_TEST_TSDB_ENGINE); it's
// always empty outside tests.
var testEngine string

// testNoNativeActive makes tests run an engine that keeps the active series itself with the ingester's tracker instead,
// to compare them (MIMIR_TEST_NO_NATIVE_ACTIVE); it's always false outside tests.
var testNoNativeActive bool

// openTenantEngine opens the tenant's TSDB in dir with the engine the ingester is configured with.
func (i *Ingester) openTenantEngine(dir, userID string, logger *slog.Logger, reg prometheus.Registerer, checkpointOnCreate bool, opts *tsdb.Options) (tenantEngine, error) {
	if i.cfg.BlocksStorageConfig.TSDB.Engine == mimir_tsdb.EngineSeriesstore || testEngine == mimir_tsdb.EngineSeriesstore {
		checkpoints := i.cfg.BlocksStorageConfig.TSDB.Engine == mimir_tsdb.EngineSeriesstore && i.cfg.BlocksStorageConfig.TSDB.SeriesstoreSnapshotInterval > 0
		var initialCheckpoint func() ([]byte, error)
		// Startup opens tenants before rewinding the offset files. Only an engine opened during ingestion can use those offsets.
		if checkpoints && checkpointOnCreate {
			initialCheckpoint = i.committedKafkaOffsets
		}
		return openSeriesstoreEngine(dir, userID, reg, opts, seriesstoreShardsFor(i.limits.MaxGlobalSeriesPerUser(userID)), checkpoints, initialCheckpoint, i.cfg.ActiveSeriesMetrics.IdleTimeout)
	}
	return openPrometheusEngine(dir, logger, reg, opts)
}

// enginesReadRecords reports whether the engines the ingester opens read Kafka records themselves: the seriesstore
// engine does.
func (i *Ingester) enginesReadRecords() bool {
	return i.cfg.BlocksStorageConfig.TSDB.Engine == mimir_tsdb.EngineSeriesstore || testEngine == mimir_tsdb.EngineSeriesstore
}

// ingestBatch is what the ingester asks an engine to store: the series of one write request.
type ingestBatch struct {
	Series []mimirpb.PreallocTimeseries
	// Record, when set, is the batch's record as the engine read it with ReadRecord: series are identified by their
	// index in it as in Series, which is nil. Decoded decodes the series at indices, which are in order, into a slice
	// of all of them, only those set.
	Record  ingestRecord
	Decoded func(indices []int) []mimirpb.PreallocTimeseries
	// Samples, histograms and exemplars outside [MinTimestampMs, MaxTimestampMs] are rejected.
	MinTimestampMs, MaxTimestampMs int64
	// Histograms are ingested only if NativeHistograms is set, and exemplars only if Exemplars is.
	NativeHistograms bool
	Exemplars        bool
	// When the request was ingested, and whether it came by OTLP, which an engine that keeps the active series
	// records in the series it ingests.
	IngestedAt time.Time
	OTLP       bool
}

// ingestRecord is a Kafka record an engine read itself, to ingest its series without them being decoded.
type ingestRecord interface {
	// Len is how many series the record has.
	Len() int
	Source() mimirpb.WriteRequest_SourceEnum
	// Complete reports whether the engine read all of the record: one it didn't is decoded.
	Complete() bool
	// MinTimestamp is the oldest timestamp of the record's samples, histograms and exemplars, math.MaxInt64 without
	// any.
	MinTimestamp() int64
	// Metadata is the record's metadata, as decoding it gives.
	Metadata() []*mimirpb.MetricMetadata
	// Series returns the labels and floats of the series at index, which refer to the record's content.
	Series(index int) ([]mimirpb.LabelAdapter, []mimirpb.Sample)
	// Subset appends to into the content of a record with only the series at indices, which are in order, to decode
	// them.
	Subset(indices []int, into []byte) []byte
	// Release gives back what the record holds once it is ingested.
	Release()
}

// recordReader is an engine that reads Kafka records itself: ok is false for a record it doesn't read, which is
// decoded. The record refers to content, which it must not outlive.
type recordReader interface {
	ReadRecord(content []byte, version int) (record ingestRecord, ok bool)
}

// The exemplar errors that don't come from an engine.
var (
	errExemplarMissingSeries  = errors.New("exemplars can only be added to a series that exists")
	errExemplarTooFarInFuture = errors.New("the exemplar is too far in the future")
	errExemplarTooFarInPast   = errors.New("the exemplar is too far in the past")
)

// ingestOutcome is what an engine ingested of a batch.
type ingestOutcome struct {
	// Samples counts floats, histograms and created-timestamp zero samples.
	Samples   int
	Exemplars int
	// CommitDuration is how long making the batch visible took.
	CommitDuration time.Duration
}
