// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"time"

	"github.com/grafana/dskit/ring"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// tenantEngine is the per-tenant storage behind userTSDB: everything the ingester does around it (limits through
// the series lifecycle callbacks, active series, owned series, compaction scheduling, idle close, shipping) stays
// in userTSDB, so another engine only has to store and serve samples. The ingester says what it wants, and when:
// how the engine does it is the engine's.
type tenantEngine interface {
	// Ingest stores the series of a write request, and makes them visible. Soft failures are reported to the sink as
	// they happen, and a hard error undoes the whole batch.
	Ingest(ctx context.Context, batch ingestBatch, sink *pushSink) (ingestOutcome, error)
	Querier(mint, maxt int64) (storage.Querier, error)
	// ChunkQuerier returns the chunks of the series in the range; with unordered, those of a series may overlap
	// and come in any order, which the engine may serve more cheaply.
	ChunkQuerier(mint, maxt int64, unordered bool) (storage.ChunkQuerier, error)
	ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error)

	Head() engineHead

	// Compact is the routine upkeep the ingester asks for on its schedule. What it does, if anything, is up to the
	// engine.
	Compact(ctx context.Context) error
	// Flush moves the samples up to upTo out of memory, and the series that have nothing newer with them: when the
	// ingester shuts down or goes idle, or has too many series in memory.
	Flush(ctx context.Context, upTo int64) error
	// Evict drops the series from memory, whatever they hold, which stays queryable: series the ingester no longer
	// owns.
	Evict(ctx context.Context, refs []storage.SeriesRef) error

	// Configure applies settings that change while the engine runs.
	Configure(settings engineSettings) error
	Close() error
}

// engineSettings are the settings of an engine that the ingester changes at runtime.
type engineSettings struct {
	// OutOfOrderTimeWindow is how far behind the newest sample a sample may be and still be accepted, in
	// milliseconds, or 0 to accept none.
	OutOfOrderTimeWindow int64
	MaxExemplars         int64
	// FloatChunkEncoding is how new chunks of floats are encoded, by the name the limit uses.
	FloatChunkEncoding string
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
	// OldestAppendableTime returns the oldest time a sample can still be appended at, when it is known.
	OldestAppendableTime() (int64, bool)

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

// testNoNativeActive makes tests run an engine that keeps the active series itself with the ingester's tracker instead,
// to compare them (MIMIR_TEST_NO_NATIVE_ACTIVE); it's always false outside tests.
var testNoNativeActive bool

// enginesReadRecords reports whether the engines the ingester opens read Kafka records themselves: the Prometheus
// TSDB doesn't.
func (i *Ingester) enginesReadRecords() bool {
	return false
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
