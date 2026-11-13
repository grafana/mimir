// SPDX-License-Identifier: AGPL-3.0-only
//go:build rustengine && cgo

package ingester

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grafana/dskit/ring"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/rustengine"
	"github.com/grafana/mimir/pkg/util/globalerror"
)

// rustEngine is a tenant's engine in Rust. Ingestion and upkeep take mu, one at a time like the tenant's records come;
// queries don't, since the Rust engine lets them run with ingestion.
type rustEngine struct {
	mu                   sync.Mutex
	native               *rustengine.Engine
	lifecycle            tsdb.SeriesLifecycleCallback
	secondaryHash        func(labels.Labels) uint32
	blockDuration        int64
	timelyCompaction     bool
	retention            int64
	idleEviction         int64
	closed               bool
	needsKafkaReplay     bool
	floatEncoding        atomic.Uint32
	defaultFloatEncoding chunkenc.Encoding
	values               rustValueCache
}

var _ tenantEngine = (*rustEngine)(nil)
var _ engineHead = (*rustEngine)(nil)
var _ recordReader = (*rustEngine)(nil)

func openTenantEngine(engine, dir string, logger *slog.Logger, reg prometheus.Registerer, opts *tsdb.Options, shipping bool, activeIdleTimeout time.Duration, _ bool) (tenantEngine, error) {
	if engine == "" || engine == "prometheus" {
		return openPrometheusEngine(dir, logger, reg, opts)
	}
	if engine != "rust" {
		return nil, fmt.Errorf("unsupported engine %q", engine)
	}
	if shipping {
		return nil, errors.New("experimental Rust engine does not produce Prometheus blocks; disable block shipping")
	}
	e, err := openRustEngine(dir, opts)
	if err != nil {
		return nil, err
	}
	// Series that stopped stay in memory past the active series' timeout, then leave it with their data queryable.
	e.idleEviction = (activeIdleTimeout + 5*time.Minute).Milliseconds()
	return e, nil
}

// openRustEngine opens the engine in dir. It has no WAL: without the head a clean close wrote, it starts empty and
// asks for Kafka to be replayed.
func openRustEngine(dir string, opts *tsdb.Options) (*rustEngine, error) {
	entries, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	for _, entry := range entries {
		_, blockErr := ulid.ParseStrict(entry.Name())
		if entry.Name() == "wal" || entry.Name() == "wbl" || entry.Name() == "chunks_head" || blockErr == nil {
			return nil, errors.New("Rust engine requires a separate directory from Prometheus TSDB data")
		}
	}
	blockDuration := opts.MinBlockDuration
	if blockDuration <= 0 {
		blockDuration = 2 * time.Hour.Milliseconds()
	}
	native, err := rustengine.Open(filepath.Join(dir, "rust-engine"), blockDuration, ingest.V2RecordSymbolOffset, ingest.V2CommonSymbols.GetSlice())
	if err != nil {
		return nil, err
	}
	e := &rustEngine{native: native, lifecycle: opts.SeriesLifecycleCallback, secondaryHash: opts.SecondaryHashFunction, blockDuration: blockDuration, timelyCompaction: opts.TimelyCompaction, retention: opts.RetentionDuration, defaultFloatEncoding: opts.FloatChunkEncoding}
	clean, err := native.RestoredClean()
	if err != nil {
		native.Close()
		return nil, err
	}
	e.needsKafkaReplay = !clean
	if err = e.Configure(engineSettings{OutOfOrderTimeWindow: opts.OutOfOrderTimeWindow, MaxExemplars: opts.MaxExemplars, FloatChunkEncoding: strings.ToLower(opts.FloatChunkEncoding.String())}); err != nil {
		native.Close()
		return nil, err
	}
	if e.lifecycle != nil {
		err = native.HeadSeries(func(_ uint64, ls []rustengine.Label) error {
			e.lifecycle.PostCreation(rustLabels(ls))
			return nil
		})
		if err != nil {
			native.Close()
			return nil, err
		}
	}
	return e, nil
}

func (e *rustEngine) Configure(settings engineSettings) error {
	if settings.MaxExemplars < 0 {
		return errors.New("negative exemplar limit")
	}
	encoding := e.defaultFloatEncoding
	switch settings.FloatChunkEncoding {
	case "", "none":
	case "xor":
		encoding = chunkenc.EncXOR
	case "xor2":
		encoding = chunkenc.EncXOR2
	default:
		return fmt.Errorf("unsupported float chunk encoding %q", settings.FloatChunkEncoding)
	}
	if encoding != chunkenc.EncXOR2 {
		encoding = chunkenc.EncXOR
	}
	if err := e.native.Configure(settings.OutOfOrderTimeWindow, settings.MaxExemplars); err != nil {
		return err
	}
	e.floatEncoding.Store(uint32(encoding))
	return nil
}

func (e *rustEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	// A head whose Kafka replay isn't done would make the next open skip what it misses.
	var err error
	if !e.needsKafkaReplay {
		err = e.native.CloseCheckpoint()
	}
	e.native.Close()
	return err
}

func (e *rustEngine) NeedsKafkaReplay() bool { return e.needsKafkaReplay }
func (e *rustEngine) KafkaOffsetsRewound() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.needsKafkaReplay = false
}

func (e *rustEngine) Head() engineHead { return e }

// Sync has nothing to do: what Kafka offsets cover comes back from Kafka after a crash.
func (e *rustEngine) Sync() error { return nil }

func (e *rustEngine) NumSeries() uint64 {
	s, err := e.native.Stats()
	if err != nil {
		return 0
	}
	return s.InHead
}

func (e *rustEngine) TimeBounds() timeBounds {
	b := timeBounds{MinTime: math.MaxInt64, MaxTime: math.MinInt64, MinOOOTime: math.MaxInt64, MaxOOOTime: math.MinInt64}
	s, err := e.native.Stats()
	if err != nil {
		return b
	}
	if s.HeadMint != rustengine.NoTime {
		b.MinTime, b.MaxTime = s.HeadMint, s.HeadMaxt
	}
	if s.OOOMint != rustengine.NoTime {
		b.MinOOOTime, b.MaxOOOTime = s.OOOMint, s.OOOMaxt
	}
	return b
}

func (e *rustEngine) OldestAppendableTime() (int64, bool) {
	s, err := e.native.Stats()
	return s.MinValid, err == nil && s.MinValid != math.MinInt64
}

func (e *rustEngine) Index() (tsdb.IndexReader, error) {
	return &rustIndex{engine: e, headOnly: true}, nil
}

func (e *rustEngine) Ownership(owned ring.TokenRanges) (int, []storage.SeriesRef) {
	var count int
	var refs []storage.SeriesRef
	_ = e.native.HeadSeries(func(ref uint64, ls []rustengine.Label) error {
		var hash uint32
		if e.secondaryHash != nil {
			hash = e.secondaryHash(rustLabels(ls))
		}
		if owned.IncludesKey(hash) {
			count++
		} else {
			refs = append(refs, storage.SeriesRef(ref))
		}
		return nil
	})
	return count, refs
}

func (e *rustEngine) ShardSeries(shard, count uint64) index.Postings {
	if count == 0 || shard >= count {
		return index.ErrPostings(errors.New("invalid shard"))
	}
	var refs []storage.SeriesRef
	err := e.native.HeadSeries(func(ref uint64, ls []rustengine.Label) error {
		if labels.StableHash(rustLabels(ls))%count == shard {
			refs = append(refs, storage.SeriesRef(ref))
		}
		return nil
	})
	if err != nil {
		return index.ErrPostings(err)
	}
	return index.NewListPostings(refs)
}

func (e *rustEngine) LabelValuesBytes() map[string]uint64 {
	out, err := e.native.LabelBytes()
	if err != nil {
		return nil
	}
	return out
}

// Compact flushes the block ranges the Prometheus TSDB would compact, evicts the series that stopped, and drops
// what is past retention.
func (e *rustEngine) Compact(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s, err := e.native.Stats()
	if err != nil {
		return err
	}
	if e.blockDuration > 0 && s.HeadMint != rustengine.NoTime {
		start := s.HeadMint
		appendable := int64(math.MinInt64)
		if s.HeadMaxt >= math.MinInt64+e.blockDuration/2 {
			appendable = s.HeadMaxt - e.blockDuration/2
		}
		appendable = max(s.MinValid, appendable)
		for {
			end := (start/e.blockDuration)*e.blockDuration + e.blockDuration
			if end <= start {
				break
			}
			if e.timelyCompaction {
				if end >= appendable {
					break
				}
			} else if uint64(s.HeadMaxt)-uint64(start) <= uint64(e.blockDuration)+uint64(e.blockDuration/2) {
				break
			}
			start = end
		}
		if start > s.HeadMint {
			if err := e.Flush(ctx, start-1); err != nil {
				return err
			}
		}
	}
	if s.OOOMint != rustengine.NoTime {
		// Prometheus compacts the whole out-of-order head with each compaction.
		e.mu.Lock()
		err := e.native.CompactOOO()
		e.mu.Unlock()
		if err != nil {
			return err
		}
	}
	if e.idleEviction > 0 {
		if err := e.removed(e.native.EvictIdle(e.idleEviction)); err != nil {
			return err
		}
	}
	if e.retention > 0 {
		e.mu.Lock()
		err := e.native.Retain(time.Now().UnixMilli() - e.retention)
		e.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *rustEngine) Flush(ctx context.Context, upTo int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	removed, err := e.native.Flush(upTo)
	e.mu.Unlock()
	return e.removed(removed, err)
}

func (e *rustEngine) Evict(ctx context.Context, refs []storage.SeriesRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ids := make([]uint64, len(refs))
	for i, r := range refs {
		ids[i] = uint64(r)
	}
	e.mu.Lock()
	removed, err := e.native.Evict(ids)
	e.mu.Unlock()
	return e.removed(removed, err)
}

// removed tells the lifecycle about the series that left the head.
func (e *rustEngine) removed(series []rustengine.RemovedSeries, err error) error {
	if err != nil {
		return err
	}
	if e.lifecycle != nil && len(series) > 0 {
		deleted := make(map[chunks.HeadSeriesRef]labels.Labels, len(series))
		for _, s := range series {
			deleted[chunks.HeadSeriesRef(s.Ref)] = rustLabels(s.Labels)
		}
		e.lifecycle.PostDeletion(deleted)
	}
	return nil
}

// rustRecord is a record the Rust engine read in place; its content must stay unchanged until Release.
type rustRecord struct {
	engine  *rustEngine
	content []byte
	rec     rustengine.Record
}

// ReadRecord reads a version 2 record in place.
func (e *rustEngine) ReadRecord(content []byte, version int) (ingestRecord, bool) {
	if version != 2 {
		return nil, false
	}
	rec, err := e.native.Parse(content)
	if err != nil {
		// Decoding it tells what is wrong with it.
		return nil, false
	}
	return &rustRecord{engine: e, content: content, rec: rec}, true
}

func (r *rustRecord) Len() int { return r.rec.Len }
func (r *rustRecord) Source() mimirpb.WriteRequest_SourceEnum {
	return mimirpb.WriteRequest_SourceEnum(r.rec.Source)
}
func (r *rustRecord) Complete() bool      { return r.rec.Complete }
func (r *rustRecord) MinTimestamp() int64 { return r.rec.MinTimestamp }
func (r *rustRecord) Metadata() []*mimirpb.MetricMetadata {
	if len(r.rec.Metadata) == 0 {
		return nil
	}
	out := make([]*mimirpb.MetricMetadata, len(r.rec.Metadata))
	for i, m := range r.rec.Metadata {
		out[i] = &mimirpb.MetricMetadata{MetricFamilyName: m.Family, Help: m.Help, Unit: m.Unit, Type: mimirpb.MetricMetadata_MetricType(m.Type)}
	}
	return out
}
func (r *rustRecord) Series(index int) ([]mimirpb.LabelAdapter, []mimirpb.Sample) {
	ls, samples, err := r.engine.native.SeriesOf(r.rec.Handle, index, r.content)
	if err != nil {
		return nil, nil
	}
	adapters := ls
	out := make([]mimirpb.Sample, len(samples))
	for i, s := range samples {
		out[i] = mimirpb.Sample{TimestampMs: int64(s[0]), Value: math.Float64frombits(s[1])}
	}
	return adapters, out
}
func (r *rustRecord) Subset(indices []int, into []byte) []byte {
	out, err := r.engine.native.Subset(r.rec.Handle, r.content, indices, into[:0])
	if err != nil {
		return into[:0]
	}
	return out
}
func (r *rustRecord) Release() {
	if r.engine != nil {
		r.engine.native.Release(r.rec.Handle)
		r.engine = nil
	}
}

func (e *rustEngine) Ingest(ctx *ingestContext, batch ingestBatch) (ingestOutcome, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ingestOutcome{}, err
	}
	var outcome ingestOutcome
	rec, ok := batch.Record.(*rustRecord)
	if !ok || rec.engine != e {
		all := make([]int, len(batch.Series))
		for i := range all {
			all[i] = i
		}
		return e.ingestThroughAppender(ctx, batch, all, outcome)
	}
	started := time.Now()
	skip := ctx.outOfOrderWindow <= 0 && ctx.minAppendTimeAvailable
	res, err := e.native.Ingest(rec.rec.Handle, rec.content, batch.MinTimestampMs, batch.MaxTimestampMs, batch.IngestedAt.UnixMilli(), skip, ctx.minAppendTime, ctx.activeSeries != nil)
	if err != nil {
		return outcome, err
	}
	outcome.Samples = int(res.Accepted)
	outcome.CommitDuration = time.Since(started)
	for _, s := range res.Ingested {
		if !ctx.IngestedRef(s.Index, storage.SeriesRef(s.Ref)) {
			ctx.Ingested(s.Index, labels.EmptyLabels(), storage.SeriesRef(s.Ref), -1)
		}
	}
	for _, failure := range res.Errors {
		err := rustAppendError(failure.Code)
		if !ctx.Error(int(failure.Series), err, failure.Timestamp) {
			return outcome, err
		}
	}
	leftovers := res.Leftovers
	for _, i := range res.Skipped {
		if !ctx.Skip(i) {
			leftovers = append(leftovers, i)
		}
	}
	if len(res.Skipped) > 0 {
		slices.Sort(leftovers)
	}
	if len(leftovers) == 0 {
		if len(res.Skipped) > 0 {
			_, err := e.native.Rollback()
			return outcome, err
		}
		return outcome, nil
	}
	batch.Series = batch.Decoded(leftovers)
	other := leftovers[:0]
	for _, i := range leftovers {
		if !ctx.Skip(i) {
			other = append(other, i)
		}
	}
	if len(other) == 0 {
		// Ends the request the engine started for the appender.
		_, err := e.native.Rollback()
		return outcome, err
	}
	return e.ingestThroughAppender(ctx, batch, other, outcome)
}

// ingestThroughAppender appends the series at indices, which are decoded, with the appender Go validates them through.
func (e *rustEngine) ingestThroughAppender(ctx *ingestContext, batch ingestBatch, indices []int, outcome ingestOutcome) (ingestOutcome, error) {
	app := &rustAppender{engine: e, ctx: ctx, otlp: batch.OTLP, ingestedMs: batch.IngestedAt.UnixMilli()}
	rest, err := appendBatch(ctx, app, batch, indices, true)
	outcome.Samples += rest.Samples
	outcome.Exemplars += rest.Exemplars
	if err != nil {
		return outcome, errors.Join(err, app.Rollback())
	}
	started := time.Now()
	if err := app.Commit(); err != nil {
		return outcome, err
	}
	outcome.CommitDuration += time.Since(started)
	return outcome, nil
}

// rustAppender stages samples in the engine's transaction, which Commit publishes: new series, histograms and
// exemplars, which records leave to Go to validate.
type rustAppender struct {
	engine     *rustEngine
	ctx        context.Context
	otlp       bool
	ingestedMs int64
	// The series GetRef found but not in the head, which appending brings back.
	inactive map[storage.SeriesRef]bool
	created  map[storage.SeriesRef]labels.Labels
	err      error
}

func (a *rustAppender) GetRef(ls labels.Labels, _ uint64) (storage.SeriesRef, labels.Labels) {
	ref, inHead, err := a.engine.native.Lookup(toRustLabels(ls))
	if err != nil {
		a.err = err
		return 0, ls
	}
	if ref != 0 && !inHead {
		if a.inactive == nil {
			a.inactive = map[storage.SeriesRef]bool{}
		}
		a.inactive[storage.SeriesRef(ref)] = true
	}
	return storage.SeriesRef(ref), ls
}

func (a *rustAppender) Append(ref storage.SeriesRef, ls labels.Labels, _, t int64, v float64, h *histogram.Histogram, fh *histogram.FloatHistogram, opts storage.AppendV2Options) (storage.SeriesRef, error) {
	if a.err != nil {
		return 0, a.err
	}
	if err := a.ctx.Err(); err != nil {
		return 0, err
	}
	if ref == 0 || a.inactive[ref] {
		if a.engine.lifecycle != nil {
			if err := a.engine.lifecycle.PreCreation(ls); err != nil {
				return 0, err
			}
		}
		created, err := a.engine.native.Create(toRustLabels(ls), 0, a.otlp)
		if err != nil {
			return 0, err
		}
		ref = storage.SeriesRef(created)
		delete(a.inactive, ref)
		if a.created == nil {
			a.created = map[storage.SeriesRef]labels.Labels{}
		}
		a.created[ref] = ls
		if a.engine.lifecycle != nil {
			a.engine.lifecycle.PostCreation(ls)
		}
	}
	if h == nil && fh == nil && value.IsStaleNaN(v) {
		// A staleness marker of a histogram series is a histogram.
		if _, kind, ok, err := a.engine.native.Last(uint64(ref)); err == nil && ok {
			switch kind {
			case rustengine.KindHistogram:
				h = &histogram.Histogram{Sum: v}
			case rustengine.KindFloatHistogram:
				fh = &histogram.FloatHistogram{Sum: v}
			}
		}
	}
	kind := uint8(rustengine.KindFloat)
	var payload []byte
	if h != nil || fh != nil {
		var proto mimirpb.Histogram
		if h != nil {
			if err := h.Validate(); err != nil {
				return 0, err
			}
			proto = mimirpb.FromHistogramToHistogramProto(t, h)
			kind = rustengine.KindHistogram
		} else {
			if err := fh.Validate(); err != nil {
				return 0, err
			}
			proto = mimirpb.FromFloatHistogramToHistogramProto(t, fh)
			kind = rustengine.KindFloatHistogram
		}
		var err error
		if payload, err = proto.Marshal(); err != nil {
			return 0, err
		}
	}
	code, err := a.engine.native.Append(uint64(ref), t, kind, v, payload, opts.RejectOutOfOrder)
	if err != nil {
		return 0, err
	}
	if code != 0 {
		return ref, rustAppendError(code)
	}
	return ref, nil
}

func (a *rustAppender) AppendExemplars(ref storage.SeriesRef, _ labels.Labels, exemplars []exemplar.Exemplar) (storage.SeriesRef, error) {
	partial := &storage.AppendPartialError{}
	for i, ex := range exemplars {
		code, err := a.engine.native.AppendExemplar(uint64(ref), ex.Ts, ex.Value, toRustLabels(ex.Labels))
		if err == nil && code != 0 {
			err = rustAppendError(code)
		}
		if err != nil {
			partial.ExemplarErrors = append(partial.ExemplarErrors, storage.ExemplarError{Index: i, Err: err})
		}
	}
	if len(partial.ExemplarErrors) > 0 {
		return ref, partial
	}
	return ref, nil
}

func (a *rustAppender) Commit() error {
	if err := a.ctx.Err(); err != nil {
		return errors.Join(err, a.Rollback())
	}
	_, err := a.engine.native.Commit(a.ingestedMs)
	return err
}

func (a *rustAppender) Rollback() error {
	removed, err := a.engine.native.Rollback()
	if err != nil {
		return err
	}
	return a.engine.removed(removed, nil)
}

// Reject codes, as native/src/head.rs numbers them.
func rustAppendError(code uint8) error {
	switch code {
	case 1, 10:
		return storage.ErrDuplicateSampleForTimestamp
	case 2:
		return storage.ErrOutOfOrderSample
	case 3:
		return storage.ErrTooOldSample
	case 4:
		return storage.ErrOutOfBounds
	case 5:
		return globalerror.SampleTooFarInPast
	case 6:
		return globalerror.SampleTooFarInFuture
	case 7:
		return storage.ErrNotFound
	case 8:
		return storage.ErrExemplarsDisabled
	case 9:
		return storage.ErrOutOfOrderExemplar
	default:
		return fmt.Errorf("Rust engine rejected the sample with code %d", code)
	}
}

func toRustLabels(ls labels.Labels) []rustengine.Label {
	return mimirpb.FromLabelsToLabelAdapters(ls)
}

// rustLabels copies the labels, whose strings refer to a response.
func rustLabels(ls []rustengine.Label) labels.Labels {
	b := labels.NewScratchBuilder(len(ls))
	for _, l := range ls {
		b.Add(l.Name, l.Value)
	}
	return b.Labels()
}
