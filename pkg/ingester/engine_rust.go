// SPDX-License-Identifier: AGPL-3.0-only
//go:build rustengine && cgo

package ingester

import (
	"context"
	"encoding/base64"
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
	"github.com/grafana/mimir/pkg/storage/rustengine"
	"github.com/grafana/mimir/pkg/util/globalerror"
)

type rustEngine struct {
	mu                   sync.Mutex
	client               *rustengine.Client
	lifecycle            tsdb.SeriesLifecycleCallback
	secondaryHash        func(labels.Labels) uint32
	blockDuration        int64
	timelyCompaction     bool
	retention            int64
	closed               bool
	walBuffered          bool
	floatEncoding        atomic.Uint32
	defaultFloatEncoding chunkenc.Encoding
	selectors            [2]rustSelectorCache
}

var _ tenantEngine = (*rustEngine)(nil)
var _ engineHead = (*rustEngine)(nil)

func openTenantEngine(engine, dir string, logger *slog.Logger, reg prometheus.Registerer, opts *tsdb.Options, shipping bool) (tenantEngine, error) {
	if engine == "" || engine == "prometheus" {
		return openPrometheusEngine(dir, logger, reg, opts)
	}
	if engine != "rust" {
		return nil, fmt.Errorf("unsupported engine %q", engine)
	}
	if shipping {
		return nil, errors.New("experimental Rust engine does not produce Prometheus blocks; disable block shipping")
	}
	return openRustEngine(dir, opts)
}
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
	client, err := rustengine.Open(filepath.Join(dir, "rust-engine"))
	if err != nil {
		return nil, err
	}
	e := &rustEngine{client: client, lifecycle: opts.SeriesLifecycleCallback, secondaryHash: opts.SecondaryHashFunction, blockDuration: opts.MinBlockDuration, timelyCompaction: opts.TimelyCompaction, retention: opts.RetentionDuration, defaultFloatEncoding: opts.FloatChunkEncoding}
	if err = e.Configure(engineSettings{OutOfOrderTimeWindow: opts.OutOfOrderTimeWindow, MaxExemplars: opts.MaxExemplars, FloatChunkEncoding: strings.ToLower(opts.FloatChunkEncoding.String())}); err != nil {
		client.Close()
		return nil, err
	}
	head, err := e.metadata()
	if err != nil {
		client.Close()
		return nil, err
	}
	if e.lifecycle != nil {
		for _, s := range head {
			e.lifecycle.PostCreation(rustLabels(s.Labels))
		}
	}
	return e, nil
}
func (e *rustEngine) Configure(settings engineSettings) error {
	e.mu.Lock()
	defer e.mu.Unlock()
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
	_, err := e.client.Call(rustengine.Request{Op: "configure", Window: settings.OutOfOrderTimeWindow, MaxExemplars: settings.MaxExemplars, UpTo: e.blockDuration})
	if err == nil {
		e.floatEncoding.Store(uint32(encoding))
	}
	return err
}
func (e *rustEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	_, err := e.client.Call(rustengine.Request{Op: "sync"})
	e.client.Close()
	return err
}
func (e *rustEngine) Head() engineHead { return e }
func (e *rustEngine) Sync() error {
	_, err := e.client.Call(rustengine.Request{Op: "sync"})
	return err
}
func (e *rustEngine) metadata() ([]rustengine.Series, error) {
	r, err := e.client.Call(rustengine.Request{Op: "head"})
	return r.Series, err
}
func (e *rustEngine) NumSeries() uint64 {
	r, err := e.client.Call(rustengine.Request{Op: "stats"})
	if err != nil {
		return 0
	}
	return r.Count
}
func (e *rustEngine) TimeBounds() timeBounds {
	r, err := e.client.Call(rustengine.Request{Op: "stats"})
	b := timeBounds{MinTime: math.MaxInt64, MaxTime: math.MinInt64, MinOOOTime: math.MaxInt64, MaxOOOTime: math.MinInt64}
	if err == nil {
		b.MinTime = r.Mint
		b.MaxTime = r.Maxt
	}
	return b
}
func (e *rustEngine) OldestAppendableTime() (int64, bool) {
	r, err := e.client.Call(rustengine.Request{Op: "stats"})
	return r.MinValid, err == nil && r.MinValid != math.MinInt64
}
func (e *rustEngine) Index() (tsdb.IndexReader, error) { return &rustIndex{engine: e}, nil }
func (e *rustEngine) Ownership(owned ring.TokenRanges) (int, []storage.SeriesRef) {
	series, err := e.metadata()
	if err != nil {
		return 0, nil
	}
	var count int
	var refs []storage.SeriesRef
	for _, s := range series {
		var hash uint32
		if e.secondaryHash != nil {
			hash = e.secondaryHash(rustLabels(s.Labels))
		}
		if owned.IncludesKey(hash) {
			count++
		} else {
			refs = append(refs, storage.SeriesRef(s.Ref))
		}
	}
	return count, refs
}
func (e *rustEngine) ShardSeries(shard, count uint64) index.Postings {
	if count == 0 || shard >= count {
		return index.ErrPostings(errors.New("invalid shard"))
	}
	series, err := e.metadata()
	if err != nil {
		return index.ErrPostings(err)
	}
	var refs []storage.SeriesRef
	for _, s := range series {
		if labels.StableHash(rustLabels(s.Labels))%count == shard {
			refs = append(refs, storage.SeriesRef(s.Ref))
		}
	}
	return index.NewListPostings(refs)
}
func (e *rustEngine) LabelValuesBytes() map[string]uint64 {
	r, err := e.client.Call(rustengine.Request{Op: "bytes"})
	if err != nil {
		return nil
	}
	return r.Bytes
}
func (e *rustEngine) Compact(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r, err := e.client.Call(rustengine.Request{Op: "stats"})
	if err != nil {
		return err
	}
	if e.blockDuration > 0 && r.Mint != math.MaxInt64 {
		start := r.Mint
		appendable := int64(math.MinInt64)
		if r.Maxt >= math.MinInt64+e.blockDuration/2 {
			appendable = r.Maxt - e.blockDuration/2
		}
		appendable = max(r.MinValid, appendable)
		for {
			end := (start/e.blockDuration)*e.blockDuration + e.blockDuration
			if end <= start {
				break
			}
			if e.timelyCompaction {
				if end >= appendable {
					break
				}
			} else if uint64(r.Maxt)-uint64(start) <= uint64(e.blockDuration)+uint64(e.blockDuration/2) {
				break
			}
			start = end
		}
		if start > r.Mint {
			// Coalesce eligible block ranges while preserving the Go engine's head horizon.
			if err := e.Flush(ctx, start-1); err != nil {
				return err
			}
		}
	}
	if e.retention > 0 {
		e.mu.Lock()
		_, err := e.client.Call(rustengine.Request{Op: "retain", UpTo: time.Now().UnixMilli() - e.retention})
		e.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return e.Sync()
}
func (e *rustEngine) Flush(ctx context.Context, upTo int64) error {
	return e.move(ctx, rustengine.Request{Op: "flush", UpTo: upTo})
}
func (e *rustEngine) Evict(ctx context.Context, refs []storage.SeriesRef) error {
	ids := make([]uint64, len(refs))
	for i, r := range refs {
		ids[i] = uint64(r)
	}
	return e.move(ctx, rustengine.Request{Op: "evict", Refs: ids})
}
func (e *rustEngine) move(ctx context.Context, req rustengine.Request) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	before, err := e.metadata()
	if err != nil {
		return err
	}
	_, moveErr := e.client.Call(req)
	after, err := e.metadata()
	if err != nil {
		return errors.Join(moveErr, err)
	}
	remaining := map[uint64]bool{}
	for _, s := range after {
		remaining[s.Ref] = true
	}
	deleted := map[chunks.HeadSeriesRef]labels.Labels{}
	for _, s := range before {
		if !remaining[s.Ref] {
			deleted[chunks.HeadSeriesRef(s.Ref)] = rustLabels(s.Labels)
		}
	}
	if e.lifecycle != nil && len(deleted) > 0 {
		e.lifecycle.PostDeletion(deleted)
	}
	return moveErr
}
func (e *rustEngine) Ingest(ctx *ingestContext, batch ingestBatch) (ingestOutcome, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ingestOutcome{}, err
	}
	if !e.walBuffered && ctx.i != nil && ctx.i.cfg.IngestStorageConfig.Enabled {
		if _, err := e.client.Call(rustengine.Request{Op: "enable_buffered_wal"}); err != nil {
			return ingestOutcome{}, err
		}
		e.walBuffered = true
	}
	app := &rustAppender{engine: e, ctx: ctx, created: map[storage.SeriesRef]labels.Labels{}, inactive: map[storage.SeriesRef]bool{}}
	// Bound transport copies while retaining one request-wide transaction.
	const maxFrameSeries, maxFrameSamples, maxFrameBytes = 256, 8192, 1 << 20
	rows := make([]rustengine.FloatSeries, 0, min(maxFrameSeries, len(batch.Series)))
	var fallback []int
	var outcome ingestOutcome
	frameSamples, frameBytes := 0, 0
	begun, committed := false, false
	flush := func(final bool) error {
		if len(rows) == 0 {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		op := "append_floats"
		if !begun {
			op = "begin_floats"
		}
		if final && len(fallback) == 0 {
			if begun {
				op = "finish_floats"
			} else {
				op = "ingest_floats"
			}
		}
		started := time.Now()
		response, err := e.client.Call(rustengine.Request{Op: op, Batch: rows, Mint: batch.MinTimestampMs, Maxt: batch.MaxTimestampMs})
		if err != nil {
			return err
		}
		begun, committed = true, response.Done
		if committed {
			outcome.CommitDuration = time.Since(started)
		}
		fallback = append(fallback, response.Leftovers...)
		for _, result := range response.Results {
			for _, failure := range result.Errors {
				err := rustAppendError(errors.New(failure.Error))
				if !ctx.Error(result.Index, err, failure.Timestamp) {
					return err
				}
			}
			outcome.Samples += result.Accepted
			if result.Accepted > 0 && !ctx.IngestedRef(result.Index, storage.SeriesRef(result.Ref)) {
				ctx.Ingested(result.Index, labels.EmptyLabels(), storage.SeriesRef(result.Ref), -1)
			}
		}
		clear(rows)
		rows = rows[:0]
		frameSamples, frameBytes = 0, 0
		return nil
	}
	for i, series := range batch.Series {
		if ctx.Skip(i) {
			continue
		}
		if len(series.Samples) == 0 || len(series.Samples) > maxFrameSamples || len(series.Histograms) > 0 || len(series.Exemplars) > 0 || series.CreatedTimestamp > 0 {
			fallback = append(fallback, i)
			continue
		}
		rowBytes := 64 + len(series.Samples)*32
		for _, l := range series.Labels {
			rowBytes += 8 + len(l.Name) + len(l.Value)
		}
		if rowBytes > maxFrameBytes-512 {
			fallback = append(fallback, i)
			continue
		}
		if len(rows) == maxFrameSeries || frameSamples+len(series.Samples) > maxFrameSamples || frameBytes+rowBytes > maxFrameBytes-512 {
			if err := flush(false); err != nil {
				return outcome, errors.Join(err, app.Rollback())
			}
		}
		row := rustengine.FloatSeries{Index: i, Labels: series.Labels, Samples: series.Samples}
		rows = append(rows, row)
		frameSamples += len(row.Samples)
		frameBytes += rowBytes
	}
	if err := flush(true); err != nil {
		return outcome, errors.Join(err, app.Rollback())
	}
	if committed {
		return outcome, nil
	}
	if !begun {
		if len(fallback) == 0 {
			return outcome, nil
		}
		if _, err := e.client.Call(rustengine.Request{Op: "begin"}); err != nil {
			return outcome, err
		}
	}
	slices.Sort(fallback)
	rest, err := appendBatch(ctx, app, batch, fallback, true)
	outcome.Samples += rest.Samples
	outcome.Exemplars += rest.Exemplars
	if err != nil {
		return outcome, errors.Join(err, app.Rollback())
	}
	started := time.Now()
	if err := app.Commit(); err != nil {
		return outcome, err
	}
	outcome.CommitDuration = time.Since(started)
	return outcome, nil
}

type rustAppender struct {
	engine   *rustEngine
	ctx      context.Context
	created  map[storage.SeriesRef]labels.Labels
	inactive map[storage.SeriesRef]bool
	err      error
}

func (a *rustAppender) GetRef(ls labels.Labels, _ uint64) (storage.SeriesRef, labels.Labels) {
	r, err := a.engine.client.Call(rustengine.Request{Op: "lookup", Labels: toRustLabels(ls)})
	if err != nil {
		a.err = err
		return 0, ls
	}
	ref := storage.SeriesRef(r.Ref)
	a.inactive[ref] = !r.Active
	return ref, ls
}
func (a *rustAppender) Append(ref storage.SeriesRef, ls labels.Labels, _, t int64, v float64, h *histogram.Histogram, fh *histogram.FloatHistogram, opts storage.AppendV2Options) (storage.SeriesRef, error) {
	if a.err != nil {
		return 0, a.err
	}
	if err := a.ctx.Err(); err != nil {
		return 0, err
	}
	newSeries := ref == 0 || a.inactive[ref]
	if _, ok := a.created[ref]; ok {
		newSeries = false
	}
	if newSeries && a.engine.lifecycle != nil {
		if err := a.engine.lifecycle.PreCreation(ls); err != nil {
			return 0, err
		}
	}
	if h == nil && fh == nil && value.IsStaleNaN(v) && ref != 0 {
		last, err := a.engine.client.Call(rustengine.Request{Op: "last", Ref: uint64(ref)})
		if err != nil {
			return 0, err
		}
		if last.Sample != nil {
			switch last.Sample.Kind {
			case 2:
				h = &histogram.Histogram{Sum: v}
			case 3:
				fh = &histogram.FloatHistogram{Sum: v}
			}
		}
	}
	sample := rustengine.Sample{Timestamp: t, Kind: uint8(chunkFloat), Bits: math.Float64bits(v)}
	if h != nil || fh != nil {
		var proto mimirpb.Histogram
		if h != nil {
			if err := h.Validate(); err != nil {
				return 0, err
			}
			proto = mimirpb.FromHistogramToHistogramProto(t, h)
			sample.Kind = 2
		} else {
			if err := fh.Validate(); err != nil {
				return 0, err
			}
			proto = mimirpb.FromFloatHistogramToHistogramProto(t, fh)
			sample.Kind = 3
		}
		bytes, err := proto.Marshal()
		if err != nil {
			return 0, err
		}
		sample.Data = base64.StdEncoding.EncodeToString(bytes)
	}
	var wireLabels []rustengine.Label
	if ref == 0 {
		wireLabels = toRustLabels(ls)
	}
	r, err := a.engine.client.Call(rustengine.Request{Op: "append", Ref: uint64(ref), Labels: wireLabels, Sample: &sample, RejectOOO: opts.RejectOutOfOrder})
	if err != nil {
		return 0, rustAppendError(err)
	}
	ref = storage.SeriesRef(r.Ref)
	if newSeries {
		a.created[ref] = ls
		a.inactive[ref] = false
		if a.engine.lifecycle != nil {
			a.engine.lifecycle.PostCreation(ls)
		}
	}
	return ref, nil
}

const chunkFloat = 1

func (a *rustAppender) AppendExemplars(ref storage.SeriesRef, _ labels.Labels, exemplars []exemplar.Exemplar) (storage.SeriesRef, error) {
	partial := &storage.AppendPartialError{}
	for i, ex := range exemplars {
		_, err := a.engine.client.Call(rustengine.Request{Op: "exemplar", Ref: uint64(ref), Exemplar: &rustengine.Exemplar{Timestamp: ex.Ts, Bits: math.Float64bits(ex.Value), Labels: toRustLabels(ex.Labels)}})
		if err != nil {
			partial.ExemplarErrors = append(partial.ExemplarErrors, storage.ExemplarError{Index: i, Err: rustAppendError(err)})
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
	_, err := a.engine.client.Call(rustengine.Request{Op: "commit"})
	if err != nil {
		a.undoCreations()
	}
	return err
}
func (a *rustAppender) Rollback() error {
	_, err := a.engine.client.Call(rustengine.Request{Op: "rollback"})
	a.undoCreations()
	return err
}
func (a *rustAppender) undoCreations() {
	if a.engine.lifecycle == nil {
		return
	}
	deleted := map[chunks.HeadSeriesRef]labels.Labels{}
	for r, ls := range a.created {
		deleted[chunks.HeadSeriesRef(r)] = ls
	}
	if len(deleted) > 0 {
		a.engine.lifecycle.PostDeletion(deleted)
	}
}
func rustAppendError(err error) error {
	switch err.Error() {
	case "duplicate":
		return storage.ErrDuplicateSampleForTimestamp
	case "out_of_order":
		return storage.ErrOutOfOrderSample
	case "too_old":
		return storage.ErrTooOldSample
	case "too_far_past":
		return globalerror.SampleTooFarInPast
	case "too_far_future":
		return globalerror.SampleTooFarInFuture
	case "out_of_bounds":
		return storage.ErrOutOfBounds
	case "not_found":
		return storage.ErrNotFound
	case "exemplars_disabled":
		return storage.ErrExemplarsDisabled
	case "exemplar_out_of_order":
		return storage.ErrOutOfOrderExemplar
	default:
		return fmt.Errorf("Rust engine: %w", err)
	}
}
func toRustLabels(ls labels.Labels) []rustengine.Label {
	out := make([]rustengine.Label, 0, ls.Len())
	ls.Range(func(l labels.Label) { out = append(out, rustengine.Label{Name: l.Name, Value: l.Value}) })
	return out
}
func rustLabels(ls []rustengine.Label) labels.Labels {
	b := labels.NewScratchBuilder(len(ls))
	for _, l := range ls {
		b.Add(l.Name, l.Value)
	}
	return b.Labels()
}
