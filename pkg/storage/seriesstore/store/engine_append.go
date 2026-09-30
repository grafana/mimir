// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/exemplars"
	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// EngineAppender is the appender Mimir's ingester pushes through: a Prometheus storage.Appender
// with GetRef.
type EngineAppender interface {
	storage.Appender
	storage.GetRef
}

type sampleKind uint8

const (
	sampleFloat sampleKind = iota
	sampleHistogram
	sampleFloatHistogram
)

// pendingSample is a sample waiting for Commit, like a Prometheus appender batch entry.
type pendingSample struct {
	ref uint64
	// Where the series was at Append, which Commit checks before looking it up again.
	location refLocation
	t        int64
	v        float64
	kind     sampleKind
	h        *histogram.Histogram
	fh       *histogram.FloatHistogram
}

type pendingExemplar struct {
	ref      uint64
	exemplar exemplar.Exemplar
}

// engineAppender follows Prometheus's head appender: samples are checked against their series as
// committed when appended, and applied at Commit, where those that no longer fit are dropped
// without an error.
type engineAppender struct {
	e *Engine
	// Taken when the appender starts, or at its first sample for a head without samples, like
	// Prometheus's initAppender.
	started      bool
	headMaxt     int64
	minValidTime int64
	oooWindow    int64
	options      *storage.AppendOptions
	samples      []pendingSample
	exemplars    []pendingExemplar
	// What each series last got from this appender, to type its staleness markers; built at the
	// first staleness marker, since most appends have none.
	types map[uint64]sampleKind
	pairs [][2]string
	// Commit's order of the samples, by shard.
	order []int32
}

// appendBuffers are an appender's buffers, which appenders reuse: the ingester opens one per
// request.
type appendBuffers struct {
	samples   []pendingSample
	exemplars []pendingExemplar
	order     []int32
}

var appendBufferPool = sync.Pool{New: func() any { return &appendBuffers{} }}

// Appender returns an appender for the head, like the TSDB's.
func (e *Engine) Appender(context.Context) EngineAppender {
	buffers := appendBufferPool.Get().(*appendBuffers)
	a := &engineAppender{e: e, samples: buffers.samples[:0], exemplars: buffers.exemplars[:0], order: buffers.order[:0]}
	if minTime, _, _ := e.headTimes(); minTime != math.MaxInt64 {
		a.start()
	}
	return a
}

func (a *engineAppender) start() {
	_, maxTime, minValid := a.e.headTimes()
	a.started = true
	a.headMaxt = maxTime
	a.minValidTime = appendableMinValidTime(maxTime, minValid)
	a.oooWindow = a.e.oooWindow.Load()
}

func (a *engineAppender) startAt(t int64) {
	if !a.started {
		a.e.initTime(t)
		a.start()
	}
}

func (a *engineAppender) SetOptions(opts *storage.AppendOptions) {
	a.options = opts
}

// GetRef returns the series' reference, and labels Append can take without copying them.
func (a *engineAppender) GetRef(lset promlabels.Labels, hash uint64) (storage.SeriesRef, promlabels.Labels) {
	ref, ok := a.e.refOf(lset, hash, &a.pairs)
	if !ok {
		return 0, promlabels.EmptyLabels()
	}
	// The engine reads a known series' labels from its own copy; these are only used if the
	// series is gone by the time the sample is appended, to create it again.
	return storage.SeriesRef(ref), lset
}

// refOf finds the series with lset.
func (e *Engine) refOf(lset promlabels.Labels, hash uint64, scratch *[][2]string) (uint64, bool) {
	pairs := pairsOf(lset, scratch)
	shard := e.store.shards[shardFor(hash, len(e.store.shards))]
	shard.RLock()
	defer shard.RUnlock()
	t, ok := shard.tenants[e.tenantID]
	if !ok {
		return 0, false
	}
	groupID, ok := t.series.names[lset.Get(metricNameLabel)]
	if !ok {
		return 0, false
	}
	var ref uint64
	t.series.groups[groupID].lookup(hash, func(entry *seriesEntry) bool {
		if entry.hash == hash && entry.labels.EqPairs(pairs) {
			ref = entry.series.ref
			return true
		}
		return false
	})
	return ref, ref != 0
}

func pairsOf(lset promlabels.Labels, scratch *[][2]string) [][2]string {
	pairs := (*scratch)[:0]
	lset.Range(func(l promlabels.Label) { pairs = append(pairs, [2]string{l.Name, l.Value}) })
	*scratch = pairs
	return pairs
}

// getOrCreate finds the series ref, or the one with lset, creating it after the lifecycle
// callback allowed it, like the head's getOrCreate.
func (a *engineAppender) getOrCreate(ref storage.SeriesRef, lset promlabels.Labels) (uint64, error) {
	if ref != 0 && a.e.exists(uint64(ref)) {
		return uint64(ref), nil
	}
	lset = lset.WithoutEmpty()
	if lset.IsEmpty() {
		return 0, fmt.Errorf("empty labelset: %w", tsdb.ErrInvalidSample)
	}
	if l, dup := lset.HasDuplicateLabelNames(); dup {
		return 0, fmt.Errorf(`label name "%s" is not unique: %w`, l, tsdb.ErrInvalidSample)
	}
	hash := lset.Hash()
	if found, ok := a.e.refOf(lset, hash, &a.pairs); ok {
		return found, nil
	}
	if err := a.e.callback.PreCreation(lset); err != nil {
		return 0, err
	}
	created, isNew := a.e.create(lset, hash, &a.pairs)
	if isNew {
		// The callback may keep the labels: give it the engine's own copy.
		a.e.callback.PostCreation(lset.Copy())
	}
	return created, nil
}

// exists reports whether the series ref is in the head.
func (e *Engine) exists(ref uint64) bool {
	shardIndex := refShard(ref)
	if shardIndex >= len(e.store.shards) {
		return false
	}
	shard := e.store.shards[shardIndex]
	shard.RLock()
	defer shard.RUnlock()
	t, ok := shard.tenants[e.tenantID]
	if !ok {
		return false
	}
	_, ok = t.byRef[ref]
	return ok
}

// create adds the series with lset, unless another appender just did.
func (e *Engine) create(lset promlabels.Labels, hash uint64, scratch *[][2]string) (uint64, bool) {
	pairs := pairsOf(lset, scratch)
	index := shardFor(hash, len(e.store.shards))
	shard := e.store.shards[index]
	shard.Lock()
	defer shard.Unlock()
	t := tenantOf(shard.tenants, e.tenantID)
	if t.byRef == nil {
		t.byRef = map[uint64]refLocation{}
	}
	entry, inserted := t.series.getOrInsert(lset.Get(metricNameLabel), hash, pairs, func() labels.Labels { return labels.FromSorted(pairs) })
	if !inserted {
		return entry.series.ref, false
	}
	ref := e.newRef(index)
	entry.series.ref = ref
	entry.series.inHead = true
	entry.series.shardHash = promlabels.StableHash(lset)
	if e.opts.SecondaryHashFunction != nil {
		entry.series.ownedHash = e.opts.SecondaryHashFunction(lset)
	}
	groupID := t.series.names[lset.Get(metricNameLabel)]
	t.byRef[ref] = refLocation{groupID, int32(len(t.series.groups[groupID].entries) - 1), hash}
	return ref, true
}

// withSeries calls read with the series ref, its shard read-locked.
func (e *Engine) withSeries(ref uint64, read func(series *Series)) bool {
	_, ok := e.withSeriesAt(ref, read)
	return ok
}

// withSeriesAt is withSeries, also returning where the series is.
func (e *Engine) withSeriesAt(ref uint64, read func(series *Series)) (refLocation, bool) {
	index := refShard(ref)
	if index >= len(e.store.shards) {
		return refLocation{}, false
	}
	shard := e.store.shards[index]
	shard.RLock()
	defer shard.RUnlock()
	t, ok := e.tenantLocked(index)
	if !ok {
		return refLocation{}, false
	}
	location, ok := t.byRef[ref]
	if !ok {
		return refLocation{}, false
	}
	entry, ok := e.locate(t, ref, location)
	if ok {
		read(&entry.series)
	}
	return location, ok
}

// lastInOrder is a series' newest in-order sample, like memSeries' last values: its time, and
// its value as a float or a histogram.
type lastInOrder struct {
	t      int64
	ok     bool
	float  float64
	isHist bool
	h      *mimirpb.Histogram
}

func lastOf(series *Series) lastInOrder {
	var last lastInOrder
	if fh := series.floatHead; fh != nil {
		last.t, last.ok = fh.lastTimestamp(), true
		last.float, _ = fh.appender.LastValue()
	}
	if hh := series.histogramHead; hh != nil {
		if h := hh.Last(); !last.ok || h.Timestamp > last.t {
			copied := *h
			last = lastInOrder{t: h.Timestamp, ok: true, isHist: true, h: &copied}
		}
	}
	return last
}

// appendableFloat is Prometheus's memSeries.appendable.
func appendableFloat(last lastInOrder, t int64, v float64, headMaxt, minValidTime, oooWindow int64) (bool, error) {
	if t >= minValidTime {
		if !last.ok {
			return false, nil
		}
		if t > last.t {
			return false, nil
		}
		if t == last.t {
			if last.isHist {
				return false, storage.NewDuplicateHistogramToFloatErr(t, v)
			}
			if math.Float64bits(last.float) != math.Float64bits(v) && math.Float64bits(v) != value.QuietZeroNaN {
				return false, storage.NewDuplicateFloatErr(t, last.float, v)
			}
			return false, nil
		}
	}
	if math.Float64bits(v) == value.QuietZeroNaN {
		return true, nil
	}
	return outOfOrderOrError(t, headMaxt, minValidTime, oooWindow)
}

// appendableHistogram is Prometheus's memSeries.appendableHistogram and
// appendableFloatHistogram.
func appendableHistogram(last lastInOrder, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram, headMaxt, minValidTime, oooWindow int64) (bool, error) {
	if t >= minValidTime {
		if !last.ok {
			return false, nil
		}
		if t > last.t {
			return false, nil
		}
		if t == last.t {
			if !equalLastHistogram(last, h, fh) {
				return false, storage.ErrDuplicateSampleForTimestamp
			}
			return false, nil
		}
	}
	return outOfOrderOrError(t, headMaxt, minValidTime, oooWindow)
}

func equalLastHistogram(last lastInOrder, h *histogram.Histogram, fh *histogram.FloatHistogram) bool {
	if !last.isHist {
		return false
	}
	if last.h.IsFloatHistogram() {
		return fh != nil && fh.Equals(mimirpb.FromHistogramProtoToFloatHistogram(last.h))
	}
	return h != nil && h.Equals(mimirpb.FromHistogramProtoToHistogram(last.h))
}

func outOfOrderOrError(t, headMaxt, minValidTime, oooWindow int64) (bool, error) {
	if oooWindow > 0 && t >= headMaxt-oooWindow {
		return true, nil
	}
	if oooWindow > 0 {
		return true, storage.ErrTooOldSample
	}
	if t < minValidTime {
		return false, storage.ErrOutOfBounds
	}
	return false, storage.ErrOutOfOrderSample
}

func (a *engineAppender) Append(ref storage.SeriesRef, lset promlabels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	a.startAt(t)
	// Fail fast without an out-of-order window, before looking the series up.
	if a.oooWindow == 0 && t < a.minValidTime {
		return 0, storage.ErrOutOfBounds
	}
	var isOOO bool
	var err error
	check := func(series *Series) {
		isOOO, err = appendableFloat(lastOf(series), t, v, a.headMaxt, a.minValidTime, a.oooWindow)
	}
	id := uint64(ref)
	stale := value.IsStaleNaN(v)
	var location refLocation
	found := false
	if ref != 0 && !stale {
		// The ingester's appends of known series: one lookup.
		location, found = a.e.withSeriesAt(id, check)
	}
	if !found {
		var createErr error
		if id, createErr = a.getOrCreate(ref, lset); createErr != nil {
			return 0, createErr
		}
		if stale {
			switch a.pendingKind(id) {
			case sampleHistogram:
				return a.AppendHistogram(storage.SeriesRef(id), lset, t, &histogram.Histogram{Sum: v}, nil)
			case sampleFloatHistogram:
				return a.AppendHistogram(storage.SeriesRef(id), lset, t, nil, &histogram.FloatHistogram{Sum: v})
			}
		}
		if location, found = a.e.withSeriesAt(id, check); !found {
			return 0, fmt.Errorf("series %d disappeared while appending", id)
		}
	}
	if err == nil && isOOO && a.options != nil && a.options.DiscardOutOfOrder {
		return 0, storage.ErrOutOfOrderSample
	}
	if err != nil {
		return 0, err
	}
	a.push(pendingSample{ref: id, location: location, t: t, v: v, kind: sampleFloat})
	return storage.SeriesRef(id), nil
}

func (a *engineAppender) push(sample pendingSample) {
	a.samples = append(a.samples, sample)
	if a.types != nil {
		a.types[sample.ref] = sample.kind
	}
}

// pendingKind is the kind of the series' last sample in this appender, sampleFloat when none.
func (a *engineAppender) pendingKind(ref uint64) sampleKind {
	if a.types == nil {
		a.types = map[uint64]sampleKind{}
		for index := range a.samples {
			a.types[a.samples[index].ref] = a.samples[index].kind
		}
	}
	return a.types[ref]
}

// release gives the appender's buffers back for the next appender.
func (a *engineAppender) release() {
	const maxKept = 1 << 16
	if cap(a.samples) > maxKept {
		return
	}
	clear(a.samples[:cap(a.samples)])
	clear(a.exemplars[:cap(a.exemplars)])
	appendBufferPool.Put(&appendBuffers{samples: a.samples[:0], exemplars: a.exemplars[:0], order: a.order[:0]})
	a.samples, a.exemplars, a.order, a.types = nil, nil, nil, nil
}

func (a *engineAppender) AppendSTZeroSample(ref storage.SeriesRef, lset promlabels.Labels, t, st int64) (storage.SeriesRef, error) {
	if st >= t {
		return 0, storage.ErrSTNewerThanSample
	}
	// Like Prometheus, the head starts at the sample, not its start time.
	a.startAt(t)
	id, err := a.getOrCreate(ref, lset)
	if err != nil {
		return 0, err
	}
	var isOOO bool
	a.e.withSeries(id, func(series *Series) {
		isOOO, err = appendableFloat(lastOf(series), st, 0, a.headMaxt, a.minValidTime, a.oooWindow)
	})
	if err != nil {
		return 0, err
	}
	if isOOO {
		return storage.SeriesRef(id), storage.ErrOutOfOrderST
	}
	a.push(pendingSample{ref: id, t: st, v: 0, kind: sampleFloat})
	return storage.SeriesRef(id), nil
}

func (a *engineAppender) AppendHistogram(ref storage.SeriesRef, lset promlabels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	a.startAt(t)
	if a.oooWindow == 0 && t < a.minValidTime {
		return 0, storage.ErrOutOfBounds
	}
	if h != nil {
		if err := h.Validate(); err != nil {
			return 0, err
		}
	}
	if fh != nil {
		if err := fh.Validate(); err != nil {
			return 0, err
		}
	}
	id, err := a.getOrCreate(ref, lset)
	if err != nil {
		return 0, err
	}
	if h == nil && fh == nil {
		return storage.SeriesRef(id), nil
	}
	a.e.withSeries(id, func(series *Series) {
		_, err = appendableHistogram(lastOf(series), t, h, fh, a.headMaxt, a.minValidTime, a.oooWindow)
	})
	if err != nil {
		return 0, err
	}
	if h != nil {
		a.push(pendingSample{ref: id, t: t, kind: sampleHistogram, h: h})
	} else {
		a.push(pendingSample{ref: id, t: t, kind: sampleFloatHistogram, fh: fh})
	}
	return storage.SeriesRef(id), nil
}

func (a *engineAppender) AppendHistogramSTZeroSample(ref storage.SeriesRef, lset promlabels.Labels, t, st int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	if st >= t {
		return 0, storage.ErrSTNewerThanSample
	}
	// Like Prometheus, the head starts at the sample, not its start time.
	a.startAt(t)
	id, err := a.getOrCreate(ref, lset)
	if err != nil {
		return 0, err
	}
	var zero pendingSample
	switch {
	case h != nil:
		zero = pendingSample{ref: id, t: st, kind: sampleHistogram, h: &histogram.Histogram{
			// The zero sample represents a counter reset by definition.
			CounterResetHint: histogram.CounterReset,
			Schema:           h.Schema,
			ZeroThreshold:    h.ZeroThreshold,
			CustomValues:     h.CustomValues,
		}}
	case fh != nil:
		zero = pendingSample{ref: id, t: st, kind: sampleFloatHistogram, fh: &histogram.FloatHistogram{
			CounterResetHint: histogram.CounterReset,
			Schema:           fh.Schema,
			ZeroThreshold:    fh.ZeroThreshold,
			CustomValues:     fh.CustomValues,
		}}
	default:
		return storage.SeriesRef(id), nil
	}
	var isOOO bool
	a.e.withSeries(id, func(series *Series) {
		isOOO, err = appendableHistogram(lastOf(series), st, zero.h, zero.fh, a.headMaxt, a.minValidTime, a.oooWindow)
	})
	if err != nil {
		if errors.Is(err, storage.ErrOutOfOrderSample) {
			return 0, storage.ErrOutOfOrderST
		}
		return 0, err
	}
	// The zero sample is never out of order: the next samples would share its start time.
	if isOOO {
		return 0, storage.ErrOutOfOrderST
	}
	a.push(zero)
	return storage.SeriesRef(id), nil
}

func (a *engineAppender) AppendExemplar(ref storage.SeriesRef, lset promlabels.Labels, e exemplar.Exemplar) (storage.SeriesRef, error) {
	capacity := a.e.maxExemplars.Load()
	if capacity <= 0 {
		return 0, nil
	}
	a.startAt(e.Ts)
	id := uint64(ref)
	if id == 0 || !a.e.exists(id) {
		found, ok := a.e.refOf(lset, lset.Hash(), &a.pairs)
		if !ok {
			return 0, fmt.Errorf("unknown HeadSeriesRef when trying to add exemplar: %d", ref)
		}
		id = found
	}
	e.Labels = e.Labels.WithoutEmpty()
	stored := toStoreExemplar(e)
	hash, ok := a.e.hashOf(id)
	if !ok {
		return 0, fmt.Errorf("unknown HeadSeriesRef when trying to add exemplar: %d", ref)
	}
	// Checked against the series' newest stored exemplar, like Prometheus's ValidateExemplar.
	a.e.store.exemplarsLock.Lock()
	tenantExemplars := a.e.tenantExemplars()
	rejection := exemplars.Validate(tenantExemplars.Capacity(), tenantExemplars.Newest(hash), &stored, a.e.oooWindow.Load())
	a.e.store.exemplarsLock.Unlock()
	switch rejection {
	case exemplars.LabelLength:
		return 0, storage.ErrExemplarLabelLength
	case exemplars.OutOfOrder:
		return 0, storage.ErrOutOfOrderExemplar
	case exemplars.Disabled:
		return 0, nil
	}
	a.exemplars = append(a.exemplars, pendingExemplar{ref: id, exemplar: e})
	return storage.SeriesRef(id), nil
}

// hashOf is the label hash of the series ref, which keys its exemplars.
func (e *Engine) hashOf(ref uint64) (uint64, bool) {
	shard := e.store.shards[refShard(ref)]
	shard.RLock()
	defer shard.RUnlock()
	t, ok := shard.tenants[e.tenantID]
	if !ok {
		return 0, false
	}
	location, ok := t.byRef[ref]
	return location.hash, ok
}

func toStoreExemplar(e exemplar.Exemplar) exemplars.Exemplar {
	out := exemplars.Exemplar{Value: e.Value, TimestampMs: e.Ts}
	e.Labels.Range(func(l promlabels.Label) {
		out.Labels = append(out.Labels, exemplars.Label{Name: l.Name, Value: l.Value})
	})
	return out
}

// UpdateMetadata keeps nothing: the ingester keeps metadata itself.
func (a *engineAppender) UpdateMetadata(ref storage.SeriesRef, _ promlabels.Labels, _ metadata.Metadata) (storage.SeriesRef, error) {
	return ref, nil
}

func (a *engineAppender) Rollback() error {
	a.release()
	return nil
}

// Commit applies the appender's samples: those that still fit their series, as checked again with
// the appender's bounds, go in order or to the out-of-order chunk, the others are dropped without
// an error, like Prometheus's commit.
func (a *engineAppender) Commit() error {
	if len(a.samples) == 0 && len(a.exemplars) == 0 {
		a.release()
		return nil
	}
	shards := len(a.e.store.shards)
	inOrderMin, inOrderMax := int64(math.MaxInt64), int64(math.MinInt64)
	oooMin, oooMax := int64(math.MaxInt64), int64(math.MinInt64)
	var errs []error
	// Samples of a shard apply in their appended order, under one lock: a counting sort by shard.
	starts := make([]int, shards+1)
	for index := range a.samples {
		starts[refShard(a.samples[index].ref)+1]++
	}
	for shard := range shards {
		starts[shard+1] += starts[shard]
	}
	a.order = slices.Grow(a.order[:0], len(a.samples))[:len(a.samples)]
	next := slices.Clone(starts[:shards])
	for index := range a.samples {
		shard := refShard(a.samples[index].ref)
		a.order[next[shard]] = int32(index)
		next[shard]++
	}
	for index := range shards {
		positions := a.order[starts[index]:starts[index+1]]
		if len(positions) == 0 {
			continue
		}
		state := a.e.store.shards[index]
		state.Lock()
		t, ok := a.e.tenantLocked(index)
		if !ok {
			state.Unlock()
			continue
		}
		for _, position := range positions {
			sample := &a.samples[position]
			entry, ok := a.e.locate(t, sample.ref, sample.location)
			if !ok {
				entry, ok = a.e.lookupLocked(t, sample.ref)
			}
			if !ok {
				continue
			}
			inOrder, ooo, err := a.commitSample(&entry.series, state, sample)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if inOrder {
				inOrderMin, inOrderMax = min(inOrderMin, sample.t), max(inOrderMax, sample.t)
			}
			if ooo {
				oooMin, oooMax = min(oooMin, sample.t), max(oooMax, sample.t)
			}
		}
		state.Unlock()
	}
	if inOrderMax != math.MinInt64 {
		a.e.commitTimes(inOrderMin, inOrderMax)
	}
	if oooMax != math.MinInt64 {
		a.e.observeOOO(oooMin, oooMax)
	}
	a.commitExemplars()
	a.release()
	return errors.Join(errs...)
}

// commitSample applies one sample, reporting whether it went in order or out of order. A storage
// failure is the only error.
func (a *engineAppender) commitSample(series *Series, state *shardState, sample *pendingSample) (bool, bool, error) {
	last := lastOf(series)
	switch sample.kind {
	case sampleFloat:
		// A staleness marker follows the type of the series' last sample.
		if value.IsStaleNaN(sample.v) && last.isHist {
			if last.h.IsFloatHistogram() {
				sample.kind, sample.fh = sampleFloatHistogram, &histogram.FloatHistogram{Sum: sample.v}
			} else {
				sample.kind, sample.h = sampleHistogram, &histogram.Histogram{Sum: sample.v}
			}
			return a.commitSample(series, state, sample)
		}
		isOOO, err := appendableFloat(last, sample.t, sample.v, a.headMaxt, a.minValidTime, a.oooWindow)
		switch {
		case err != nil:
			return false, false, nil
		case isOOO && math.Float64bits(sample.v) == value.QuietZeroNaN:
			return false, false, nil
		case isOOO:
			result, err := insertOutOfOrder(series, state.disk, oooSample{T: sample.t, F: sample.v})
			return false, err == nil && !result.noop, err
		}
		v := sample.v
		if math.Float64bits(v) == value.QuietZeroNaN {
			v = 0
		}
		result, err := appendFloatInOrder(series, state.disk, sample.t, v, true)
		return err == nil && !result.noop, false, err
	default:
		isOOO, err := appendableHistogram(last, sample.t, sample.h, sample.fh, a.headMaxt, a.minValidTime, a.oooWindow)
		if err != nil {
			return false, false, nil
		}
		var proto mimirpb.Histogram
		if sample.h != nil {
			proto = mimirpb.FromHistogramToHistogramProto(sample.t, sample.h)
		} else {
			proto = mimirpb.FromFloatHistogramToHistogramProto(sample.t, sample.fh)
		}
		if isOOO {
			result, err := insertOutOfOrder(series, state.disk, oooSample{T: sample.t, H: &proto})
			return false, err == nil && !result.noop, err
		}
		// An exact duplicate of the last histogram is accepted without storing it again.
		if last.ok && sample.t == last.t {
			return false, false, nil
		}
		result, err := appendHistogramInOrder(series, state.disk, &proto, true)
		return err == nil && !result.noop, false, err
	}
}

func (a *engineAppender) commitExemplars() {
	if len(a.exemplars) == 0 {
		return
	}
	type keyed struct {
		hash   uint64
		labels labels.Labels
	}
	resolved := make([]keyed, len(a.exemplars))
	for index := range a.exemplars {
		ref := a.exemplars[index].ref
		shard := a.e.store.shards[refShard(ref)]
		shard.RLock()
		if t, ok := shard.tenants[a.e.tenantID]; ok {
			if entry, ok := a.e.lookupLocked(t, ref); ok {
				resolved[index] = keyed{entry.hash, entry.labels}
			}
		}
		shard.RUnlock()
	}
	window := a.e.oooWindow.Load()
	a.e.store.exemplarsLock.Lock()
	defer a.e.store.exemplarsLock.Unlock()
	tenantExemplars := a.e.tenantExemplars()
	for index := range a.exemplars {
		key := resolved[index]
		if key.labels == "" {
			continue
		}
		stored := toStoreExemplar(a.exemplars[index].exemplar)
		tenantExemplars.Add(key.hash, func() labels.Labels { return key.labels }, stored, window)
	}
}
