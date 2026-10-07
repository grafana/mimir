// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"cmp"
	"math"
	"slices"
	"sync"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
	"github.com/grafana/mimir/pkg/util/globalerror"
)

// FloatSink is told what happens to the series AppendFloats ingests, which are identified by their index in the
// slice it was given.
type FloatSink interface {
	// Error reports a sample that wasn't ingested. It returns whether the error is soft: the ingestion goes on
	// after a soft error, and stops after another.
	Error(series int, err error, timestampMs int64) (soft bool)
	// Ingested reports a series that got a sample, with its labels, which are valid only during the call.
	Ingested(series int, lbls promlabels.Labels, ref storage.SeriesRef)
}

type floatSeries struct {
	index int
	hash  uint64
	// The series' metric name, and where its label pairs are in the pairs of the call.
	name             string
	pairsAt, pairsTo int32
}

// floatError is a sample that wasn't ingested, which the sink hears of once the shards are unlocked.
type floatError struct {
	series      int
	err         error
	timestampMs int64
}

// floatIngested is a series that got a sample.
type floatIngested struct {
	series int
	ref    storage.SeriesRef
}

// AppendFloats appends the float samples of the series at indices to the series the engine already has, and
// returns the indices, in order, of those it didn't handle, which the caller appends another way: series it doesn't have
// yet, and series with a staleness marker, whose type follows the series' last sample. Samples are appended and
// visible when it returns. It takes each store shard's lock once, where an appender takes it for every sample
// twice. Series need floats and no histograms or exemplars, and the head needs to have its first sample.
func (e *Engine) AppendFloats(timeseries []mimirpb.PreallocTimeseries, indices []int, minTimestampMs, maxTimestampMs int64, sink FloatSink) (leftover []int, ingested int, err error) {
	minTime, maxTime, minValid := e.headTimes()
	if minTime == math.MaxInt64 {
		return indices, 0, nil
	}
	var (
		headMaxt     = maxTime
		minValidTime = appendableMinValidTime(maxTime, minValid)
		oooWindow    = e.oooWindow.Load()
		appender     = &engineAppender{e: e, headMaxt: headMaxt, minValidTime: minValidTime, oooWindow: oooWindow}
	)

	// Group the series by their store shard, which the labels' hash decides, before taking any lock.
	shards := len(e.store.shards)
	buffers := floatBuffers.Get().(*floatScratch)
	defer func() {
		buffers.reset()
		floatBuffers.Put(buffers)
	}()
	var (
		builder  promlabels.ScratchBuilder
		scratch  promlabels.Labels
		pairs    = buffers.pairs
		prepared = buffers.prepared[:0]
		counts   = slices.Grow(buffers.counts[:0], shards+1)[:shards+1]
	)
	clear(counts)
	pairs = pairs[:0]
	for _, index := range indices {
		at := len(pairs)
		var name string
		for _, adapter := range timeseries[index].Labels {
			pairs = append(pairs, [2]string{adapter.Name, adapter.Value})
			if adapter.Name == metricNameLabel {
				name = adapter.Value
			}
		}
		var hash uint64
		if adapterHashMatches() {
			// Without building the labels, which an appender does only to hash them.
			hash = labels.HashPairs(pairs[at:])
		} else {
			mimirpb.FromLabelAdaptersOverwriteLabels(&builder, timeseries[index].Labels, &scratch)
			hash = scratch.Hash()
		}
		prepared = append(prepared, floatSeries{index: index, hash: hash, name: name, pairsAt: int32(at), pairsTo: int32(len(pairs))})
		counts[shardFor(hash, shards)+1]++
	}
	for shard := range shards {
		counts[shard+1] += counts[shard]
	}
	ordered := slices.Grow(buffers.ordered[:0], len(prepared))[:len(prepared)]
	next := append(buffers.next[:0], counts[:shards]...)
	for position := range prepared {
		shard := shardFor(prepared[position].hash, shards)
		ordered[next[shard]] = int32(position)
		next[shard]++
	}

	var (
		inOrderMin, inOrderMax = int64(math.MaxInt64), int64(math.MinInt64)
		oooMin, oooMax         = int64(math.MaxInt64), int64(math.MinInt64)
		stop                   bool
		failures               = buffers.failures[:0]
		accepted               = buffers.accepted[:0]
		touched                = buffers.touched[:0]
	)
	// Appenders walk the shards from different ones, and one that finds a shard locked comes back to it after the
	// others: they would otherwise queue up on each shard in turn.
	pending := buffers.pending[:0]
	start := int(e.ingestStart.Add(1))
	for offset := range shards {
		if shardIndex := (start + offset) % shards; counts[shardIndex+1] > counts[shardIndex] {
			pending = append(pending, shardIndex)
		}
	}
	for pass := 0; pass < 2 && len(pending) > 0 && !stop; pass++ {
		busy := buffers.busy[:0]
		for _, shardIndex := range pending {
			if stop {
				break
			}
			positions := ordered[counts[shardIndex]:counts[shardIndex+1]]
			state := e.store.shards[shardIndex]
			if pass == 0 {
				if !state.TryLock() {
					busy = append(busy, shardIndex)
					continue
				}
			} else {
				state.Lock()
			}
			t, ok := e.tenantLocked(shardIndex)
			if !ok {
				state.Unlock()
				for _, position := range positions {
					leftover = append(leftover, prepared[position].index)
				}
				continue
			}
			for _, position := range positions {
				prep := prepared[position]
				ts := &timeseries[prep.index]
				seriesPairs := pairs[prep.pairsAt:prep.pairsTo]
				var entry *seriesEntry
				if groupID, ok := t.series.names[prep.name]; ok {
					g := &t.series.groups[groupID]
					for index, ok := g.first.get(prep.hash, g.entries); ok && index >= 0; index = g.entries[index].next {
						if candidate := &g.entries[index]; candidate.hash == prep.hash && candidate.labels.EqPairs(seriesPairs) {
							entry = candidate
							break
						}
					}
				}
				if entry == nil || entry.series.ref == 0 || hasStaleMarker(ts.Samples) {
					leftover = append(leftover, prep.index)
					continue
				}

				oldIngested := ingested
				// Every sample is checked against what the series held before the batch, as an appender does at
				// Append, and the ones that passed are applied in order, as it does at Commit.
				last := lastOf(&entry.series)
				accepted = accepted[:0]
				for _, sample := range ts.Samples {
					if sample.TimestampMs > maxTimestampMs {
						failures = append(failures, floatError{prep.index, globalerror.SampleTooFarInFuture, sample.TimestampMs})
						continue
					} else if sample.TimestampMs < minTimestampMs {
						failures = append(failures, floatError{prep.index, globalerror.SampleTooFarInPast, sample.TimestampMs})
						continue
					}
					if _, appendErr := appendableFloat(last, sample.TimestampMs, sample.Value, headMaxt, minValidTime, oooWindow); appendErr != nil {
						failures = append(failures, floatError{prep.index, appendErr, sample.TimestampMs})
						continue
					}
					accepted = append(accepted, sample)
				}
				ingested += len(accepted)
				for _, sample := range accepted {
					pending := pendingSample{ref: entry.series.ref, t: sample.TimestampMs, v: sample.Value, kind: sampleFloat}
					inOrder, ooo, commitErr := appender.commitSample(&entry.series, state, &pending)
					if commitErr != nil {
						stop, err = true, commitErr
						break
					}
					if inOrder {
						inOrderMin, inOrderMax = min(inOrderMin, sample.TimestampMs), max(inOrderMax, sample.TimestampMs)
					}
					if ooo {
						oooMin, oooMax = min(oooMin, sample.TimestampMs), max(oooMax, sample.TimestampMs)
					}
				}
				if ingested > oldIngested {
					touched = append(touched, floatIngested{int(position), storage.SeriesRef(entry.series.ref)})
				}
				if stop {
					break
				}
			}
			state.Unlock()
		}
		buffers.pending, buffers.busy = busy, pending
		pending = busy
	}
	if inOrderMax != math.MinInt64 {
		e.commitTimes(inOrderMin, inOrderMax)
	}
	if oooMax != math.MinInt64 {
		e.observeOOO(oooMin, oooMax)
	}
	buffers.prepared, buffers.counts, buffers.ordered, buffers.next = prepared, counts, ordered, next
	buffers.pairs, buffers.failures, buffers.accepted, buffers.touched = pairs, failures, accepted, touched
	slices.Sort(leftover)
	if err != nil {
		return leftover, ingested, err
	}

	// The sink may look the series up, which it can't while a shard is locked. It hears of the failures in the
	// order of the request.
	slices.SortStableFunc(failures, func(a, b floatError) int { return cmp.Compare(a.series, b.series) })
	for _, failure := range failures {
		if !sink.Error(failure.series, failure.err, failure.timestampMs) {
			return leftover, ingested, failure.err
		}
	}
	// In the order of the request, which the prepared series are in.
	slices.SortFunc(touched, func(a, b floatIngested) int { return cmp.Compare(a.series, b.series) })
	for _, series := range touched {
		index := prepared[series.series].index
		mimirpb.FromLabelAdaptersOverwriteLabels(&builder, timeseries[index].Labels, &scratch)
		sink.Ingested(index, scratch, series.ref)
	}
	return leftover, ingested, nil
}

// adapterPairs returns the label pairs of the adapters, in the order they are in, reusing buffer.
func adapterPairs(adapters []mimirpb.LabelAdapter, buffer [][2]string) [][2]string {
	buffer = buffer[:0]
	for _, adapter := range adapters {
		buffer = append(buffer, [2]string{adapter.Name, adapter.Value})
	}
	return buffer
}

func adapterName(adapters []mimirpb.LabelAdapter) string {
	for _, adapter := range adapters {
		if adapter.Name == metricNameLabel {
			return adapter.Value
		}
	}
	return ""
}

var (
	adapterHashOnce sync.Once
	adapterHashOK   bool
)

// adapterHashMatches reports whether hashing label pairs gives the hash of the Prometheus labels with them, which is
// the stringlabels one: a build with other labels hashes them another way.
func adapterHashMatches() bool {
	adapterHashOnce.Do(func() {
		sample := promlabels.FromStrings("__name__", "metric", "job", "a", "pod", "pod-1")
		adapterHashOK = labels.HashPairs([][2]string{{"__name__", "metric"}, {"job", "a"}, {"pod", "pod-1"}}) == sample.Hash()
	})
	return adapterHashOK
}

// floatScratch holds what AppendFloats needs for a batch, which batches reuse: it was a quarter of the allocations
// of ingesting.
type floatScratch struct {
	prepared      []floatSeries
	counts, next  []int
	ordered       []int32
	pairs         [][2]string
	failures      []floatError
	accepted      []mimirpb.Sample
	touched       []floatIngested
	pending, busy []int
}

func (s *floatScratch) reset() {
	// Errors hold what they were about.
	clear(s.failures)
	clear(s.accepted)
}

var floatBuffers = sync.Pool{New: func() any { return &floatScratch{} }}

// hasStaleMarker reports whether a sample is a staleness marker.
func hasStaleMarker(samples []mimirpb.Sample) bool {
	for _, sample := range samples {
		if value.IsStaleNaN(sample.Value) {
			return true
		}
	}
	return false
}
