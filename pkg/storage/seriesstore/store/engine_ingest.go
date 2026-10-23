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
	// Ingested reports a series that got a sample, with its labels, which are valid only during the call, and
	// only built if NeedsLabels says so.
	Ingested(series int, lbls promlabels.Labels, ref storage.SeriesRef)
	// NeedsLabels reports whether Ingested wants the labels of a series, which cost building.
	NeedsLabels() bool
}

// RefSink is a FloatSink that may know a series by its ref already: Ingested is then called, with the series' labels
// built, only for the series IngestedRef doesn't know.
type RefSink interface {
	IngestedRef(series int, ref storage.SeriesRef) bool
}

// OwnedHashSink is a FloatSink told the owned series token of each series that got a sample, as
// EngineOptions.SecondaryHashFunction gave it: what hashing the series' labels again would.
type OwnedHashSink interface {
	IngestedOwnedHash(series int, ownedHash uint32)
}

type floatSeries struct {
	index int
	hash  uint64
	// The series' metric name, and where its label pairs are in the pairs of the call.
	name             string
	pairsAt, pairsTo int32
	// Where the series' labels, encoded, are in the keys of the call, when keyTo isn't negative.
	keyAt, keyTo int32
}

// floatError is a sample that wasn't ingested, which the sink hears of once the shards are unlocked.
type floatError struct {
	series      int
	err         error
	timestampMs int64
}

// floatIngested is a series that got a sample.
type floatIngested struct {
	series    int
	ref       storage.SeriesRef
	ownedHash uint32
}

// AppendFloats appends the float samples of the series at indices to the series the engine already has, and
// returns the indices, in order, of those it didn't handle, which the caller appends another way: series it doesn't have
// yet, and series with a staleness marker, whose type follows the series' last sample. Samples are appended and
// visible when it returns, and the series that got samples are marked ingested at ingestedAtMs. It takes each store shard's lock once, where an appender takes it for every sample
// twice. Series need floats and no histograms or exemplars, and the head needs to have its first sample.
func (e *Engine) AppendFloats(timeseries []mimirpb.PreallocTimeseries, indices []int, minTimestampMs, maxTimestampMs, ingestedAtMs int64, otlp bool, sink FloatSink) (leftover []int, ingested int, err error) {
	return e.appendFloats(adapterSource(timeseries), indices, minTimestampMs, maxTimestampMs, ingestedAtMs, otlp, sink)
}

// floatSource is the series appendFloats ingests, by their index.
type floatSource interface {
	// pairs appends the series' label pairs to into, and returns its metric name.
	pairs(index int, into [][2]string) ([][2]string, string)
	samples(index int) []mimirpb.Sample
}

// adapterSource is the series of a decoded request.
type adapterSource []mimirpb.PreallocTimeseries

func (s adapterSource) pairs(index int, into [][2]string) ([][2]string, string) {
	var name string
	for _, adapter := range s[index].Labels {
		into = append(into, [2]string{adapter.Name, adapter.Value})
		if adapter.Name == metricNameLabel {
			name = adapter.Value
		}
	}
	return into, name
}

func (s adapterSource) samples(index int) []mimirpb.Sample { return s[index].Samples }

// keySource is a floatSource that encodes a series' labels as labels.Labels does: comparing a series with those bytes
// is one string comparison, where labels.Labels.EqPairs resolves and compares each name. The fast path compares every
// series it appends to.
type keySource interface {
	// key appends the series' encoded labels to into, or reports false when the engine can't have the series.
	key(index int, into []byte) ([]byte, bool)
}

// sameLabels reports whether l are the labels of series.
func sameLabels(l labels.Labels, series *floatSeries, keys []byte, pairs [][2]string) bool {
	if series.keyTo >= 0 {
		return string(l) == string(keys[series.keyAt:series.keyTo])
	}
	return l.EqPairs(pairs[series.pairsAt:series.pairsTo])
}

func (e *Engine) appendFloats(source floatSource, indices []int, minTimestampMs, maxTimestampMs, ingestedAtMs int64, otlp bool, sink FloatSink) (leftover []int, ingested int, err error) {
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
		keys     = buffers.keys[:0]
		prepared = buffers.prepared[:0]
		counts   = slices.Grow(buffers.counts[:0], shards+1)[:shards+1]
	)
	clear(counts)
	pairs = pairs[:0]
	keyed, _ := source.(keySource)
	for _, index := range indices {
		at := len(pairs)
		var name string
		pairs, name = source.pairs(index, pairs)
		var hash uint64
		if adapterHashMatches() {
			// Without building the labels, which an appender does only to hash them.
			hash = labels.HashPairs(pairs[at:])
		} else {
			scratch = pairsLabels(&builder, pairs[at:])
			hash = scratch.Hash()
		}
		keyAt, keyTo := len(keys), -1
		if keyed != nil {
			var ok bool
			if keys, ok = keyed.key(index, keys); ok {
				keyTo = len(keys)
			} else {
				keys = keys[:keyAt]
			}
		}
		prepared = append(prepared, floatSeries{index: index, hash: hash, name: name, pairsAt: int32(at), pairsTo: int32(len(pairs)), keyAt: int32(keyAt), keyTo: int32(keyTo)})
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
			// A request has a metric's series in the order they first came in, which is their order in the metric's
			// group, so a series is likely the entry after the last one of its metric: reading it is sequential, where
			// the location cache and the group's table are reads anywhere.
			var (
				nextName  string
				nextGroup uint32
				nextIndex = -1
			)
			for chunkAt := 0; chunkAt < len(positions) && !stop; chunkAt += lookupBatch {
				chunk := positions[chunkAt:min(chunkAt+lookupBatch, len(positions))]
				// The series' entries are found for the whole batch before any is checked and appended to: the cache
				// misses of the series overlap, where looking each up when its turn comes waits for them one by one.
				candidates := buffers.candidates[:0]
				for _, position := range chunk {
					candidates = append(candidates, t.series.locs.candidate(prepared[position].hash, t.series.groups))
				}
				buffers.candidates = candidates
				for k, position := range chunk {
					prep := prepared[position]
					seriesSamples := source.samples(prep.index)
					var (
						entry      *seriesEntry
						foundGroup uint32
						foundIndex int
					)
					if nextIndex >= 0 && prep.name == nextName {
						if entries := t.series.groups[nextGroup].entries; nextIndex < len(entries) {
							if candidate := &entries[nextIndex]; candidate.hash == prep.hash && sameLabels(candidate.labels, &prep, keys, pairs) {
								entry, foundGroup, foundIndex = candidate, nextGroup, nextIndex
							}
						}
					}
					if candidate := candidates[k]; entry == nil && candidate.entry != nil && sameLabels(candidate.entry.labels, &prep, keys, pairs) {
						entry, foundGroup, foundIndex = candidate.entry, candidate.group, candidate.index
					}
					if entry == nil {
						// Where the series was last found is one read; its name group and the group's table are more.
						t.series.locs.findAll(prep.hash, func(group uint32, index int) bool {
							if entries := t.series.groups[group].entries; index < len(entries) {
								if candidate := &entries[index]; candidate.hash == prep.hash && sameLabels(candidate.labels, &prep, keys, pairs) {
									entry, foundGroup, foundIndex = candidate, group, index
									return true
								}
							}
							return false
						})
					}
					if entry == nil {
						if groupID, ok := t.series.names[prep.name]; ok {
							g := &t.series.groups[groupID]
							for index, ok := g.first.get(prep.hash, g.entries); ok && index >= 0; index = g.entries[index].next {
								if candidate := &g.entries[index]; candidate.hash == prep.hash && sameLabels(candidate.labels, &prep, keys, pairs) {
									entry, foundGroup, foundIndex = candidate, groupID, int(index)
									t.series.locs.remember(prep.hash, groupID, int(index), t.series.len)
									break
								}
							}
						}
					}
					if entry != nil {
						nextName, nextGroup, nextIndex = prep.name, foundGroup, foundIndex+1
					}
					if entry == nil || entry.series.ref == 0 || hasStaleMarker(seriesSamples) {
						switch {
						case entry == nil:
							e.leftovers[0].Add(1)
						case entry.series.ref == 0:
							e.leftovers[1].Add(1)
						default:
							e.leftovers[2].Add(1)
						}
						leftover = append(leftover, prep.index)
						continue
					}

					oldIngested := ingested
					// Every sample is checked against what the series held before the batch, as an appender does at
					// Append, and the ones that passed are applied in order, as it does at Commit.
					last := lastOf(&entry.series)
					accepted = accepted[:0]
					acceptedInOrder := true
					for _, sample := range seriesSamples {
						if sample.TimestampMs > maxTimestampMs {
							failures = append(failures, floatError{prep.index, globalerror.SampleTooFarInFuture, sample.TimestampMs})
							continue
						} else if sample.TimestampMs < minTimestampMs {
							failures = append(failures, floatError{prep.index, globalerror.SampleTooFarInPast, sample.TimestampMs})
							continue
						}
						isOOO, appendErr := appendableFloat(last, sample.TimestampMs, sample.Value, headMaxt, minValidTime, oooWindow)
						if appendErr != nil {
							failures = append(failures, floatError{prep.index, appendErr, sample.TimestampMs})
							continue
						}
						acceptedInOrder = acceptedInOrder && !isOOO && math.Float64bits(sample.Value) != value.QuietZeroNaN && !last.isHist
						accepted = append(accepted, sample)
					}
					ingested += len(accepted)
					if len(accepted) == 1 && acceptedInOrder {
						// The usual scrape: one in-order float, which the series' state still allows, so it is
						// appended without checking it again.
						sample := accepted[0]
						result, appendErr := appendFloatInOrder(&entry.series, state.disk, sample.TimestampMs, sample.Value, true)
						if appendErr != nil {
							stop, err = true, appendErr
						} else if !result.noop {
							inOrderMin, inOrderMax = min(inOrderMin, sample.TimestampMs), max(inOrderMax, sample.TimestampMs)
						}
						accepted = accepted[:0]
					}
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
						// The series are active, as the active series counts tell, and not by a histogram.
						entry.series.markIngested(ingestedAtMs, otlp, -1)
						touched = append(touched, floatIngested{int(position), storage.SeriesRef(entry.series.ref), entry.series.ownedHash})
					}
					if stop {
						break
					}
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
	buffers.pairs, buffers.keys, buffers.failures, buffers.accepted, buffers.touched = pairs, keys, failures, accepted, touched
	slices.Sort(leftover)
	if err != nil {
		return leftover, ingested, err
	}

	if hashSink, ok := sink.(OwnedHashSink); ok {
		for _, series := range touched {
			hashSink.IngestedOwnedHash(prepared[series.series].index, series.ownedHash)
		}
	}
	// The sink may look the series up, which it can't while a shard is locked. It hears of the failures in the
	// order of the request.
	slices.SortStableFunc(failures, func(a, b floatError) int { return cmp.Compare(a.series, b.series) })
	for _, failure := range failures {
		if !sink.Error(failure.series, failure.err, failure.timestampMs) {
			return leftover, ingested, failure.err
		}
	}
	// In the order of the request, which the prepared series are in: placed by position, a series ref never being 0,
	// rather than sorted.
	refs := slices.Grow(buffers.refs[:0], len(prepared))[:len(prepared)]
	clear(refs)
	for _, series := range touched {
		refs[series.series] = series.ref
	}
	buffers.refs = refs
	needsLabels := sink.NeedsLabels()
	refSink, byRef := sink.(RefSink)
	for position, ref := range refs {
		if ref == 0 {
			continue
		}
		prep := prepared[position]
		if byRef {
			if refSink.IngestedRef(prep.index, ref) {
				continue
			}
			scratch = pairsLabels(&builder, pairs[prep.pairsAt:prep.pairsTo])
		} else if needsLabels {
			scratch = pairsLabels(&builder, pairs[prep.pairsAt:prep.pairsTo])
		}
		sink.Ingested(prep.index, scratch, ref)
	}
	return leftover, ingested, nil
}

// pairsLabels builds the labels of the pairs, which are sorted, with builder.
func pairsLabels(builder *promlabels.ScratchBuilder, pairs [][2]string) promlabels.Labels {
	builder.Reset()
	for _, pair := range pairs {
		builder.Add(pair[0], pair[1])
	}
	return builder.Labels()
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
	candidates    []locationCandidate
	refs          []storage.SeriesRef
	counts, next  []int
	ordered       []int32
	pairs         [][2]string
	keys          []byte
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

// lookupBatch is how many series of a shard the float fast path looks up together: about what the processor's
// outstanding misses and L1 cache hold.
const lookupBatch = 32

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
