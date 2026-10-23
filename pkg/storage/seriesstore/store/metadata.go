// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"container/heap"
	"context"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/prometheus/common/model"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// maxMetricMetadataBytes bounds the type, unit and help of one value, as the Prometheus TSDB's native metric metadata
// does: a bigger value isn't kept, and is counted.
const maxMetricMetadataBytes = 8 << 10

// metricMetadataDictionary holds an engine's distinct metric metadata values, each once, by id: series refer to the
// values they have and had by their ids, and a value with no references left goes at the next compaction. Values are
// copied in, so they refer to nothing of the requests or records they came from.
//
// An id is only handed out with a reference, and only resolved by a holder of one or under the lock of the shard of
// a series that holds one, so a reclaimed id, which a new value may take, is never resolved to it by mistake.
type metricMetadataDictionary struct {
	mu   sync.Mutex
	ids  map[metadata.Metadata]uint32
	free []uint32
	// The next id never given out; 0 is no value.
	next uint32
	// The values by id, in pages that never move, so that an id resolves without the lock while the dictionary grows.
	pages atomic.Pointer[[]*metricMetadataPage]

	values atomic.Int64
	bytes  atomic.Int64
}

const metricMetadataPageSize = 1024

type metricMetadataPage [metricMetadataPageSize]metricMetadataValue

type metricMetadataValue struct {
	metadata metadata.Metadata
	refs     atomic.Int64
	bytes    int64
}

func newMetricMetadataDictionary() *metricMetadataDictionary {
	d := &metricMetadataDictionary{ids: map[metadata.Metadata]uint32{}, next: 1}
	d.pages.Store(&[]*metricMetadataPage{})
	return d
}

func (d *metricMetadataDictionary) entry(id uint32) *metricMetadataValue {
	pages := *d.pages.Load()
	return &pages[id/metricMetadataPageSize][id%metricMetadataPageSize]
}

// intern returns the id of m, with a reference to it that the caller releases.
func (d *metricMetadataDictionary) intern(m metadata.Metadata) uint32 {
	d.mu.Lock()
	defer d.mu.Unlock()
	if id, ok := d.ids[m]; ok {
		d.entry(id).refs.Add(1)
		return id
	}
	var id uint32
	if n := len(d.free); n > 0 {
		id, d.free = d.free[n-1], d.free[:n-1]
	} else {
		id = d.next
		d.next++
		if pages := *d.pages.Load(); int(id/metricMetadataPageSize) >= len(pages) {
			grown := append(slices.Clip(pages), &metricMetadataPage{})
			d.pages.Store(&grown)
		}
	}
	owned := metadata.Metadata{Type: model.MetricType(strings.Clone(string(m.Type))), Unit: strings.Clone(m.Unit), Help: strings.Clone(m.Help)}
	value := d.entry(id)
	value.metadata, value.bytes = owned, int64(len(owned.Type)+len(owned.Unit)+len(owned.Help))
	value.refs.Store(1)
	d.ids[owned] = id
	d.values.Add(1)
	d.bytes.Add(value.bytes)
	return id
}

// acquire takes another reference to id, which the caller already holds one to, or reads under the lock of the shard
// of a series that holds one.
func (d *metricMetadataDictionary) acquire(id uint32) {
	d.entry(id).refs.Add(1)
}

// release gives up a reference to id.
func (d *metricMetadataDictionary) release(id uint32) {
	d.entry(id).refs.Add(-1)
}

// value is the value of id, which the caller holds a reference to, or reads under the lock of the shard of a series
// that holds one.
func (d *metricMetadataDictionary) value(id uint32) metadata.Metadata {
	return d.entry(id).metadata
}

// reclaim drops the values that have no references left, and returns how many it dropped.
func (d *metricMetadataDictionary) reclaim() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	reclaimed := 0
	for m, id := range d.ids {
		value := d.entry(id)
		if value.refs.Load() > 0 {
			continue
		}
		delete(d.ids, m)
		d.free = append(d.free, id)
		d.bytes.Add(-value.bytes)
		*value = metricMetadataValue{}
		reclaimed++
	}
	d.values.Add(-int64(reclaimed))
	return reclaimed
}

// metricMetadataStats are an engine's counters of what its native metric metadata didn't keep as it came.
type metricMetadataStats struct {
	// Values over maxMetricMetadataBytes, which aren't kept.
	oversized atomic.Uint64
	// Observations older than their shard's base, whose runs start at the base instead.
	clamped atomic.Uint64
	// Lookups of a value by a batch's memo, and the ones it didn't have yet.
	memoHits, memoMisses atomic.Uint64
}

// shardMetadata is a shard's part of an engine's native metric metadata, guarded by the shard's lock.
//
// A series' current value is inline in it, as an id and the start of its run, an offset from the shard's base.
// The older runs of the series whose value ever changed are here, by series reference, oldest first. Every run holds
// a reference to its value.
type shardMetadata struct {
	dictionary *metricMetadataDictionary
	stats      *metricMetadataStats
	// What run starts are offsets from, in sample time; set by the first observation.
	base    int64
	baseSet bool
	older   map[uint64][]metadataRun
	// The series with a value, and the older runs of all of them.
	series, runs int
}

// metadataRun is a value from a time until the next run's start, or for the current run, on.
type metadataRun struct {
	from int64
	id   uint32
}

func newShardMetadata(dictionary *metricMetadataDictionary, stats *metricMetadataStats) *shardMetadata {
	return &shardMetadata{dictionary: dictionary, stats: stats, older: map[uint64][]metadataRun{}}
}

// metadataBaseMargin is how far before the first observation a shard's base is: samples down to the out-of-order
// window or half a block range before it can still come in, and a replay after an unclean shutdown starts further
// back still, which clamps.
func metadataBaseMargin(oooWindowMs int64) int64 {
	return max(oooWindowMs, chunkRangeMs/2)
}

func (m *shardMetadata) start(series *Series) int64 {
	return m.base + int64(series.metadataStart)
}

// offset is t as an offset from the base, which it sets on the first observation; t before the base clamps to it,
// and is counted.
func (m *shardMetadata) offset(t, oooWindowMs int64) uint32 {
	if !m.baseSet {
		m.base, m.baseSet = t-metadataBaseMargin(oooWindowMs), true
	}
	switch delta := t - m.base; {
	case delta < 0:
		m.stats.clamped.Add(1)
		return 0
	case delta > math.MaxUint32:
		// Head garbage collection rebases the shard long before its series' runs start this far from the base.
		m.stats.clamped.Add(1)
		return math.MaxUint32
	default:
		return uint32(delta)
	}
}

// observe records that series has the value id from t on: the id of a value the caller holds a reference to, which
// the series takes another one of if it keeps it. Equal neighbouring runs merge, the last of observations at the same
// time wins, and an observation older than the series' current run is put among its older runs.
func (m *shardMetadata) observe(series *Series, id uint32, t, oooWindowMs int64) {
	if series.metadataID == 0 {
		m.dictionary.acquire(id)
		series.metadataID, series.metadataStart = id, m.offset(t, oooWindowMs)
		m.series++
		return
	}
	start := m.start(series)
	if t >= start && series.metadataID == id {
		// The usual observation: the series' value, as it has been.
		return
	}
	if t > start {
		m.dictionary.acquire(id)
		m.older[series.ref] = append(m.older[series.ref], metadataRun{from: start, id: series.metadataID})
		m.runs++
		series.metadataID, series.metadataStart = id, m.offset(t, oooWindowMs)
		return
	}
	// At or before the current run's start: the series' runs as change points, with the observation put among them.
	runs := append(m.older[series.ref], metadataRun{from: start, id: series.metadataID})
	at, found := slices.BinarySearchFunc(runs, t, func(r metadataRun, t int64) int {
		switch {
		case r.from < t:
			return -1
		case r.from > t:
			return 1
		}
		return 0
	})
	switch {
	case found && runs[at].id == id:
		return
	case found:
		m.dictionary.release(runs[at].id)
		runs[at].id = id
	case at > 0 && runs[at-1].id == id:
		// Within a run of the same value.
		return
	default:
		runs = slices.Insert(runs, at, metadataRun{from: t, id: id})
	}
	m.dictionary.acquire(id)
	// Equal neighbours merge into the earlier one.
	merged := runs[:1]
	for _, run := range runs[1:] {
		if last := merged[len(merged)-1]; last.id == run.id {
			m.dictionary.release(run.id)
			continue
		}
		merged = append(merged, run)
	}
	m.setRuns(series, merged, oooWindowMs)
}

// setRuns makes runs, oldest first, the series' runs: the newest inline, and the others its older ones.
func (m *shardMetadata) setRuns(series *Series, runs []metadataRun, oooWindowMs int64) {
	newest := runs[len(runs)-1]
	series.metadataID, series.metadataStart = newest.id, m.offset(newest.from, oooWindowMs)
	m.runs -= len(m.older[series.ref])
	if older := runs[:len(runs)-1]; len(older) > 0 {
		m.older[series.ref] = slices.Clip(older)
		m.runs += len(older)
	} else {
		delete(m.older, series.ref)
	}
}

// runsOf are series' runs, oldest first, which refer to values the caller resolves under the shard's lock.
func (m *shardMetadata) runsOf(series *Series, into []metadataRun) []metadataRun {
	if series.metadataID == 0 {
		return into
	}
	into = append(into, m.older[series.ref]...)
	return append(into, metadataRun{from: m.start(series), id: series.metadataID})
}

// trim drops series' older runs that end at or before minTime, the head's min time, which they're no longer in.
func (m *shardMetadata) trim(series *Series, minTime int64) {
	older := m.older[series.ref]
	keep := 0
	for keep < len(older) {
		end := m.start(series)
		if keep+1 < len(older) {
			end = older[keep+1].from
		}
		if end > minTime {
			break
		}
		m.dictionary.release(older[keep].id)
		keep++
	}
	if keep == 0 {
		return
	}
	m.runs -= keep
	if keep == len(older) {
		delete(m.older, series.ref)
		return
	}
	m.older[series.ref] = slices.Clone(older[keep:])
}

// drop gives up series' metadata, as the series leaves the engine's memory.
func (m *shardMetadata) drop(series *Series) {
	if series.metadataID == 0 {
		return
	}
	for _, run := range m.older[series.ref] {
		m.dictionary.release(run.id)
	}
	m.runs -= len(m.older[series.ref])
	delete(m.older, series.ref)
	m.dictionary.release(series.metadataID)
	series.metadataID, series.metadataStart = 0, 0
	m.series--
}

// metricMetadataOf is a series' own metadata as the native metric metadata keeps it, and whether it's kept: an empty
// one is none, and an oversized one is counted.
func metricMetadataOf(m mimirpb.SeriesMetadata, stats *metricMetadataStats) (metadata.Metadata, bool) {
	if m.IsEmpty() {
		return metadata.Metadata{}, false
	}
	return keptMetricMetadata(metadata.Metadata{Type: mimirpb.MetricMetadataMetricTypeToMetricType(m.Type), Unit: m.Unit, Help: m.Help}, stats)
}

// keptMetricMetadata is metadata an appender got as the native metric metadata keeps it: without a type, its type is
// unknown, and an oversized one isn't kept, and is counted.
func keptMetricMetadata(m metadata.Metadata, stats *metricMetadataStats) (metadata.Metadata, bool) {
	if m.IsEmpty() {
		return metadata.Metadata{}, false
	}
	if m.Type == "" {
		m.Type = model.MetricTypeUnknown
	}
	if len(m.Type)+len(m.Unit)+len(m.Help) > maxMetricMetadataBytes {
		stats.oversized.Add(1)
		return metadata.Metadata{}, false
	}
	return m, true
}

// metadataMemo is a batch's ids of the metadata values it observed, by content and, for a record, by the symbol
// references of their type, help and unit, each with a reference it holds until release. 0 is a value it doesn't
// keep.
type metadataMemo struct {
	dictionary *metricMetadataDictionary
	stats      *metricMetadataStats
	refs       map[[3]uint32]uint32
	content    map[metadata.Metadata]uint32
}

func (e *Engine) newMetadataMemo() *metadataMemo {
	return &metadataMemo{dictionary: e.metricMetadata, stats: &e.metricMetadataStats}
}

// byRefs is the id of a record's metadata value, by its type and symbol references.
func (m *metadataMemo) byRefs(key [3]uint32, value mimirpb.SeriesMetadata) uint32 {
	if id, ok := m.refs[key]; ok {
		m.stats.memoHits.Add(1)
		return id
	}
	m.stats.memoMisses.Add(1)
	if m.refs == nil {
		m.refs = map[[3]uint32]uint32{}
	}
	var id uint32
	if kept, ok := metricMetadataOf(value, m.stats); ok {
		id = m.dictionary.intern(kept)
	}
	m.refs[key] = id
	return id
}

// byContent is the id of a series' own metadata.
func (m *metadataMemo) byContent(value mimirpb.SeriesMetadata) uint32 {
	kept, ok := metricMetadataOf(value, m.stats)
	if !ok {
		return 0
	}
	return m.byValue(kept)
}

// byValue is the id of a value as the native metric metadata keeps it.
func (m *metadataMemo) byValue(value metadata.Metadata) uint32 {
	if id, ok := m.content[value]; ok {
		m.stats.memoHits.Add(1)
		return id
	}
	m.stats.memoMisses.Add(1)
	if m.content == nil {
		m.content = map[metadata.Metadata]uint32{}
	}
	id := m.dictionary.intern(value)
	m.content[value] = id
	return id
}

func (m *metadataMemo) release() {
	for _, id := range m.refs {
		if id != 0 {
			m.dictionary.release(id)
		}
	}
	for _, id := range m.content {
		m.dictionary.release(id)
	}
	clear(m.refs)
	clear(m.content)
}

// SetNativeMetricMetadata sets whether the series' own metric metadata is kept, from the next batch on.
func (e *Engine) SetNativeMetricMetadata(enabled bool) {
	e.nativeMetadata.Store(enabled)
}

// shardMetadataLocked is the shard's part of the engine's native metric metadata, for a caller holding its lock.
func (e *Engine) shardMetadataLocked(t *tenant) *shardMetadata {
	if t.nativeMetadata == nil {
		t.nativeMetadata = newShardMetadata(e.metricMetadata, &e.metricMetadataStats)
	}
	return t.nativeMetadata
}

// observeMetadataLocked records that series has the value id from timestamp on, for a caller holding its shard's lock
// and a reference to id.
func (e *Engine) observeMetadataLocked(t *tenant, series *Series, id uint32, timestamp int64) {
	if series.metadataID == 0 {
		for observed := e.metadataObservedFrom.Load(); timestamp < observed && !e.metadataObservedFrom.CompareAndSwap(observed, timestamp); {
			observed = e.metadataObservedFrom.Load()
		}
	}
	e.shardMetadataLocked(t).observe(series, id, timestamp, e.oooWindow.Load())
}

// metadataRebaseAfter is how far past its shard's base a series' run may start before the shard is rebased: half the
// range of the offsets.
const metadataRebaseAfter = 1 << 31

// trimMetadataLocked drops the older runs of the shard's series that end by minTime, the head's min time, and rebases
// the shard's run starts onto minTime once the head's max time nears the end of the offsets' range. The caller holds
// the shard's lock.
func (e *Engine) trimMetadataLocked(t *tenant, minTime, maxTime int64) {
	m := t.nativeMetadata
	if m == nil {
		return
	}
	for ref := range m.older {
		if entry, ok := e.lookupLocked(t, ref); ok {
			m.trim(&entry.series, minTime)
		}
	}
	if !m.baseSet || maxTime-m.base < metadataRebaseAfter {
		return
	}
	base := minTime - metadataBaseMargin(e.oooWindow.Load())
	for group := range t.series.groups {
		entries := t.series.groups[group].entries
		for index := range entries {
			if series := &entries[index].series; series.metadataID != 0 {
				// A run that started before the new base clamps to it, which its older runs, by time, don't need.
				series.metadataStart = uint32(max(0, m.base+int64(series.metadataStart)-base))
			}
		}
	}
	m.base = base
}

// MetricMetadataVersion is a series' metadata value from a time on.
type MetricMetadataVersion struct {
	EffectiveFrom int64
	Metadata      metadata.Metadata
}

// metricMetadataVersions are the series ref's metadata versions, oldest first.
func (e *Engine) metricMetadataVersions(ref uint64) []MetricMetadataVersion {
	index := refShard(ref)
	state := e.store.shards[index]
	state.RLock()
	defer state.RUnlock()
	t, ok := e.tenantLocked(index)
	if !ok || t.nativeMetadata == nil {
		return nil
	}
	entry, ok := e.lookupLocked(t, ref)
	if !ok {
		return nil
	}
	return e.versionsLocked(t.nativeMetadata, &entry.series, nil)
}

// versionsLocked appends series' versions, which the caller resolves under the shard's lock, to into.
func (e *Engine) versionsLocked(m *shardMetadata, series *Series, into []MetricMetadataVersion) []MetricMetadataVersion {
	var runs [8]metadataRun
	for _, run := range m.runsOf(series, runs[:0]) {
		into = append(into, MetricMetadataVersion{EffectiveFrom: run.from, Metadata: e.metricMetadata.value(run.id)})
	}
	return into
}

// NativeMetricMetadataSeries is a head series' native metric metadata.
type NativeMetricMetadataSeries struct {
	Labels promlabels.Labels
	// The versions, oldest first.
	Versions []MetricMetadataVersion
}

// NativeMetricMetadata returns the native metric metadata of the head series that match any of matcherSets, in label
// order, at most limit of them with a positive limit, and whether more matched.
func (e *Engine) NativeMetricMetadata(ctx context.Context, matcherSets [][]*promlabels.Matcher, limit int) ([]NativeMetricMetadataSeries, bool, error) {
	compiled := make([][]compiledMatcher, 0, len(matcherSets))
	for _, matchers := range matcherSets {
		c, err := compilePromMatchers(nil, matchers)
		if err != nil {
			return nil, false, err
		}
		compiled = append(compiled, c)
	}
	// With a limit, the first series by labels are kept in a heap of the limit's size, largest on top, rather than all
	// of them sorted.
	selected := &metadataSelection{}
	seen := map[uint64]struct{}{}
	limited := false
	var err error
	e.perShard(func(_ int, t *tenant, _ *chunks.DiskMapper, _ *coldState) {
		if err = ctx.Err(); err != nil || t.nativeMetadata == nil {
			return
		}
		for _, matchers := range compiled {
			t.series.matching(matchers, func(entry *seriesEntry) bool {
				if entry.series.metadataID == 0 {
					return true
				}
				if _, ok := seen[entry.series.ref]; ok {
					return true
				}
				seen[entry.series.ref] = struct{}{}
				if limit > 0 && selected.Len() == limit {
					limited = true
					if labels.Compare(entry.labels, selected.series[0].labels) >= 0 {
						return true
					}
					heap.Pop(selected)
				}
				heap.Push(selected, selectedMetadata{labels: entry.labels, versions: e.versionsLocked(t.nativeMetadata, &entry.series, nil)})
				return true
			})
		}
	})
	if err != nil {
		return nil, false, err
	}
	slices.SortFunc(selected.series, func(a, b selectedMetadata) int { return labels.Compare(a.labels, b.labels) })
	out := make([]NativeMetricMetadataSeries, 0, len(selected.series))
	var builder promlabels.ScratchBuilder
	for _, s := range selected.series {
		out = append(out, NativeMetricMetadataSeries{Labels: toPromLabels(s.labels, &builder), Versions: s.versions})
	}
	return out, limited, nil
}

// NativeMetricMetadataCoverageStart is when the native metric metadata starts: the later of the head's min time and
// the oldest time a series' metadata was first observed at since the engine opened.
func (e *Engine) NativeMetricMetadataCoverageStart() int64 {
	return max(e.MinTime(), e.metadataObservedFrom.Load())
}

type selectedMetadata struct {
	labels   labels.Labels
	versions []MetricMetadataVersion
}

// metadataSelection is a max-heap of series by labels.
type metadataSelection struct{ series []selectedMetadata }

func (s *metadataSelection) Len() int { return len(s.series) }
func (s *metadataSelection) Less(i, j int) bool {
	return labels.Compare(s.series[i].labels, s.series[j].labels) > 0
}
func (s *metadataSelection) Swap(i, j int) { s.series[i], s.series[j] = s.series[j], s.series[i] }
func (s *metadataSelection) Push(x any)    { s.series = append(s.series, x.(selectedMetadata)) }
func (s *metadataSelection) Pop() any {
	last := s.series[len(s.series)-1]
	s.series = s.series[:len(s.series)-1]
	return last
}

// MetricMetadataStats are an engine's native metric metadata, as its metrics report it.
type MetricMetadataStats struct {
	// The series with metadata, and their versions: one each, and their older runs.
	Series, Versions int64
	// The dictionary's values, and the bytes of their type, unit and help.
	Values, ValueBytes int64
	// The entries the series' groups have room for, which every series' inline metadata is paid in.
	EntryCapacity int64
	// The values too big to keep, the runs whose start clamped to their shard's base, and the batches' memo lookups.
	Oversized, Clamped, MemoHits, MemoMisses uint64
}

// MetricMetadataStats returns the engine's native metric metadata as its metrics report it.
func (e *Engine) MetricMetadataStats() MetricMetadataStats {
	stats := MetricMetadataStats{
		Values:     e.metricMetadata.values.Load(),
		ValueBytes: e.metricMetadata.bytes.Load(),
		Oversized:  e.metricMetadataStats.oversized.Load(),
		Clamped:    e.metricMetadataStats.clamped.Load(),
		MemoHits:   e.metricMetadataStats.memoHits.Load(),
		MemoMisses: e.metricMetadataStats.memoMisses.Load(),
	}
	for index, state := range e.store.shards {
		state.RLock()
		if t, ok := e.tenantLocked(index); ok {
			if m := t.nativeMetadata; m != nil {
				stats.Series += int64(m.series)
				stats.Versions += int64(m.series + m.runs)
			}
			for group := range t.series.groups {
				stats.EntryCapacity += int64(cap(t.series.groups[group].entries))
			}
		}
		state.RUnlock()
	}
	return stats
}
