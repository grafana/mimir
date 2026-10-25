// SPDX-License-Identifier: AGPL-3.0-only

//go:build !native_metadata_ablation

package store

import (
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/metadata"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// seriesMetadata is a series' current native metric metadata: its value's id in the engine's dictionary, or 0 for
// none, and the start of its run, as an offset from its shard's base. Its older runs, if any, are its shard's.
type seriesMetadata struct {
	metadataID, metadataStart uint32
}

func (s *seriesMetadata) hasMetricMetadata() bool { return s.metadataID != 0 }

// metricMetadataValues are the engine's metric metadata values: a dictionary, in this build.
type metricMetadataValues = metricMetadataDictionary

func newMetricMetadataValues() *metricMetadataValues { return newMetricMetadataDictionary() }

// maxMetadataRunsPerSeries bounds a series' older runs, whatever the change limit lets through, such as out-of-order
// changes: past it, the oldest go, the series is truncated, as the Prometheus TSDB's version cap truncates it, and the
// evictions are counted.
const maxMetadataRunsPerSeries = 64

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

// metadataCoalesced marks a run's id as a coalesced run's: the changes over the change limit in a window, which the run
// stands for, with the latest of their values. The dictionary takes such ids as their value's.
const metadataCoalesced = uint32(1) << 31

func (d *metricMetadataDictionary) entry(id uint32) *metricMetadataValue {
	id &^= metadataCoalesced
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
		if id == metadataCoalesced {
			panic("native metric metadata dictionary has no ids left")
		}
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

// metadataLimits are how many changes of its metadata a series may have in a window of sample time, of the size of the
// window; past them, the changes coalesce until the window ends. No changes, or no window, is no limit.
type metadataLimits struct {
	maxChanges int
	windowMs   int64
}

func (l metadataLimits) windowOf(t int64) int64 {
	return t - ((t%l.windowMs)+l.windowMs)%l.windowMs
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
	// The changes of the series that changed, as the change limit counts them.
	changes map[uint64]metadataChanges
	// The series whose older runs went past maxMetadataRunsPerSeries, until they leave memory.
	truncated map[uint64]struct{}
	// The series with a value, and the older runs of all of them.
	series, runs int
}

// metadataChanges are a series' changes in the window it last changed in, and when the series' current run is a
// coalesced one, the end of its window and when its latest value was observed.
type metadataChanges struct {
	window, coalescedEnd, latestAt int64
	count                          int
}

// metadataRun is a value from a time until the next run's start, or for the current run, on.
type metadataRun struct {
	from int64
	id   uint32
}

func newShardMetadata(dictionary *metricMetadataDictionary, stats *metricMetadataStats) *shardMetadata {
	return &shardMetadata{dictionary: dictionary, stats: stats, older: map[uint64][]metadataRun{}, changes: map[uint64]metadataChanges{}, truncated: map[uint64]struct{}{}}
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
// time wins, and an observation older than the series' current run is put among its older runs. A change over the
// limits' coalesces with the window's later ones.
func (m *shardMetadata) observe(series *Series, id uint32, t, oooWindowMs int64, limits metadataLimits) {
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
	if series.metadataID&metadataCoalesced != 0 {
		if m.absorb(series, id, t, start, oooWindowMs) {
			return
		}
		start = m.start(series)
		if t >= start && series.metadataID == id {
			return
		}
	}
	if t > start {
		if !m.charge(series.ref, t, limits) {
			m.coalesceFrom(series, id, t, oooWindowMs, limits)
			return
		}
		m.dictionary.acquire(id)
		m.older[series.ref] = append(m.older[series.ref], metadataRun{from: start, id: series.metadataID})
		m.runs++
		series.metadataID, series.metadataStart = id, m.offset(t, oooWindowMs)
		m.capRuns(series.ref)
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
	case found && runs[at].id&metadataCoalesced != 0 || !found && at > 0 && runs[at-1].id&metadataCoalesced != 0:
		// Into a coalesced run, which stands for whatever changed in its range.
		return
	case !found && at > 0 && runs[at-1].id == id:
		// Within a run of the same value.
		return
	case !m.charge(series.ref, t, limits):
		// An out-of-order change over the limit, which has no coalesced run to go in, is absorbed.
		m.stats.coalesce()
		return
	case found:
		m.dictionary.release(runs[at].id)
		runs[at].id = id
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
		m.capRuns(series.ref)
	} else {
		delete(m.older, series.ref)
	}
}

// capRuns evicts the series' oldest runs past maxMetadataRunsPerSeries.
func (m *shardMetadata) capRuns(ref uint64) {
	older := m.older[ref]
	evict := len(older) - maxMetadataRunsPerSeries
	if evict <= 0 {
		return
	}
	for _, run := range older[:evict] {
		m.dictionary.release(run.id)
	}
	// In place: a series past the cap that keeps changing would otherwise copy its runs on every change.
	kept := copy(older, older[evict:])
	clear(older[kept:])
	m.older[ref] = older[:kept]
	m.runs -= evict
	m.truncated[ref] = struct{}{}
	m.stats.evictions.Add(uint64(evict))
}

// absorb takes an observation into the series' coalesced current run, which started at start: one in its window is
// absorbed, the latest by time being its value. One after its window closes it: the series goes on from the window's
// end with that value, as a run of its own, and absorb returns false for the caller to go on with the observation.
func (m *shardMetadata) absorb(series *Series, id uint32, t, start, oooWindowMs int64) bool {
	c := m.changes[series.ref]
	if t >= c.coalescedEnd {
		m.older[series.ref] = append(m.older[series.ref], metadataRun{from: start, id: series.metadataID})
		m.runs++
		latest := series.metadataID &^ metadataCoalesced
		m.dictionary.acquire(latest)
		series.metadataID, series.metadataStart = latest, m.offset(c.coalescedEnd, oooWindowMs)
		m.changes[series.ref] = metadataChanges{window: c.coalescedEnd}
		m.capRuns(series.ref)
		return false
	}
	if t < start {
		return false
	}
	if t >= c.latestAt {
		if latest := series.metadataID &^ metadataCoalesced; latest != id {
			m.dictionary.acquire(id)
			m.dictionary.release(latest)
			series.metadataID = id | metadataCoalesced
		}
		c.latestAt = t
		m.changes[series.ref] = c
	}
	return true
}

// charge counts a change of the series at t against the limits, and reports whether it's within them. A change in an
// older window than the series' last one, which an out-of-order observation makes, isn't counted.
func (m *shardMetadata) charge(ref uint64, t int64, limits metadataLimits) bool {
	if limits.maxChanges <= 0 || limits.windowMs <= 0 {
		return true
	}
	window := limits.windowOf(t)
	c, ok := m.changes[ref]
	switch {
	case ok && window < c.window:
		return true
	case !ok || window > c.window:
		c = metadataChanges{window: window}
	}
	if c.count >= limits.maxChanges {
		return false
	}
	c.count++
	m.changes[ref] = c
	return true
}

// coalesceFrom starts the series' coalesced run at t, with the value id, to the end of t's window.
func (m *shardMetadata) coalesceFrom(series *Series, id uint32, t, oooWindowMs int64, limits metadataLimits) {
	m.dictionary.acquire(id)
	m.older[series.ref] = append(m.older[series.ref], metadataRun{from: m.start(series), id: series.metadataID})
	m.runs++
	series.metadataID, series.metadataStart = id|metadataCoalesced, m.offset(t, oooWindowMs)
	m.capRuns(series.ref)
	window := limits.windowOf(t)
	m.changes[series.ref] = metadataChanges{window: window, coalescedEnd: window + limits.windowMs, latestAt: t, count: limits.maxChanges}
	m.stats.coalesce()
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
	delete(m.changes, series.ref)
	delete(m.truncated, series.ref)
	m.dictionary.release(series.metadataID)
	series.metadataID, series.metadataStart = 0, 0
	m.series--
}

// metadataMemo is a batch's ids of the metadata values it observed, by content and, for a record, by the symbol
// references of their type, help and unit, each with a reference it holds until release. 0 is a value it doesn't
// keep.
type metadataMemo struct {
	dictionary *metricMetadataDictionary
	stats      *metricMetadataStats
	refs       map[[3]uint32]uint32
	content    map[metadata.Metadata]uint32
	// The last value byRefs found, which a record's next series, of the same family, likely has too.
	lastRefs  [3]uint32
	lastRefID uint32
	lastSet   bool
	// Without the record memo, the ids looked up by content for each series, each with a reference.
	uncached []uint32
}

func (e *Engine) newMetadataMemo() *metadataMemo {
	return &metadataMemo{dictionary: e.metricMetadata, stats: &e.metricMetadataStats}
}

// byRefs is the id of a record's metadata value, by its type and symbol references.
func (m *metadataMemo) byRefs(key [3]uint32, value mimirpb.SeriesMetadata) uint32 {
	if !recordMemo {
		kept, ok := metricMetadataOf(value, m.stats)
		if !ok {
			return 0
		}
		m.stats.memoMisses.Add(1)
		id := m.dictionary.intern(kept)
		m.uncached = append(m.uncached, id)
		return id
	}
	if m.lastSet && key == m.lastRefs {
		m.stats.memoHits.Add(1)
		return m.lastRefID
	}
	if id, ok := m.refs[key]; ok {
		m.stats.memoHits.Add(1)
		m.lastRefs, m.lastRefID = key, id
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
	m.lastRefs, m.lastRefID, m.lastSet = key, id, true
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
	for _, id := range m.uncached {
		m.dictionary.release(id)
	}
	clear(m.refs)
	clear(m.content)
	m.lastSet = false
	m.uncached = m.uncached[:0]
}

// observeMetadataLocked records that series has the value id from timestamp on, for a caller holding its shard's lock
// and, through memo, a reference to id.
func (e *Engine) observeMetadataLocked(t *tenant, series *Series, _ *metadataMemo, id uint32, timestamp int64) {
	if series.metadataID == 0 {
		for observed := e.metadataObservedFrom.Load(); timestamp < observed && !e.metadataObservedFrom.CompareAndSwap(observed, timestamp); {
			observed = e.metadataObservedFrom.Load()
		}
	}
	limits := metadataLimits{maxChanges: int(e.metadataMaxChanges.Load()), windowMs: e.metadataChangeWindowMs.Load()}
	e.shardMetadataLocked(t).observe(series, id, timestamp, e.oooWindow.Load(), limits)
}

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

// versionsLocked appends series' versions, which the caller resolves under the shard's lock, to into, and reports
// whether any of them is a coalesced one, and whether the series is truncated.
func (e *Engine) versionsLocked(m *shardMetadata, series *Series, into []MetricMetadataVersion) ([]MetricMetadataVersion, bool, bool) {
	var runs [8]metadataRun
	coalesced := false
	for _, run := range m.runsOf(series, runs[:0]) {
		into = append(into, MetricMetadataVersion{EffectiveFrom: run.from, Metadata: e.metricMetadata.value(run.id)})
		coalesced = coalesced || run.id&metadataCoalesced != 0
	}
	_, truncated := m.truncated[series.ref]
	return into, coalesced, truncated
}
