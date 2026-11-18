// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"sync/atomic"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/metadata"
)

// The engine's native metadata in its head snapshot: snapshot.go's layout under a magic of its own, with each series'
// native state after the series' fields, and the engine's after the shards, before the exemplars.
//
// A series' native state is its metric metadata, then its resource: the current run's id (uvarint, 0 for none, and
// nothing else of it then) and start (i64), the older runs (uvarint count, each from i64 and id uvarint), whether it has
// changes (u8), then the change window, the coalesced run's end (i64 each), for metric metadata the coalesced run's
// latest observation (i64), and the count (uvarint); for metric metadata last, whether it's truncated (u8).
//
// The engine's is its metric metadata's coverage start (i64) and dictionary: the next id (uvarint), then the values
// (uvarint count, each id uvarint, and type, unit and help as bytes). Then its resources': the coverage start (i64), the
// identities' and the descriptions' dictionaries, like the metric metadata's with each value's encoding as bytes, the
// identities' versions (uvarint count, each the identity's id and uvarint count, each version from i64, description id
// uvarint and optional fields), the identities' changes (uvarint count, each id uvarint, window and coalesced run's end
// i64, and count uvarint), and the new identities' window (i64) and count (uvarint).
//
// A version's optional fields are a length (uvarint) and as many bytes, none so far: the room for its EntityRefs, which
// a reader that doesn't know them skips.
//
// Run starts are times, not offsets from their shard's base, so a restore takes them on a base of its own. Ids are
// kept, so the restored runs refer to the same values; what derives from the rest is rebuilt: the reverse index, the
// identities' series, the dictionaries' reference counts and their free ids.

// nativeSnapshotMagic is snapshotMagic with its S swapped for an N: the plain layout of that version, with the native
// sections. Derived, it follows the plain layout's version, whose older layouts the reader keeps reading; a version
// number of our own in the plain layout's scheme could alias one of its next.
var nativeSnapshotMagic = nativeMagicOf(snapshotMagic)

// nativeSnapshotFirstVersion is the plain layout the native sections were first written with: no older one has them.
const nativeSnapshotFirstVersion = 8

const (
	plainMagicPrefix  = "MIMIRHS"
	nativeMagicPrefix = "MIMIRHN"
)

func nativeMagicOf(plain []byte) []byte {
	if !bytes.HasPrefix(plain, []byte(plainMagicPrefix)) {
		panic(fmt.Sprintf("head snapshot magic %q doesn't start with %s", plain, plainMagicPrefix))
	}
	return append([]byte(nativeMagicPrefix), plain[len(plainMagicPrefix):]...)
}

// plainMagic reports whether magic is a native layout's, and makes it the plain layout's that it extends.
func plainMagic(magic []byte) bool {
	if !bytes.HasPrefix(magic, []byte(nativeMagicPrefix)) {
		return false
	}
	copy(magic, plainMagicPrefix)
	return true
}

// magic is the snapshot's: the native layout's when it has native metadata.
func (p snapshotPlan) magic() []byte {
	if p.native != nil {
		return nativeSnapshotMagic
	}
	return snapshotMagic
}

// nativeSnapshot writes an engine's native metadata into its head snapshot.
type nativeSnapshot struct {
	e *Engine
}

// nativeSnapshot is how the engine's next head snapshot writes its native metadata, nil when it has none: a tenant that
// never kept any is written in the plain layout, which builds without the native one read. Native metadata that comes
// after, while a checkpoint is written, is in what a restore of it replays.
func (e *Engine) nativeSnapshot() *nativeSnapshot {
	has := e.metadataObservedFrom.Load() != math.MaxInt64 || e.resourcesObservedFrom.Load() != math.MaxInt64
	for index := 0; !has && index < len(e.store.shards); index++ {
		state := e.store.shards[index]
		state.RLock()
		t := e.tenant(index)
		has = t != nil && (t.nativeMetadata != nil || t.nativeResources != nil)
		state.RUnlock()
	}
	if !has {
		return nil
	}
	return &nativeSnapshot{e: e}
}

// writeSeries writes the series' native state, with its shard locked: the shard's state of it is by its ref, which a
// restore gives it anew.
func (n *nativeSnapshot) writeSeries(w *snapshotWriter, t *tenant, entry *seriesEntry) error {
	series := &entry.series
	if err := w.uvarint(uint64(series.metadataID)); err != nil {
		return err
	}
	if series.metadataID != 0 {
		m := t.nativeMetadata
		if err := w.i64(m.start(series)); err != nil {
			return err
		}
		older := m.older[series.ref]
		if err := w.uvarint(uint64(len(older))); err != nil {
			return err
		}
		for _, run := range older {
			if err := errors.Join(w.i64(run.from), w.uvarint(uint64(run.id))); err != nil {
				return err
			}
		}
		c, changed := m.changes[series.ref]
		if err := w.u8(boolByte(changed)); err != nil {
			return err
		}
		if changed {
			if err := errors.Join(w.i64(c.window), w.i64(c.coalescedEnd), w.i64(c.latestAt), w.uvarint(uint64(c.count))); err != nil {
				return err
			}
		}
		_, truncated := m.truncated[series.ref]
		if err := w.u8(boolByte(truncated)); err != nil {
			return err
		}
	}
	if err := w.uvarint(uint64(series.resourceID)); err != nil || series.resourceID == 0 {
		return err
	}
	m := t.nativeResources
	if err := w.i64(m.start(series)); err != nil {
		return err
	}
	older := m.older[series.ref]
	if err := w.uvarint(uint64(len(older))); err != nil {
		return err
	}
	for _, run := range older {
		if err := errors.Join(w.i64(run.from), w.uvarint(uint64(run.id))); err != nil {
			return err
		}
	}
	c, changed := m.changes[series.ref]
	if err := w.u8(boolByte(changed)); err != nil {
		return err
	}
	if changed {
		return errors.Join(w.i64(c.window), w.i64(c.coalescedEnd), w.uvarint(uint64(c.count)))
	}
	return nil
}

// writeEngine writes the engine's native state, once the shards are written. While a checkpoint writes them, appends go
// on and may take new values, but no value goes before the next compaction, whose lock the snapshot holds: every value
// the written runs refer to is still there.
//
// A replay from the checkpoint goes over what the series written before an append didn't have of it. Observations
// change nothing that the engine's state has already, but what the limits refused is decided again, against this
// state: a new identity that was refused may then be taken, once the dictionary has it from a later observation, or
// the window of the newest new identity has room for it.
func (n *nativeSnapshot) writeEngine(w *snapshotWriter) error {
	e := n.e
	metadataNext, metadataValues := e.metricMetadata.image()
	resources := e.resources.image()
	if err := errors.Join(w.i64(e.metadataObservedFrom.Load()), w.uvarint(uint64(metadataNext)), w.uvarint(uint64(len(metadataValues)))); err != nil {
		return err
	}
	for _, id := range slices.Sorted(maps.Keys(metadataValues)) {
		m := metadataValues[id]
		if err := errors.Join(w.uvarint(uint64(id)), w.str(string(m.Type)), w.str(m.Unit), w.str(m.Help)); err != nil {
			return err
		}
	}
	if err := errors.Join(w.i64(e.resourcesObservedFrom.Load()), writeResourceDictionary(w, resources.identities), writeResourceDictionary(w, resources.descriptions)); err != nil {
		return err
	}
	if err := w.uvarint(uint64(len(resources.versions))); err != nil {
		return err
	}
	for _, identity := range slices.Sorted(maps.Keys(resources.versions)) {
		versions := resources.versions[identity]
		if err := errors.Join(w.uvarint(uint64(identity)), w.uvarint(uint64(len(versions)))); err != nil {
			return err
		}
		for _, v := range versions {
			// No optional fields yet.
			if err := errors.Join(w.i64(v.from), w.uvarint(uint64(v.id)), w.uvarint(0)); err != nil {
				return err
			}
		}
	}
	if err := w.uvarint(uint64(len(resources.changes))); err != nil {
		return err
	}
	for _, identity := range slices.Sorted(maps.Keys(resources.changes)) {
		c := resources.changes[identity]
		if err := errors.Join(w.uvarint(uint64(identity)), w.i64(c.window), w.i64(c.coalescedEnd), w.uvarint(uint64(c.count))); err != nil {
			return err
		}
	}
	return errors.Join(w.i64(resources.newWindow), w.uvarint(uint64(resources.newCount)))
}

func writeResourceDictionary(w *snapshotWriter, d resourceDictionaryImage) error {
	if err := errors.Join(w.uvarint(uint64(d.next)), w.uvarint(uint64(len(d.values)))); err != nil {
		return err
	}
	for _, id := range slices.Sorted(maps.Keys(d.values)) {
		if err := errors.Join(w.uvarint(uint64(id)), w.str(d.values[id])); err != nil {
			return err
		}
	}
	return nil
}

// image is the dictionary's next id and its values by id.
func (d *metricMetadataDictionary) image() (uint32, map[uint32]metadata.Metadata) {
	d.mu.Lock()
	defer d.mu.Unlock()
	values := make(map[uint32]metadata.Metadata, len(d.ids))
	for m, id := range d.ids {
		values[id] = m
	}
	return d.next, values
}

// resourceDictionaryImage is a resource dictionary's next id and its values' encodings by id.
type resourceDictionaryImage struct {
	next   uint32
	values map[uint32]string
}

// imageLocked is the dictionary's image, for a caller holding its lock.
func (d *resourceDictionary) imageLocked() resourceDictionaryImage {
	values := make(map[uint32]string, len(d.ids))
	for encoded, id := range d.ids {
		values[id] = encoded
	}
	return resourceDictionaryImage{next: d.next, values: values}
}

// resourcesImage is an engine's native resources' state but for their series: the dictionaries, the identities'
// versions and changes, and the new identities' window. A head snapshot has the coverage start besides.
type resourcesImage struct {
	identities, descriptions resourceDictionaryImage
	versions                 map[uint32][]resourceRun
	changes                  map[uint32]resourceChanges
	newWindow                int64
	newCount                 int
}

// image copies the resources' state, consistent between the identities' versions and the dictionaries: they're locked
// in the order intern locks them, the identities' dictionary before the state, and the descriptions' dictionary is
// never locked with another lock but here.
func (r *resources) image() resourcesImage {
	r.identities.mu.Lock()
	defer r.identities.mu.Unlock()
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	r.descriptions.mu.Lock()
	defer r.descriptions.mu.Unlock()
	versions := make(map[uint32][]resourceRun, len(r.state.versions))
	for identity, runs := range r.state.versions {
		versions[identity] = slices.Clone(runs)
	}
	return resourcesImage{
		identities:   r.identities.imageLocked(),
		descriptions: r.descriptions.imageLocked(),
		versions:     versions,
		changes:      maps.Clone(r.state.changes),
		newWindow:    r.state.newWindow,
		newCount:     r.state.newCount,
	}
}

// nativeSnapshotImage is the native metadata of a head snapshot, until the engine restored from it adopts it.
type nativeSnapshotImage struct {
	// By the shard's tenant read from the snapshot.
	tenants                     map[*tenant]*nativeTenantImage
	metadataFrom, resourcesFrom int64
	metadataNext                uint32
	metadataValues              map[uint32]metadata.Metadata
	resources                   resourcesImage
}

// nativeTenantImage is the native state of a shard's series, by the series' ids in the postings: a tenant read from a
// snapshot gives its series their ids in the order it reads them.
type nativeTenantImage struct {
	series []nativeSeriesImage
	// The shard's state of the series that have any.
	metadataOlder   map[uint32][]metadataRun
	metadataChanges map[uint32]metadataChanges
	truncated       map[uint32]struct{}
	resourceOlder   map[uint32][]resourceRun
	resourceChanges map[uint32]resourceChanges
}

// nativeSeriesImage is a series' current runs: their ids, 0 for none, and their starts.
type nativeSeriesImage struct {
	metadataID, resourceID       uint32
	metadataStart, resourceStart int64
}

func newNativeSnapshotImage(version int) (*nativeSnapshotImage, error) {
	if version < nativeSnapshotFirstVersion {
		return nil, errors.New("head snapshot has invalid magic")
	}
	return &nativeSnapshotImage{tenants: map[*tenant]*nativeTenantImage{}}, nil
}

// nativeReader reads a snapshot's native sections, keeping the first error.
type nativeReader struct {
	r   *snapshotReader
	err error
}

func (n *nativeReader) u8() uint8 {
	var value uint8
	if n.err == nil {
		value, n.err = n.r.u8()
	}
	return value
}

func (n *nativeReader) i64() int64 {
	var value int64
	if n.err == nil {
		value, n.err = n.r.i64()
	}
	return value
}

func (n *nativeReader) uvarint(maximum uint64) uint64 {
	var value uint64
	if n.err == nil {
		value, n.err = n.r.uvarint(maximum)
	}
	return value
}

func (n *nativeReader) id() uint32 {
	return uint32(n.uvarint(math.MaxUint32))
}

// bytes is a value's bytes, which the encodings of resources and the metric metadata's help needn't be UTF-8 for.
func (n *nativeReader) bytes() string {
	var value []byte
	if n.err == nil {
		value, n.err = n.r.readBytes()
	}
	return string(value)
}

// Bounds of a corrupt snapshot's counts, far over what's written.
const (
	maxNativeRuns   = 1 << 16
	maxNativeValues = 1 << 31
)

// readSeries reads the native state of the series readSeries just read into t.
func (img *nativeSnapshotImage) readSeries(r *snapshotReader, t *tenant) error {
	ti := img.tenants[t]
	if ti == nil {
		ti = &nativeTenantImage{metadataOlder: map[uint32][]metadataRun{}, metadataChanges: map[uint32]metadataChanges{}, truncated: map[uint32]struct{}{}, resourceOlder: map[uint32][]resourceRun{}, resourceChanges: map[uint32]resourceChanges{}}
		img.tenants[t] = ti
	}
	// The series has the last id in the postings, which is its index here.
	index := uint32(len(ti.series))
	if len(t.series.refs) != len(ti.series)+1 {
		return errors.New("head snapshot series out of order with their native state")
	}
	n := &nativeReader{r: r}
	var s nativeSeriesImage
	if s.metadataID = n.id(); s.metadataID != 0 {
		s.metadataStart = n.i64()
		var older []metadataRun
		for range n.uvarint(maxNativeRuns) {
			older = append(older, metadataRun{from: n.i64(), id: n.id()})
		}
		if len(older) > 0 {
			ti.metadataOlder[index] = older
		}
		if n.u8() == 1 {
			ti.metadataChanges[index] = metadataChanges{window: n.i64(), coalescedEnd: n.i64(), latestAt: n.i64(), count: int(n.uvarint(math.MaxInt32))}
		}
		if n.u8() == 1 {
			ti.truncated[index] = struct{}{}
		}
	}
	if s.resourceID = n.id(); s.resourceID != 0 {
		s.resourceStart = n.i64()
		var older []resourceRun
		for range n.uvarint(maxNativeRuns) {
			older = append(older, resourceRun{from: n.i64(), id: n.id()})
		}
		if len(older) > 0 {
			ti.resourceOlder[index] = older
		}
		if n.u8() == 1 {
			ti.resourceChanges[index] = resourceChanges{window: n.i64(), coalescedEnd: n.i64(), count: int(n.uvarint(math.MaxInt32))}
		}
	}
	ti.series = append(ti.series, s)
	return n.err
}

// readEngine reads the engine's native state, after the shards, and checks that every id the series and versions refer
// to has a value: a snapshot that doesn't is corrupt.
func (img *nativeSnapshotImage) readEngine(r *snapshotReader) error {
	n := &nativeReader{r: r}
	img.metadataFrom = n.i64()
	img.metadataNext = n.id()
	img.metadataValues = map[uint32]metadata.Metadata{}
	for range n.uvarint(maxNativeValues) {
		id := n.id()
		img.metadataValues[id] = metadata.Metadata{Type: model.MetricType(n.bytes()), Unit: n.bytes(), Help: n.bytes()}
	}
	img.resourcesFrom = n.i64()
	img.resources.identities = readResourceDictionary(n)
	img.resources.descriptions = readResourceDictionary(n)
	img.resources.versions = map[uint32][]resourceRun{}
	for range n.uvarint(maxNativeValues) {
		identity := n.id()
		var versions []resourceRun
		for range n.uvarint(maxNativeRuns) {
			versions = append(versions, resourceRun{from: n.i64(), id: n.id()})
			// The optional fields, which this build has none of.
			if skip := n.uvarint(64 << 20); n.err == nil {
				_, n.err = io.CopyN(io.Discard, r, int64(skip))
			}
		}
		img.resources.versions[identity] = versions
	}
	img.resources.changes = map[uint32]resourceChanges{}
	for range n.uvarint(maxNativeValues) {
		identity := n.id()
		img.resources.changes[identity] = resourceChanges{window: n.i64(), coalescedEnd: n.i64(), count: int(n.uvarint(math.MaxInt32))}
	}
	img.resources.newWindow = n.i64()
	img.resources.newCount = int(n.uvarint(math.MaxInt32))
	if n.err != nil {
		return n.err
	}
	return img.check()
}

func readResourceDictionary(n *nativeReader) resourceDictionaryImage {
	d := resourceDictionaryImage{next: n.id(), values: map[uint32]string{}}
	for range n.uvarint(maxNativeValues) {
		id := n.id()
		d.values[id] = n.bytes()
	}
	return d
}

// check reports a reference to an id without a value, or a value that couldn't have its id.
func (img *nativeSnapshotImage) check() error {
	if err := checkDictionary(img.metadataNext, metadataCoalesced, img.metadataValues); err != nil {
		return err
	}
	for _, d := range []resourceDictionaryImage{img.resources.identities, img.resources.descriptions} {
		if err := checkDictionary(d.next, resourceUnknown, d.values); err != nil {
			return err
		}
	}
	metadataID := func(id uint32) bool {
		_, ok := img.metadataValues[id&^metadataCoalesced]
		return ok
	}
	// A resource's id is 0 when it's unknown.
	resourceID := func(d resourceDictionaryImage, id uint32) bool {
		_, ok := d.values[id&^resourceUnknown]
		return ok || id&^resourceUnknown == 0
	}
	known := true
	for _, ti := range img.tenants {
		for _, s := range ti.series {
			known = known && (s.metadataID == 0 || metadataID(s.metadataID)) && (s.resourceID == 0 || resourceID(img.resources.identities, s.resourceID))
		}
		for _, runs := range ti.metadataOlder {
			for _, run := range runs {
				known = known && metadataID(run.id)
			}
		}
		for _, runs := range ti.resourceOlder {
			for _, run := range runs {
				known = known && resourceID(img.resources.identities, run.id)
			}
		}
	}
	for identity, versions := range img.resources.versions {
		_, ok := img.resources.identities.values[identity]
		known = known && ok
		for _, v := range versions {
			known = known && resourceID(img.resources.descriptions, v.id)
		}
	}
	if !known {
		return errors.New("head snapshot native metadata refers to a value it doesn't have")
	}
	return nil
}

// checkDictionary reports a dictionary's value whose id is 0 or isn't below next, a next past the flag ids have, and a
// value that two ids have.
func checkDictionary[V comparable](next, flag uint32, values map[uint32]V) error {
	if next == 0 || next > flag {
		return errors.New("head snapshot native dictionary has an invalid next id")
	}
	unique := make(map[V]struct{}, len(values))
	for id, value := range values {
		if id == 0 || id >= next {
			return errors.New("head snapshot native dictionary has an invalid id")
		}
		unique[value] = struct{}{}
	}
	if len(unique) != len(values) {
		return errors.New("head snapshot native dictionary has a value twice")
	}
	return nil
}

// adoptRestoredNative takes the native metadata of the snapshot the engine restored, once adoptRestored gave the
// series their refs: the dictionaries and the identities' state, then each series' runs, which take their references
// and are keyed by the series' new refs.
func (e *Engine) adoptRestoredNative(img *nativeSnapshotImage) error {
	for index, shard := range e.store.shards {
		if ti := img.tenants[shard.tenant.Load()]; ti != nil && e.tenant(index).series.len != len(ti.series) {
			return fmt.Errorf("head snapshot shard %d has %d series and the native state of %d", index, e.tenant(index).series.len, len(ti.series))
		}
	}
	e.metricMetadata.restore(img.metadataNext, img.metadataValues)
	e.resources.restore(img.resources)
	e.metadataObservedFrom.Store(img.metadataFrom)
	e.resourcesObservedFrom.Store(img.resourcesFrom)
	oooWindow := e.oooWindow.Load()
	for _, shard := range e.store.shards {
		shard.Lock()
		if t := shard.tenant.Load(); t != nil && img.tenants[t] != nil {
			e.adoptNativeSeriesLocked(t, img.tenants[t], oooWindow)
		}
		shard.Unlock()
	}
	return nil
}

// adoptNativeSeriesLocked gives the shard's series their native state, with the shard locked. The shard's bases are
// below every restored start by the margin a shard's base has below its first observation.
func (e *Engine) adoptNativeSeriesLocked(t *tenant, ti *nativeTenantImage, oooWindow int64) {
	metadataFrom, resourcesFrom := int64(math.MaxInt64), int64(math.MaxInt64)
	for _, s := range ti.series {
		if s.metadataID != 0 {
			metadataFrom = min(metadataFrom, s.metadataStart)
		}
		if s.resourceID != 0 {
			resourcesFrom = min(resourcesFrom, s.resourceStart)
		}
	}
	var m *shardMetadata
	if metadataFrom != math.MaxInt64 {
		m = e.shardMetadataLocked(t)
		m.base, m.baseSet = satSub(metadataFrom, metadataBaseMargin(oooWindow)), true
	}
	var res *shardResources
	if resourcesFrom != math.MaxInt64 {
		if t.nativeResources == nil {
			t.nativeResources = newShardResources(e.resources)
		}
		res = t.nativeResources
		res.base, res.baseSet = satSub(resourcesFrom, metadataBaseMargin(oooWindow)), true
	}
	t.series.forEach(func(entry *seriesEntry) {
		s, series := ti.series[entry.id], &entry.series
		if s.metadataID != 0 {
			e.metricMetadata.acquire(s.metadataID)
			series.metadataID, series.metadataStart = s.metadataID, m.offset(s.metadataStart, oooWindow)
			m.series++
			if older := ti.metadataOlder[entry.id]; len(older) > 0 {
				for _, run := range older {
					e.metricMetadata.acquire(run.id)
				}
				m.older[series.ref] = older
				m.runs += len(older)
			}
			if c, ok := ti.metadataChanges[entry.id]; ok {
				m.changes[series.ref] = c
			}
			if _, ok := ti.truncated[entry.id]; ok {
				m.truncated[series.ref] = struct{}{}
			}
		}
		if s.resourceID != 0 {
			e.resources.identities.acquire(s.resourceID)
			e.resources.addSeries(s.resourceID, series.ref)
			series.resourceID, series.resourceStart = s.resourceID, res.offset(s.resourceStart, oooWindow)
			res.series++
			if older := ti.resourceOlder[entry.id]; len(older) > 0 {
				for _, run := range older {
					e.resources.identities.acquire(run.id)
					e.resources.addSeries(run.id, series.ref)
				}
				res.older[series.ref] = older
				res.runs += len(older)
			}
			if c, ok := ti.resourceChanges[entry.id]; ok {
				res.changes[series.ref] = c
			}
		}
	})
}

// restore takes the values of a snapshot's dictionary, by their ids, without references: the restored runs acquire
// theirs. The ids below next without a value are free.
func (d *metricMetadataDictionary) restore(next uint32, values map[uint32]metadata.Metadata) {
	d.mu.Lock()
	defer d.mu.Unlock()
	pages := *d.pages.Load()
	for len(pages)*metricMetadataPageSize < int(next) {
		pages = append(slices.Clip(pages), &metricMetadataPage{})
	}
	d.pages.Store(&pages)
	for id, m := range values {
		value := d.entry(id)
		value.metadata, value.bytes = m, int64(len(m.Type)+len(m.Unit)+len(m.Help))
		d.ids[m] = id
		d.values.Add(1)
		d.bytes.Add(value.bytes)
	}
	d.next = next
	for id := uint32(1); id < next; id++ {
		if _, ok := values[id]; !ok {
			d.free = append(d.free, id)
		}
	}
}

// restore takes the values of a snapshot's dictionary, like the metric metadata dictionary's restore, with their
// bytes in total.
func (d *resourceDictionary) restore(img resourceDictionaryImage, total *atomic.Int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	pages := *d.pages.Load()
	for len(pages)*resourcePageSize < int(img.next) {
		pages = append(slices.Clip(pages), &resourcePage{})
	}
	d.pages.Store(&pages)
	for id, encoded := range img.values {
		d.entry(id).encoded = encoded
		d.ids[encoded] = id
		d.values.Add(1)
		d.bytes.Add(int64(len(encoded)))
		total.Add(int64(len(encoded)))
	}
	d.next = img.next
	for id := uint32(1); id < img.next; id++ {
		if _, ok := img.values[id]; !ok {
			d.free = append(d.free, id)
		}
	}
}

// restore takes a snapshot's resources' state but for their series, and rebuilds the reverse index from the
// identities and their versions' descriptions, which the versions hold references to.
func (r *resources) restore(img resourcesImage) {
	r.identities.restore(img.identities, &r.bytes)
	r.descriptions.restore(img.descriptions, &r.bytes)
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	r.state.versions, r.state.changes = img.versions, img.changes
	r.state.newWindow, r.state.newCount = img.newWindow, img.newCount
	for identity := range img.identities.values {
		r.indexIdentityLocked(identity, 0)
		for _, v := range img.versions[identity] {
			r.descriptions.acquire(v.id)
			r.indexIdentityLocked(identity, v.id)
		}
	}
}
