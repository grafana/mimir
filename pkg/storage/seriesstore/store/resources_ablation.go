// SPDX-License-Identifier: AGPL-3.0-only

//go:build native_resources_ablation

package store

import (
	"encoding/base64"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/seriesmetadata"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// The native metadata prototype's Phase 3 ablation: the engine's native resources kept in #17740's in-memory store, as
// the TSDB's head keeps them, with each mechanism of #17740's that the agreed classification calls a choice:
//   - a series' versions in a 256-way MemStore, keyed by the series' labels.StableHash, which the engine keeps as
//     shardHash;
//   - content deduplicated by its hash;
//   - the reverse index, kept up to date from the engine's open, with the configured descriptive attributes;
//   - the content-hash shortcut, which leaves out a resource the series' latest version has.
//
// #17740's appender keeps one update per series per appender, which the engine's batches don't reproduce: that's an
// intended difference. #17740 has no limits, keeps a series' versions until the series goes and never reclaims
// content, and keeps values as strings, as the TSDB's resource step gives them.
//
// A series' marker of having a resource, like #17740's non-zero stableHash, takes the byte of padding the Series has
// without one, so the build's series are the size of the engine's without a resource reference.

// resources is an engine's native resources, in #17740's store.
type resources struct {
	meta  *seriesmetadata.MemSeriesMetadata
	stats struct {
		// Lookups of a resource by a batch's memo, and the ones it didn't have yet.
		memoHits, memoMisses atomic.Uint64
	}
}

// newResources returns an engine's native resources, whose reverse index has the indexed descriptive attributes. The
// store has no limits, so onLimited is never called.
func newResources(indexed []string, _ func(ResourceLimit)) *resources {
	meta := seriesmetadata.NewMemSeriesMetadata()
	var set map[string]struct{}
	if len(indexed) > 0 {
		set = make(map[string]struct{}, len(indexed))
		for _, name := range indexed {
			set[name] = struct{}{}
		}
	}
	meta.SetIndexedResourceAttrs(set)
	meta.InitResourceAttrIndex()
	return &resources{meta: meta}
}

// trim does nothing: #17740 keeps a series' versions until the series goes.
func (r *resources) trim(int64) {}

// shardResources is a shard's part of an engine's native resources, guarded by the shard's lock.
type shardResources struct {
	r *resources
	// The series with a resource.
	series  int
	keysBuf []string
}

// seriesResource is whether a series has a resource in the store, under its shardHash.
type seriesResource struct {
	hasResource bool
}

func newShardResources(r *resources) *shardResources {
	return &shardResources{r: r}
}

// observe commits the observation's resource to the series from t on, as the head's commit does, unless the series'
// latest version has its content.
func (m *shardResources) observe(series *Series, o *resourceObservation, t int64) {
	meta := m.r.meta
	if series.hasResource && meta.ResourceHasContentHash(series.shardHash, o.contentHash) {
		return
	}
	var changed bool
	var old, cur *seriesmetadata.VersionedResource
	changed, old, cur, m.keysBuf = seriesmetadata.CommitResourceToStoreReusableWithRef(meta.ResourceStore(), series.shardHash, seriesmetadata.ResourceCommitData{
		Identifying: o.identifying,
		Descriptive: o.descriptive,
		MinTime:     t,
		MaxTime:     t,
		// The memo builds the maps for the batch, and never reuses them.
		Owned: true,
	}, series.ref, m.keysBuf)
	if !series.hasResource {
		series.hasResource = true
		m.series++
	}
	if changed {
		meta.UpdateResourceAttrIndex(series.shardHash, old, cur)
	}
}

// drop gives up series' resource, as the series leaves the engine's memory, as the head's cleanupSharedMetadata does.
func (m *shardResources) drop(series *Series) {
	if !series.hasResource {
		return
	}
	meta := m.r.meta
	if versioned, ok := meta.GetVersionedResource(series.shardHash); ok {
		meta.RemoveFromResourceAttrIndex(series.shardHash, versioned)
	}
	meta.DeleteResource(series.shardHash)
	series.hasResource = false
	m.series--
}

// has reports whether series has a resource.
func (m *shardResources) has(series *Series) bool {
	return series.hasResource
}

// resourceObservation is a batch's resource as #17740 keeps it: its identifying and descriptive attributes, with values
// as strings, in maps that the store may keep, and their content hash.
type resourceObservation struct {
	identifying, descriptive map[string]string
	contentHash              uint64
}

// resourceMemo is a batch's resources, by the record's resource ref or by entry.
type resourceMemo struct {
	r         *resources
	byRef     map[uint32]*resourceObservation
	byEntry   map[*mimirpb.ResourceEntry]*resourceObservation
	byContext map[*storage.ResourceContext]*resourceObservation
	keysBuf   []string
	scratch   []mimirpb.ResourceAttribute
}

func (e *Engine) newResourceMemo() *resourceMemo {
	return &resourceMemo{r: e.resources}
}

// resolve builds the observation of attributes, as the TSDB's resource step does.
func (m *resourceMemo) resolve(attributes []mimirpb.ResourceAttribute) *resourceObservation {
	m.r.stats.memoMisses.Add(1)
	o := &resourceObservation{}
	for _, a := range attributes {
		into := &o.descriptive
		if isIdentifyingResourceAttribute(a.Key) {
			into = &o.identifying
		}
		if *into == nil {
			*into = make(map[string]string, len(attributes))
		}
		(*into)[a.Key] = resourceAttributeString(a)
	}
	o.contentHash, m.keysBuf = seriesmetadata.ResourceContentHash(o.identifying, o.descriptive, m.keysBuf)
	return o
}

// byRecordRef is the observation of a record's resource, by its ref into the record's table, whose entry decodes from
// encoded.
func (m *resourceMemo) byRecordRef(ref uint32, encoded []byte, _ int64) *resourceObservation {
	if o, ok := m.byRef[ref]; ok {
		m.r.stats.memoHits.Add(1)
		return o
	}
	var entry mimirpb.ResourceEntry
	if err := entry.Unmarshal(encoded); err != nil {
		return nil
	}
	o := m.resolve(entry.Attributes)
	if m.byRef == nil {
		m.byRef = map[uint32]*resourceObservation{}
	}
	m.byRef[ref] = o
	return o
}

// byResourceEntry is the observation of a decoded series' resource.
func (m *resourceMemo) byResourceEntry(entry *mimirpb.ResourceEntry, _ int64) *resourceObservation {
	if o, ok := m.byEntry[entry]; ok {
		m.r.stats.memoHits.Add(1)
		return o
	}
	o := m.resolve(entry.Attributes)
	if m.byEntry == nil {
		m.byEntry = map[*mimirpb.ResourceEntry]*resourceObservation{}
	}
	m.byEntry[entry] = o
	return o
}

// byResourceContext is the observation of the resource an appender got, from its typed attributes: without them,
// it's none.
func (m *resourceMemo) byResourceContext(rc *storage.ResourceContext, _ int64) *resourceObservation {
	if len(rc.Attributes) == 0 {
		return nil
	}
	if o, ok := m.byContext[rc]; ok {
		m.r.stats.memoHits.Add(1)
		return o
	}
	m.scratch = m.scratch[:0]
	for _, a := range rc.Attributes {
		m.scratch = append(m.scratch, mimirpb.ResourceAttribute{
			Key: a.Key, Type: mimirpb.ResourceAttribute_Type(a.Type), StringValue: a.String, BoolValue: a.Bool, IntValue: a.Int, DoubleValue: a.Double, BytesValue: a.Bytes,
		})
	}
	o := m.resolve(m.scratch)
	if m.byContext == nil {
		m.byContext = map[*storage.ResourceContext]*resourceObservation{}
	}
	m.byContext[rc] = o
	return o
}

// release forgets the batch's observations, whose maps the store may keep.
func (m *resourceMemo) release() {
	clear(m.byRef)
	clear(m.byEntry)
	clear(m.byContext)
}

// resourceAttributeString is an attribute's value as a string, as the ingester's TSDB resource step gives it.
func resourceAttributeString(a mimirpb.ResourceAttribute) string {
	switch a.Type {
	case mimirpb.RESOURCE_ATTRIBUTE_BOOL:
		return strconv.FormatBool(a.BoolValue)
	case mimirpb.RESOURCE_ATTRIBUTE_INT:
		return strconv.FormatInt(a.IntValue, 10)
	case mimirpb.RESOURCE_ATTRIBUTE_DOUBLE:
		return pcommon.NewValueDouble(a.DoubleValue).AsString()
	case mimirpb.RESOURCE_ATTRIBUTE_BYTES:
		return base64.StdEncoding.EncodeToString(a.BytesValue)
	default:
		return a.StringValue
	}
}

// observeResourceLocked records that series has the observation's resource from timestamp on, for a caller holding
// its shard's lock.
func (e *Engine) observeResourceLocked(t *tenant, series *Series, o *resourceObservation, timestamp int64) {
	if o == nil {
		return
	}
	if !series.hasResource {
		e.resourceObservedAt(timestamp)
	}
	if t.nativeResources == nil {
		t.nativeResources = newShardResources(e.resources)
	}
	t.nativeResources.observe(series, o, timestamp)
}

// trimResourcesLocked does nothing: #17740 keeps a series' versions until the series goes.
func (e *Engine) trimResourcesLocked(*tenant, int64, int64) {}

// SetResourceLimits does nothing: #17740 has no limits.
func (e *Engine) SetResourceLimits(ResourceLimits) {}

// resourceVersionsLocked appends the series' resource versions in force during [mint, maxt], oldest first, as the
// TSDB's engine extension reads them: each version in force from its MinTime until the next one's.
func (e *Engine) resourceVersionsLocked(m *shardResources, series *Series, mint, maxt int64, into []ResourceVersion) []ResourceVersion {
	versioned, ok := m.r.meta.GetVersionedResource(series.shardHash)
	if !ok {
		return into
	}
	for i, v := range versioned.Versions {
		if v.MinTime > maxt || (i+1 < len(versioned.Versions) && versioned.Versions[i+1].MinTime <= mint) {
			continue
		}
		into = append(into, ResourceVersion{EffectiveFrom: v.MinTime, Identifying: stringAttributes(v.Identifying), Descriptive: stringAttributes(v.Descriptive)})
	}
	return into
}

// stringAttributes is attributes as string attributes, by key; nil without any.
func stringAttributes(attributes map[string]string) []mimirpb.ResourceAttribute {
	if len(attributes) == 0 {
		return nil
	}
	out := make([]mimirpb.ResourceAttribute, 0, len(attributes))
	for key, value := range attributes {
		out = append(out, mimirpb.ResourceAttribute{Key: key, StringValue: value})
	}
	slices.SortFunc(out, func(a, b mimirpb.ResourceAttribute) int { return strings.Compare(a.Key, b.Key) })
	return out
}

// ResourceStats returns the engine's native resources as its metrics report them: the store's series, as runs too,
// its distinct contents, as descriptions, its versions, and the memo's lookups. The store keeps no identities apart
// and doesn't count bytes.
func (e *Engine) ResourceStats() ResourceStats {
	r := e.resources
	series := int64(r.meta.TotalResources())
	return ResourceStats{
		Series:       series,
		Runs:         series,
		Descriptions: int64(r.meta.ResourceStore().TotalCanonical()),
		Versions:     int64(r.meta.TotalResourceVersions()),
		MemoHits:     r.stats.memoHits.Load(),
		MemoMisses:   r.stats.memoMisses.Load(),
	}
}
