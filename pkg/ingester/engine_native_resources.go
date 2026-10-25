// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"encoding/base64"
	"errors"
	"slices"
	"strconv"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/seriesmetadata"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// resourceStep gives the resource to append a series with, as AppendV2Options.Resource takes it, given the series'
// resource entry and its reference, 0 for a new series. It's nil to leave the resource out, as an engine can for one
// the series already has.
type resourceStep interface {
	resource(entry *mimirpb.ResourceEntry, ref storage.SeriesRef) *storage.ResourceContext
}

// isIdentifyingResourceAttribute reports whether the attribute identifies a resource without EntityRefs: the service
// attributes, which job and instance derive from, as #17740 splits them.
func isIdentifyingResourceAttribute(key string) bool {
	return key == "service.name" || key == "service.namespace" || key == "service.instance.id"
}

// resourceAttributeString is an attribute's value as a string, as OTLP's AsString gives it, which is how #17740 stores
// values.
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

// splitResourceAttributes returns the entry's identifying and descriptive attributes, with their values as strings, in
// freshly allocated maps. A map is nil without any attributes.
func splitResourceAttributes(entry *mimirpb.ResourceEntry) (identifying, descriptive map[string]string) {
	for _, a := range entry.Attributes {
		m := &descriptive
		if isIdentifyingResourceAttribute(a.Key) {
			m = &identifying
		}
		if *m == nil {
			*m = make(map[string]string, len(entry.Attributes))
		}
		(*m)[a.Key] = resourceAttributeString(a)
	}
	return identifying, descriptive
}

// indexedResourceAttributes is the TSDB's set of the descriptive attributes it indexes, nil for none.
func indexedResourceAttributes(names []string) map[string]struct{} {
	if len(names) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}
	return set
}

// tsdbResources is the TSDB's resource step. It builds each resource once per batch, in maps that the head keeps, and
// leaves out a resource whose content the series already has.
type tsdbResources struct {
	db      *tsdb.DB
	built   map[*mimirpb.ResourceEntry]tsdbResource
	keysBuf []string
}

type tsdbResource struct {
	context     *storage.ResourceContext
	contentHash uint64
}

func newTSDBResources(db *tsdb.DB) *tsdbResources {
	return &tsdbResources{db: db, built: map[*mimirpb.ResourceEntry]tsdbResource{}}
}

func (r *tsdbResources) resource(entry *mimirpb.ResourceEntry, ref storage.SeriesRef) *storage.ResourceContext {
	built, ok := r.built[entry]
	if !ok {
		identifying, descriptive := splitResourceAttributes(entry)
		built.context = &storage.ResourceContext{Identifying: identifying, Descriptive: descriptive}
		built.contentHash, r.keysBuf = seriesmetadata.ResourceContentHash(identifying, descriptive, r.keysBuf)
		r.built[entry] = built
	}
	if ref != 0 && r.db.ResourceHasContentHashByRef(ref, built.contentHash) {
		return nil
	}
	return built.context
}

// nativeResourcesEngine is a tenantEngine that keeps native resources: the OTel resource attributes of each series, as
// versions over time.
type nativeResourcesEngine interface {
	// nativeResources returns the resource versions in force during [mint, maxt] of the series matching any of the
	// matcher sets, in label order, and with a positive limit at most that many series. A version is in force from its
	// EffectiveFrom until the next one's. It returns errNativeResourcesDisabled when the engine keeps none.
	nativeResources(ctx context.Context, matcherSets [][]*labels.Matcher, mint, maxt int64, limit int) (nativeResourcesResult, error)
}

var _ nativeResourcesEngine = prometheusEngine{}

var errNativeResourcesDisabled = tsdb.ErrNativeMetadataDisabled

// nativeResourcesResult is native resources, in the same shape from either engine.
type nativeResourcesResult struct {
	Series []nativeResourcesSeries
	// Limited reports whether more series matched than the limit let through.
	Limited bool
	// CoverageStart is when the resources start: the later of the head's min time and, as neither engine persists them,
	// its max time when the TSDB opened, or the first observation since the seriesstore engine did.
	CoverageStart int64
}

// nativeResourcesSeries is a series' resource versions as change points, oldest first.
type nativeResourcesSeries struct {
	Labels   labels.Labels
	Versions []nativeResourceVersion
	// Coalesced reports that changes over the seriesstore engine's limits were merged into a range of unknown
	// resource.
	Coalesced bool
}

// nativeResourceVersion is a resource's attributes from a sample time on, with values as strings.
type nativeResourceVersion struct {
	EffectiveFrom            int64
	Identifying, Descriptive map[string]string
}

func (e prometheusEngine) nativeResources(ctx context.Context, matcherSets [][]*labels.Matcher, mint, maxt int64, limit int) (nativeResourcesResult, error) {
	if !e.nativeResourcesEnabled {
		return nativeResourcesResult{}, errNativeResourcesDisabled
	}
	head := e.DB.Head()
	reader, err := head.SeriesMetadata()
	if err != nil {
		return nativeResourcesResult{}, err
	}
	defer reader.Close()
	ir, err := head.Index()
	if err != nil {
		return nativeResourcesResult{}, err
	}
	defer ir.Close()

	var refs []storage.SeriesRef
	for _, matchers := range matcherSets {
		postings, err := tsdb.PostingsForMatchers(ctx, ir, matchers...)
		if err != nil {
			return nativeResourcesResult{}, err
		}
		for postings.Next() {
			refs = append(refs, postings.At())
		}
		if err := postings.Err(); err != nil {
			return nativeResourcesResult{}, err
		}
	}
	slices.Sort(refs)
	refs = slices.Compact(refs)

	result := nativeResourcesResult{CoverageStart: max(head.MinTime(), e.nativeMetadataSince)}
	var builder labels.ScratchBuilder
	for _, ref := range refs {
		if err := ir.Series(ref, &builder, nil); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return nativeResourcesResult{}, err
		}
		lset := builder.Labels()
		versioned, ok := reader.GetVersionedResource(labels.StableHash(lset))
		if !ok {
			continue
		}
		// #17740 keeps each version's observed range, and an unchanged resource doesn't extend it, so a version is in
		// force until the next one, as its VersionAt has it.
		var versions []nativeResourceVersion
		for i, v := range versioned.Versions {
			if v.MinTime > maxt || (i+1 < len(versioned.Versions) && versioned.Versions[i+1].MinTime <= mint) {
				continue
			}
			versions = append(versions, nativeResourceVersion{EffectiveFrom: v.MinTime, Identifying: v.Identifying, Descriptive: v.Descriptive})
		}
		if len(versions) > 0 {
			result.Series = append(result.Series, nativeResourcesSeries{Labels: lset, Versions: versions})
		}
	}
	slices.SortFunc(result.Series, func(a, b nativeResourcesSeries) int { return labels.Compare(a.Labels, b.Labels) })
	if limit > 0 && len(result.Series) > limit {
		result.Series, result.Limited = result.Series[:limit], true
	}
	return result, nil
}
