// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"encoding/base64"
	"strconv"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/store"
)

// resourceStep gives the resource to append a series with, as AppendV2Options.Resource takes it, given the series'
// resource entry and its reference, 0 for a new series. It's nil to leave the resource out, as an engine can for one
// the series already has.
type resourceStep interface {
	resource(entry *mimirpb.ResourceEntry, ref storage.SeriesRef) *storage.ResourceContext
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

// nativeResourcesEngine is a tenantEngine that keeps native resources: the OTel resource attributes of each series, as
// versions over time.
type nativeResourcesEngine interface {
	// nativeResources returns the resource versions in force during [mint, maxt] of the series matching any of the
	// matcher sets, in label order, and with a positive limit at most that many series. A version is in force from its
	// EffectiveFrom until the next one's. With a positive maxSeries, it fails with errNativeLookupTooManySeries once
	// more series with resources than that match. opts are the tenant's optional paths of the lookup, which an engine
	// may not have: with Unsorted and no limit, the series may come in any order.
	nativeResources(ctx context.Context, matcherSets [][]*labels.Matcher, mint, maxt int64, limit, maxSeries int, opts store.NativeResourcesOptions) (nativeResourcesResult, error)
}

var _ nativeResourcesEngine = seriesstoreEngine{}

// nativeResourcesResult is native resources, in the same shape from either engine.
type nativeResourcesResult struct {
	Series []nativeResourcesSeries
	// Limited reports whether more series matched than the limit let through.
	Limited bool
	// CoverageStart is when the resources start: the later of the head's min time and the first observation, which the
	// seriesstore engine keeps in its snapshots, and starts again when the tenant's flag goes off.
	CoverageStart int64
	// SeriesVisited is how many series with resources matched, whatever the limit let through.
	SeriesVisited int
}

// nativeResourcesSeries is a series' resource versions as change points, oldest first.
type nativeResourcesSeries struct {
	Labels   labels.Labels
	Versions []nativeResourceVersion
	// Coalesced reports that changes over the seriesstore engine's limits were merged into a range of unknown
	// resource.
	Coalesced bool
	// NewestSample is the timestamp of the series' newest sample the engine holds, which its last version lasts until,
	// or math.MinInt64 when it holds none.
	NewestSample int64
}

// nativeResourceVersion is a resource's attributes from a sample time on, with values as strings.
type nativeResourceVersion struct {
	EffectiveFrom            int64
	Identifying, Descriptive map[string]string
}

// seriesstoreResources is the seriesstore engine's resource step: its appender takes a series' resource with its types,
// so it builds each resource once per batch, as typed attributes only.
type seriesstoreResources struct {
	built map[*mimirpb.ResourceEntry]*storage.ResourceContext
}

func newSeriesstoreResources() *seriesstoreResources {
	return &seriesstoreResources{built: map[*mimirpb.ResourceEntry]*storage.ResourceContext{}}
}

func (r *seriesstoreResources) resource(entry *mimirpb.ResourceEntry, _ storage.SeriesRef) *storage.ResourceContext {
	if built, ok := r.built[entry]; ok {
		return built
	}
	built := &storage.ResourceContext{Attributes: make([]storage.ResourceAttribute, len(entry.Attributes))}
	for i, a := range entry.Attributes {
		built.Attributes[i] = storage.ResourceAttribute{
			Key: a.Key, Type: storage.ResourceAttributeType(a.Type), String: a.StringValue, Bool: a.BoolValue, Int: a.IntValue, Double: a.DoubleValue, Bytes: a.BytesValue,
		}
	}
	r.built[entry] = built
	return built
}

func (e seriesstoreEngine) nativeResources(ctx context.Context, matcherSets [][]*labels.Matcher, mint, maxt int64, limit, maxSeries int, opts store.NativeResourcesOptions) (nativeResourcesResult, error) {
	series, limited, visited, err := e.NativeResourcesCapped(ctx, matcherSets, mint, maxt, limit, maxSeries, opts)
	if err != nil {
		return nativeResourcesResult{SeriesVisited: visited}, err
	}
	result := nativeResourcesResult{Series: make([]nativeResourcesSeries, 0, len(series)), Limited: limited, CoverageStart: e.NativeResourcesCoverageStart(), SeriesVisited: visited}
	for _, s := range series {
		out := nativeResourcesSeries{Labels: s.Labels, Versions: make([]nativeResourceVersion, 0, len(s.Versions)), NewestSample: s.NewestSample}
		for _, v := range s.Versions {
			out.Coalesced = out.Coalesced || v.Coalesced
			out.Versions = append(out.Versions, nativeResourceVersion{
				EffectiveFrom: v.EffectiveFrom,
				Identifying:   resourceAttributesAsStrings(v.Identifying),
				Descriptive:   resourceAttributesAsStrings(v.Descriptive),
			})
		}
		result.Series = append(result.Series, out)
	}
	return result, nil
}

// resourceAttributesAsStrings is attributes as #17740 keeps them, with values as strings; nil without any.
func resourceAttributesAsStrings(attributes []mimirpb.ResourceAttribute) map[string]string {
	if len(attributes) == 0 {
		return nil
	}
	out := make(map[string]string, len(attributes))
	for _, a := range attributes {
		out[a.Key] = resourceAttributeString(a)
	}
	return out
}
