// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// resourceStep gives the resource to append a series with, as AppendV2Options.Resource takes it, given the series'
// resource entry and its reference, 0 for a new series. It's nil to leave the resource out, as an engine can for one
// the series already has.
type resourceStep interface {
	resource(entry *mimirpb.ResourceEntry, ref storage.SeriesRef) *storage.ResourceContext
}

// nativeResourcesEngine is a tenantEngine that keeps native resources: the OTel resource attributes of each series, as
// versions over time.
type nativeResourcesEngine interface {
	// nativeResources returns the resource versions in force during [mint, maxt] of the series matching any of the
	// matcher sets, in label order, and with a positive limit at most that many series. A version is in force from its
	// EffectiveFrom until the next one's.
	nativeResources(ctx context.Context, matcherSets [][]*labels.Matcher, mint, maxt int64, limit int) (nativeResourcesResult, error)
}

// nativeResourcesResult is native resources, in the same shape from either engine.
type nativeResourcesResult struct {
	Series []nativeResourcesSeries
	// Limited reports whether more series matched than the limit let through.
	Limited bool
	// CoverageStart is when the resources start: the later of the head's min time and, as the seriesstore engine
	// doesn't persist them, the first observation since it opened.
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
