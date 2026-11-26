// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"

	"github.com/grafana/mimir/pkg/storage/seriesstore/store"
)

// nativeMetadataEngine is a tenantEngine that keeps native metric metadata: the type, unit and help of each series,
// as change points over time.
type nativeMetadataEngine interface {
	// nativeMetricMetadata returns the metadata of the series matching any of the matcher sets, in label order, and
	// with a positive limit at most that many series. With a positive maxSeries, it fails with
	// errNativeLookupTooManySeries once more series with metadata than that match.
	nativeMetricMetadata(ctx context.Context, matcherSets [][]*labels.Matcher, limit, maxSeries int) (nativeMetadataResult, error)
}

// errNativeLookupTooManySeries is the error of a lookup of native metadata that more series matched than its cap.
var errNativeLookupTooManySeries = store.ErrLookupTooManySeries

var _ nativeMetadataEngine = seriesstoreEngine{}

// nativeMetadataResult is native metric metadata, in the same shape from either engine.
type nativeMetadataResult struct {
	Series []nativeMetadataSeries
	// Limited reports whether more series matched than the limit let through.
	Limited bool
	// CoverageStart is when the metadata starts: the later of the head's min time and the first observation, which the
	// seriesstore engine keeps in its snapshots, and starts again when the tenant's flag goes off.
	CoverageStart int64
	// SeriesVisited is how many series with metadata matched, whatever the limit let through.
	SeriesVisited int
}

// nativeMetadataSeries is a series' metadata as change points, oldest first.
type nativeMetadataSeries struct {
	Labels   labels.Labels
	Versions []nativeMetricMetadataVersion
	// Truncated reports that the engine's cap on versions per series has evicted older ones.
	Truncated bool
	// Coalesced reports that changes over the seriesstore engine's limit were merged into one range.
	Coalesced bool
	// NewestSample is the timestamp of the series' newest sample the engine holds, which its last version lasts until,
	// or math.MinInt64 when it holds none.
	NewestSample int64
}

// nativeMetricMetadataVersion is a series' metric metadata from a sample time on.
type nativeMetricMetadataVersion struct {
	EffectiveFrom int64
	Metadata      metadata.Metadata
}

func (e seriesstoreEngine) nativeMetricMetadata(ctx context.Context, matcherSets [][]*labels.Matcher, limit, maxSeries int) (nativeMetadataResult, error) {
	series, limited, visited, err := e.NativeMetricMetadataCapped(ctx, matcherSets, limit, maxSeries)
	if err != nil {
		return nativeMetadataResult{SeriesVisited: visited}, err
	}
	result := nativeMetadataResult{
		Series:        make([]nativeMetadataSeries, 0, len(series)),
		Limited:       limited,
		CoverageStart: e.NativeMetricMetadataCoverageStart(),
		SeriesVisited: visited,
	}
	for _, s := range series {
		versions := make([]nativeMetricMetadataVersion, 0, len(s.Versions))
		for _, v := range s.Versions {
			versions = append(versions, nativeMetricMetadataVersion{EffectiveFrom: v.EffectiveFrom, Metadata: v.Metadata})
		}
		result.Series = append(result.Series, nativeMetadataSeries{Labels: s.Labels, Versions: versions, Truncated: s.Truncated, Coalesced: s.Coalesced, NewestSample: s.NewestSample})
	}
	return result, nil
}
