// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
)

// nativeMetadataEngine is a tenantEngine that keeps native metric metadata: the type, unit and help of each series,
// as change points over time.
type nativeMetadataEngine interface {
	// nativeMetricMetadata returns the metadata of the series matching any of the matcher sets, in label order, and
	// with a positive limit at most that many series.
	nativeMetricMetadata(ctx context.Context, matcherSets [][]*labels.Matcher, limit int) (nativeMetadataResult, error)
}

var _ nativeMetadataEngine = seriesstoreEngine{}

// nativeMetadataResult is native metric metadata, in the same shape from either engine.
type nativeMetadataResult struct {
	Series []nativeMetadataSeries
	// Limited reports whether more series matched than the limit let through.
	Limited bool
	// CoverageStart is when the metadata starts: the later of the head's min time and, as the seriesstore engine
	// doesn't persist native metadata, the first observation since it opened.
	CoverageStart int64
}

// nativeMetadataSeries is a series' metadata as change points, oldest first.
type nativeMetadataSeries struct {
	Labels   labels.Labels
	Versions []nativeMetricMetadataVersion
	// Truncated reports that the engine's cap on versions per series has evicted older ones.
	Truncated bool
	// Coalesced reports that changes over the seriesstore engine's limit were merged into one range.
	Coalesced bool
}

// nativeMetricMetadataVersion is a series' metric metadata from a sample time on.
type nativeMetricMetadataVersion struct {
	EffectiveFrom int64
	Metadata      metadata.Metadata
}

func (e seriesstoreEngine) nativeMetricMetadata(ctx context.Context, matcherSets [][]*labels.Matcher, limit int) (nativeMetadataResult, error) {
	series, limited, err := e.NativeMetricMetadata(ctx, matcherSets, limit)
	if err != nil {
		return nativeMetadataResult{}, err
	}
	result := nativeMetadataResult{
		Series:        make([]nativeMetadataSeries, 0, len(series)),
		Limited:       limited,
		CoverageStart: e.NativeMetricMetadataCoverageStart(),
	}
	for _, s := range series {
		versions := make([]nativeMetricMetadataVersion, 0, len(s.Versions))
		for _, v := range s.Versions {
			versions = append(versions, nativeMetricMetadataVersion{EffectiveFrom: v.EffectiveFrom, Metadata: v.Metadata})
		}
		result.Series = append(result.Series, nativeMetadataSeries{Labels: s.Labels, Versions: versions})
	}
	return result, nil
}
