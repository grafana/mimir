// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
)

// nativeMetadataEngine is a tenantEngine that keeps native metric metadata: the type, unit and help of each series,
// as change points over time.
type nativeMetadataEngine interface {
	// nativeMetricMetadata returns the metadata of the series matching any of the matcher sets, in label order, and
	// with a positive limit at most that many series. It returns tsdb.ErrNativeMetadataDisabled when the engine
	// keeps none.
	nativeMetricMetadata(ctx context.Context, matcherSets [][]*labels.Matcher, limit int) (nativeMetadataResult, error)
}

var (
	_ nativeMetadataEngine = prometheusEngine{}
	_ nativeMetadataEngine = seriesstoreEngine{}
)

// nativeMetadataResult is native metric metadata, in the same shape from either engine.
type nativeMetadataResult struct {
	Series []nativeMetadataSeries
	// Limited reports whether more series matched than the limit let through.
	Limited bool
	// CoverageStart is when the metadata starts: the later of the head's min time and its max time when the engine
	// opened, since neither engine persists native metadata.
	CoverageStart int64
}

// nativeMetadataSeries is a series' metadata as change points, oldest first.
type nativeMetadataSeries struct {
	Labels   labels.Labels
	Versions []tsdb.NativeMetricMetadataVersion
	// Truncated reports that the TSDB's cap on versions per series has evicted older ones.
	Truncated bool
	// Coalesced reports that changes over the seriesstore engine's limit were merged into one range.
	Coalesced bool
}

func (e prometheusEngine) nativeMetricMetadata(ctx context.Context, matcherSets [][]*labels.Matcher, limit int) (nativeMetadataResult, error) {
	series, limited, err := e.NativeMetricMetadata(ctx, matcherSets, limit)
	if err != nil {
		return nativeMetadataResult{}, err
	}
	result := nativeMetadataResult{
		Series:        make([]nativeMetadataSeries, 0, len(series)),
		Limited:       limited,
		CoverageStart: max(e.DB.Head().MinTime(), e.nativeMetadataSince),
	}
	for _, s := range series {
		result.Series = append(result.Series, nativeMetadataSeries{Labels: s.Labels, Versions: s.Versions, Truncated: s.Truncated})
	}
	return result, nil
}

func (e seriesstoreEngine) nativeMetricMetadata(context.Context, [][]*labels.Matcher, int) (nativeMetadataResult, error) {
	return nativeMetadataResult{}, tsdb.ErrNativeMetadataDisabled
}
