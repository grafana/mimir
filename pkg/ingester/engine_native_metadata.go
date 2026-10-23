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
