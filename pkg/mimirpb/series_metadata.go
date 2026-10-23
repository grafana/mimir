// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

// SeriesMetadata is a series' own metric metadata, which Remote Write 2.0 and OTLP send with each series, unlike the
// request-level MetricMetadata of metric families.
type SeriesMetadata struct {
	Type MetricMetadata_MetricType
	Help string
	Unit string
}

// IsEmpty reports whether there's no metadata.
func (m SeriesMetadata) IsEmpty() bool {
	return m.Type == UNKNOWN && m.Help == "" && m.Unit == ""
}
