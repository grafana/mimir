// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
)

// ContentTypeRemoteReadStreamedChunks is taken from the prometheus protobuf definitions documentation.
// See: https://github.com/prometheus/prometheus/blob/d9d51c565c622cdc7d626d3e7569652bc28abe15/prompb/remote.proto#L48
const ContentTypeRemoteReadStreamedChunks = "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"

// ContentTypeActiveSeriesFramed is the media type of the length-delimited active series
// response format: each series is an unsigned varint length prefix followed by that many
// bytes of the series' JSON object, with no outer envelope.
const ContentTypeActiveSeriesFramed = "application/vnd.mimir.active-series-framed"

// MaxActiveSeriesFrameSize guards against a corrupt length prefix causing an unbounded allocation.
const MaxActiveSeriesFrameSize = 16 * 1024 * 1024 // 16MB

type LabelValuesCardinalityResponse struct {
	SeriesCountTotal uint64                  `json:"series_count_total"`
	Labels           []LabelNamesCardinality `json:"labels"`
}

type LabelNamesCardinality struct {
	LabelName        string                   `json:"label_name"`
	LabelValuesCount uint64                   `json:"label_values_count"`
	SeriesCount      uint64                   `json:"series_count"`
	Cardinality      []LabelValuesCardinality `json:"cardinality"`
}

type LabelValuesCardinality struct {
	LabelValue  string `json:"label_value"`
	SeriesCount uint64 `json:"series_count"`
}

type LabelNamesCardinalityResponse struct {
	LabelValuesCountTotal int                          `json:"label_values_count_total"`
	LabelNamesCount       int                          `json:"label_names_count"`
	Cardinality           []*LabelNamesCardinalityItem `json:"cardinality"`
}

type LabelNamesCardinalityItem struct {
	LabelName        string `json:"label_name"`
	LabelValuesCount int    `json:"label_values_count"`
}

type ActiveSeriesResponse struct {
	Data []labels.Labels `json:"data"`
}

// MetricMetadataFetcher fetches metric metadata for a set of metric names,
// returning it keyed by metric name.
//
// matcherSets are the request's OR-ed search selectors (one inner slice per
// match[] entry). A tenant-federation-aware implementation uses only their
// tenant (__tenant_id__) matchers, to scope the fetch to the union of tenants
// the selectors touched — so metadata matches the tenants the search saw.
// Implementations backed by the ingester metadata store are keyed by (tenant,
// metric name) and cannot filter by any other label, so they ignore
// matcherSets entirely.
type MetricMetadataFetcher interface {
	FetchMetricMetadata(ctx context.Context, names []string, matcherSets [][]*labels.Matcher) (map[string]metadata.Metadata, error)
}
