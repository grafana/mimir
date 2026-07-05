// SPDX-License-Identifier: AGPL-3.0-only

package api

import "github.com/prometheus/prometheus/model/labels"

// ContentTypeRemoteReadStreamedChunks is taken from the prometheus protobuf definitions documentation.
// See: https://github.com/prometheus/prometheus/blob/d9d51c565c622cdc7d626d3e7569652bc28abe15/prompb/remote.proto#L48
const ContentTypeRemoteReadStreamedChunks = "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"

// ContentTypeActiveSeriesFramed is the media type of the length-delimited active series
// response format: each series is an unsigned varint length prefix followed by that many
// bytes of the series' JSON object, with no outer envelope.
const ContentTypeActiveSeriesFramed = "application/vnd.mimir.active-series-framed"

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
