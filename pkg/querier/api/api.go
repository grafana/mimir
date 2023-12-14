// SPDX-License-Identifier: AGPL-3.0-only

package api

import "github.com/prometheus/prometheus/model/labels"

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
