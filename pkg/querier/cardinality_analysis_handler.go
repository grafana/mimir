// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/tenant"
	"github.com/grafana/mimir/pkg/util"
)

func LabelValuesCardinalityHandler(distributor Distributor) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		// Guarantee request's context is for a single tenant id
		_, err := tenant.TenantID(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Parse query params from GET requests and parse request body for POST requests
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		labelNames, err := getLabelNamesParam(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		matchers, err := getSelectorParam(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		limit, err := getLimitParam(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		seriesCountTotal, labelNamesMap, err := distributor.LabelValuesCardinality(ctx, labelNames, matchers)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		util.WriteJSONResponse(w, toLabelValuesCardinalityResponse(seriesCountTotal, labelNamesMap, limit))
	})
}

const maxCardinalityAnalysisLimitQueryParam = 500

// Parse and get label_names query parameter containing an array of label values
func getLabelNamesParam(r *http.Request) ([]model.LabelName, error) {
	labelNamesParams := r.Form["label_names[]"]
	if len(labelNamesParams) == 0 {
		return nil, fmt.Errorf("label_names param is required")
	}

	var labelNames []model.LabelName
	for _, labelNameParam := range labelNamesParams {
		labelName := model.LabelName(labelNameParam)
		if !labelName.IsValid() {
			return nil, fmt.Errorf("invalid label_names param '%v'", labelNameParam)
		}
		labelNames = append(labelNames, labelName)
	}

	return labelNames, nil
}

// Parse and get selector query parameter containing a single matcher
func getSelectorParam(r *http.Request) ([]*labels.Matcher, error) {
	selectorParams := r.Form["selector[]"]
	if len(selectorParams) == 0 {
		return nil, nil
	}
	if len(selectorParams) > 1 {
		return nil, fmt.Errorf("multiple `selector` params are not allowed")
	}
	return parser.ParseMetricSelector(selectorParams[0])
}

// getLimitParam parses and validates request param `limit` if it's defined, otherwise returns default value.
func getLimitParam(r *http.Request) (limit int, err error) {
	limitParam := r.URL.Query().Get("limit")
	if len(limitParam) == 0 {
		return 20, nil
	}

	limit, err = strconv.Atoi(limitParam)
	if err != nil {
		return 0, err
	}
	if limit < 0 {
		return 0, errors.New("limit param can not be negative")
	}

	if limit > maxCardinalityAnalysisLimitQueryParam {
		return 0, fmt.Errorf("limit param can not greater than %v", maxCardinalityAnalysisLimitQueryParam)
	}

	return limit, nil
}

func toLabelValuesCardinalityResponse(seriesCountTotal uint64, labelNamesMap map[string]map[string]uint64, limit int) *labelValuesCardinalityResponse {
	labels := make([]labelNamesCardinality, 0, len(labelNamesMap))

	for labelName, labelValueSeriesCountMap := range labelNamesMap {
		var labelValuesSeriesCountTotal uint64 = 0

		cardinality := make([]labelValuesCardinality, 0, len(labelValueSeriesCountMap))
		for labelValue, seriesCount := range labelValueSeriesCountMap {
			labelValuesSeriesCountTotal += seriesCount
			cardinality = append(cardinality, labelValuesCardinality{
				LabelValue:  labelValue,
				SeriesCount: seriesCount,
			})
		}

		labels = append(labels, labelNamesCardinality{
			LabelName:        labelName,
			LabelValuesCount: uint64(len(labelValueSeriesCountMap)),
			SeriesCount:      labelValuesSeriesCountTotal,
			Cardinality:      limitLabelValuesCardinality(sortBySeriesCountAndLabelValue(cardinality), limit),
		})
	}

	return &labelValuesCardinalityResponse{
		SeriesCountTotal: seriesCountTotal,
		Labels:           labels,
	}
}

func sortBySeriesCountAndLabelValue(labelValuesCardinality []labelValuesCardinality) []labelValuesCardinality {
	sort.Slice(labelValuesCardinality, func(l, r int) bool {
		left := labelValuesCardinality[l]
		right := labelValuesCardinality[r]
		return left.SeriesCount > right.SeriesCount || (left.SeriesCount == right.SeriesCount && left.LabelValue < right.LabelValue)
	})
	return labelValuesCardinality
}

func limitLabelValuesCardinality(labelValuesCardinality []labelValuesCardinality, limit int) []labelValuesCardinality {
	if len(labelValuesCardinality) < limit {
		return labelValuesCardinality
	}
	return labelValuesCardinality[:limit]
}

type labelValuesCardinality struct {
	LabelValue  string `json:"label_value"`
	SeriesCount uint64 `json:"series_count"`
}

type labelNamesCardinality struct {
	LabelName        string                   `json:"label_name"`
	LabelValuesCount uint64                   `json:"label_values_count"`
	SeriesCount      uint64                   `json:"series_count"`
	Cardinality      []labelValuesCardinality `json:"cardinality"`
}

type labelValuesCardinalityResponse struct {
	SeriesCountTotal uint64                  `json:"series_count_total"`
	Labels           []labelNamesCardinality `json:"labels"`
}
