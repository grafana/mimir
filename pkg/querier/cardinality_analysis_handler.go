// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/tenant"
	"github.com/grafana/mimir/pkg/util"
	util_math "github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	minLimit     = 0
	maxLimit     = 500
	defaultLimit = 20
)

// LabelNamesCardinalityHandler creates handler for label names cardinality endpoint.
func LabelNamesCardinalityHandler(d Distributor, limits *validation.Overrides) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		tenantID, err := tenant.TenantID(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !limits.CardinalityAnalysisEnabled(tenantID) {
			http.Error(w, fmt.Sprintf("cardinality analysis is disabled for the tenant: %v", tenantID), http.StatusBadRequest)
			return
		}
		matchers, limit, err := extractLabelNamesRequestParams(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		response, err := d.LabelNamesAndValues(ctx, matchers)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		cardinalityResponse := toLabelNamesCardinalityResponse(response, limit)
		util.WriteJSONResponse(w, cardinalityResponse)
	})
}

// LabelValuesCardinalityHandler creates handler for label values cardinality endpoint.
func LabelValuesCardinalityHandler(distributor Distributor, limits *validation.Overrides) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		// Guarantee request's context is for a single tenant id
		tenantID, err := tenant.TenantID(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !limits.CardinalityAnalysisEnabled(tenantID) {
			http.Error(w, fmt.Sprintf("cardinality analysis is disabled for the tenant: %v", tenantID), http.StatusBadRequest)
			return
		}

		labelNames, matchers, limit, err := extractLabelValuesRequestParams(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		seriesCountTotal, cardinalityResponse, err := distributor.LabelValuesCardinality(ctx, labelNames, matchers)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		util.WriteJSONResponse(w, toLabelValuesCardinalityResponse(seriesCountTotal, cardinalityResponse, limit))
	})
}

func extractLabelNamesRequestParams(r *http.Request) ([]*labels.Matcher, int, error) {
	err := r.ParseForm()
	if err != nil {
		return nil, 0, err
	}
	matchers, err := extractSelector(r)
	if err != nil {
		return nil, 0, err
	}
	limit, err := extractLimit(r)
	if err != nil {
		return nil, 0, err
	}
	return matchers, limit, nil
}

// extractLabelValuesRequestParams parses query params from GET requests and parses request body from POST requests
func extractLabelValuesRequestParams(r *http.Request) (labelNames []model.LabelName, matchers []*labels.Matcher, limit int, err error) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, 0, err
	}

	labelNames, err = extractLabelNames(r)
	if err != nil {
		return nil, nil, 0, err
	}

	matchers, err = extractSelector(r)
	if err != nil {
		return nil, nil, 0, err
	}

	limit, err = extractLimit(r)
	if err != nil {
		return nil, nil, 0, err
	}

	return labelNames, matchers, limit, nil
}

// extractSelector parses and gets selector query parameter containing a single matcher
func extractSelector(r *http.Request) (matchers []*labels.Matcher, err error) {
	selectorParams := r.Form["selector"]
	if len(selectorParams) == 0 {
		return nil, nil
	}
	if len(selectorParams) > 1 {
		return nil, fmt.Errorf("multiple 'selector' params are not allowed")
	}
	return parser.ParseMetricSelector(selectorParams[0])
}

// extractLimit parses and validates request param `limit` if it's defined, otherwise returns default value.
func extractLimit(r *http.Request) (limit int, err error) {
	limitParams := r.Form["limit"]
	if len(limitParams) == 0 {
		return defaultLimit, nil
	}
	if len(limitParams) > 1 {
		return 0, fmt.Errorf("multiple 'limit' params are not allowed")
	}
	limit, err = strconv.Atoi(limitParams[0])
	if err != nil {
		return 0, err
	}
	if limit < minLimit {
		return 0, fmt.Errorf("'limit' param cannot be less than '%v'", minLimit)
	}
	if limit > maxLimit {
		return 0, fmt.Errorf("'limit' param cannot be greater than '%v'", maxLimit)
	}
	return limit, nil
}

// extractLabelNames parses and gets label_names query parameter containing an array of label values
func extractLabelNames(r *http.Request) ([]model.LabelName, error) {
	labelNamesParams := r.Form["label_names[]"]
	if len(labelNamesParams) == 0 {
		return nil, fmt.Errorf("'label_names[]' param is required")
	}

	labelNames := make([]model.LabelName, 0, len(labelNamesParams))
	for _, labelNameParam := range labelNamesParams {
		labelName := model.LabelName(labelNameParam)
		if !labelName.IsValid() {
			return nil, fmt.Errorf("invalid 'label_names' param '%v'", labelNameParam)
		}
		labelNames = append(labelNames, labelName)
	}

	return labelNames, nil
}

// toLabelNamesCardinalityResponse converts ingester's response to LabelNamesCardinalityResponse
func toLabelNamesCardinalityResponse(response *ingester_client.LabelNamesAndValuesResponse, limit int) *LabelNamesCardinalityResponse {
	labelsWithValues := response.Items
	sortByValuesCountAndName(labelsWithValues)
	valuesCountTotal := getValuesCountTotal(labelsWithValues)
	items := make([]*LabelNamesCardinalityItem, util_math.Min(len(labelsWithValues), limit))
	for i := 0; i < len(items); i++ {
		items[i] = &LabelNamesCardinalityItem{LabelName: labelsWithValues[i].LabelName, LabelValuesCount: len(labelsWithValues[i].Values)}
	}
	return &LabelNamesCardinalityResponse{
		LabelValuesCountTotal: valuesCountTotal,
		LabelNamesCount:       len(response.Items),
		Cardinality:           items,
	}
}

func sortByValuesCountAndName(labelsWithValues []*ingester_client.LabelValues) {
	sort.Slice(labelsWithValues, func(i, j int) bool {
		left := labelsWithValues[i]
		right := labelsWithValues[j]
		return len(left.Values) > len(right.Values) || (len(left.Values) == len(right.Values) && left.LabelName < right.LabelName)
	})
}

func getValuesCountTotal(labelsWithValues []*ingester_client.LabelValues) int {
	var valuesCountTotal int
	for _, item := range labelsWithValues {
		valuesCountTotal += len(item.Values)
	}
	return valuesCountTotal
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

func toLabelValuesCardinalityResponse(seriesCountTotal uint64, cardinalityResponse *ingester_client.LabelValuesCardinalityResponse, limit int) *labelValuesCardinalityResponse {
	labels := make([]labelNamesCardinality, 0, len(cardinalityResponse.Items))

	for _, cardinalityItem := range cardinalityResponse.Items {
		var labelValuesSeriesCountTotal uint64 = 0
		cardinality := make([]labelValuesCardinality, 0, len(cardinalityItem.LabelValueSeries))

		for labelValue, seriesCount := range cardinalityItem.LabelValueSeries {
			labelValuesSeriesCountTotal += seriesCount
			cardinality = append(cardinality, labelValuesCardinality{
				LabelValue:  labelValue,
				SeriesCount: seriesCount,
			})
		}

		labels = append(labels, labelNamesCardinality{
			LabelName:        cardinalityItem.LabelName,
			LabelValuesCount: uint64(len(cardinalityItem.LabelValueSeries)),
			SeriesCount:      labelValuesSeriesCountTotal,
			Cardinality:      limitLabelValuesCardinality(sortBySeriesCountAndLabelValue(cardinality), limit),
		})
	}

	return &labelValuesCardinalityResponse{
		SeriesCountTotal: seriesCountTotal,
		Labels:           sortByLabelValuesSeriesCountAndLabelName(labels),
	}
}

// sortByLabelValuesSeriesCountAndLabelName sorts labelNamesCardinality array in DESC order by SeriesCount and
// ASC order by LabelName
func sortByLabelValuesSeriesCountAndLabelName(labelNamesCardinality []labelNamesCardinality) []labelNamesCardinality {
	sort.Slice(labelNamesCardinality, func(l, r int) bool {
		left := labelNamesCardinality[l]
		right := labelNamesCardinality[r]
		return left.SeriesCount > right.SeriesCount || (left.SeriesCount == right.SeriesCount && left.LabelName < right.LabelName)
	})
	return labelNamesCardinality
}

// sortBySeriesCountAndLabelValue sorts labelValuesCardinality array in DESC order by SeriesCount and
// ASC order by LabelValue
func sortBySeriesCountAndLabelValue(labelValuesCardinality []labelValuesCardinality) []labelValuesCardinality {
	sort.Slice(labelValuesCardinality, func(l, r int) bool {
		left := labelValuesCardinality[l]
		right := labelValuesCardinality[r]
		return left.SeriesCount > right.SeriesCount || (left.SeriesCount == right.SeriesCount && left.LabelValue < right.LabelValue)
	})
	return labelValuesCardinality
}

func limitLabelValuesCardinality(labelValuesCardinality []labelValuesCardinality, limit int) []labelValuesCardinality {
	if len(labelValuesCardinality) <= limit {
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
