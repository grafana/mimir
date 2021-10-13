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
)

const (
	minLimit     = 0
	maxLimit     = 500
	defaultLimit = 20
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

		matchers, limit, err := extractRequestParams(r)

		labelNames, err := getLabelNamesParam(r)
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

// LabelNamesCardinalityHandler creates handler for label names cardinality endpoint.
func LabelNamesCardinalityHandler(d Distributor) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		_, err := tenant.TenantID(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		matchers, limit, err := extractRequestParams(r)
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

func extractRequestParams(r *http.Request) ([]*labels.Matcher, int, error) {
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

func extractSelector(r *http.Request) (matchers []*labels.Matcher, err error) {
	selectorParams := r.Form["selector"]
	if len(selectorParams) == 0 {
		return nil, nil
	}
	if len(selectorParams) > 1 {
		return nil, fmt.Errorf("multiple `selector` params are not allowed")
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
		return 0, fmt.Errorf("multiple `limit` params are not allowed")
	}
	limit, err = strconv.Atoi(limitParams[0])
	if err != nil {
		return 0, err
	}
	if limit < minLimit {
		return 0, fmt.Errorf("limit param can not be less than %v", minLimit)
	}
	if limit > maxLimit {
		return 0, fmt.Errorf("limit param can not be greater than %v", maxLimit)
	}
	return limit, nil
}

// toLabelNamesCardinalityResponse converts ingester's response to LabelNamesCardinalityResponse
func toLabelNamesCardinalityResponse(response *ingester_client.LabelNamesAndValuesResponse, limit int) *LabelNamesCardinalityResponse {
	labelsWithValues := response.Items
	sortByValuesCountAndName(labelsWithValues)
	valuesCountTotal := getValuesCountTotal(labelsWithValues)
	items := make([]*LabelNamesCardinalityItem, util_math.Min(len(labelsWithValues), limit))
	for i := 0; i < len(items); i++ {
		items[i] = &LabelNamesCardinalityItem{LabelName: labelsWithValues[i].LabelName, ValuesCount: len(labelsWithValues[i].Values)}
	}
	return &LabelNamesCardinalityResponse{
		ValuesCountTotal: valuesCountTotal,
		LabelNamesCount:  len(response.Items),
		Cardinality:      items,
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
	ValuesCountTotal int                          `json:"values_count_total"`
	LabelNamesCount  int                          `json:"label_names_count"`
	Cardinality      []*LabelNamesCardinalityItem `json:"cardinality"`
}

type LabelNamesCardinalityItem struct {
	LabelName   string `json:"label_name"`
	ValuesCount int    `json:"values_count"`
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
