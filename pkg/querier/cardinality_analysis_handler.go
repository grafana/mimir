// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"

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
		items[i] = &LabelNamesCardinalityItem{LabelName: labelsWithValues[i].LabelName, LabelValuesCount: len(labelsWithValues[i].Values)}
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
	LabelName        string `json:"label_name"`
	LabelValuesCount int    `json:"label_values_count"`
}
