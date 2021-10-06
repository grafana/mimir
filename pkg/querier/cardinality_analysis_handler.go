// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/prometheus/prometheus/promql/parser"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/tenant"
	"github.com/grafana/mimir/pkg/util"
	util_math "github.com/grafana/mimir/pkg/util/math"
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
		matchersParams := r.URL.Query()["match[]"]
		matchers, err := parser.ParseMetricSelector(fmt.Sprintf("{%v}", strings.Join(matchersParams, ", ")))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		limit, err := extractLimit(r)
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

// extractLimit parses and validates request param `limit` if it's defined, otherwise returns default value.
func extractLimit(r *http.Request) (limit int, err error) {
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
	maxLimit := 500
	if limit > maxLimit {
		return 0, fmt.Errorf("limit param can not greater than %v", maxLimit)
	}
	return limit, nil
}

// toLabelNamesCardinalityResponse converts ingester's response to LabelNamesCardinalityResponse
func toLabelNamesCardinalityResponse(response *ingester_client.LabelNamesAndValuesResponse, limit int) *LabelNamesCardinalityResponse {
	labelsWithValues := response.Items
	sortByValuesCountAndName(&labelsWithValues)
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

func sortByValuesCountAndName(labelsWithValues *[]*ingester_client.LabelValues) {
	labelValues := *labelsWithValues
	sort.Slice(labelValues, func(i, j int) bool {
		left := labelValues[i]
		right := labelValues[j]
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
