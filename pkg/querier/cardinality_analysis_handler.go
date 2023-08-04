// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"fmt"
	"net/http"
	"sort"

	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/grafana/mimir/pkg/cardinality"
	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/util"
	util_math "github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/validation"
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

		cardinalityRequest, err := cardinality.DecodeLabelNamesRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		response, err := d.LabelNamesAndValues(ctx, cardinalityRequest.Matchers)
		if err != nil {
			respondFromError(err, w)
			return
		}
		cardinalityResponse := toLabelNamesCardinalityResponse(response, cardinalityRequest.Limit)
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

		cardinalityRequest, err := cardinality.DecodeLabelValuesRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		seriesCountTotal, cardinalityResponse, err := distributor.LabelValuesCardinality(ctx, cardinalityRequest.LabelNames, cardinalityRequest.Matchers, cardinalityRequest.CountMethod)
		if err != nil {
			respondFromError(err, w)
			return
		}

		util.WriteJSONResponse(w, toLabelValuesCardinalityResponse(seriesCountTotal, cardinalityResponse, cardinalityRequest.Limit))
	})
}

func respondFromError(err error, w http.ResponseWriter) {
	httpResp, ok := httpgrpc.HTTPResponseFromError(errors.Cause(err))
	if !ok {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(int(httpResp.Code))
	w.Write(httpResp.Body) //nolint
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
		var labelValuesSeriesCountTotal uint64
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
