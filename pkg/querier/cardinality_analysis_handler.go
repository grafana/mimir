// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/tenant"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/distributor"
	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/querier/worker"
	"github.com/grafana/mimir/pkg/util"
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
		response, err := d.LabelNamesAndValues(ctx, cardinalityRequest.Matchers, cardinalityRequest.CountMethod)
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

func ActiveSeriesCardinalityHandler(d Distributor, limits *validation.Overrides) http.Handler {
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

		req, err := cardinality.DecodeActiveSeriesRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		res, err := d.ActiveSeries(ctx, req.Matchers)
		if err != nil {
			if errors.Is(err, distributor.ErrResponseTooLarge) {
				// http.StatusRequestEntityTooLarge (413) is about the request (not the response)
				// body size, but it's the closest we have, and we're using the same status code
				// in the query scheduler to express the same error condition.
				http.Error(w, fmt.Errorf("%w: try increasing the requested shard count", err).Error(), http.StatusRequestEntityTooLarge)
				return
			}
			respondFromError(err, w)
			return
		}

		var json = jsoniter.ConfigCompatibleWithStandardLibrary
		bytes, err := json.Marshal(api.ActiveSeriesResponse{Data: res})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(bytes)))
		w.Header().Set(worker.ResponseStreamingEnabledHeader, "true")

		// Nothing we can do about this error, so ignore it.
		_, _ = w.Write(bytes)
	})
}

func ActiveNativeHistogramMetricsHandler(d Distributor, limits *validation.Overrides) http.Handler {
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

		req, err := cardinality.DecodeActiveSeriesRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		res, err := d.ActiveNativeHistogramMetrics(ctx, req.Matchers)
		if err != nil {
			if errors.Is(err, distributor.ErrResponseTooLarge) {
				// http.StatusRequestEntityTooLarge (413) is about the request (not the response)
				// body size, but it's the closest we have, and we're using the same status code
				// in the query scheduler to express the same error condition.
				http.Error(w, fmt.Errorf("%w: try increasing the requested shard count", err).Error(), http.StatusRequestEntityTooLarge)
				return
			}
			respondFromError(err, w)
			return
		}

		var json = jsoniter.ConfigCompatibleWithStandardLibrary
		bytes, err := json.Marshal(res)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(bytes)))
		w.Header().Set(worker.ResponseStreamingEnabledHeader, "true")

		// Nothing we can do about this error, so ignore it.
		_, _ = w.Write(bytes)
	})
}

func respondFromError(err error, w http.ResponseWriter) {
	httpResp, ok := httpgrpc.HTTPResponseFromError(err)
	if !ok {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(int(httpResp.Code))
	w.Write(httpResp.Body) //nolint
}

// toLabelNamesCardinalityResponse converts ingester's response to api.LabelNamesCardinalityResponse
func toLabelNamesCardinalityResponse(response *ingester_client.LabelNamesAndValuesResponse, limit int) *api.LabelNamesCardinalityResponse {
	labelsWithValues := response.Items
	sortByValuesCountAndName(labelsWithValues)
	valuesCountTotal := getValuesCountTotal(labelsWithValues)
	items := make([]*api.LabelNamesCardinalityItem, min(len(labelsWithValues), limit))
	for i := 0; i < len(items); i++ {
		items[i] = &api.LabelNamesCardinalityItem{LabelName: labelsWithValues[i].LabelName, LabelValuesCount: len(labelsWithValues[i].Values)}
	}
	return &api.LabelNamesCardinalityResponse{
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

func toLabelValuesCardinalityResponse(seriesCountTotal uint64, cardinalityResponse *ingester_client.LabelValuesCardinalityResponse, limit int) *api.LabelValuesCardinalityResponse {
	labels := make([]api.LabelNamesCardinality, 0, len(cardinalityResponse.Items))

	for _, cardinalityItem := range cardinalityResponse.Items {
		var labelValuesSeriesCountTotal uint64
		cardinality := make([]api.LabelValuesCardinality, 0, len(cardinalityItem.LabelValueSeries))

		for labelValue, seriesCount := range cardinalityItem.LabelValueSeries {
			labelValuesSeriesCountTotal += seriesCount
			cardinality = append(cardinality, api.LabelValuesCardinality{
				LabelValue:  labelValue,
				SeriesCount: seriesCount,
			})
		}

		labels = append(labels, api.LabelNamesCardinality{
			LabelName:        cardinalityItem.LabelName,
			LabelValuesCount: uint64(len(cardinalityItem.LabelValueSeries)),
			SeriesCount:      labelValuesSeriesCountTotal,
			Cardinality:      limitLabelValuesCardinality(sortBySeriesCountAndLabelValue(cardinality), limit),
		})
	}

	return &api.LabelValuesCardinalityResponse{
		SeriesCountTotal: seriesCountTotal,
		Labels:           sortByLabelValuesSeriesCountAndLabelName(labels),
	}
}

// sortByLabelValuesSeriesCountAndLabelName sorts api.LabelNamesCardinality array in DESC order by SeriesCount and
// ASC order by LabelName
func sortByLabelValuesSeriesCountAndLabelName(labelNamesCardinality []api.LabelNamesCardinality) []api.LabelNamesCardinality {
	sort.Slice(labelNamesCardinality, func(l, r int) bool {
		left := labelNamesCardinality[l]
		right := labelNamesCardinality[r]
		return left.SeriesCount > right.SeriesCount || (left.SeriesCount == right.SeriesCount && left.LabelName < right.LabelName)
	})
	return labelNamesCardinality
}

// sortBySeriesCountAndLabelValue sorts api.LabelValuesCardinality array in DESC order by SeriesCount and
// ASC order by LabelValue
func sortBySeriesCountAndLabelValue(labelValuesCardinality []api.LabelValuesCardinality) []api.LabelValuesCardinality {
	sort.Slice(labelValuesCardinality, func(l, r int) bool {
		left := labelValuesCardinality[l]
		right := labelValuesCardinality[r]
		return left.SeriesCount > right.SeriesCount || (left.SeriesCount == right.SeriesCount && left.LabelValue < right.LabelValue)
	})
	return labelValuesCardinality
}

func limitLabelValuesCardinality(labelValuesCardinality []api.LabelValuesCardinality, limit int) []api.LabelValuesCardinality {
	if len(labelValuesCardinality) <= limit {
		return labelValuesCardinality
	}
	return labelValuesCardinality[:limit]
}
