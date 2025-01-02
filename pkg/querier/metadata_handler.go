// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/metadata_handler.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	"net/http"
	"strconv"

	"github.com/prometheus/prometheus/scrape"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/util"
)

const (
	statusSuccess = "success"
	statusError   = "error"
)

// MetadataSupplier is the metadata specific part of the Distributor interface. It
// exists to allow us to wrap the default implementation (the distributor embedded
// in a querier) with logic for handling tenant federated metadata requests.
type MetadataSupplier interface {
	MetricsMetadata(ctx context.Context, req *client.MetricsMetadataRequest) ([]scrape.MetricMetadata, error)
}

type metricMetadata struct {
	Type string `json:"type"`
	Help string `json:"help"`
	Unit string `json:"unit"`
}

type metadataSuccessResult struct {
	Status string                      `json:"status"`
	Data   map[string][]metricMetadata `json:"data"`
}

type metadataErrorResult struct {
	Status string `json:"status"`
	Error  string `json:"error"`
}

// NewMetadataHandler creates a http.Handler for serving metric metadata held by
// Mimir for a given tenant. It is kept and returned as a set.
func NewMetadataHandler(m MetadataSupplier) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limit := int32(-1)
		if s := r.FormValue("limit"); s != "" {
			parsed, err := strconv.ParseInt(s, 10, 32)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				util.WriteJSONResponse(w, metadataErrorResult{Status: statusError, Error: "limit must be a number"})
				return
			}

			limit = int32(parsed)
		}

		limitPerMetric := int32(-1)
		if s := r.FormValue("limit_per_metric"); s != "" {
			parsed, err := strconv.ParseInt(s, 10, 32)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				util.WriteJSONResponse(w, metadataErrorResult{Status: statusError, Error: "limit_per_metric must be a number"})
				return
			}

			limitPerMetric = int32(parsed)
		}

		metric := r.FormValue("metric")
		req := &client.MetricsMetadataRequest{
			Limit:          limit,
			LimitPerMetric: limitPerMetric,
			Metric:         metric,
		}

		resp, err := m.MetricsMetadata(r.Context(), req)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			util.WriteJSONResponse(w, metadataErrorResult{Status: statusError, Error: err.Error()})
			return
		}

		// Put all the elements of the pseudo-set into a map of slices for marshalling.
		metrics := map[string][]metricMetadata{}
		for _, m := range resp {
			ms, ok := metrics[m.Metric]
			// We enforce this both here and in the ingesters. Doing it in the ingesters is
			// more efficient as it is earlier in the process, but since that one is per user,
			// we still need to do it here after all the results are merged.
			if limitPerMetric > 0 && len(ms) >= int(limitPerMetric) {
				continue
			}
			if !ok {
				if limit >= 0 && len(metrics) >= int(limit) {
					break
				}
				// Most metrics will only hold 1 copy of the same metadata.
				ms = make([]metricMetadata, 0, 1)
				metrics[m.Metric] = ms
			}
			metrics[m.Metric] = append(ms, metricMetadata{Type: string(m.Type), Help: m.Help, Unit: m.Unit})
		}

		util.WriteJSONResponse(w, metadataSuccessResult{Status: statusSuccess, Data: metrics})
	})
}
