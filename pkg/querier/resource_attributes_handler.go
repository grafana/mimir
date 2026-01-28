// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"net/http"

	"github.com/grafana/mimir/pkg/util"
)

// ResourceAttributesResponse matches the Prometheus API response format.
type ResourceAttributesResponse struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// NewResourceAttributesHandler creates a http.Handler for the /api/v1/resources endpoint.
// This endpoint is for querying OTel resource attributes persisted per time series.
// Note: In the current implementation, resource attributes are persisted to TSDB blocks
// but distributed querying is not yet supported. This handler returns an appropriate
// message indicating the feature status.
func NewResourceAttributesHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Resource attributes querying requires aggregating data from ingesters and
		// store-gateways, which is not yet implemented. Return a clear error message.
		w.WriteHeader(http.StatusNotImplemented)
		util.WriteJSONResponse(w, ResourceAttributesResponse{
			Status: statusError,
			Error:  "resource attributes querying is not yet supported in Mimir's distributed architecture; resource attributes are persisted to TSDB blocks but distributed query aggregation is pending implementation",
		})
	})
}
