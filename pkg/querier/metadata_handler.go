// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/metadata_handler.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	"net/http"

	"github.com/prometheus/prometheus/scrape"

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
	MetricsMetadata(ctx context.Context) ([]scrape.MetricMetadata, error)
}

type metricMetadata struct {
	Type string `json:"type"`
	Help string `json:"help"`
	Unit string `json:"unit"`
}

type metadataResult struct {
	Status string                      `json:"status"`
	Data   map[string][]metricMetadata `json:"data,omitempty"`
	Error  string                      `json:"error,omitempty"`
}

// NewMetadataHandler creates a http.Handler for serving metric metadata held by
// Mimir for a given tenant. It is kept and returned as a set.
func NewMetadataHandler(m MetadataSupplier) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp, err := m.MetricsMetadata(r.Context())
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			util.WriteJSONResponse(w, metadataResult{Status: statusError, Error: err.Error()})
			return
		}

		// Put all the elements of the pseudo-set into a map of slices for marshalling.
		metrics := map[string][]metricMetadata{}
		for _, m := range resp {
			ms, ok := metrics[m.Metric]
			if !ok {
				// Most metrics will only hold 1 copy of the same metadata.
				ms = make([]metricMetadata, 0, 1)
				metrics[m.Metric] = ms
			}
			metrics[m.Metric] = append(ms, metricMetadata{Type: string(m.Type), Help: m.Help, Unit: m.Unit})
		}

		util.WriteJSONResponse(w, metadataResult{Status: statusSuccess, Data: metrics})
	})
}
