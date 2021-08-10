// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/http_server.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"net/http"

	"github.com/grafana/mimir/pkg/util"
)

// UserStats models ingestion statistics for one user.
type UserStats struct {
	IngestionRate     float64 `json:"ingestionRate"`
	NumSeries         uint64  `json:"numSeries"`
	APIIngestionRate  float64 `json:"APIIngestionRate"`
	RuleIngestionRate float64 `json:"RuleIngestionRate"`
}

// UserStatsHandler handles user stats to the Distributor.
func (d *Distributor) UserStatsHandler(w http.ResponseWriter, r *http.Request) {
	stats, err := d.UserStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	util.WriteJSONResponse(w, stats)
}
