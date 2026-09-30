// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/gorilla/mux"

	"github.com/grafana/mimir/pkg/util"
)

type metricNameCount struct {
	Name  string `json:"name"`
	Count int64  `json:"count"`
}

type metricNameCountsResponse struct {
	MinTime   int64             `json:"min_time"`
	MaxTime   int64             `json:"max_time"`
	Blocks    []string          `json:"blocks"`
	Names     int               `json:"names"`
	Series    int64             `json:"series"`
	ElapsedMS float64           `json:"elapsed_ms"`
	Counts    []metricNameCount `json:"counts"`
}

// MetricNameCountsHandler serves, for one tenant, the series count of every
// metric name over [start, end), counted from the index-headers of the blocks
// this store-gateway holds for that range. The window must be exactly one
// block range; counts across several ranges need deduplication and are not
// served. The optional limit keeps the largest counts.
func (g *StoreGateway) MetricNameCountsHandler(w http.ResponseWriter, req *http.Request) {
	tenantID := mux.Vars(req)["tenant"]
	if tenantID == "" {
		http.Error(w, "tenant ID can't be empty", http.StatusBadRequest)
		return
	}
	minT, err := util.ParseTime(req.FormValue("start"))
	if err != nil {
		http.Error(w, fmt.Sprintf("start: %v", err), http.StatusBadRequest)
		return
	}
	maxT, err := util.ParseTime(req.FormValue("end"))
	if err != nil || maxT <= minT {
		http.Error(w, "end must be a timestamp after start", http.StatusBadRequest)
		return
	}
	limit := 0
	if v := req.FormValue("limit"); v != "" {
		if limit, err = strconv.Atoi(v); err != nil || limit < 0 {
			http.Error(w, "limit must be a non-negative integer", http.StatusBadRequest)
			return
		}
	}

	store := g.stores.getStore(tenantID)
	if store == nil {
		http.Error(w, fmt.Sprintf("this store-gateway holds no blocks for tenant %q", tenantID), http.StatusNotFound)
		return
	}

	start := time.Now()
	res, err := store.metricNameCounts(req.Context(), minT, maxT)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	elapsed := time.Since(start)

	out := metricNameCountsResponse{MinTime: minT, MaxTime: maxT, Names: len(res.counts), ElapsedMS: float64(elapsed.Microseconds()) / 1000}
	for _, id := range res.blocks {
		out.Blocks = append(out.Blocks, id.String())
	}
	slices.Sort(out.Blocks)
	out.Counts = make([]metricNameCount, 0, len(res.counts))
	for name, n := range res.counts {
		out.Counts = append(out.Counts, metricNameCount{Name: name, Count: n})
		out.Series += n
	}
	slices.SortFunc(out.Counts, func(a, b metricNameCount) int {
		if a.Count != b.Count {
			if a.Count > b.Count {
				return -1
			}
			return 1
		}
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}
		return 0
	})
	if limit > 0 && limit < len(out.Counts) {
		out.Counts = out.Counts[:limit]
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(out); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
