// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/util"
)

type metricNameCount struct {
	Name  string `json:"name"`
	Count int64  `json:"count"`
}

type metricNameCountsResponse struct {
	MinTime       int64             `json:"min_time"`
	MaxTime       int64             `json:"max_time"`
	Blocks        int               `json:"blocks"`
	StoreGateways int               `json:"store_gateways"`
	Names         int               `json:"names"`
	Series        int64             `json:"series"`
	ElapsedMS     float64           `json:"elapsed_ms"`
	Counts        []metricNameCount `json:"counts"`
}

// MetricNameCountsHandler serves every metric name's series count over one
// block range, counted by the store-gateways from their index-headers (see
// BlocksStoreQueryable.MetricNameCounts). It takes start, end and an optional
// limit that keeps the largest counts. Experimental.
func MetricNameCountsHandler(q *BlocksStoreQueryable) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenantID, err := tenant.TenantID(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		minT, err := util.ParseTime(r.FormValue("start"))
		if err != nil {
			http.Error(w, fmt.Sprintf("start: %v", err), http.StatusBadRequest)
			return
		}
		maxT, err := util.ParseTime(r.FormValue("end"))
		if err != nil || maxT <= minT {
			http.Error(w, "end must be a timestamp after start", http.StatusBadRequest)
			return
		}
		limit := 0
		if v := r.FormValue("limit"); v != "" {
			if limit, err = strconv.Atoi(v); err != nil || limit < 0 {
				http.Error(w, "limit must be a non-negative integer", http.StatusBadRequest)
				return
			}
		}

		start := time.Now()
		res, err := q.MetricNameCounts(r.Context(), tenantID, minT, maxT)
		if err != nil {
			code := http.StatusInternalServerError
			if strings.Contains(err.Error(), "block range") || strings.Contains(err.Error(), "cuts through") || strings.Contains(err.Error(), "may share series") {
				code = http.StatusUnprocessableEntity
			}
			http.Error(w, err.Error(), code)
			return
		}

		out := metricNameCountsResponse{
			MinTime: minT, MaxTime: maxT, Blocks: len(res.Blocks), StoreGateways: res.StoreGateways,
			Names: len(res.Counts), ElapsedMS: float64(time.Since(start).Microseconds()) / 1000,
			Counts: make([]metricNameCount, 0, len(res.Counts)),
		}
		for name, n := range res.Counts {
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
			return strings.Compare(a.Name, b.Name)
		})
		if limit > 0 && limit < len(out.Counts) {
			out.Counts = out.Counts[:limit]
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(out); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

// WithMetricNameCountsRoute serves MetricNameCountsHandler on routePath and
// passes every other request to next.
func WithMetricNameCountsRoute(next http.Handler, routePath string, q *BlocksStoreQueryable) http.Handler {
	h := MetricNameCountsHandler(q)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == routePath && (r.Method == http.MethodGet || r.Method == http.MethodPost) {
			h.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}
