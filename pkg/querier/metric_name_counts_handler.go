// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

type metricNameCount struct {
	Name  string `json:"name"`
	Count int64  `json:"count"`
}

type metricNameCountsResponse struct {
	MinTime       int64             `json:"min_time"`
	MaxTime       int64             `json:"max_time"`
	Snapped       bool              `json:"snapped,omitempty"`
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
// limit that keeps the largest counts. With snap=true a window inside one
// block range is widened to that range, and min_time and max_time in the
// response say which range was counted. Experimental.
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
		snapped := false
		if r.FormValue("snap") == "true" {
			lo, hi, err := q.SnapToBlockRange(r.Context(), tenantID, minT, maxT)
			if err != nil {
				http.Error(w, err.Error(), http.StatusUnprocessableEntity)
				return
			}
			snapped = lo != minT || hi != maxT
			minT, maxT = lo, hi
		}
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
			MinTime: minT, MaxTime: maxT, Snapped: snapped, Blocks: len(res.Blocks), StoreGateways: res.StoreGateways,
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

type seriesCountGroup struct {
	Value  string  `json:"value"`
	Count  int64   `json:"count"`
	Counts []int64 `json:"counts,omitempty"`
}

type seriesCountsResponse struct {
	MinTime              int64              `json:"min_time"`
	MaxTime              int64              `json:"max_time"`
	StepMS               int64              `json:"step_ms,omitempty"`
	GroupBy              string             `json:"group_by"`
	Dedup                bool               `json:"dedup"`
	LowerBound           bool               `json:"lower_bound"`
	Blocks               int                `json:"blocks"`
	StoreGateways        int                `json:"store_gateways"`
	Groups               int                `json:"groups"`
	Series               int64              `json:"series"`
	SeriesCounted        int64              `json:"series_counted"`
	PostingsFetchedBytes int64              `json:"postings_fetched_bytes"`
	SeriesFetchedBytes   int64              `json:"series_fetched_bytes"`
	ElapsedMS            float64            `json:"elapsed_ms"`
	Counts               []seriesCountGroup `json:"counts"`
	Compare              *seriesCountsCheck `json:"compare,omitempty"`
}

// seriesCountsCheck is the compare=true result: the same request answered
// by loading chunks, and whether every group agrees.
type seriesCountsCheck struct {
	Match            bool           `json:"match"`
	MismatchedGroups int            `json:"mismatched_groups"`
	Examples         []string       `json:"examples,omitempty"`
	Skipped          string         `json:"skipped,omitempty"`
	ElapsedMS        float64        `json:"elapsed_ms"`
	Cost             chunksPathCost `json:"cost"`
}

// SeriesCountsHandler serves series counts read from the full index by the
// store-gateways (see BlocksStoreQueryable.SeriesCounts). It takes start,
// end, an optional match[] selector, group_by (default __name__), step for
// per-bucket counts, budget for a series cap per store-gateway, and limit
// to keep the largest groups. With a step, a group's count is its largest
// bucket. With compare=true it also answers the request by loading chunks,
// as a PromQL count would, and reports whether the two agree and what each
// cost. Experimental.
func SeriesCountsHandler(q *BlocksStoreQueryable) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenantID, err := tenant.TenantID(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		req, limit, err := parseSeriesCountsRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		start := time.Now()
		res, err := q.SeriesCounts(r.Context(), tenantID, req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		indexElapsed := time.Since(start)
		if q.metrics != nil {
			q.metrics.seriesCountsDuration.WithLabelValues("index").Observe(indexElapsed.Seconds())
		}
		out := seriesCountsResponse{
			MinTime: req.MinT, MaxTime: req.MaxT, StepMS: req.Step, GroupBy: req.GroupBy,
			Dedup: res.Dedup, LowerBound: res.LowerBound, Blocks: len(res.Blocks), StoreGateways: res.StoreGateways,
			Groups: len(res.Counts), SeriesCounted: res.SeriesCounted,
			PostingsFetchedBytes: res.PostingsFetchedBytes, SeriesFetchedBytes: res.SeriesFetchedBytes,
			Counts: make([]seriesCountGroup, 0, len(res.Counts)),
		}
		for v, counts := range res.Counts {
			g := seriesCountGroup{Value: v, Count: slices.Max(counts)}
			if req.Step > 0 {
				g.Counts = counts
			}
			out.Counts = append(out.Counts, g)
			out.Series += g.Count
		}
		out.ElapsedMS = float64(indexElapsed.Microseconds()) / 1000
		if r.FormValue("compare") == "true" {
			check, err := compareWithChunks(r.Context(), q, req, res)
			if err != nil {
				http.Error(w, fmt.Sprintf("chunks path: %v", err), http.StatusInternalServerError)
				return
			}
			out.Compare = check
		}
		slices.SortFunc(out.Counts, func(a, b seriesCountGroup) int {
			if a.Count != b.Count {
				if a.Count > b.Count {
					return -1
				}
				return 1
			}
			return strings.Compare(a.Value, b.Value)
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

func compareWithChunks(ctx context.Context, q *BlocksStoreQueryable, req SeriesCountsRequest, res SeriesCountsResult) (*seriesCountsCheck, error) {
	if res.LowerBound {
		// A partial answer can't be compared with a full one.
		return &seriesCountsCheck{Skipped: "the index path stopped at the budget"}, nil
	}
	start := time.Now()
	chunks, cost, err := q.SeriesCountsFromChunks(ctx, req)
	if err != nil {
		return nil, err
	}
	check := &seriesCountsCheck{ElapsedMS: float64(time.Since(start).Microseconds()) / 1000, Cost: cost}
	check.Examples, check.MismatchedGroups = countsDiff(res.Counts, chunks, 5)
	check.Match = check.MismatchedGroups == 0
	if q.metrics != nil {
		q.metrics.seriesCountsDuration.WithLabelValues("chunks").Observe(time.Since(start).Seconds())
		result := "match"
		if !check.Match {
			result = "mismatch"
		}
		q.metrics.seriesCountsCompared.WithLabelValues(result).Inc()
	}
	return check, nil
}

func parseSeriesCountsRequest(r *http.Request) (SeriesCountsRequest, int, error) {
	var req SeriesCountsRequest
	var err error
	if req.MinT, err = util.ParseTime(r.FormValue("start")); err != nil {
		return req, 0, fmt.Errorf("start: %w", err)
	}
	if req.MaxT, err = util.ParseTime(r.FormValue("end")); err != nil || req.MaxT <= req.MinT {
		return req, 0, errors.New("end must be a timestamp after start")
	}
	if sel := r.Form["match[]"]; len(sel) > 1 {
		return req, 0, errors.New("at most one match[] selector")
	} else if len(sel) == 1 {
		if req.Matchers, err = promqlext.NewPromQLParser().ParseMetricSelector(sel[0]); err != nil {
			return req, 0, fmt.Errorf("match[]: %w", err)
		}
	}
	req.GroupBy = r.FormValue("group_by")
	if req.GroupBy == "" {
		req.GroupBy = labels.MetricName
	}
	if v := r.FormValue("step"); v != "" {
		d, err := model.ParseDuration(v)
		if err != nil || d <= 0 {
			return req, 0, errors.New("step must be a positive duration such as 1h")
		}
		req.Step = time.Duration(d).Milliseconds()
	}
	if v := r.FormValue("budget"); v != "" {
		if req.MaxSeries, err = strconv.ParseInt(v, 10, 64); err != nil || req.MaxSeries < 0 {
			return req, 0, errors.New("budget must be a non-negative integer")
		}
	}
	limit := 0
	if v := r.FormValue("limit"); v != "" {
		if limit, err = strconv.Atoi(v); err != nil || limit < 0 {
			return req, 0, errors.New("limit must be a non-negative integer")
		}
	}
	return req, limit, nil
}

// WithCardinalityCountsRoutes serves MetricNameCountsHandler and
// SeriesCountsHandler under prefix and passes every other request to next.
func WithCardinalityCountsRoutes(next http.Handler, prefix string, q *BlocksStoreQueryable) http.Handler {
	routes := map[string]http.Handler{
		path.Join(prefix, "/api/v1/cardinality/metric_name_counts"): MetricNameCountsHandler(q),
		path.Join(prefix, "/api/v1/cardinality/series_counts"):      SeriesCountsHandler(q),
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h, ok := routes[r.URL.Path]; ok && (r.Method == http.MethodGet || r.Method == http.MethodPost) {
			h.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}
