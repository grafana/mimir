// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/labelaccess"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

type estimateGroup struct {
	Value  string  `json:"value"`
	Count  int64   `json:"count"`
	Counts []int64 `json:"counts,omitempty"`
	// Estimated is set when the count comes from an HLL sketch.
	Estimated bool `json:"estimated,omitempty"`
}

type estimateResponse struct {
	MinTime int64  `json:"min_time"`
	MaxTime int64  `json:"max_time"`
	Snapped bool   `json:"snapped,omitempty"`
	StepMS  int64  `json:"step_ms,omitempty"`
	GroupBy string `json:"group_by"`
	Read    string `json:"read"`
	// Source says where the counts come from: "blocks" for the
	// store-gateways. The ingesters are not consulted yet.
	Source string `json:"source"`
	// LowerBound is set when some blocks were not counted by any
	// store-gateway; MissingBlocks lists them.
	LowerBound    bool     `json:"lower_bound"`
	MissingBlocks []string `json:"missing_blocks,omitempty"`
	Retries       int      `json:"retries,omitempty"`
	// HasMore is set when limit cut the table. Groups dropped by min_count
	// don't set it, since the caller asked for them to be left out.
	HasMore              bool            `json:"has_more"`
	Dedup                bool            `json:"dedup"`
	Blocks               int             `json:"blocks"`
	StoreGateways        int             `json:"store_gateways"`
	Groups               int             `json:"groups"`
	Series               int64           `json:"series"`
	SeriesCounted        int64           `json:"series_counted"`
	PostingsFetchedBytes int64           `json:"postings_fetched_bytes"`
	SeriesFetchedBytes   int64           `json:"series_fetched_bytes"`
	IndexBytes           int64           `json:"index_bytes"`
	EstimatedGroups      int             `json:"estimated_groups"`
	HashBytes            int64           `json:"hash_bytes"`
	SketchBytes          int64           `json:"sketch_bytes"`
	ElapsedMS            float64         `json:"elapsed_ms"`
	Counts               []estimateGroup `json:"counts"`
	Compare              *estimateCheck  `json:"compare,omitempty"`
}

// estimateCheck is the compare=true result: the same request answered by
// loading chunks, and whether every group agrees.
type estimateCheck struct {
	Match            bool           `json:"match"`
	MismatchedGroups int            `json:"mismatched_groups"`
	Examples         []string       `json:"examples,omitempty"`
	Skipped          string         `json:"skipped,omitempty"`
	ElapsedMS        float64        `json:"elapsed_ms"`
	Cost             chunksPathCost `json:"cost"`
}

// CardinalityEstimateHandler serves series counts from the store-gateways
// without loading chunks (see BlocksStoreQueryable.CardinalityEstimate). It
// takes start, end, an optional match[] selector, group_by (default
// __name__), step for per-bucket counts, max_index_bytes to cap the index
// bytes the request reads (over it the request fails with 422 and nothing is
// counted), sketch_above to send an HLL sketch instead of hashes for a group
// with more series than that when deduplicating across block ranges
// (default 10000, 0 for always exact), min_count to drop groups below a
// count, limit to keep the largest groups, and snap=true to widen a window
// inside one block range to that range. With a step, a group's count is its
// largest bucket. read in the response says how it was answered. With
// compare=true, allowed only when cfg.CompareEnabled, it also answers the
// request by loading chunks, as a PromQL count would, and reports whether
// the two agree and what each cost.
//
// Errors: 400 when the request can't be answered as asked, 422 when a limit
// refused it, 500 otherwise. With label-based access control enabled, the
// request's enforced selector is added to the matchers; a policy with more
// than one selector is refused, as the other cardinality endpoints do.
// Experimental.
func CardinalityEstimateHandler(q *BlocksStoreQueryable, cfg CardinalityEstimateConfig, labelAccessControlEnabled bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenantID, err := tenant.TenantID(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		req, opts, err := parseCardinalityEstimateRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		limit := opts.limit
		if opts.compare && !cfg.CompareEnabled {
			http.Error(w, "compare=true is disabled; enable -querier.cardinality-estimate.compare-enabled", http.StatusBadRequest)
			return
		}
		if labelAccessControlEnabled {
			if err := applyLabelAccessPolicies(r.Context(), tenantID, &req); err != nil {
				http.Error(w, err.Error(), estimateHTTPStatus(err))
				return
			}
		}

		start := time.Now()
		res, err := q.CardinalityEstimate(r.Context(), tenantID, &req)
		if err != nil {
			http.Error(w, err.Error(), estimateHTTPStatus(err))
			return
		}
		elapsed := time.Since(start)
		if q.metrics != nil {
			q.metrics.estimateDuration.WithLabelValues(res.Read).Observe(elapsed.Seconds())
		}
		out := estimateResponse{
			MinTime: req.MinT, MaxTime: req.MaxT, Snapped: res.Snapped, StepMS: req.Step, GroupBy: req.GroupBy, Read: res.Read,
			Source: sourceBlocks, LowerBound: res.LowerBound, MissingBlocks: convertULIDsToString(res.MissingBlocks), Retries: res.Retries,
			Dedup: res.Dedup, Blocks: len(res.Blocks), StoreGateways: res.StoreGateways,
			Groups: len(res.Counts), SeriesCounted: res.SeriesCounted,
			PostingsFetchedBytes: res.PostingsFetchedBytes, SeriesFetchedBytes: res.SeriesFetchedBytes, IndexBytes: res.IndexBytes,
			EstimatedGroups: len(res.Estimated), HashBytes: res.HashBytes, SketchBytes: res.SketchBytes,
			ElapsedMS: float64(elapsed.Microseconds()) / 1000,
			Counts:    make([]estimateGroup, 0, len(res.Counts)),
		}
		for v, counts := range res.Counts {
			g := estimateGroup{Value: v, Count: slices.Max(counts), Estimated: res.Estimated[v]}
			if req.Step > 0 {
				g.Counts = counts
			}
			out.Series += g.Count
			if g.Count < opts.minCount {
				continue
			}
			out.Counts = append(out.Counts, g)
		}
		if opts.compare {
			out.Compare = compareWithChunks(r.Context(), q, req, res)
		}
		slices.SortFunc(out.Counts, func(a, b estimateGroup) int {
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
			out.HasMore = true
		}
		w.Header().Set("Content-Type", "application/json")
		if err := writeEstimateResponse(w, &out); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

// applyLabelAccessPolicies adds the tenant's enforced selector to req, so
// the count covers only the series the caller may read. The selector forces
// the full-index read, since the index-header count can't apply matchers.
func applyLabelAccessPolicies(ctx context.Context, tenantID string, req *CardinalityEstimateRequest) error {
	policies, err := labelaccess.ExtractLabelMatchersContext(ctx)
	if err != nil {
		return fmt.Errorf("label-based access control: %w", err)
	}
	selectors := policies[tenantID]
	switch len(selectors) {
	case 0:
		return nil
	case 1:
		req.Matchers = append(req.Matchers, selectors[0].Selector...)
		return nil
	default:
		return badEstimateRequest("the cardinality estimate endpoint doesn't support access policies with more than one selector")
	}
}

// writeEstimateResponse writes the header fields in one JSON object and then
// the groups one at a time, so a table of millions of names is never
// marshalled as one value.
func writeEstimateResponse(w io.Writer, out *estimateResponse) error {
	head, err := json.Marshal(struct {
		*estimateResponse
		Counts []estimateGroup `json:"counts,omitempty"`
	}{estimateResponse: out})
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(w)
	// head ends with "}", which the counts array replaces.
	if _, err := bw.Write(head[:len(head)-1]); err != nil {
		return err
	}
	if _, err := bw.WriteString(`,"counts":[`); err != nil {
		return err
	}
	for i := range out.Counts {
		if i > 0 {
			if err := bw.WriteByte(','); err != nil {
				return err
			}
		}
		b, err := json.Marshal(&out.Counts[i])
		if err != nil {
			return err
		}
		if _, err := bw.Write(b); err != nil {
			return err
		}
	}
	if _, err := bw.WriteString("]}\n"); err != nil {
		return err
	}
	return bw.Flush()
}

// compareWithChunks reports a chunks-path failure, such as a query limit,
// in Skipped rather than failing the request, since that is a result too.
func compareWithChunks(ctx context.Context, q *BlocksStoreQueryable, req CardinalityEstimateRequest, res CardinalityEstimateResult) *estimateCheck {
	start := time.Now()
	chunks, cost, err := q.SeriesCountsFromChunks(ctx, req)
	check := &estimateCheck{ElapsedMS: float64(time.Since(start).Microseconds()) / 1000, Cost: cost}
	if err != nil {
		check.Skipped = "chunks path failed: " + err.Error()
		return check
	}
	check.Examples, check.MismatchedGroups = countsDiff(res.Counts, chunks, 5)
	check.Match = check.MismatchedGroups == 0
	if q.metrics != nil {
		q.metrics.estimateDuration.WithLabelValues(readChunks).Observe(time.Since(start).Seconds())
		result := "match"
		if !check.Match {
			result = "mismatch"
		}
		q.metrics.estimateCompared.WithLabelValues(result).Inc()
	}
	return check
}

// Sources a cardinality estimate can count from, as reported in the
// response's source field.
const sourceBlocks = "blocks"

// estimateRequestOptions are the route's parameters that shape the response
// rather than the count.
type estimateRequestOptions struct {
	limit    int
	minCount int64
	compare  bool
}

func parseCardinalityEstimateRequest(r *http.Request) (CardinalityEstimateRequest, estimateRequestOptions, error) {
	var (
		req  CardinalityEstimateRequest
		opts estimateRequestOptions
		err  error
	)
	if req.MinT, err = util.ParseTime(r.FormValue("start")); err != nil {
		return req, opts, fmt.Errorf("start: %w", err)
	}
	if req.MaxT, err = util.ParseTime(r.FormValue("end")); err != nil || req.MaxT <= req.MinT {
		return req, opts, errors.New("end must be a timestamp after start")
	}
	req.Snap = r.FormValue("snap") == "true"
	opts.compare = r.FormValue("compare") == "true"
	if sel := r.Form["match[]"]; len(sel) > 1 {
		return req, opts, errors.New("at most one match[] selector")
	} else if len(sel) == 1 {
		if req.Matchers, err = promqlext.NewPromQLParser().ParseMetricSelector(sel[0]); err != nil {
			return req, opts, fmt.Errorf("match[]: %w", err)
		}
	}
	req.GroupBy = r.FormValue("group_by")
	if req.GroupBy == "" {
		req.GroupBy = labels.MetricName
	}
	if v := r.FormValue("step"); v != "" {
		d, err := model.ParseDuration(v)
		if err != nil || d <= 0 {
			return req, opts, errors.New("step must be a positive duration such as 1h")
		}
		req.Step = time.Duration(d).Milliseconds()
	}
	if v := r.FormValue("max_index_bytes"); v != "" {
		if req.MaxIndexBytes, err = strconv.ParseInt(v, 10, 64); err != nil || req.MaxIndexBytes < 0 {
			return req, opts, errors.New("max_index_bytes must be a non-negative integer")
		}
	}
	req.SketchAbove = defaultSketchAbove
	if v := r.FormValue("sketch_above"); v != "" {
		if req.SketchAbove, err = strconv.ParseInt(v, 10, 64); err != nil || req.SketchAbove < 0 {
			return req, opts, errors.New("sketch_above must be a non-negative integer")
		}
	}
	if v := r.FormValue("shards"); v != "" {
		if req.Shards, err = strconv.Atoi(v); err != nil || req.Shards < 0 || req.Shards > 128 {
			return req, opts, errors.New("shards must be an integer from 0 to 128")
		}
	}
	if v := r.FormValue("min_count"); v != "" {
		if opts.minCount, err = strconv.ParseInt(v, 10, 64); err != nil || opts.minCount < 0 {
			return req, opts, errors.New("min_count must be a non-negative integer")
		}
	}
	if v := r.FormValue("limit"); v != "" {
		if opts.limit, err = strconv.Atoi(v); err != nil || opts.limit < 0 {
			return req, opts, errors.New("limit must be a non-negative integer")
		}
	}
	return req, opts, nil
}

// defaultSketchAbove is the design's switch from exact hashes to a sketch.
const defaultSketchAbove = 10_000

// WithCardinalityEstimateRoute serves CardinalityEstimateHandler on
// prefix/api/v1/cardinality/estimate and passes every other request to next.
func WithCardinalityEstimateRoute(next http.Handler, prefix string, q *BlocksStoreQueryable, cfg CardinalityEstimateConfig, labelAccessControlEnabled bool) http.Handler {
	route, h := path.Join(prefix, "/api/v1/cardinality/estimate"), CardinalityEstimateHandler(q, cfg, labelAccessControlEnabled)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == route && (r.Method == http.MethodGet || r.Method == http.MethodPost) {
			h.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}
