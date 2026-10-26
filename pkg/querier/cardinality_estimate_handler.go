// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"bufio"
	"bytes"
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

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/labelaccess"
	"github.com/grafana/mimir/pkg/querier/worker"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

type estimateGroup struct {
	Value  string  `json:"value"`
	Count  int64   `json:"count"`
	Counts []int64 `json:"counts,omitempty"`
	// Estimated is set when the count comes from an HLL sketch.
	Estimated bool `json:"estimated,omitempty"`
}

// estimateWindow is the window actually counted, [Start, End) in
// milliseconds, after any snap.
type estimateWindow struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
	// Snapped is set when snap=true widened the window asked for to the
	// block range holding it.
	Snapped bool `json:"snapped,omitempty"`
}

type estimateResponse struct {
	Window estimateWindow `json:"window"`
	StepMS int64          `json:"step_ms,omitempty"`
	// WindowMS is each bucket's width with step_ms when it is not the step:
	// the count at a bucket's end is the series with a sample in the
	// window_ms before it, and window starts window_ms before the first
	// bucket's end.
	WindowMS int64  `json:"window_ms,omitempty"`
	GroupBy  string `json:"group_by"`
	Read     string `json:"read"`
	// Source says where the counts come from: "blocks" for the
	// store-gateways, "head" for the ingesters, "blocks+head" for both.
	Source string `json:"source"`
	// HeadWhole is set when the counts are every series held in the
	// ingesters' heads, whatever the window, since head postings have no
	// time axis.
	HeadWhole bool `json:"head_whole,omitempty"`
	// HeadFallback says why the head walk was refused when the head was
	// counted as one bucket instead.
	HeadFallback string `json:"head_fallback,omitempty"`
	// HeadMinTime is the time from which every ingester still holds data,
	// when it is after the window's start; the counts are then a lower bound.
	HeadMinTime int64 `json:"head_min_time,omitempty"`
	// CoverageGap is set when part of the window is held by no block and
	// by no ingester; the counts are then a lower bound.
	CoverageGap bool `json:"coverage_gap,omitempty"`
	// LowerBound is set when some blocks were not counted by any
	// store-gateway, which MissingBlocks lists, or when the window starts
	// before what the ingesters hold, which HeadMinTime says.
	LowerBound    bool     `json:"lower_bound"`
	MissingBlocks []string `json:"missing_blocks,omitempty"`
	Retries       int      `json:"retries,omitempty"`
	// HasMore is set when limit cut the table. Groups dropped by min_count
	// don't set it, since the caller asked for them to be left out.
	HasMore       bool `json:"has_more"`
	Dedup         bool `json:"dedup"`
	Blocks        int  `json:"blocks"`
	StoreGateways int  `json:"store_gateways"`
	GroupCount    int  `json:"group_count"`
	// Total is the sum of every group's count, including the groups that
	// min_count or limit left out of the table.
	Total                int64 `json:"total"`
	SeriesCounted        int64 `json:"series_counted"`
	PostingsFetchedBytes int64 `json:"postings_fetched_bytes"`
	SeriesFetchedBytes   int64 `json:"series_fetched_bytes"`
	IndexBytes           int64 `json:"index_bytes"`
	// PostingsOffsetsTableBytes is the postings offsets table the
	// store-gateways walked, which max_index_bytes bounds with index_bytes.
	PostingsOffsetsTableBytes int64           `json:"postings_offsets_table_bytes"`
	BlocksFromPostings        int64           `json:"blocks_from_postings,omitempty"`
	EstimatedGroups           int             `json:"estimated_groups"`
	HashBytes                 int64           `json:"hash_bytes"`
	SketchBytes               int64           `json:"sketch_bytes"`
	ElapsedMS                 float64         `json:"elapsed_ms"`
	Groups                    []estimateGroup `json:"groups"`
	Compare                   *estimateCheck  `json:"compare,omitempty"`
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
// or the ingesters without loading chunks (see CardinalityEstimator.Estimate).
// It takes start, end, an optional match[] selector, group_by (default
// __name__), step for per-bucket counts, max_index_bytes to cap the index
// bytes the request reads, at most the tenant's
// -querier.cardinality-estimate-max-index-bytes and that limit when unset
// (over it the request fails with 422 and nothing is counted), sketch_above to send an HLL sketch instead of hashes for a group
// with more series than that when deduplicating across block ranges
// (default 10000, 0 for always exact), min_count to drop groups below a
// count, limit to keep the largest groups, and snap=true to widen a window
// inside one block range to that range. With a step, a group's count is its
// largest bucket. The table is sorted largest first, except a name table
// read from the index-headers without a limit, which is merged from the
// store-gateways' sorted streams as it is written and so is sorted by name
// (see writeNameTable). read in the response says how it was answered. With
// compare=true, allowed only when cfg.CompareEnabled, it also answers the
// request by loading chunks, as a PromQL count would, and reports whether
// the two agree and what each cost.
//
// Errors: 400 when the endpoint is disabled for the tenant
// (-querier.cardinality-estimate-enabled) or the request can't be answered
// as asked, 422 when a limit refused it, 500 otherwise. With label-based
// access control enabled, the request's enforced selector is added to the
// matchers; a policy with more than one selector is refused, as the other
// cardinality endpoints do.
// Experimental.
func CardinalityEstimateHandler(e *CardinalityEstimator, cfg CardinalityEstimateConfig, labelAccessControlEnabled bool) http.Handler {
	q := e.blocks
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenantID, err := tenant.TenantID(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !e.enabledFor(tenantID) {
			http.Error(w, fmt.Sprintf("cardinality estimate is disabled for the tenant: %v", tenantID), http.StatusBadRequest)
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
		// The name table from the index-headers is merged as it is written,
		// except for compare=true, which needs the counts to compare, and
		// per-bucket counts, which answer each bucket as its own estimate.
		req.StreamNames = !opts.compare && req.Step == 0
		res, err := e.Estimate(r.Context(), tenantID, &req)
		if err != nil {
			logEstimateSourceError(r.Context(), q.logger, err)
			http.Error(w, err.Error(), estimateHTTPStatus(err))
			return
		}
		elapsed := time.Since(start)
		out := estimateResponse{
			Window: estimateWindow{Start: req.MinT, End: req.MaxT, Snapped: res.Snapped}, StepMS: req.Step, WindowMS: req.windowMs(), GroupBy: req.GroupBy, Read: res.Read,
			Source: res.Source, HeadWhole: res.HeadWhole, HeadFallback: res.HeadFallback, HeadMinTime: res.HeadMinTime, CoverageGap: res.CoverageGap, LowerBound: res.LowerBound, MissingBlocks: convertULIDsToString(res.MissingBlocks), Retries: res.Retries,
			Dedup: res.Dedup, Blocks: len(res.Blocks), StoreGateways: res.StoreGateways,
			GroupCount: len(res.Counts), SeriesCounted: res.SeriesCounted,
			PostingsFetchedBytes: res.PostingsFetchedBytes, SeriesFetchedBytes: res.SeriesFetchedBytes, IndexBytes: res.IndexBytes, PostingsOffsetsTableBytes: res.PostingsOffsetsTableBytes, BlocksFromPostings: res.BlocksFromPostings,
			EstimatedGroups: len(res.Estimated), HashBytes: res.HashBytes, SketchBytes: res.SketchBytes,
			ElapsedMS: float64(elapsed.Microseconds()) / 1000,
			Groups:    make([]estimateGroup, 0, len(res.Counts)),
		}
		if res.names != nil {
			defer res.names.close()
			started, err := writeNameTable(r.Context(), w, &out, res.names, opts, start)
			if err != nil {
				logEstimateSourceError(r.Context(), q.logger, err)
				if !started {
					http.Error(w, err.Error(), estimateHTTPStatus(err))
					return
				}
				// The status is sent: the body says what failed, and so does the log.
				logger := log.Logger(log.NewNopLogger())
				if q.logger != nil {
					logger = q.logger
				}
				level.Warn(util_log.WithContext(r.Context(), logger)).Log("msg", "cardinality estimate name table failed after it started", "err", err)
				return
			}
			if q.metrics != nil {
				q.metrics.estimateDuration.WithLabelValues(res.Read).Observe(time.Since(start).Seconds())
			}
			return
		}
		if q.metrics != nil {
			q.metrics.estimateDuration.WithLabelValues(res.Read).Observe(elapsed.Seconds())
		}
		for v, counts := range res.Counts {
			g := estimateGroup{Value: v, Count: slices.Max(counts), Estimated: res.Estimated[v]}
			if req.Step > 0 {
				g.Counts = counts
			}
			out.Total += g.Count
			if g.Count < opts.minCount {
				continue
			}
			out.Groups = append(out.Groups, g)
		}
		if opts.compare {
			out.Compare = compareWithChunks(r.Context(), q, req, res)
		}
		slices.SortFunc(out.Groups, func(a, b estimateGroup) int {
			if a.Count != b.Count {
				if a.Count > b.Count {
					return -1
				}
				return 1
			}
			return strings.Compare(a.Value, b.Value)
		})
		if limit > 0 && limit < len(out.Groups) {
			out.Groups = out.Groups[:limit]
			out.HasMore = true
		}
		w.Header().Set("Content-Type", "application/json")
		setEstimateCacheControl(w, &out)
		if err := writeEstimateResponse(w, &out); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

// logEstimateSourceError logs the address of the store-gateway an estimate
// error came from, which the error text returned to the caller leaves out.
func logEstimateSourceError(ctx context.Context, logger log.Logger, err error) {
	addr := estimateSourceAddr(err)
	if addr == "" || logger == nil {
		return
	}
	level.Warn(util_log.WithContext(ctx, logger)).Log("msg", "cardinality estimate failed on a store-gateway", "store_gateway", addr, "err", err)
}

// setEstimateCacheControl marks an answer that is not complete, that counts
// the whole head whatever the window (head_whole, also when one bucket of a
// per-bucket answer did), or that loaded chunks for compare=true, so the
// query-frontend's cache doesn't keep it. A table cut by limit or min_count
// is complete for the parameters asked, which are in the cache key.
func setEstimateCacheControl(w http.ResponseWriter, out *estimateResponse) {
	if out.LowerBound || out.CoverageGap || out.HeadWhole || out.Compare != nil {
		w.Header().Set("Cache-Control", "no-store")
	}
}

// nameTableBufferBytes is how much of a name table's rows the handler holds
// before it starts sending them. A table that fits is answered whole, so a
// failure while merging still gets an error status. It is the
// query-frontend cache's body cap, so a table that doesn't fit could not be
// cached anyway.
const nameTableBufferBytes = 1 << 20

// writeNameTable merges t into the response's table and writes it. With a
// limit, only the largest groups are held, and the table is written once
// merged, largest first. Without one, rows are written in name order as they
// are merged; a table larger than nameTableBufferBytes is sent before the
// merge ends, marked Cache-Control: no-store and streamed from the querier
// to the query-frontend in chunks, with groups, total and has_more after the
// counts. started reports whether anything was written when err is returned;
// a merge that fails after that ends the table with an error field in place
// of the totals. A store-gateway lost while merging fails the table: its
// blocks are retried only while the calls are opened (openNameTable), since
// rows already merged can't be taken back.
func writeNameTable(ctx context.Context, w http.ResponseWriter, out *estimateResponse, t *nameTable, opts estimateRequestOptions, start time.Time) (started bool, err error) {
	if opts.limit > 0 {
		top := &topGroups{n: opts.limit, tracker: t.tracker}
		if err := t.merge(ctx, func(name string, count int64) error {
			out.GroupCount++
			out.Total += count
			if count < opts.minCount {
				return nil
			}
			return top.offer(estimateGroup{Value: name, Count: count})
		}); err != nil {
			return false, err
		}
		out.Groups, out.HasMore = top.sorted()
		out.ElapsedMS = float64(time.Since(start).Microseconds()) / 1000
		w.Header().Set("Content-Type", "application/json")
		setEstimateCacheControl(w, out)
		return true, writeEstimateResponse(w, out)
	}

	var (
		rows bytes.Buffer
		n    int
	)
	writeHead := func() error {
		started = true
		out.ElapsedMS = float64(time.Since(start).Microseconds()) / 1000
		return writeNameTableHead(w, out)
	}
	mergeErr := t.merge(ctx, func(name string, count int64) error {
		out.GroupCount++
		out.Total += count
		if count < opts.minCount {
			return nil
		}
		if n > 0 {
			rows.WriteByte(',')
		}
		n++
		b, err := json.Marshal(estimateGroup{Value: name, Count: count})
		if err != nil {
			return err
		}
		rows.Write(b)
		if rows.Len() < nameTableBufferBytes {
			return nil
		}
		if !started {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set(worker.ResponseStreamingEnabledHeader, "true")
			if err := writeHead(); err != nil {
				return err
			}
		}
		_, err = w.Write(rows.Bytes())
		rows.Reset()
		return err
	})
	if mergeErr != nil && !started {
		return false, mergeErr
	}
	if !started {
		w.Header().Set("Content-Type", "application/json")
		setEstimateCacheControl(w, out)
		if err := writeHead(); err != nil {
			return true, err
		}
	}
	if _, err := w.Write(rows.Bytes()); err != nil {
		return true, err
	}
	if mergeErr != nil {
		msg, _ := json.Marshal(mergeErr.Error())
		_, err := fmt.Fprintf(w, `],"error":%s}`+"\n", msg)
		return true, errors.Join(mergeErr, err)
	}
	_, err = fmt.Fprintf(w, `],"group_count":%d,"total":%d,"has_more":false}`+"\n", out.GroupCount, out.Total)
	return true, err
}

// writeNameTableHead writes the response up to the start of its groups
// array, leaving out the fields known only once the table is merged.
func writeNameTableHead(w io.Writer, out *estimateResponse) error {
	head, err := json.Marshal(struct {
		*estimateResponse
		Groups     []estimateGroup `json:"groups,omitempty"`
		Total      *int64          `json:"total,omitempty"`
		GroupCount *int            `json:"group_count,omitempty"`
		HasMore    *bool           `json:"has_more,omitempty"`
	}{estimateResponse: out})
	if err != nil {
		return err
	}
	// head ends with "}", which the groups array replaces.
	if _, err := w.Write(head[:len(head)-1]); err != nil {
		return err
	}
	_, err = io.WriteString(w, `,"groups":[`)
	return err
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
		Groups []estimateGroup `json:"groups,omitempty"`
	}{estimateResponse: out})
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(w)
	// head ends with "}", which the groups array replaces.
	if _, err := bw.Write(head[:len(head)-1]); err != nil {
		return err
	}
	if _, err := bw.WriteString(`,"groups":[`); err != nil {
		return err
	}
	for i := range out.Groups {
		if i > 0 {
			if err := bw.WriteByte(','); err != nil {
				return err
			}
		}
		b, err := json.Marshal(&out.Groups[i])
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
	// window, with step, is how far back each bucket's count looks from the
	// bucket's end. The buckets still end every step from start+step, so
	// zooming changes how many there are, not what each one counts; the
	// window read starts one window before the first bucket's end.
	if v := r.FormValue("window"); v != "" {
		d, err := model.ParseDuration(v)
		if err != nil || d <= 0 {
			return req, opts, errors.New("window must be a positive duration such as 1h")
		}
		if req.Step == 0 {
			return req, opts, errors.New("window needs step")
		}
		if req.Snap {
			return req, opts, errors.New("window can't be combined with snap")
		}
		req.Window = time.Duration(d).Milliseconds()
		if req.Window < req.Step {
			return req, opts, errors.New("window must be at least step, so every sample falls in a bucket")
		}
		if (req.MaxT-req.MinT)%req.Step != 0 {
			return req, opts, fmt.Errorf("step %d doesn't divide the window [%d, %d)", req.Step, req.MinT, req.MaxT)
		}
		req.MinT += req.Step - req.Window
		if req.MinT < 0 {
			return req, opts, errors.New("window reaches back before time 0 from the first bucket's end; use a later start or a shorter window")
		}
	}
	// Every step must cut the window into whole buckets, with or without a
	// window parameter. A snapped window is cut once it is widened to a block
	// range, so its buckets are checked then. The sources check this too, but
	// a version that ignores step_ms would not.
	if req.Step > 0 && !req.Snap {
		if _, err := cardinality.Buckets(req.MinT, req.MaxT, req.Step, req.Window); err != nil {
			return req, opts, fmt.Errorf("step: %w", err)
		}
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
func WithCardinalityEstimateRoute(next http.Handler, prefix string, e *CardinalityEstimator, cfg CardinalityEstimateConfig, labelAccessControlEnabled bool) http.Handler {
	route, h := path.Join(prefix, "/api/v1/cardinality/estimate"), CardinalityEstimateHandler(e, cfg, labelAccessControlEnabled)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == route && (r.Method == http.MethodGet || r.Method == http.MethodPost) {
			h.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}
