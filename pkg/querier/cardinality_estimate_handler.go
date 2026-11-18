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
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/labelaccess"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/promqlext"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type estimateGroup struct {
	Value string `json:"value"`
	// Count is the series in the group; with step, the largest of Counts.
	Count int64 `json:"count"`
	// Counts, with step, is the group's count in each bucket, in time order.
	Counts []int64 `json:"counts,omitempty"`
}

// estimateWindow is the window actually counted, [Start, End) in milliseconds.
type estimateWindow struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

type estimateResponse struct {
	Window estimateWindow `json:"window"`
	StepMS int64          `json:"step_ms,omitempty"`
	// WindowMS is each bucket's width when it is not the step.
	WindowMS int64  `json:"window_ms,omitempty"`
	GroupBy  string `json:"group_by"`
	// Read says how the sources were read, one of the Read values.
	Read string `json:"read"`
	// Source says where the counts come from, one of the Source values.
	Source string `json:"source"`
	// HeadMinTime is the time from which every ingester holds data, when after the window's start. The counts are then a lower bound.
	HeadMinTime int64 `json:"head_min_time,omitempty"`
	// CoverageGap is set when part of the window is held by no block and no ingester.
	CoverageGap bool `json:"coverage_gap,omitempty"`
	// LowerBound is set when the counts leave out series: MissingBlocks, MissingShards or HeadMinTime say which.
	LowerBound    bool           `json:"lower_bound"`
	MissingBlocks []string       `json:"missing_blocks,omitempty"`
	MissingShards []missingShard `json:"missing_shards,omitempty"`
	// HasMore is set when limit cut the table. Groups dropped by min_count don't set it.
	HasMore bool `json:"has_more"`
	Dedup   bool `json:"dedup"`
	// GroupCount is the number of groups, including those left out of the table.
	GroupCount int `json:"group_count"`
	// Total is the sum of every group's count, including those left out of the table. With step it sums each group's largest bucket.
	Total int64 `json:"total"`
	// Totals, with step, is the sum of every group's count in each bucket, including those left out.
	Totals []int64         `json:"totals,omitempty"`
	Groups []estimateGroup `json:"groups"`
}

// NewCardinalityEstimateHandler serves series counts from the store-gateways or the ingesters without loading chunks (see CardinalityEstimator.Estimate).
// It takes start, end, match[], group_by (default __name__), step, window, max_index_bytes, min_count and limit.
// The table is sorted largest first.
// start is moved up to -querier.max-query-lookback and -compactor.blocks-retention-period, and a window wholly before them is answered with no groups.
// Errors: 400 when the endpoint is disabled or the request can't be answered as asked, 422 when a limit refused it, 503 past -querier.timeout or when every source asked was at its limit of concurrent counts, 500 otherwise, each with a Prometheus API error body.
// With label-based access control, the enforced selector is added to the matchers and a policy with more than one selector is refused.
// Experimental.
func NewCardinalityEstimateHandler(e *CardinalityEstimator, cfg CardinalityEstimateConfig, labelAccessControlEnabled bool) http.Handler {
	q := e.blocks
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		tenantID, err := tenant.TenantID(r.Context())
		if err != nil {
			q.observeRefusedRequest(time.Since(start), badEstimateRequest("refused before the estimate"))
			writeEstimateErrorStatus(w, http.StatusBadRequest, err.Error())
			return
		}
		if !e.enabledFor(tenantID) {
			q.observeRefusedRequest(time.Since(start), badEstimateRequest("refused before the estimate"))
			status, body := cardinality.DisabledResponse(tenantID)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write(body)
			return
		}
		req, opts, err := parseCardinalityEstimateRequest(r, e.maxGroupByLength(tenantID))
		if err != nil {
			q.observeRefusedRequest(time.Since(start), badEstimateRequest("refused before the estimate"))
			writeEstimateErrorStatus(w, http.StatusBadRequest, err.Error())
			return
		}
		limit := opts.limit
		if labelAccessControlEnabled {
			restricted, err := applyLabelAccessPolicies(r.Context(), tenantID, &req)
			if err != nil {
				q.observeRefusedRequest(time.Since(start), err)
				writeEstimateError(w, err)
				return
			}
			// The answer holds only what the policy lets its caller see, so no cache may hand it to another, whichever cache key generator the query-frontend runs.
			if restricted {
				w.Header().Set("Cache-Control", "no-store")
			}
		}

		// -querier.timeout bounds an estimate as it bounds a query.
		ctx := r.Context()
		if cfg.Timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, cfg.Timeout)
			defer cancel()
		}
		// timedOut classes an error by whether the estimate timed out or its caller went away.
		timedOut := func(err error) error {
			switch {
			case err == nil:
				return nil
			case errors.Is(r.Context().Err(), context.Canceled):
				return canceledEstimate(err)
			case errors.Is(ctx.Err(), context.DeadlineExceeded):
				return timedOutEstimate(cfg.Timeout, err)
			}
			return err
		}
		// A window the tenant's queries can't read is answered empty, as a query over it is.
		res := CardinalityEstimateResult{Source: SourceBlocks, Read: ReadFullIndex, Counts: map[string][]int64{}}
		if e.clampToLookback(spanlogger.FromContext(ctx, q.logger), tenantID, &req) {
			// The bucket cap applies to the window the clamp left.
			if _, err = req.expectedBuckets(); err == nil {
				res, err = e.Estimate(ctx, tenantID, &req)
			}
		}
		// The sources apply max_groups to their own answer, their merge can hold more.
		if limit := req.MaxGroups; err == nil && limit > 0 && int64(len(res.Counts)) > limit {
			err = errOverMaxGroups("the sources", limit, req.GroupBy)
		}
		if err = timedOut(err); err != nil {
			q.observeEstimate(r.Context(), req, res, nil, time.Since(start), err)
			writeEstimateError(w, err)
			return
		}
		out := estimateResponse{
			Window: estimateWindow{Start: req.MinT, End: req.MaxT}, StepMS: req.Step, WindowMS: req.windowMs(), GroupBy: req.GroupBy, Read: res.Read,
			Source: res.Source, HeadMinTime: res.HeadMinTime, CoverageGap: res.CoverageGap, LowerBound: res.LowerBound, MissingBlocks: convertULIDsToString(res.MissingBlocks), MissingShards: res.MissingShards,
			Dedup: res.Dedup, GroupCount: len(res.Counts),
			Groups: make([]estimateGroup, 0, len(res.Counts)),
		}
		for v, counts := range res.Counts {
			g := estimateGroup{Value: v, Count: slices.Max(counts)}
			if req.Step > 0 {
				g.Counts = counts
				if out.Totals == nil {
					out.Totals = make([]int64, len(counts))
				}
				for i, c := range counts {
					out.Totals[i] += c
				}
			}
			out.Total += g.Count
			if g.Count < opts.minCount {
				continue
			}
			out.Groups = append(out.Groups, g)
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
		// A failed write has started the body, so the failure goes to the log line.
		err = timedOut(writeEstimateResponse(w, &out))
		q.observeEstimate(r.Context(), req, res, &out, time.Since(start), err)
	})
}

// setEstimateCacheControl marks an answer that is incomplete, so the query-frontend's cache doesn't keep it.
func setEstimateCacheControl(w http.ResponseWriter, out *estimateResponse) {
	if out.LowerBound || out.CoverageGap {
		w.Header().Set("Cache-Control", "no-store")
	}
}

// applyLabelAccessPolicies adds the tenant's enforced selector to req, and reports whether there was one. It forces the full-index read, since the index-header count can't apply matchers.
func applyLabelAccessPolicies(ctx context.Context, tenantID string, req *CardinalityEstimateRequest) (bool, error) {
	policies, err := labelaccess.ExtractLabelMatchersContext(ctx)
	if err != nil {
		return false, fmt.Errorf("label-based access control: %w", err)
	}
	selectors := policies[tenantID]
	switch len(selectors) {
	case 0:
		return false, nil
	case 1:
		// A policy with no selector is malformed, and is refused rather than answered unrestricted.
		if selectors[0] == nil || len(selectors[0].Selector) == 0 {
			return false, badEstimateRequest("label-based access control: the tenant's access policy has no selector")
		}
		req.Matchers = append(req.Matchers, selectors[0].Selector...)
		return true, nil
	default:
		return false, badEstimateRequest("the cardinality estimate endpoint doesn't support access policies with more than one selector")
	}
}

// writeEstimateResponse writes the header fields and then the groups one at a time, so a large table is never marshalled as one value.
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

// estimateRequestOptions are the route's parameters that shape the response, not the count.
type estimateRequestOptions struct {
	limit    int
	minCount int64
}

// CardinalityEstimateRequestParams are every parameter parseCardinalityEstimateRequest reads, for the query-frontend's cache key. A test checks the two agree.
var CardinalityEstimateRequestParams = []string{"start", "end", "match[]", "group_by", "step", "window", "limit", "min_count", "max_index_bytes"}

// parseCardinalityEstimateRequest reads the route's parameters. maxGroupBy caps the length of group_by.
func parseCardinalityEstimateRequest(r *http.Request, maxGroupBy int) (CardinalityEstimateRequest, estimateRequestOptions, error) {
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
	if sel := r.Form["match[]"]; len(sel) > 1 {
		return req, opts, errors.New("at most one match[] selector")
	} else if len(sel) == 1 {
		if req.Matchers, err = promqlext.NewPromQLParser().ParseMetricSelector(sel[0]); err != nil {
			return req, opts, fmt.Errorf("match[]: %w", err)
		}
		req.Matchers = slices.DeleteFunc(req.Matchers, matchesEveryName)
		if len(req.Matchers) == 0 {
			req.Matchers = nil
		}
	}
	req.GroupBy = r.FormValue("group_by")
	if req.GroupBy == "" {
		req.GroupBy = model.MetricNameLabel
	}
	// group_by is echoed into the response, log and spans, so it must be a label name the tenant can have written.
	if maxGroupBy > 0 && len(req.GroupBy) > maxGroupBy {
		return req, opts, fmt.Errorf("group_by is %d bytes long, more than the %d a label name may be", len(req.GroupBy), maxGroupBy)
	}
	if !model.UTF8Validation.IsValidLabelName(req.GroupBy) {
		return req, opts, errors.New("group_by must be a label name")
	}
	if strings.ContainsFunc(req.GroupBy, unicode.IsControl) {
		return req, opts, errors.New("group_by must not hold control characters")
	}
	if v := r.FormValue("step"); v != "" {
		d, err := model.ParseDuration(v)
		if err != nil || d <= 0 {
			return req, opts, errors.New("step must be a positive duration such as 1h")
		}
		req.Step = time.Duration(d).Milliseconds()
	}
	// window, with step, is how far back each bucket's count looks from its end.
	if v := r.FormValue("window"); v != "" {
		d, err := model.ParseDuration(v)
		if err != nil || d <= 0 {
			return req, opts, errors.New("window must be a positive duration such as 1h")
		}
		if req.Step == 0 {
			return req, opts, errors.New("window needs step")
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
	// Every step must cut the window into whole buckets. Their number is checked once the window is clamped to the lookback.
	if req.Step > 0 {
		if _, err := cardinality.Buckets(req.MinT, req.MaxT, req.Step, req.Window); err != nil && !errors.Is(err, cardinality.ErrTooManyBuckets) {
			return req, opts, fmt.Errorf("step: %w", err)
		}
	}
	if v := r.FormValue("max_index_bytes"); v != "" {
		if req.MaxIndexBytes, err = strconv.ParseInt(v, 10, 64); err != nil || req.MaxIndexBytes < 0 {
			return req, opts, errors.New("max_index_bytes must be a non-negative integer")
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

// matchesEveryName reports whether m is a matcher on __name__ that every series passes, such as =~".+". Read as a matcher it would fetch every name's postings instead of taking the no-matcher paths.
func matchesEveryName(m *labels.Matcher) bool {
	if m.Name != model.MetricNameLabel {
		return false
	}
	switch m.Type {
	case labels.MatchRegexp:
		return m.Value == ".+" || m.Value == ".*"
	case labels.MatchNotEqual, labels.MatchNotRegexp:
		return m.Value == ""
	}
	return false
}
