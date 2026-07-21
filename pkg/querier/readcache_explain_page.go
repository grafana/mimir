// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

// ReadcacheExplainPathPrefix is the URL prefix under which the querier's
// readcache-lookup explain page is mounted.
const ReadcacheExplainPathPrefix = "/querier/readcache-lookups"

// ReadcacheQueryExplainer resolves the readcache QueryStream fan-out for
// a single Select without executing it. *distributor.Distributor
// satisfies this.
type ReadcacheQueryExplainer interface {
	ExplainReadcacheQuery(ctx context.Context, userID string, from, to model.Time, matchers []*labels.Matcher) distributor.ReadcacheQueryPlan
}

// ReadcacheExplainHandler serves an operator debug page that, given a
// PromQL query and a time range, lists every readcache QueryStream call
// the querier would make and for which (partition, owner) — without
// executing any of them. It analyzes the query exactly as pasted: it
// does not emulate the query-frontend's query-sharding rewrite, so the
// fan-out shown is for this single (unsharded) query.
type ReadcacheExplainHandler struct {
	explainer     ReadcacheQueryExplainer
	lookbackDelta time.Duration
	logger        log.Logger
}

// NewReadcacheExplainHandler builds the readcache-lookup explain page
// handler. lookbackDelta should be the querier's configured lookback so
// per-selector time windows match query evaluation.
func NewReadcacheExplainHandler(explainer ReadcacheQueryExplainer, lookbackDelta time.Duration, logger log.Logger) *ReadcacheExplainHandler {
	if lookbackDelta <= 0 {
		lookbackDelta = 5 * time.Minute
	}
	return &ReadcacheExplainHandler{explainer: explainer, lookbackDelta: lookbackDelta, logger: logger}
}

// selectorInfo is one vector/matrix selector extracted from the query,
// with the effective sample-time window its Select would cover.
type selectorInfo struct {
	expr     string
	matchers []*labels.Matcher
	rng      time.Duration // matrix range, 0 for an instant vector selector
	offset   time.Duration
	from     model.Time
	to       model.Time
}

func (h *ReadcacheExplainHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("failed to parse form: %v", err), http.StatusBadRequest)
		return
	}

	data := explainPageData{
		Query:         strings.TrimSpace(r.FormValue("query")),
		Tenant:        strings.TrimSpace(r.FormValue("tenant")),
		Start:         strings.TrimSpace(r.FormValue("start")),
		End:           strings.TrimSpace(r.FormValue("end")),
		Step:          strings.TrimSpace(r.FormValue("step")),
		LookbackDelta: h.lookbackDelta.String(),
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
	}

	if data.Query == "" {
		h.render(w, data)
		return
	}
	data.Submitted = true

	if data.Tenant == "" {
		data.Error = "a tenant (X-Scope-OrgID) is required to resolve readcache routing"
		h.render(w, data)
		return
	}

	now := time.Now()
	end, err := parseTimeParam(data.End, now)
	if err != nil {
		data.Error = fmt.Sprintf("invalid end time: %v", err)
		h.render(w, data)
		return
	}
	start, err := parseTimeParam(data.Start, end.Add(-time.Hour))
	if err != nil {
		data.Error = fmt.Sprintf("invalid start time: %v", err)
		h.render(w, data)
		return
	}
	if end.Before(start) {
		data.Error = "end time is before start time"
		h.render(w, data)
		return
	}
	data.ResolvedStart = start.UTC().Format(time.RFC3339)
	data.ResolvedEnd = end.UTC().Format(time.RFC3339)

	selectors, err := h.extractSelectors(data.Query, start, end)
	if err != nil {
		data.Error = fmt.Sprintf("failed to parse query: %v", err)
		h.render(w, data)
		return
	}
	if len(selectors) == 0 {
		data.Error = "the query contains no series selectors"
		h.render(w, data)
		return
	}

	distinctPartitions := map[int32]struct{}{}
	distinctOwners := map[string]struct{}{}
	for _, sel := range selectors {
		plan := h.explainer.ExplainReadcacheQuery(r.Context(), data.Tenant, sel.from, sel.to, sel.matchers)
		view := buildSelectorView(sel, plan)
		data.TotalCalls += view.CallCount
		for _, p := range plan.Partitions {
			distinctPartitions[p.PartitionID] = struct{}{}
			for _, c := range p.Calls {
				distinctOwners[c.Owner] = struct{}{}
			}
		}
		data.Selectors = append(data.Selectors, view)
	}
	data.DistinctPartitions = len(distinctPartitions)
	data.DistinctOwners = len(distinctOwners)

	level.Debug(h.logger).Log(
		"msg", "served readcache-lookup explain",
		"tenant", data.Tenant,
		"selectors", len(data.Selectors),
		"total_query_stream_calls", data.TotalCalls,
	)

	h.render(w, data)
}

// extractSelectors parses the PromQL expression and returns every
// vector/matrix selector with the effective sample-time window its
// Select would cover for a query over [start, end].
//
// The window is an approximation of Prometheus' getTimeRangesForSelector:
// an instant vector selector reaches back one lookbackDelta before the
// query start; a matrix (range) selector reaches back by its range; both
// shift by any offset. It intentionally ignores @-modifier pinning and
// sub-query step, which don't change which readcache partitions/owners a
// Select fans out to in the common case.
func (h *ReadcacheExplainHandler) extractSelectors(query string, start, end time.Time) ([]selectorInfo, error) {
	expr, err := promqlext.NewPromQLParser().ParseExpr(query)
	if err != nil {
		return nil, err
	}

	// A VectorSelector nested in a MatrixSelector must be attributed the
	// matrix's range. Inspect visits the parent MatrixSelector before its
	// child VectorSelector, so record the range keyed by the child.
	matrixRange := map[*parser.VectorSelector]time.Duration{}
	var out []selectorInfo
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		switch n := node.(type) {
		case *parser.MatrixSelector:
			if vs, ok := n.VectorSelector.(*parser.VectorSelector); ok {
				matrixRange[vs] = n.Range
			}
		case *parser.VectorSelector:
			rng := matrixRange[n]
			selStart := start
			if rng > 0 {
				selStart = start.Add(-rng)
			} else {
				selStart = start.Add(-h.lookbackDelta)
			}
			selStart = selStart.Add(-n.OriginalOffset)
			selEnd := end.Add(-n.OriginalOffset)
			out = append(out, selectorInfo{
				expr:     n.String(),
				matchers: n.LabelMatchers,
				rng:      rng,
				offset:   n.OriginalOffset,
				from:     model.TimeFromUnixNano(selStart.UnixNano()),
				to:       model.TimeFromUnixNano(selEnd.UnixNano()),
			})
		}
		return nil
	})
	return out, nil
}

// parseTimeParam accepts a unix timestamp (seconds, possibly
// fractional), an RFC3339 timestamp, or an empty string (returns def).
func parseTimeParam(s string, def time.Time) (time.Time, error) {
	if s == "" {
		return def, nil
	}
	if secs, err := strconv.ParseFloat(s, 64); err == nil {
		sec := int64(secs)
		nsec := int64((secs - float64(sec)) * 1e9)
		return time.Unix(sec, nsec).UTC(), nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("expected unix seconds or RFC3339, got %q", s)
	}
	return t.UTC(), nil
}

func buildSelectorView(sel selectorInfo, plan distributor.ReadcacheQueryPlan) explainSelectorView {
	view := explainSelectorView{
		Expr:           sel.expr,
		Matchers:       util.LabelMatchersToString(sel.matchers),
		FromStr:        sel.from.Time().UTC().Format(time.RFC3339),
		ToStr:          sel.to.Time().UTC().Format(time.RFC3339),
		Unavailable:    plan.Unavailable,
		RoutingEnabled: plan.RoutingEnabled,
	}
	if sel.rng > 0 {
		view.RangeStr = sel.rng.String()
	}
	if sel.offset != 0 {
		view.OffsetStr = sel.offset.String()
	}
	if plan.Named {
		view.Mode = fmt.Sprintf("metric-name (%s, hashrange [%d, %d])", plan.MetricName, plan.HashLo, plan.HashHi)
	} else {
		view.Mode = "full-fanout (no exact __name__ matcher)"
	}
	if !plan.W0.IsZero() || !plan.W1.IsZero() {
		view.WindowStr = fmt.Sprintf("%s → %s", plan.W0.UTC().Format(time.RFC3339), plan.W1.UTC().Format(time.RFC3339))
	}

	for _, p := range plan.Partitions {
		pv := explainPartitionView{PartitionID: p.PartitionID}
		for _, c := range p.Calls {
			ov := explainOwnerView{Owner: c.Owner, InstanceID: c.InstanceID}
			if !c.LeaseFrom.IsZero() || !c.LeaseTo.IsZero() {
				ov.LeaseStr = fmt.Sprintf("%s → %s", c.LeaseFrom.UTC().Format("15:04:05"), c.LeaseTo.UTC().Format("15:04:05"))
			}
			pv.Owners = append(pv.Owners, ov)
		}
		view.CallCount += len(pv.Owners)
		view.Partitions = append(view.Partitions, pv)
	}
	// Stable ordering for a deterministic page.
	sort.Slice(view.Partitions, func(i, j int) bool {
		return view.Partitions[i].PartitionID < view.Partitions[j].PartitionID
	})
	return view
}

func (h *ReadcacheExplainHandler) render(w http.ResponseWriter, data explainPageData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := readcacheExplainTemplate.Execute(w, data); err != nil {
		http.Error(w, fmt.Sprintf("template error: %v", err), http.StatusInternalServerError)
	}
}
