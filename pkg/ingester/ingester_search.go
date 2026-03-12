// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"github.com/grafana/mimir/pkg/util"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// TODO - what is a sane value here?
const searchBatchSize = 1024

func initSpanLog(spanlog *spanlogger.SpanLogger, userId string, mint, maxt int64, hints *storage.LabelHints, matchers []*labels.Matcher, filter *client.SearchLabelValuesFilter) {
	spanlog.SetSpanAndLogTag("user", userId)
	spanlog.SetSpanAndLogTag("minT", mint)
	spanlog.SetSpanAndLogTag("maxT", maxt)

	if hints != nil && hints.Limit > 0 {
		spanlog.SetSpanAndLogTag("limit", hints.Limit)
	}

	if len(matchers) > 0 {
		spanlog.SetSpanAndLogTag("matchers", util.LabelMatchersToString(matchers))

	}

	if filter != nil {
		if len(filter.SearchTerms) > 0 {
			spanlog.SetSpanAndLogTag("searchTerms", strings.Join(filter.SearchTerms, ","))
			spanlog.SetSpanAndLogTag("operator", filter.Operator)
			spanlog.SetSpanAndLogTag("fuzz_threshold", filter.FuzzThreshold)
		}
		if filter.SortBy != 0 {
			spanlog.SetSpanAndLogTag("sort_by", filter.SortBy)
			spanlog.SetSpanAndLogTag("sort_dir", filter.SortOrder)
		}
	}
}

// SearchLabelNames implements client.IngesterServer.
// It is identical to LabelNames but applies the SearchLabelValuesFilter before the limit,
// and streams results back in batches.
func (i *Ingester) SearchLabelNames(req *client.SearchLabelValuesRequest, stream client.Ingester_SearchLabelNamesServer) (err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()
	now := time.Now()

	spanlog, ctx := spanlogger.New(stream.Context(), i.logger, tracer, "Ingester.SearchLabelNames")
	defer spanlog.Finish()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		level.Error(spanlog).Log("msg", "unable to determine tenant from context", "err", err)
		return err
	}

	_, mint, maxt, hints, matchers, err := client.FromSearchRequest(req)
	if err != nil {
		level.Error(spanlog).Log("msg", "unable to parse search label names request", "err", err)
		return err
	}

	initSpanLog(spanlog, userID, mint, maxt, hints, matchers, req.Filter)

	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		level.Error(spanlog).Log("msg", "unable to enforce read consistency", "err", err)
		return err
	}

	db := i.getTSDB(userID)
	if db == nil {
		level.Error(spanlog).Log("msg", "unable to access TSDB for user", "err", err)
		return nil
	}

	q, err := db.Querier(mint, maxt)
	if err != nil {
		level.Error(spanlog).Log("msg", "unable to access Querier", "err", err)
		return err
	}
	defer q.Close()

	searchFilter := buildSearchFilter(req.Filter)

	// TODO: replace q.LabelNames with a streaming Searcher once Prometheus exposes one —
	// that would avoid buffering all label names in memory before filtering/sorting/streaming,
	// letting us pipe values directly from TSDB into the gRPC stream.
	names, _, err := q.LabelNames(ctx, hints, matchers...)
	if err != nil {
		level.Error(spanlog).Log("msg", "unable to query LabelValues", "err", err)
		return err
	}

	names = streaminglabelvalues.ApplyFilterChains(names, searchFilter)
	names = applySearchSort(names, req.Filter, searchFilter)

	if hints != nil && hints.Limit > 0 && len(names) > hints.Limit {
		names = names[:hints.Limit]
	}

	scoreSortActive := req.Filter != nil && req.Filter.SortBy == client.SORT_BY_SCORE

	level.Info(spanlog).Log("msg", "Preparing to stream batch", "results", len(names), "ms", time.Since(now).Milliseconds())

	// Pre-allocate result structs once; stream.Send marshals synchronously so they can be reused.
	resultsBuf := make([]*client.SearchLabelValuesResult, searchBatchSize)
	for k := range resultsBuf {
		resultsBuf[k] = &client.SearchLabelValuesResult{}
	}

	for start := 0; start < len(names); start += searchBatchSize {
		end := min(start+searchBatchSize, len(names))
		results := resultsBuf[:end-start]
		for j, v := range names[start:end] {
			score := 0.0
			if scoreSortActive && searchFilter != nil {
				_, score = searchFilter.Accept(v)
			}
			results[j].Value = strings.Clone(v)
			results[j].Score = score
		}

		if err := stream.Send(&client.SearchLabelValuesResponse{Results: results}); err != nil {
			level.Error(spanlog).Log("msg", "unexpected error sending results to stream", "err", err)
			return err
		}
		if err := ctx.Err(); err != nil {
			return nil
		}
	}

	level.Info(spanlog).Log("msg", "Complete", "results", len(names), "ms", time.Since(now).Milliseconds())

	return nil
}

// SearchLabelValues implements client.IngesterServer.
// It is identical to LabelValues but applies the SearchLabelValuesFilter before the limit,
// and streams results back in batches.
func (i *Ingester) SearchLabelValues(req *client.SearchLabelValuesRequest, stream client.Ingester_SearchLabelValuesServer) (err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()
	now := time.Now()

	spanlog, ctx := spanlogger.New(stream.Context(), i.logger, tracer, "Ingester.SearchLabelValues")
	defer spanlog.Finish()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		level.Error(spanlog).Log("msg", "unable to determine tenant from context", "err", err)
		return err
	}

	labelName, mint, maxt, hints, matchers, err := client.FromSearchRequest(req)
	if err != nil {
		level.Error(spanlog).Log("msg", "unable to parse search label values request", "err", err)
		return err
	}

	initSpanLog(spanlog, userID, mint, maxt, hints, matchers, req.Filter)
	spanlog.SetSpanAndLogTag("label", labelName)

	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		level.Error(spanlog).Log("msg", "unable to enforce read consistency", "err", err)
		return err
	}

	db := i.getTSDB(userID)
	if db == nil {
		level.Error(spanlog).Log("msg", "unable to access TSDB for user", "err", err)
		return nil
	}

	q, err := db.Querier(mint, maxt)
	if err != nil {
		level.Error(spanlog).Log("msg", "unable to access Querier", "err", err)
		return err
	}
	defer q.Close()

	searchFilter := buildSearchFilter(req.Filter)

	// TODO: replace q.LabelValues with a streaming Searcher once Prometheus exposes one —
	// that would avoid buffering all label values in memory before filtering/sorting/streaming,
	// letting us pipe values directly from TSDB into the gRPC stream.
	vals, _, err := q.LabelValues(ctx, labelName, hints, matchers...)
	if err != nil {
		level.Error(spanlog).Log("msg", "unable to query LabelValues", "err", err)
		return err
	}

	vals = streaminglabelvalues.ApplyFilterChains(vals, searchFilter)
	vals = applySearchSort(vals, req.Filter, searchFilter)

	if hints != nil && hints.Limit > 0 && len(vals) > hints.Limit {
		vals = vals[:hints.Limit]
	}

	level.Info(spanlog).Log("msg", "Preparing to stream batch", "results", len(vals), "ms", time.Since(now).Milliseconds())

	scoreSortActive := req.Filter != nil && req.Filter.SortBy == client.SORT_BY_SCORE

	// Pre-allocate result structs once; stream.Send marshals synchronously so they can be reused.
	resultsBuf := make([]*client.SearchLabelValuesResult, searchBatchSize)
	for k := range resultsBuf {
		resultsBuf[k] = &client.SearchLabelValuesResult{}
	}

	for start := 0; start < len(vals); start += searchBatchSize {
		end := min(start+searchBatchSize, len(vals))
		results := resultsBuf[:end-start]
		for j, v := range vals[start:end] {
			score := 0.0
			if scoreSortActive && searchFilter != nil {
				_, score = searchFilter.Accept(v)
			}
			// Copy strings since label value strings may point to memory-mapped regions
			// that become invalid after Querier.Close is called.
			results[j].Value = strings.Clone(v)
			results[j].Score = score
		}

		if err := stream.Send(&client.SearchLabelValuesResponse{Results: results}); err != nil {
			level.Error(spanlog).Log("msg", "unexpected error sending results to stream", "err", err)
			return err
		}
		if err := ctx.Err(); err != nil {
			return nil
		}
	}

	level.Info(spanlog).Log("msg", "Complete", "results", len(vals), "ms", time.Since(now).Milliseconds())
	return nil
}

// buildSearchFilter converts a proto SearchLabelValuesFilter into a FilterChains ready for use.
// Returns nil if sf is nil or has no search terms.
func buildSearchFilter(sf *client.SearchLabelValuesFilter) *streaminglabelvalues.FilterChains {
	if sf == nil || len(sf.SearchTerms) == 0 {
		return nil
	}
	op := streaminglabelvalues.Or
	if sf.Operator == client.AND {
		op = streaminglabelvalues.And
	}
	return streaminglabelvalues.BuildFilterChains(sf.SearchTerms, sf.CaseInsensitive, op, sf.FuzzThreshold)
}

// applySearchSort sorts values according to the sort_by/sort_order fields in sf.
// For alpha-asc (default from TSDB) no reordering is needed.
// For alpha-desc the slice is reversed.
// For score sort, ScoreAndSort is called using the pre-built filter.
// If sf is nil or sort_by is 0, values are returned unchanged.
func applySearchSort(values []string, sf *client.SearchLabelValuesFilter, filter *streaminglabelvalues.FilterChains) []string {
	if sf == nil || sf.SortBy == client.SORT_BY_NONE {
		return values
	}
	switch sf.SortBy {
	case client.SORT_BY_ALPHA:
		if sf.SortOrder == client.SORT_ORDER_DESC {
			slices.Reverse(values)
		}
	case client.SORT_BY_SCORE: // filter provides scoring; falls back to unchanged if no filter
		values = streaminglabelvalues.ScoreAndSort(values, filter, sf.SortOrder != client.SORT_ORDER_ASC)
	}
	return values
}
