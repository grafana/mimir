// SPDX-License-Identifier: AGPL-3.0-only

// Architecture: see docs/internal/streaming-label-value-search/README.md.

package ingester

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
)

// searchBatchSize bounds the number of results emitted per SearchResultBatch
// stream send. A small number keeps memory bounded and surfaces results to
// the client incrementally; a larger number reduces gRPC framing overhead.
const searchBatchSize = 256

// SearchLabelNames streams label names matching the search filter.
func (i *Ingester) SearchLabelNames(req *client.SearchLabelNamesRequest, stream client.Ingester_SearchLabelNamesServer) (err error) {
	// Validate user input ahead of the deferred read-error mapper so request
	// errors surface as codes.InvalidArgument rather than being re-tagged as
	// codes.Internal — matching the store-gateway side (bucket_search.go).
	hints, matchers, err := buildSearchHints(req.Filter, req.Ordering, req.Limit, req.Matchers)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	ctx := stream.Context()
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return nil
	}
	q, err := db.Querier(req.StartTimestampMs, req.EndTimestampMs)
	if err != nil {
		return fmt.Errorf("open querier: %w", err)
	}
	defer q.Close()
	searcher, ok := q.(storage.Searcher)
	if !ok {
		return fmt.Errorf("ingester querier does not implement storage.Searcher")
	}

	rs := searcher.SearchLabelNames(ctx, hints, matchers...)
	defer rs.Close()
	return streamSearchResults(ctx, rs, req.Filter.GetResumeAfter() != "", stream.Send)
}

// SearchLabelValues streams label values for req.Name matching the search filter.
func (i *Ingester) SearchLabelValues(req *client.SearchLabelValuesRequest, stream client.Ingester_SearchLabelValuesServer) (err error) {
	// See SearchLabelNames for why validation runs ahead of the deferred mapper.
	hints, matchers, err := buildSearchHints(req.Filter, req.Ordering, req.Limit, req.Matchers)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	ctx := stream.Context()
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return nil
	}
	q, err := db.Querier(req.StartTimestampMs, req.EndTimestampMs)
	if err != nil {
		return fmt.Errorf("open querier: %w", err)
	}
	defer q.Close()
	searcher, ok := q.(storage.Searcher)
	if !ok {
		return fmt.Errorf("ingester querier does not implement storage.Searcher")
	}

	rs := searcher.SearchLabelValues(ctx, req.Name, hints, matchers...)
	defer rs.Close()
	return streamSearchResults(ctx, rs, req.Filter.GetResumeAfter() != "", stream.Send)
}

// buildSearchHints constructs storage.SearchHints from the wire request,
// including the cursor of a previous page. limit follows the Prometheus PR
// #18573 convention: 0 means no limit;
// negatives are rejected as malformed input (the querier never sends them);
// values above math.MaxInt are clamped to math.MaxInt so the int(limit)
// cast cannot wrap on 32-bit builds.
func buildSearchHints(wf *client.SearchFilter, ord client.SearchOrdering, limit int64, wireMatchers []*client.LabelMatcher) (*storage.SearchHints, []*labels.Matcher, error) {
	matchers, err := client.FromLabelMatchers(wireMatchers)
	if err != nil {
		return nil, nil, err
	}
	params, err := protoToParams(wf)
	if err != nil {
		return nil, nil, err
	}
	order := protoToOrdering(ord)
	// With a cursor, every page reads and sorts all values of the label in
	// each block; see BuildSearchFilter.
	filter, err := streaminglabelvalues.BuildSearchFilter(params, order)
	if err != nil {
		return nil, nil, err
	}
	hintsLimit, err := searchLimit(limit)
	if err != nil {
		return nil, nil, err
	}
	hints := &storage.SearchHints{
		Filter:  filter,
		OrderBy: order,
		Limit:   hintsLimit,
	}
	return hints, matchers, nil
}

func searchLimit(limit int64) (int, error) {
	if limit < 0 {
		return 0, fmt.Errorf("limit must be >= 0, got %d", limit)
	}
	if limit > int64(math.MaxInt) {
		return math.MaxInt, nil
	}
	return int(limit), nil
}

// SearchMetricsMetadata streams the names of metrics whose name and HELP text
// match the search filter.
func (i *Ingester) SearchMetricsMetadata(req *client.SearchMetricsMetadataRequest, stream client.Ingester_SearchMetricsMetadataServer) (err error) {
	// See SearchLabelNames for why validation runs ahead of the deferred mapper.
	params, filter, err := buildMetadataSearchFilter(req.Filter, req.Ordering)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	limit, err := searchLimit(req.Limit)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	ctx := stream.Context()
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return err
	}

	userMetadata := i.getUserMetadata(userID)
	if userMetadata == nil {
		return nil
	}
	results, err := userMetadata.search(ctx, filter, params.CaseSensitive, protoToOrdering(req.Ordering), params.ResumeAfter, limit)
	if err != nil {
		return err
	}
	return streamSearchResults(ctx, storage.NewSearchResultSetFromSlice(results, nil), params.ResumeAfter != "", stream.Send)
}

// buildMetadataSearchFilter validates the filter of a SearchMetricsMetadata
// request and builds it. The filter sees the joined text of a metric, so it
// carries no cursor; userMetricsMetadata.search applies the cursor to the
// name.
func buildMetadataSearchFilter(wf *client.SearchFilter, ord client.SearchOrdering) (*streaminglabelvalues.Params, storage.Filter, error) {
	// protoToOrdering maps an unknown ordering to ascending, so a sender that is
	// newer than this server would get pages in the wrong order.
	switch ord {
	case client.ORDER_BY_VALUE_ASC, client.ORDER_BY_VALUE_DESC:
	case client.ORDER_BY_SCORE_DESC:
		return nil, nil, errors.New("ORDER_BY_SCORE_DESC is not supported by metric metadata search, because every match scores 1.0")
	default:
		return nil, nil, fmt.Errorf("unknown search ordering %d", ord)
	}
	if wf.GetFuzzAlg() != client.FUZZ_ALG_SUBSTRING {
		return nil, nil, fmt.Errorf("metric metadata search requires fuzz algorithm FUZZ_ALG_SUBSTRING, got %s", wf.GetFuzzAlg())
	}
	params, err := protoToParams(wf)
	if err != nil {
		return nil, nil, err
	}
	// The text of a metric joins its HELP texts with newlines in map order, so
	// a term with a newline could match differently on each request.
	if params.HasTermContaining("\n") {
		return nil, nil, errors.New("a metric metadata search term must not contain a newline")
	}
	filter, err := streaminglabelvalues.BuildFilter(params)
	if err != nil {
		return nil, nil, err
	}
	// Without terms, the search would return every metric with metadata. The
	// MetricsMetadata RPC already serves that.
	if filter == nil {
		return nil, nil, errors.New("metric metadata search requires search terms or an expression")
	}
	return params, filter, nil
}

// protoToParams converts a wire SearchFilter into a validated
// streaminglabelvalues.Params via NewTermsOrExpressionParams and WithCursor.
// A nil input returns (nil, nil). Terms and an expression together return an
// error that wraps ErrTermsAndExpression. An unknown FuzzAlg is an error. A
// sender that is newer than this server can send a value that this server does
// not know, and a silent fallback to another algorithm would return wrong
// results.
func protoToParams(wf *client.SearchFilter) (*streaminglabelvalues.Params, error) {
	if wf == nil {
		return nil, nil
	}
	var alg streaminglabelvalues.FuzzAlg
	switch wf.FuzzAlg {
	case client.FUZZ_ALG_SUBSEQUENCE:
		alg = streaminglabelvalues.FuzzAlgSubsequence
	case client.FUZZ_ALG_JARO_WINKLER:
		alg = streaminglabelvalues.FuzzAlgJaroWinkler
	case client.FUZZ_ALG_SUBSTRING_LEFT:
		alg = streaminglabelvalues.FuzzAlgSubstringLeft
	case client.FUZZ_ALG_SUBSTRING:
		alg = streaminglabelvalues.FuzzAlgSubstring
	default:
		return nil, fmt.Errorf("unknown fuzz algorithm %d", wf.FuzzAlg)
	}
	params, err := streaminglabelvalues.NewTermsOrExpressionParams(wf.Terms, wf.Expression, !wf.CaseInsensitive, alg, int(wf.FuzzThreshold))
	if err != nil {
		return nil, err
	}
	return params.WithCursor(wf.ResumeAfter, wf.ScoreAfter)
}

// protoToOrdering maps the wire SearchOrdering enum onto the Prometheus
// storage.Ordering enum.
func protoToOrdering(o client.SearchOrdering) storage.Ordering {
	switch o {
	case client.ORDER_BY_VALUE_DESC:
		return storage.OrderByValueDesc
	case client.ORDER_BY_SCORE_DESC:
		return storage.OrderByScoreDesc
	default:
		return storage.OrderByValueAsc
	}
}

// searchResultSender is the Send method of Ingester_SearchLabelNamesServer,
// Ingester_SearchLabelValuesServer and Ingester_SearchMetricsMetadataServer.
type searchResultSender func(*client.SearchResultBatch) error

// streamSearchResults reads from rs in batches of searchBatchSize and emits
// each batch via send. Any warnings accumulated by rs are attached to the
// final batch (or sent alone if no results were produced). Returns rs.Err()
// at termination. The stream context is checked before iteration starts and
// at each batch boundary so a cancelled client stops the loop promptly even
// when rs.Next() does not yet honour context cancellation itself. cursorApplied
// marks every batch, so the caller can tell this server applied its cursor.
func streamSearchResults(ctx context.Context, rs storage.SearchResultSet, cursorApplied bool, send searchResultSender) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	batch := &client.SearchResultBatch{Results: make([]client.SearchResultBatch_Result, 0, searchBatchSize), CursorApplied: cursorApplied}
	for rs.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		v := rs.At()
		batch.Results = append(batch.Results, client.SearchResultBatch_Result{Value: v.Value, Score: v.Score})
		if len(batch.Results) >= searchBatchSize {
			if err := send(batch); err != nil {
				return err
			}
			// stream.Send is synchronous (gogoproto marshals before
			// returning) so the backing array can be reset in place.
			batch.Results = batch.Results[:0]
		}
	}
	if err := rs.Err(); err != nil {
		return err
	}
	batch.Warnings = warningsToStrings(rs.Warnings())
	if len(batch.Results) > 0 || len(batch.Warnings) > 0 {
		if err := send(batch); err != nil {
			return err
		}
	}
	return nil
}

// warningsToStrings flattens annotations into a string slice for wire transport.
// Returns nil for empty input so the proto field is omitted on the wire.
func warningsToStrings(a annotations.Annotations) []string {
	if len(a) == 0 {
		return nil
	}
	out := make([]string, 0, len(a))
	for _, w := range a {
		out = append(out, w.Error())
	}
	return out
}
