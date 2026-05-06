// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"fmt"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
)

// searchBatchSize bounds the number of results emitted per SearchResultBatch
// stream send. A small number keeps memory bounded and surfaces results to
// the client incrementally; a larger number reduces gRPC framing overhead.
const searchBatchSize = 256

// SearchLabelNames streams label names matching the search filter.
func (i *Ingester) SearchLabelNames(req *client.SearchLabelNamesRequest, stream client.Ingester_SearchLabelNamesServer) (err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	ctx := stream.Context()
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return err
	}

	hints, matchers, err := buildSearchHints(req.Filter, req.Ordering, req.Limit, req.Matchers)
	if err != nil {
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
	return streamSearchResults(rs, stream.Send)
}

// SearchLabelValues streams label values for req.Name matching the search filter.
func (i *Ingester) SearchLabelValues(req *client.SearchLabelValuesRequest, stream client.Ingester_SearchLabelValuesServer) (err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	ctx := stream.Context()
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return err
	}

	hints, matchers, err := buildSearchHints(req.Filter, req.Ordering, req.Limit, req.Matchers)
	if err != nil {
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
	return streamSearchResults(rs, stream.Send)
}

// buildSearchHints constructs storage.SearchHints from the wire request.
func buildSearchHints(wf *client.SearchFilter, ord client.SearchOrdering, limit int64, wireMatchers []*client.LabelMatcher) (*storage.SearchHints, []*labels.Matcher, error) {
	matchers, err := client.FromLabelMatchers(wireMatchers)
	if err != nil {
		return nil, nil, err
	}
	params := protoToParams(wf)
	filter, err := streaminglabelvalues.BuildFilter(params)
	if err != nil {
		return nil, nil, err
	}
	hints := &storage.SearchHints{
		Filter:  filter,
		OrderBy: protoToOrdering(ord),
		Limit:   int(limit),
	}
	return hints, matchers, nil
}

// protoToParams converts a wire SearchFilter into the streaminglabelvalues.Params
// shape that BuildFilter consumes. A nil input returns nil.
func protoToParams(wf *client.SearchFilter) *streaminglabelvalues.Params {
	if wf == nil {
		return nil
	}
	p := &streaminglabelvalues.Params{
		Terms:         wf.Terms,
		CaseSensitive: wf.CaseSensitive,
		FuzzThreshold: int(wf.FuzzThreshold),
	}
	switch wf.FuzzAlg {
	case client.FUZZ_ALG_JARO_WINKLER:
		p.FuzzAlg = streaminglabelvalues.FuzzAlgJaroWinkler
	default:
		p.FuzzAlg = streaminglabelvalues.FuzzAlgSubsequence
	}
	return p
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

// searchResultSender is the minimal interface satisfied by both
// Ingester_SearchLabelNamesServer and Ingester_SearchLabelValuesServer.
type searchResultSender func(*client.SearchResultBatch) error

// streamSearchResults reads from rs in batches of searchBatchSize and emits
// each batch via send. Any warnings accumulated by rs are attached to the
// final batch (or sent alone if no results were produced). Returns rs.Err()
// at termination.
func streamSearchResults(rs storage.SearchResultSet, send searchResultSender) error {
	batch := &client.SearchResultBatch{Results: make([]client.SearchResultBatch_Result, 0, searchBatchSize)}
	for rs.Next() {
		v := rs.At()
		batch.Results = append(batch.Results, client.SearchResultBatch_Result{Value: v.Value, Score: v.Score})
		if len(batch.Results) >= searchBatchSize {
			if err := send(batch); err != nil {
				return err
			}
			batch = &client.SearchResultBatch{Results: make([]client.SearchResultBatch_Result, 0, searchBatchSize)}
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
