// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"io"

	"github.com/grafana/dskit/ring"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	mimirstorage "github.com/grafana/mimir/pkg/storage"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
)

// SearchLabelNames fans out the search RPC across the ingester replication
// set, drains each replica's scored stream into a per-replica
// []storage.SearchResult, and merges via mimirstorage.MergeSearchResults
// (value-dedup with max-score, then ordering + limit per hints). Returns a
// slice-backed storage.SearchResultSet.
//
// Score preservation: leaf-computed scores are propagated end-to-end. We do
// NOT re-apply the filter at this layer — the ingester applied req.Filter
// already, so re-running it would burn CPU without changing the result
// (Spec invariant 3 guarantees Score is deterministic per (Value, Filter)).
//
// params is the wire-decoupled form of hints.Filter, forwarded to the
// ingesters so each leaf builds its own filter chain. hints carries the
// ordering and limit applied at the merge layer; hints.Filter is NOT used
// here. Caller must build params and the (unused-here) hints.Filter from
// the same streaminglabelvalues.Params for cross-layer consistency in
// PR #4's HTTP path.
//
// from/to bound the query time range; PR #4's HTTP handler resolves these
// from the request and Task 4's distributorQuerier threads them through.
//
// Drains run concurrently per replica; this method blocks until every
// replica has been drained to EOF or has errored. Memory is bounded by
// replicas * hints.Limit.
func (d *Distributor) SearchLabelNames(
	ctx context.Context,
	from, to model.Time,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	matchers []*labels.Matcher,
) storage.SearchResultSet {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}
	req, err := buildSearchLabelNamesRequest(from, to, params, hints, matchers)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}
	perReplica, warns, err := d.searchLabelNamesFanOut(ctx, replicationSets, req)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}
	return storage.NewSearchResultSetFromSlice(mimirstorage.MergeSearchResults(perReplica, hints), warns)
}

func (d *Distributor) searchLabelNamesFanOut(
	ctx context.Context,
	replicationSets []ring.ReplicationSet,
	req *ingester_client.SearchLabelNamesRequest,
) ([][]storage.SearchResult, annotations.Annotations, error) {
	resps, err := forReplicationSets(ctx, d, replicationSets, func(ctx context.Context, c ingester_client.IngesterClient) (drainResult, error) {
		stream, err := c.SearchLabelNames(ctx, req)
		if err != nil {
			return drainResult{}, err
		}
		return drainIngesterSearchStream(stream)
	})
	if err != nil {
		return nil, nil, err
	}
	return collectResults(resps), mergeWarnings(resps), nil
}

// SearchLabelValues fans out the search RPC for label name's values across
// the ingester replication set, drains each replica's scored stream into a
// per-replica []storage.SearchResult, and merges via
// mimirstorage.MergeSearchResults. Mirrors SearchLabelNames; differs only in
// the wire request shape (carries the Name field for the label whose values
// are being searched).
//
// See SearchLabelNames for the score-preservation / no-re-filter rationale
// and the params/hints/filter contract.
func (d *Distributor) SearchLabelValues(
	ctx context.Context,
	from, to model.Time,
	name string,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	matchers []*labels.Matcher,
) storage.SearchResultSet {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}
	req, err := buildSearchLabelValuesRequest(from, to, name, params, hints, matchers)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}
	perReplica, warns, err := d.searchLabelValuesFanOut(ctx, replicationSets, req)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}
	return storage.NewSearchResultSetFromSlice(mimirstorage.MergeSearchResults(perReplica, hints), warns)
}

func (d *Distributor) searchLabelValuesFanOut(
	ctx context.Context,
	replicationSets []ring.ReplicationSet,
	req *ingester_client.SearchLabelValuesRequest,
) ([][]storage.SearchResult, annotations.Annotations, error) {
	resps, err := forReplicationSets(ctx, d, replicationSets, func(ctx context.Context, c ingester_client.IngesterClient) (drainResult, error) {
		stream, err := c.SearchLabelValues(ctx, req)
		if err != nil {
			return drainResult{}, err
		}
		return drainIngesterSearchStream(stream)
	})
	if err != nil {
		return nil, nil, err
	}
	return collectResults(resps), mergeWarnings(resps), nil
}

func buildSearchLabelValuesRequest(from, to model.Time, name string, params *streaminglabelvalues.Params, hints *storage.SearchHints, matchers []*labels.Matcher) (*ingester_client.SearchLabelValuesRequest, error) {
	wireMatchers, err := ingester_client.ToLabelMatchers(matchers)
	if err != nil {
		return nil, err
	}
	req := &ingester_client.SearchLabelValuesRequest{
		Name:             name,
		StartTimestampMs: int64(from),
		EndTimestampMs:   int64(to),
		Matchers:         wireMatchers,
		Filter:           paramsToProto(params),
		Ordering:         orderingToProto(hints),
	}
	if hints != nil {
		req.Limit = int64(hints.Limit)
	}
	return req, nil
}

// searchStream is the common Recv-only surface of
// Ingester_SearchLabelNamesClient and Ingester_SearchLabelValuesClient,
// allowing drainIngesterSearchStream to be reused for both RPCs.
type searchStream interface {
	Recv() (*ingester_client.SearchResultBatch, error)
}

// drainResult is the per-replica drain product. Scores from the wire are
// preserved verbatim — the leaf ingester computed them under spec invariant 3
// (deterministic per (Value, Filter)), and the merge layer does not re-score.
type drainResult struct {
	results []storage.SearchResult
	warns   annotations.Annotations
}

func drainIngesterSearchStream(stream searchStream) (drainResult, error) {
	var out drainResult
	for {
		batch, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return drainResult{}, err
		}
		for _, r := range batch.Results {
			out.results = append(out.results, storage.SearchResult{Value: r.Value, Score: r.Score})
		}
		for _, w := range batch.Warnings {
			out.warns.Add(errors.New(w))
		}
	}
}

// collectResults returns the per-replica result slices for passing to
// mimirstorage.MergeSearchResults. Each replica's slice is already filtered
// and ordered by its leaf ingester; the merger only needs to dedup across
// replicas, re-order if hints.OrderBy is non-default, and apply the limit.
func collectResults(results []drainResult) [][]storage.SearchResult {
	if len(results) == 0 {
		return nil
	}
	out := make([][]storage.SearchResult, 0, len(results))
	for _, r := range results {
		if len(r.results) > 0 {
			out = append(out, r.results)
		}
	}
	return out
}

func mergeWarnings(results []drainResult) annotations.Annotations {
	var out annotations.Annotations
	for _, r := range results {
		out.Merge(r.warns)
	}
	return out
}

func buildSearchLabelNamesRequest(from, to model.Time, params *streaminglabelvalues.Params, hints *storage.SearchHints, matchers []*labels.Matcher) (*ingester_client.SearchLabelNamesRequest, error) {
	wireMatchers, err := ingester_client.ToLabelMatchers(matchers)
	if err != nil {
		return nil, err
	}
	req := &ingester_client.SearchLabelNamesRequest{
		StartTimestampMs: int64(from),
		EndTimestampMs:   int64(to),
		Matchers:         wireMatchers,
		Filter:           paramsToProto(params),
		Ordering:         orderingToProto(hints),
	}
	if hints != nil {
		req.Limit = int64(hints.Limit)
	}
	return req, nil
}

// paramsToProto converts the wire-decoupled streaminglabelvalues.Params into
// the ingester SearchFilter proto. Returns nil when p is nil or has no
// terms — the ingester then scores every value at 1.0.
func paramsToProto(p *streaminglabelvalues.Params) *ingester_client.SearchFilter {
	if p == nil || len(p.Terms) == 0 {
		return nil
	}
	wf := &ingester_client.SearchFilter{
		Terms:           p.Terms,
		CaseInsensitive: !p.CaseSensitive,
		FuzzThreshold:   int32(p.FuzzThreshold),
	}
	switch p.FuzzAlg {
	case streaminglabelvalues.FuzzAlgJaroWinkler:
		wf.FuzzAlg = ingester_client.FUZZ_ALG_JARO_WINKLER
	default:
		wf.FuzzAlg = ingester_client.FUZZ_ALG_SUBSEQUENCE
	}
	return wf
}

// orderingToProto translates hints.OrderBy into the wire enum. Defaults to
// ORDER_BY_VALUE_ASC when hints is nil — same default applied by
// mimirstorage.MergeSearchResults at the merge side, keeping leaf-side and
// merge-side ordering aligned.
func orderingToProto(hints *storage.SearchHints) ingester_client.SearchOrdering {
	if hints == nil {
		return ingester_client.ORDER_BY_VALUE_ASC
	}
	switch hints.OrderBy {
	case storage.OrderByValueDesc:
		return ingester_client.ORDER_BY_VALUE_DESC
	case storage.OrderByScoreDesc:
		return ingester_client.ORDER_BY_SCORE_DESC
	default:
		return ingester_client.ORDER_BY_VALUE_ASC
	}
}
