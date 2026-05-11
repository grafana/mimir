// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"io"
	"slices"

	"github.com/grafana/dskit/ring"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util"
)

// SearchLabelNames fans out the search RPC across the ingester replication
// set, drains each replica's stream, merges per-replica []string via
// util.MergeSlices, then re-applies the search filter via
// storage.ApplySearchHints for deterministic scoring. Returns a
// slice-backed storage.SearchResultSet.
//
// params is the wire-decoupled form of hints.Filter and is forwarded to the
// ingesters so each leaf can build its own filter chain. hints.Filter is the
// already-constructed filter used to re-score the merged result-set on this
// side; the Querier-side caller (Task 4) holds both and forwards both.
// Caller must build params and hints.Filter from the same
// streaminglabelvalues.Params; mismatched filters silently shrink results.
//
// from/to bound the query time range; PR #4's HTTP handler resolves these
// from the request and Task 4's distributorQuerier threads them through.
//
// Drains run concurrently per replica; this method blocks until every
// replica has been drained to EOF or has errored. Memory is bounded by
// replicas * hints.Limit; the eventual HTTP layer (PR #4) is responsible
// for sane limits.
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
	values, warns, err := d.searchLabelNamesFanOut(ctx, replicationSets, req)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}
	return storage.NewSearchResultSetFromSlice(storage.ApplySearchHints(values, hints), warns)
}

func (d *Distributor) searchLabelNamesFanOut(
	ctx context.Context,
	replicationSets []ring.ReplicationSet,
	req *ingester_client.SearchLabelNamesRequest,
) ([]string, annotations.Annotations, error) {
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
	return mergeDrainResults(resps), mergeWarnings(resps), nil
}

// SearchLabelValues fans out the search RPC for label name's values across
// the ingester replication set, drains each replica's stream, merges
// per-replica []string via util.MergeSlices, then re-applies the search
// filter via storage.ApplySearchHints for deterministic scoring. Returns a
// slice-backed storage.SearchResultSet.
//
// params is the wire-decoupled form of hints.Filter and is forwarded to the
// ingesters so each leaf can build its own filter chain. hints.Filter is the
// already-constructed filter used to re-score the merged result-set on this
// side; the Querier-side caller (Task 4) holds both and forwards both.
// Caller must build params and hints.Filter from the same
// streaminglabelvalues.Params; mismatched filters silently shrink results.
//
// from/to bound the query time range; name is the label whose values are
// being searched. PR #4's HTTP handler resolves these from the request and
// Task 4's distributorQuerier threads them through.
//
// Drains run concurrently per replica; this method blocks until every
// replica has been drained to EOF or has errored. Memory is bounded by
// replicas * hints.Limit; the eventual HTTP layer (PR #4) is responsible
// for sane limits.
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
	values, warns, err := d.searchLabelValuesFanOut(ctx, replicationSets, req)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}
	return storage.NewSearchResultSetFromSlice(storage.ApplySearchHints(values, hints), warns)
}

func (d *Distributor) searchLabelValuesFanOut(
	ctx context.Context,
	replicationSets []ring.ReplicationSet,
	req *ingester_client.SearchLabelValuesRequest,
) ([]string, annotations.Annotations, error) {
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
	return mergeDrainResults(resps), mergeWarnings(resps), nil
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

// drainResult is the per-replica drain product.
type drainResult struct {
	values []string
	warns  annotations.Annotations
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
			out.values = append(out.values, r.Value)
		}
		for _, w := range batch.Warnings {
			out.warns.Add(errors.New(w))
		}
	}
}

// mergeDrainResults sort-merges every replica's []string and dedups across
// replicas (mirrors PR #1's BucketStore cross-block pattern).
func mergeDrainResults(results []drainResult) []string {
	if len(results) == 0 {
		return nil
	}
	sets := make([][]string, 0, len(results))
	for _, r := range results {
		sorted := append([]string(nil), r.values...)
		slices.Sort(sorted)
		sets = append(sets, sorted)
	}
	return util.MergeSlices(sets...)
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
// ORDER_BY_VALUE_ASC when hints is nil — the same default applied by
// storage.ApplySearchHints at the merge side.
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
