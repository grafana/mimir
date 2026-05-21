// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// searchWarning lifts a wire warning string to an error without the per-call
// allocation of errors.New. Used to feed annotations.Annotations.Add.
type searchWarning string

func (w searchWarning) Error() string { return string(w) }

// SearchLabelNames fans out across the store-gateways owning the relevant
// blocks, drains each stream into a scored []storage.SearchResult, and
// returns the cross-SG merged set. Leaf scores propagate verbatim — no
// re-filter at the merge layer.
//
// The signature intentionally diverges from storage.Searcher by taking a
// *streaminglabelvalues.Params alongside the SearchHints (the upstream
// interface has no field for fuzzy-algorithm/threshold/case-sensitivity).
// The HTTP wiring in the follow-up PR therefore reaches this method
// through a Mimir-local interface, not via a storage.Searcher type
// assertion.
func (q *blocksStoreQuerier) SearchLabelNames(
	ctx context.Context,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	matchers ...*labels.Matcher,
) storage.SearchResultSet {
	spanLog, ctx := spanlogger.New(ctx, q.logger, tracer, "blocksStoreQuerier.SearchLabelNames")
	defer spanLog.Finish()

	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}

	minT, maxT := q.minT, q.maxT
	spanLog.DebugLog("start", util.TimeFromMillis(minT).UTC().String(), "end",
		util.TimeFromMillis(maxT).UTC().String(), "matchers", util.MatchersStringer(matchers))

	// Clamp minT to MaxLabelsQueryLength; mirrors blocksStoreQuerier.LabelNames.
	maxQueryLength := q.limits.MaxLabelsQueryLength(tenantID)
	if maxQueryLength != 0 {
		minT = clampToMaxLabelQueryLength(spanLog, minT, maxT, time.Now().UnixMilli(), maxQueryLength.Milliseconds())
	}

	var (
		mtx               sync.Mutex
		resResultSets     = [][]storage.SearchResult{}
		resWarnings       annotations.Annotations
		convertedMatchers = convertMatchersToLabelMatcher(matchers)
	)

	queryF := func(clients map[BlocksStoreClient][]ulid.ULID, qMinT, qMaxT int64, indexMeta *bucketindex.Metadata) ([]ulid.ULID, error) {
		resultSets, warnings, queriedBlocks, err := q.fetchSearchLabelNamesFromStore(ctx, clients, qMinT, qMaxT, tenantID, params, hints, convertedMatchers, indexMeta)
		if err != nil {
			return nil, err
		}
		mtx.Lock()
		resResultSets = append(resResultSets, resultSets...)
		resWarnings.Merge(warnings)
		mtx.Unlock()
		return queriedBlocks, nil
	}

	if err := q.queryWithConsistencyCheck(ctx, spanLog, minT, maxT, tenantID, nil, queryF); err != nil {
		return storage.ErrSearchResultSet(err)
	}

	return wrapAsMergingSearchResultSet(resResultSets, resWarnings, hints)
}

// wrapAsMergingSearchResultSet feeds per-SG result slices into the k-way
// merger as slice-backed sources. Warnings collected outside any SG stream
// ride a synthetic warnings-only source so the merger surfaces them.
//
// The slice drain happens because queryWithConsistencyCheck needs
// queriedBlocks back before it can decide whether to retry.
func wrapAsMergingSearchResultSet(perSG [][]storage.SearchResult, extraWarnings annotations.Annotations, hints *storage.SearchHints) storage.SearchResultSet {
	sources := make([]storage.SearchResultSet, 0, len(perSG)+1)
	for _, s := range perSG {
		if len(s) > 0 {
			sources = append(sources, storage.NewSearchResultSetFromSlice(s, nil))
		}
	}
	if len(extraWarnings) > 0 {
		sources = append(sources, storage.NewSearchResultSetFromSlice(nil, extraWarnings))
	}
	return storage.MergeSearchResultSets(sources, hints)
}

// fetchSearchLabelNamesFromStore drains every SG client in parallel and
// returns only the blocks that store-gateways report they actually queried
// (via SearchResponseHints) so queryWithConsistencyCheck can retry against
// any block still missing.
func (q *blocksStoreQuerier) fetchSearchLabelNamesFromStore(
	ctx context.Context,
	clients map[BlocksStoreClient][]ulid.ULID,
	minT int64,
	maxT int64,
	tenantID string,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	wireMatchers []storepb.LabelMatcher,
	indexMeta *bucketindex.Metadata,
) ([][]storage.SearchResult, annotations.Annotations, []ulid.ULID, error) {
	reqCtx := grpcContextWithBucketStoreRequestMeta(ctx, tenantID, indexMeta)

	var (
		g, gCtx       = errgroup.WithContext(reqCtx)
		mtx           = sync.Mutex{}
		resultSets    = [][]storage.SearchResult{}
		warnings      annotations.Annotations
		queriedBlocks = []ulid.ULID(nil)
		spanLog       = spanlogger.FromContext(ctx, q.logger)
	)

	for c, blockIDs := range clients {
		g.Go(func() error {
			req := buildSGSearchLabelNamesRequest(minT, maxT, blockIDs, params, hints, wireMatchers)
			stream, err := c.SearchLabelNames(gCtx, req)
			if err != nil {
				if shouldRetry(err) {
					level.Warn(spanLog).Log("msg", "failed to fetch search label names; error is retriable", "remote", c.RemoteAddress(), "err", err)
					return nil
				}
				return errors.Wrapf(err, "non-retriable error while fetching search label names from store %s", c.RemoteAddress())
			}

			results, sgWarnings, myQueriedBlocks, err := drainSGSearchLabelStream(stream)
			if err != nil {
				if shouldRetry(err) {
					level.Warn(spanLog).Log("msg", "failed to drain search label names stream; error is retriable", "remote", c.RemoteAddress(), "err", err)
					return nil
				}
				return errors.Wrapf(err, "failed to drain SearchLabelNames stream from store %s", c.RemoteAddress())
			}

			spanLog.DebugLog("msg", "received search label names from store-gateway",
				"instance", c,
				"num values", len(results),
				"requested blocks", strings.Join(convertULIDsToString(blockIDs), " "),
				"queried blocks", strings.Join(convertULIDsToString(myQueriedBlocks), " "))

			mtx.Lock()
			if len(results) > 0 {
				resultSets = append(resultSets, results)
			}
			warnings.Merge(sgWarnings)
			queriedBlocks = append(queriedBlocks, myQueriedBlocks...)
			mtx.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, nil, err
	}
	return resultSets, warnings, queriedBlocks, nil
}

// buildSGSearchLabelNamesRequest assembles the wire request, scoping the SG
// search to the blocks assigned by queryWithConsistencyCheck via request_hints
// so the SG does not re-scan every block it owns on every replica attempt.
func buildSGSearchLabelNamesRequest(
	minT, maxT int64,
	blockIDs []ulid.ULID,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	wireMatchers []storepb.LabelMatcher,
) *storepb.SearchLabelNamesRequest {
	req := &storepb.SearchLabelNamesRequest{
		Start:        minT,
		End:          maxT,
		Matchers:     wireMatchers,
		Filter:       paramsToSGProto(params),
		Ordering:     orderingToSGProto(hints),
		RequestHints: &storepb.SearchLabelNamesRequestHints{BlockMatchers: blockIDsToBlockMatchers(blockIDs)},
	}
	if hints != nil {
		req.Limit = int64(hints.Limit)
	}
	return req
}

// blockIDsToBlockMatchers builds the per-RPC block_matchers hint. Returns nil
// for an empty slice so the SG's BlockMatchers branch is not taken when no
// scoping is requested (matches LabelNames/LabelValues semantics).
//
// regexp.QuoteMeta is a no-op for ULIDs today (they're Crockford base32 — no
// regex metacharacters), but we escape defensively so future ID-format changes
// don't silently turn into a broken matcher.
func blockIDsToBlockMatchers(blockIDs []ulid.ULID) []storepb.LabelMatcher {
	if len(blockIDs) == 0 {
		return nil
	}
	ids := convertULIDsToString(blockIDs)
	for i, id := range ids {
		ids[i] = regexp.QuoteMeta(id)
	}
	return []storepb.LabelMatcher{
		{
			Type:  storepb.LabelMatcher_RE,
			Name:  block.BlockIDLabel,
			Value: strings.Join(ids, "|"),
		},
	}
}

func paramsToSGProto(p *streaminglabelvalues.Params) *storepb.SearchFilter {
	if p == nil || len(p.Terms) == 0 {
		return nil
	}
	wf := &storepb.SearchFilter{
		Terms:           p.Terms,
		CaseInsensitive: !p.CaseSensitive,
		FuzzThreshold:   int32(p.FuzzThreshold),
	}
	switch p.FuzzAlg {
	case streaminglabelvalues.FuzzAlgJaroWinkler:
		wf.FuzzAlg = storepb.FUZZ_ALG_JARO_WINKLER
	default:
		wf.FuzzAlg = storepb.FUZZ_ALG_SUBSEQUENCE
	}
	return wf
}

// orderingToSGProto defaults nil hints to ORDER_BY_VALUE_ASC, matching
// NewMergingSearchResultSet at the merge side.
func orderingToSGProto(hints *storage.SearchHints) storepb.SearchOrdering {
	if hints == nil {
		return storepb.ORDER_BY_VALUE_ASC
	}
	switch hints.OrderBy {
	case storage.OrderByValueDesc:
		return storepb.ORDER_BY_VALUE_DESC
	case storage.OrderByScoreDesc:
		return storepb.ORDER_BY_SCORE_DESC
	default:
		return storepb.ORDER_BY_VALUE_ASC
	}
}

// sgSearchStream is the Recv surface shared by both SG search streams.
type sgSearchStream interface {
	Recv() (*storepb.SearchResultBatch, error)
}

// SearchLabelValues mirrors SearchLabelNames; the wire request additionally
// carries the label whose values are being searched.
func (q *blocksStoreQuerier) SearchLabelValues(
	ctx context.Context,
	name string,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	matchers ...*labels.Matcher,
) storage.SearchResultSet {
	spanLog, ctx := spanlogger.New(ctx, q.logger, tracer, "blocksStoreQuerier.SearchLabelValues")
	defer spanLog.Finish()

	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}

	minT, maxT := q.minT, q.maxT
	spanLog.DebugLog("name", name, "start", util.TimeFromMillis(minT).UTC().String(), "end",
		util.TimeFromMillis(maxT).UTC().String(), "matchers", util.MatchersStringer(matchers))

	// Clamp minT to MaxLabelsQueryLength; mirrors blocksStoreQuerier.LabelValues.
	maxQueryLength := q.limits.MaxLabelsQueryLength(tenantID)
	if maxQueryLength != 0 {
		minT = clampToMaxLabelQueryLength(spanLog, minT, maxT, time.Now().UnixMilli(), maxQueryLength.Milliseconds())
	}

	var (
		mtx               sync.Mutex
		resResultSets     = [][]storage.SearchResult{}
		resWarnings       annotations.Annotations
		convertedMatchers = convertMatchersToLabelMatcher(matchers)
	)

	queryF := func(clients map[BlocksStoreClient][]ulid.ULID, qMinT, qMaxT int64, indexMeta *bucketindex.Metadata) ([]ulid.ULID, error) {
		resultSets, warnings, queriedBlocks, err := q.fetchSearchLabelValuesFromStore(ctx, name, clients, qMinT, qMaxT, tenantID, params, hints, convertedMatchers, indexMeta)
		if err != nil {
			return nil, err
		}
		mtx.Lock()
		resResultSets = append(resResultSets, resultSets...)
		resWarnings.Merge(warnings)
		mtx.Unlock()
		return queriedBlocks, nil
	}

	if err := q.queryWithConsistencyCheck(ctx, spanLog, minT, maxT, tenantID, nil, queryF); err != nil {
		return storage.ErrSearchResultSet(err)
	}

	return wrapAsMergingSearchResultSet(resResultSets, resWarnings, hints)
}

// fetchSearchLabelValuesFromStore mirrors fetchSearchLabelNamesFromStore.
func (q *blocksStoreQuerier) fetchSearchLabelValuesFromStore(
	ctx context.Context,
	name string,
	clients map[BlocksStoreClient][]ulid.ULID,
	minT int64,
	maxT int64,
	tenantID string,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	wireMatchers []storepb.LabelMatcher,
	indexMeta *bucketindex.Metadata,
) ([][]storage.SearchResult, annotations.Annotations, []ulid.ULID, error) {
	reqCtx := grpcContextWithBucketStoreRequestMeta(ctx, tenantID, indexMeta)

	var (
		g, gCtx       = errgroup.WithContext(reqCtx)
		mtx           = sync.Mutex{}
		resultSets    = [][]storage.SearchResult{}
		warnings      annotations.Annotations
		queriedBlocks = []ulid.ULID(nil)
		spanLog       = spanlogger.FromContext(ctx, q.logger)
	)

	for c, blockIDs := range clients {
		g.Go(func() error {
			req := buildSGSearchLabelValuesRequest(minT, maxT, name, blockIDs, params, hints, wireMatchers)
			stream, err := c.SearchLabelValues(gCtx, req)
			if err != nil {
				if shouldRetry(err) {
					level.Warn(spanLog).Log("msg", "failed to fetch search label values; error is retriable", "remote", c.RemoteAddress(), "err", err)
					return nil
				}
				return errors.Wrapf(err, "non-retriable error while fetching search label values from store %s", c.RemoteAddress())
			}

			results, sgWarnings, myQueriedBlocks, err := drainSGSearchLabelStream(stream)
			if err != nil {
				if shouldRetry(err) {
					level.Warn(spanLog).Log("msg", "failed to drain search label values stream; error is retriable", "remote", c.RemoteAddress(), "err", err)
					return nil
				}
				return errors.Wrapf(err, "failed to drain SearchLabelValues stream from store %s", c.RemoteAddress())
			}

			spanLog.DebugLog("msg", "received search label values from store-gateway",
				"instance", c,
				"label", name,
				"num values", len(results),
				"requested blocks", strings.Join(convertULIDsToString(blockIDs), " "),
				"queried blocks", strings.Join(convertULIDsToString(myQueriedBlocks), " "))

			mtx.Lock()
			if len(results) > 0 {
				resultSets = append(resultSets, results)
			}
			warnings.Merge(sgWarnings)
			queriedBlocks = append(queriedBlocks, myQueriedBlocks...)
			mtx.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, nil, err
	}
	return resultSets, warnings, queriedBlocks, nil
}

// buildSGSearchLabelValuesRequest mirrors buildSGSearchLabelNamesRequest
// with the additional Label field.
func buildSGSearchLabelValuesRequest(
	minT, maxT int64,
	name string,
	blockIDs []ulid.ULID,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	wireMatchers []storepb.LabelMatcher,
) *storepb.SearchLabelValuesRequest {
	req := &storepb.SearchLabelValuesRequest{
		Start:        minT,
		End:          maxT,
		Label:        name,
		Matchers:     wireMatchers,
		Filter:       paramsToSGProto(params),
		Ordering:     orderingToSGProto(hints),
		RequestHints: &storepb.SearchLabelValuesRequestHints{BlockMatchers: blockIDsToBlockMatchers(blockIDs)},
	}
	if hints != nil {
		req.Limit = int64(hints.Limit)
	}
	return req
}

// drainSGSearchLabelStream reads the stream to EOF, copying each batch's
// scored results into []storage.SearchResult and accumulating warnings into
// annotations. Scores are preserved.
//
// Response-hints contract: SearchResponseHints.QueriedBlocks may be carried
// on any batch and the drain accumulates them across the full stream. Today
// the store-gateway only attaches them to the trailer batch (see
// pkg/storegateway/bucket_search.go::streamBucketSearchResults), but the
// accumulator tolerates hints on intermediate batches — change the SG and
// the querier remains correct without code-changes here.
func drainSGSearchLabelStream(stream sgSearchStream) ([]storage.SearchResult, annotations.Annotations, []ulid.ULID, error) {
	var (
		results       []storage.SearchResult
		warnings      annotations.Annotations
		queriedBlocks []ulid.ULID
	)
	for {
		batch, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return results, warnings, queriedBlocks, nil
		}
		if err != nil {
			return nil, nil, nil, err
		}
		for _, r := range batch.Results {
			results = append(results, storage.SearchResult{Value: r.Value, Score: r.Score})
		}
		for _, w := range batch.Warnings {
			warnings.Add(searchWarning(w))
		}
		if batch.ResponseHints != nil {
			ids, err := convertBlockHintsToULIDs(batch.ResponseHints.QueriedBlocks)
			if err != nil {
				return nil, nil, nil, errors.Wrap(err, "failed to parse queried block IDs from search response hints")
			}
			queriedBlocks = append(queriedBlocks, ids...)
		}
	}
}
