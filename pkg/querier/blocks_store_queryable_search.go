// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"io"
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

	mimirstorage "github.com/grafana/mimir/pkg/storage"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// SearchLabelNames fans out across the store-gateways owning the relevant
// blocks, drains each stream into a scored []storage.SearchResult, and
// returns the cross-SG merged set. Leaf scores propagate verbatim — no
// re-filter at the merge layer.
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

// wrapAsMergingSearchResultSet feeds the per-SG result slices into the
// k-way merger as slice-backed sources. Warnings collected outside an SG
// stream (e.g. retriable open errors) ride a synthetic warnings-only
// source so the merger surfaces them.
//
// This layer drains into slices because queryWithConsistencyCheck requires
// queriedBlocks to be returned before deciding whether to retry; a
// fully-streaming alternative would have to reshape that contract first.
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
	return mimirstorage.NewMergingSearchResultSet(sources, hints)
}

// fetchSearchLabelNamesFromStore drains every SG client in parallel.
//
// Caveat: the SG search RPC does not echo back queried block IDs (unlike
// LabelNames). We optimistically credit each SG with all blockIDs we asked
// it to search and rely on bucket-index sharding upstream to keep that set
// tight. If a future RPC revision adds a QueriedBlocks field, wire it up
// here.
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
			req := buildSGSearchLabelNamesRequest(minT, maxT, params, hints, wireMatchers)
			stream, err := c.SearchLabelNames(gCtx, req)
			if err != nil {
				if shouldRetry(err) {
					level.Warn(spanLog).Log("msg", "failed to fetch search label names; error is retriable", "remote", c.RemoteAddress(), "err", err)
					return nil
				}
				return errors.Wrapf(err, "non-retriable error while fetching search label names from store %s", c.RemoteAddress())
			}

			results, sgWarnings, err := drainSGSearchLabelStream(stream)
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
				"requested blocks", strings.Join(convertULIDsToString(blockIDs), " "))

			mtx.Lock()
			if len(results) > 0 {
				resultSets = append(resultSets, results)
			}
			warnings.Merge(sgWarnings)
			// Optimistic queriedBlocks — see function doc.
			queriedBlocks = append(queriedBlocks, blockIDs...)
			mtx.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, nil, err
	}
	return resultSets, warnings, queriedBlocks, nil
}

// buildSGSearchLabelNamesRequest assembles the wire request. The SG search
// RPC has no block-ID hints; the SG searches every block it owns in the
// request's time range.
func buildSGSearchLabelNamesRequest(
	minT, maxT int64,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	wireMatchers []storepb.LabelMatcher,
) *storepb.SearchLabelNamesRequest {
	req := &storepb.SearchLabelNamesRequest{
		Start:    minT,
		End:      maxT,
		Matchers: wireMatchers,
		Filter:   paramsToSGProto(params),
		Ordering: orderingToSGProto(hints),
	}
	if hints != nil {
		req.Limit = int64(hints.Limit)
	}
	return req
}

// paramsToSGProto is the storepb twin of distributor's paramsToProto.
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

// fetchSearchLabelValuesFromStore mirrors fetchSearchLabelNamesFromStore;
// see that function for the queriedBlocks caveat.
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
			req := buildSGSearchLabelValuesRequest(minT, maxT, name, params, hints, wireMatchers)
			stream, err := c.SearchLabelValues(gCtx, req)
			if err != nil {
				if shouldRetry(err) {
					level.Warn(spanLog).Log("msg", "failed to fetch search label values; error is retriable", "remote", c.RemoteAddress(), "err", err)
					return nil
				}
				return errors.Wrapf(err, "non-retriable error while fetching search label values from store %s", c.RemoteAddress())
			}

			results, sgWarnings, err := drainSGSearchLabelStream(stream)
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
				"requested blocks", strings.Join(convertULIDsToString(blockIDs), " "))

			mtx.Lock()
			if len(results) > 0 {
				resultSets = append(resultSets, results)
			}
			warnings.Merge(sgWarnings)
			// Optimistic queriedBlocks — see fetchSearchLabelNamesFromStore.
			queriedBlocks = append(queriedBlocks, blockIDs...)
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
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	wireMatchers []storepb.LabelMatcher,
) *storepb.SearchLabelValuesRequest {
	req := &storepb.SearchLabelValuesRequest{
		Start:    minT,
		End:      maxT,
		Label:    name,
		Matchers: wireMatchers,
		Filter:   paramsToSGProto(params),
		Ordering: orderingToSGProto(hints),
	}
	if hints != nil {
		req.Limit = int64(hints.Limit)
	}
	return req
}

// drainSGSearchLabelStream reads the stream to EOF, copying each batch's
// scored results into []storage.SearchResult and accumulating warnings into
// annotations. Scores are preserved verbatim from the leaf SG.
func drainSGSearchLabelStream(stream sgSearchStream) ([]storage.SearchResult, annotations.Annotations, error) {
	var (
		results  []storage.SearchResult
		warnings annotations.Annotations
	)
	for {
		batch, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return results, warnings, nil
		}
		if err != nil {
			return nil, nil, err
		}
		for _, r := range batch.Results {
			results = append(results, storage.SearchResult{Value: r.Value, Score: r.Score})
		}
		for _, w := range batch.Warnings {
			warnings.Add(errors.New(w))
		}
	}
}
