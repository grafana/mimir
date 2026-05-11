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

	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// SearchLabelNames fans the request out across store-gateway replicas owning the
// blocks in the query window, drains each replica's SearchLabelNames stream,
// merges per-replica name slices, then re-scores via storage.ApplySearchHints
// to apply the requested filter, ordering, and limit.
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
		resNameSets       = [][]string{}
		resWarnings       annotations.Annotations
		convertedMatchers = convertMatchersToLabelMatcher(matchers)
	)

	queryF := func(clients map[BlocksStoreClient][]ulid.ULID, qMinT, qMaxT int64, indexMeta *bucketindex.Metadata) ([]ulid.ULID, error) {
		nameSets, warnings, queriedBlocks, err := q.fetchSearchLabelNamesFromStore(ctx, clients, qMinT, qMaxT, tenantID, params, hints, convertedMatchers, indexMeta)
		if err != nil {
			return nil, err
		}
		mtx.Lock()
		resNameSets = append(resNameSets, nameSets...)
		resWarnings.Merge(warnings)
		mtx.Unlock()
		return queriedBlocks, nil
	}

	if err := q.queryWithConsistencyCheck(ctx, spanLog, minT, maxT, tenantID, nil, queryF); err != nil {
		return storage.ErrSearchResultSet(err)
	}

	merged := util.MergeSlices(resNameSets...)
	return storage.NewSearchResultSetFromSlice(storage.ApplySearchHints(merged, hints), resWarnings)
}

// fetchSearchLabelNamesFromStore mirrors fetchLabelNamesFromStore but uses the
// streaming SearchLabelNames RPC. It accumulates the per-client values, merges
// warnings, and returns the union of blockIDs that were requested.
//
// Caveat: the SG SearchLabelNames RPC does not echo back queried block IDs
// (unlike LabelNames). We optimistically credit the SG with all blockIDs we
// requested it search — the SG actually searches every block it owns in the
// time range (see pkg/storegateway/bucket_search.go), so we trust the
// bucket-index sharding upstream to keep this set tight. If a future RPC
// revision adds a QueriedBlocks field, wire it up here.
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
) ([][]string, annotations.Annotations, []ulid.ULID, error) {
	reqCtx := grpcContextWithBucketStoreRequestMeta(ctx, tenantID, indexMeta)

	var (
		g, gCtx       = errgroup.WithContext(reqCtx)
		mtx           = sync.Mutex{}
		nameSets      = [][]string{}
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

			values, sgWarnings, err := drainSGSearchLabelStream(stream)
			if err != nil {
				if shouldRetry(err) {
					level.Warn(spanLog).Log("msg", "failed to drain search label names stream; error is retriable", "remote", c.RemoteAddress(), "err", err)
					return nil
				}
				return errors.Wrapf(err, "failed to drain SearchLabelNames stream from store %s", c.RemoteAddress())
			}

			spanLog.DebugLog("msg", "received search label names from store-gateway",
				"instance", c,
				"num values", len(values),
				"requested blocks", strings.Join(convertULIDsToString(blockIDs), " "))

			mtx.Lock()
			nameSets = append(nameSets, values)
			warnings.Merge(sgWarnings)
			// See caveat in the function doc — the RPC does not echo back the
			// blocks the SG actually visited; we optimistically credit the
			// blocks we asked it to search.
			queriedBlocks = append(queriedBlocks, blockIDs...)
			mtx.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, nil, err
	}
	return nameSets, warnings, queriedBlocks, nil
}

// buildSGSearchLabelNamesRequest assembles the wire request for the SG
// SearchLabelNames RPC. Unlike LabelNames the request carries no block-ID
// hints: the SG searches every block it owns in the request's time range.
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

// paramsToSGProto is the storegateway/storepb twin of distributor's
// paramsToProto. Returns nil when there are no terms — the SG then scores
// every value at 1.0.
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

// orderingToSGProto translates hints.OrderBy into the SG wire enum. Defaults
// to ORDER_BY_VALUE_ASC when hints is nil — same default as
// storage.ApplySearchHints.
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

// sgSearchStream is the narrow Recv interface satisfied by both
// StoreGateway_SearchLabelNamesClient and StoreGateway_SearchLabelValuesClient.
type sgSearchStream interface {
	Recv() (*storepb.SearchResultBatch, error)
}

// drainSGSearchLabelStream reads the stream to EOF, accumulating values from
// each batch's Results and string warnings into annotations.
func drainSGSearchLabelStream(stream sgSearchStream) ([]string, annotations.Annotations, error) {
	var (
		values   []string
		warnings annotations.Annotations
	)
	for {
		batch, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return values, warnings, nil
		}
		if err != nil {
			return nil, nil, err
		}
		for _, r := range batch.Results {
			values = append(values, r.Value)
		}
		for _, w := range batch.Warnings {
			warnings.Add(errors.New(w))
		}
	}
}
