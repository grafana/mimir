// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"golang.org/x/sync/errgroup"

	mimirstorage "github.com/grafana/mimir/pkg/storage"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// SearchLabelNames implements mimirstorage.MimirSearcher.
// It fans out to store-gateways concurrently, applies the search filter, and streams
// deduplicated results via a SearchValueSet. Sorting and limiting are handled by SearchValueSet.
func (q *blocksStoreQuerier) SearchLabelNames(ctx context.Context, hints *mimirstorage.MimirSearchHints, matchers ...*labels.Matcher) (mimirstorage.SearchResultSet, annotations.Annotations) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return mimirstorage.ErrorSearchResultSet(err), nil
	}

	minT, maxT := q.minT, q.maxT

	sortBy, sortOrder, limit := 0, 0, 0
	if hints != nil {
		sortBy = int(hints.SortBy)
		sortOrder = int(hints.SortOrder)
		limit = hints.Limit
	}

	maxBytesLimit := q.limits.StoreGatewaySearchLabelsValuesMaxSizeBytes(tenantID)
	convertedMatchers := convertMatchersToLabelMatcher(matchers)
	storeHints := searchHintsToLabelHints(hints)
	sf := mimirHintsToStoreSearchFilter(hints)

	produce := func(ch chan<- mimirstorage.SearchResult) (annotations.Annotations, error) {
		spanLog, spanCtx := spanlogger.New(ctx, q.logger, tracer, "blocksStoreQuerier.SearchLabelNames")
		defer spanLog.Finish()

		queryF := func(clients map[BlocksStoreClient][]ulid.ULID, minT, maxT int64, indexMeta *bucketindex.Metadata) ([]ulid.ULID, error) {
			return q.fetchLabelNamesFromStoreStreaming(spanCtx, clients, minT, maxT, tenantID, storeHints, convertedMatchers, sf, indexMeta, ch)
		}
		return nil, q.queryWithConsistencyCheck(spanCtx, spanLog, minT, maxT, tenantID, nil, queryF)
	}

	return mimirstorage.NewSearchValueSet(produce, sortBy, sortOrder, limit, maxBytesLimit, nil), nil
}

// SearchLabelValues implements mimirstorage.MimirSearcher.
// It returns a SearchResultSet immediately; fetching and deduplication run in the background.
// As each store-gateway responds its label values are deduplicated and streamed to the caller.
func (q *blocksStoreQuerier) SearchLabelValues(ctx context.Context, name string, hints *mimirstorage.MimirSearchHints, matchers ...*labels.Matcher) (mimirstorage.SearchResultSet, annotations.Annotations) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return mimirstorage.ErrorSearchResultSet(err), nil
	}

	minT, maxT := q.minT, q.maxT

	sortBy, sortOrder, limit := 0, 0, 0
	if hints != nil {
		sortBy = int(hints.SortBy)
		sortOrder = int(hints.SortOrder)
		limit = hints.Limit
	}

	maxBytesLimit := q.limits.StoreGatewaySearchLabelsValuesMaxSizeBytes(tenantID)
	storeHints := searchHintsToLabelHints(hints)
	sf := mimirHintsToStoreSearchFilter(hints)

	produce := func(ch chan<- mimirstorage.SearchResult) (annotations.Annotations, error) {
		spanLog, spanCtx := spanlogger.New(ctx, q.logger, tracer, "blocksStoreQuerier.SearchLabelValues")
		defer spanLog.Finish()

		queryF := func(clients map[BlocksStoreClient][]ulid.ULID, minT, maxT int64, indexMeta *bucketindex.Metadata) ([]ulid.ULID, error) {
			return q.fetchLabelValuesFromStoreStreaming(spanCtx, name, clients, minT, maxT, tenantID, storeHints, matchers, sf, indexMeta, ch)
		}
		return nil, q.queryWithConsistencyCheck(spanCtx, spanLog, minT, maxT, tenantID, nil, queryF)
	}

	return mimirstorage.NewSearchValueSet(produce, sortBy, sortOrder, limit, maxBytesLimit, nil), nil
}

// mimirHintsToStoreSearchFilter converts MimirSearchHints to a storepb.SearchFilter so the
// store-gateway can apply filter and sort server-side before transmitting values over the wire.
// Returns nil when there is nothing useful to send (no search terms and no sort).
func mimirHintsToStoreSearchFilter(h *mimirstorage.MimirSearchHints) *storepb.SearchFilter {
	if h == nil || (len(h.Search) == 0 && h.SortBy == 0) {
		return nil
	}
	return &storepb.SearchFilter{
		SearchTerms:     h.Search,
		CaseInsensitive: h.CaseInsensitive,
		FuzzAlg:         h.FuzzAlg,
		FuzzThreshold:   h.FuzzThreshold,
		SortBy:          storepb.SortBy(h.SortBy),
		SortOrder:       storepb.SortOrder(h.SortOrder),
	}
}

// createSearchLabelNamesRequest builds a SearchLabelNamesRequest scoped to the given block IDs and
// carrying an optional SearchFilter for server-side filtering.
func createSearchLabelNamesRequest(minT, maxT int64, blockIDs []ulid.ULID, hints *storage.LabelHints, matchers []storepb.LabelMatcher, sf *storepb.SearchFilter) *storepb.SearchLabelNamesRequest {
	var limit int64
	if hints != nil && hints.Limit > 0 {
		limit = int64(hints.Limit)
	}
	return &storepb.SearchLabelNamesRequest{
		Start:    minT,
		End:      maxT,
		Matchers: matchers,
		Limit:    limit,
		RequestHints: &storepb.LabelNamesRequestHints{
			BlockMatchers: []storepb.LabelMatcher{
				{
					Type:  storepb.LabelMatcher_RE,
					Name:  block.BlockIDLabel,
					Value: strings.Join(convertULIDsToString(blockIDs), "|"),
				},
			},
		},
		SearchFilter: sf,
	}
}

// createSearchLabelValuesRequest builds a SearchLabelValuesRequest scoped to the given block IDs and
// carrying an optional SearchFilter for server-side filtering.
func createSearchLabelValuesRequest(minT, maxT int64, label string, blockIDs []ulid.ULID, hints *storage.LabelHints, matchers []*labels.Matcher, sf *storepb.SearchFilter) *storepb.SearchLabelValuesRequest {
	var limit int64
	if hints != nil && hints.Limit > 0 {
		limit = int64(hints.Limit)
	}
	return &storepb.SearchLabelValuesRequest{
		Label:    label,
		Start:    minT,
		End:      maxT,
		Matchers: convertMatchersToLabelMatcher(matchers),
		Limit:    limit,
		RequestHints: &storepb.LabelValuesRequestHints{
			BlockMatchers: []storepb.LabelMatcher{
				{
					Type:  storepb.LabelMatcher_RE,
					Name:  block.BlockIDLabel,
					Value: strings.Join(convertULIDsToString(blockIDs), "|"),
				},
			},
		},
		SearchFilter: sf,
	}
}

// fetchLabelNamesFromStoreStreaming fans out label-names requests to the given store-gateway
// clients concurrently. As each client responds its sorted Names slice is sent to resultCh.
// It mirrors fetchLabelNamesFromStore but pushes results eagerly rather than collecting them.
func (q *blocksStoreQuerier) fetchLabelNamesFromStoreStreaming(
	ctx context.Context,
	clients map[BlocksStoreClient][]ulid.ULID,
	minT int64,
	maxT int64,
	tenantID string,
	hints *storage.LabelHints,
	matchers []storepb.LabelMatcher,
	sf *storepb.SearchFilter,
	indexMeta *bucketindex.Metadata,
	resultCh chan<- mimirstorage.SearchResult,
) ([]ulid.ULID, error) {
	reqCtx := grpcContextWithBucketStoreRequestMeta(ctx, tenantID, indexMeta)

	var (
		g, gCtx       = errgroup.WithContext(reqCtx)
		mtx           sync.Mutex
		queriedBlocks []ulid.ULID
		spanLog       = spanlogger.FromContext(ctx, q.logger)
	)

	for c, blockIDs := range clients {
		g.Go(func() error {
			req := createSearchLabelNamesRequest(minT, maxT, blockIDs, hints, matchers, sf)

			nameStream, err := c.SearchLabelNames(gCtx, req)
			if err != nil {
				if shouldRetry(err) {
					level.Warn(spanLog).Log("msg", "failed to fetch label names; error is retriable", "remote", c.RemoteAddress(), "err", err)
					return nil
				}
				return fmt.Errorf("non-retriable error while fetching label names from store: %w", err)
			}

			var myQueriedBlocks []ulid.ULID
			var totalNames int
			for {
				batch, recvErr := nameStream.Recv()
				if errors.Is(recvErr, io.EOF) {
					break
				}
				if recvErr != nil {
					if shouldRetry(recvErr) {
						level.Warn(spanLog).Log("msg", "failed to recv label names batch; error is retriable", "remote", c.RemoteAddress(), "err", recvErr)
						return nil
					}
					return fmt.Errorf("non-retriable error while receiving label names from store: %w", recvErr)
				}
				for _, r := range batch.Results {
					select {
					case resultCh <- mimirstorage.SearchResult{Value: r.Value, Score: r.Score}:
					case <-gCtx.Done():
						return gCtx.Err()
					}
				}
				totalNames += len(batch.Results)
				if batch.ResponseHints != nil {
					blocks, parseErr := convertBlockHintsToULIDs(batch.ResponseHints.QueriedBlocks)
					if parseErr != nil {
						return parseErr
					}
					myQueriedBlocks = append(myQueriedBlocks, blocks...)
				}
			}

			spanLog.DebugLog("msg", "received label names from store-gateway",
				"instance", c,
				"num labels", totalNames,
				"requested blocks", strings.Join(convertULIDsToString(blockIDs), " "),
				"queried blocks", strings.Join(convertULIDsToString(myQueriedBlocks), " "))

			mtx.Lock()
			queriedBlocks = append(queriedBlocks, myQueriedBlocks...)
			mtx.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return queriedBlocks, nil
}

// fetchLabelValuesFromStoreStreaming fans out label-values requests to the given store-gateway
// clients concurrently. As each client responds its sorted Values slice is sent to resultCh.
// It mirrors fetchLabelValuesFromStore but pushes results eagerly rather than collecting them.
func (q *blocksStoreQuerier) fetchLabelValuesFromStoreStreaming(
	ctx context.Context,
	name string,
	clients map[BlocksStoreClient][]ulid.ULID,
	minT int64,
	maxT int64,
	tenantID string,
	hints *storage.LabelHints,
	matchers []*labels.Matcher,
	sf *storepb.SearchFilter,
	indexMeta *bucketindex.Metadata,
	resultCh chan<- mimirstorage.SearchResult,
) ([]ulid.ULID, error) {
	reqCtx := grpcContextWithBucketStoreRequestMeta(ctx, tenantID, indexMeta)

	var (
		g, gCtx       = errgroup.WithContext(reqCtx)
		mtx           sync.Mutex
		queriedBlocks []ulid.ULID
		spanLog       = spanlogger.FromContext(ctx, q.logger)
	)

	for c, blockIDs := range clients {
		g.Go(func() error {
			req := createSearchLabelValuesRequest(minT, maxT, name, blockIDs, hints, matchers, sf)

			valueStream, err := c.SearchLabelValues(gCtx, req)
			if err != nil {
				if shouldRetry(err) {
					level.Warn(spanLog).Log("msg", "failed to fetch label values; error is retriable", "remote", c.RemoteAddress(), "err", err)
					return nil
				}
				return fmt.Errorf("non-retriable error while fetching label values from store: %w", err)
			}

			var myQueriedBlocks []ulid.ULID
			var totalValues int
			for {
				batch, recvErr := valueStream.Recv()
				if errors.Is(recvErr, io.EOF) {
					break
				}
				if recvErr != nil {
					if shouldRetry(recvErr) {
						level.Warn(spanLog).Log("msg", "failed to recv label values batch; error is retriable", "remote", c.RemoteAddress(), "err", recvErr)
						return nil
					}
					return fmt.Errorf("non-retriable error while receiving label values from store: %w", recvErr)
				}
				for _, r := range batch.Results {
					select {
					case resultCh <- mimirstorage.SearchResult{Value: r.Value, Score: r.Score}:
					case <-gCtx.Done():
						return gCtx.Err()
					}
				}
				totalValues += len(batch.Results)
				if batch.ResponseHints != nil {
					blocks, parseErr := convertBlockHintsToULIDs(batch.ResponseHints.QueriedBlocks)
					if parseErr != nil {
						return parseErr
					}
					myQueriedBlocks = append(myQueriedBlocks, blocks...)
				}
			}

			spanLog.DebugLog("msg", "received label values from store-gateway",
				"instance", c.RemoteAddress(),
				"num values", totalValues,
				"requested blocks", strings.Join(convertULIDsToString(blockIDs), " "),
				"queried blocks", strings.Join(convertULIDsToString(myQueriedBlocks), " "))

			mtx.Lock()
			queriedBlocks = append(queriedBlocks, myQueriedBlocks...)
			mtx.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return queriedBlocks, nil
}

// searchHintsToLabelHints converts a MimirSearchHints to a storage.LabelHints for use with
// store-gateway requests. No limit is pushed when sorting is requested, because all results
// must be returned to apply the global sort correctly.
func searchHintsToLabelHints(hints *mimirstorage.MimirSearchHints) *storage.LabelHints {
	if hints == nil || hints.SortBy != 0 {
		return nil
	}
	return &storage.LabelHints{Limit: hints.Limit}
}
