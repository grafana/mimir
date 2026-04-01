// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"sync"
	"time"

	"github.com/grafana/dskit/runutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/annotations"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mimirstorage "github.com/grafana/mimir/pkg/storage"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
)

// SearchLabelNames implements the storegatewaypb.StoreGatewayServer interface.
// It filters and sorts label names using SearchFilter, streaming results back in batches.
func (s *BucketStore) SearchLabelNames(req *storepb.SearchLabelNamesRequest, stream storegatewaypb.StoreGateway_SearchLabelNamesServer) error {
	reqSeriesMatchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
	if err != nil {
		return status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request labels matchers").Error())
	}

	stats := newSafeQueryStats()
	defer s.recordLabelNamesCallResult(stats)
	defer s.recordRequestAmbientTime(stats, time.Now())
	defer s.recordBucketIndexDiscoveryDiff(stream.Context())

	var reqBlockMatchers []*labels.Matcher
	if req.RequestHints != nil {
		reqBlockMatchers, err = storepb.MatchersToPromMatchers(req.RequestHints.BlockMatchers...)
		if err != nil {
			return status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request hints labels matchers").Error())
		}
	}

	searchFilter, err := buildSearchFilter(req.SearchFilter)
	if err != nil {
		return status.Error(codes.InvalidArgument, errors.Wrap(err, "build search filter").Error())
	}
	resHints := &storepb.StoreSearchResponseHints{}

	producer := func(ch chan<- mimirstorage.SearchResult) (annotations.Annotations, error) {
		return s.produceSearchLabelNames(stream.Context(), req, searchFilter, reqSeriesMatchers, reqBlockMatchers, stats, resHints, ch)
	}

	sortBy, sortOrder := sortParamsFromFilter(req.SearchFilter)
	iter := mimirstorage.NewSearchValueSet(producer, sortBy, sortOrder, int(req.Limit), s.searchMaxBytesLimitFn(), nil)
	defer iter.Close()

	return streamStoreSearchResults(iter, stream, resHints, int(req.Limit), s.searchLabelValuesStreamingBatchSize)
}

// produceSearchLabelNames fans out blockLabelNames calls concurrently, applies the search filter,
// and sends each accepted name to ch with ctx-aware backpressure.
func (s *BucketStore) produceSearchLabelNames(
	ctx context.Context,
	req *storepb.SearchLabelNamesRequest,
	searchFilter *streaminglabelvalues.FilterChains,
	reqSeriesMatchers, reqBlockMatchers []*labels.Matcher,
	stats *safeQueryStats,
	resHints *storepb.StoreSearchResponseHints,
	ch chan<- mimirstorage.SearchResult,
) (annotations.Annotations, error) {
	var (
		g, gctx                  = errgroup.WithContext(ctx)
		hintsMtx                 sync.Mutex
		blocksQueried            int
		blocksQueriedByBlockMeta = make(map[blockQueriedMeta]int)
		seriesLimiter            = s.seriesLimiterFactory(s.metrics.queriesDropped.WithLabelValues("series"))
	)

	s.blockSet.filter(req.Start, req.End, reqBlockMatchers, func(b *bucketBlock) {
		hintsMtx.Lock()
		resHints.AddQueriedBlock(b.meta.ULID)
		blocksQueriedByBlockMeta[newBlockQueriedMeta(b.meta)]++
		hintsMtx.Unlock()

		indexr := b.indexReader(s.postingsStrategy)

		g.Go(func() error {
			defer runutil.CloseWithLogOnErr(s.logger, indexr, "label names")
			b.ensureIndexHeaderLoaded(gctx, stats)

			result, err := blockLabelNames(gctx, indexr, reqSeriesMatchers, seriesLimiter, s.maxSeriesPerBatch, s.logger, stats)
			if err != nil {
				return errors.Wrapf(err, "block %s", b.meta.ULID)
			}

			if len(result) > 0 {
				hintsMtx.Lock()
				blocksQueried++
				hintsMtx.Unlock()
			}

			var score float64
			var accepted bool
			for _, v := range result {

				if searchFilter != nil {
					accepted, score = searchFilter.Accept(v)
					if !accepted {
						continue
					}
				}
				select {
				case ch <- mimirstorage.SearchResult{Value: v, Score: score}:
				case <-gctx.Done():
					return gctx.Err()
				}
			}
			return nil
		})
	})

	if err := g.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, status.Error(codes.Canceled, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	stats.update(func(st *queryStats) {
		st.blocksQueried = blocksQueried
		for sl, count := range blocksQueriedByBlockMeta {
			st.blocksQueriedByBlockMeta[sl] = count
		}
	})

	return nil, nil
}

// SearchLabelValues implements the storegatewaypb.StoreGatewayServer interface.
// It filters and sorts label values using SearchFilter, streaming results back in batches.
func (s *BucketStore) SearchLabelValues(req *storepb.SearchLabelValuesRequest, stream storegatewaypb.StoreGateway_SearchLabelValuesServer) error {
	reqSeriesMatchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
	if err != nil {
		return status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request labels matchers").Error())
	}

	stats := newSafeQueryStats()
	defer s.recordLabelValuesCallResult(stats)
	defer s.recordRequestAmbientTime(stats, time.Now())
	defer s.recordBucketIndexDiscoveryDiff(stream.Context())

	if req.Label == "" {
		return status.Error(codes.InvalidArgument, "missing label name")
	}

	var reqBlockMatchers []*labels.Matcher
	if req.RequestHints != nil {
		reqBlockMatchers, err = storepb.MatchersToPromMatchers(req.RequestHints.BlockMatchers...)
		if err != nil {
			return status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request hints labels matchers").Error())
		}
	}

	searchFilter, err := buildSearchFilter(req.SearchFilter)
	if err != nil {
		return status.Error(codes.InvalidArgument, errors.Wrap(err, "build search filter").Error())
	}
	resHints := &storepb.StoreSearchResponseHints{}

	produce := func(ch chan<- mimirstorage.SearchResult) (annotations.Annotations, error) {
		return s.produceSearchLabelValues(stream.Context(), req, searchFilter, reqSeriesMatchers, reqBlockMatchers, stats, resHints, ch)
	}

	sortBy, sortOrder := sortParamsFromFilter(req.SearchFilter)
	iter := mimirstorage.NewSearchValueSet(produce, sortBy, sortOrder, int(req.Limit), s.searchMaxBytesLimitFn(), nil)
	defer iter.Close()

	return streamStoreSearchResults(iter, stream, resHints, int(req.Limit), s.searchLabelValuesStreamingBatchSize)
}

// produceSearchLabelValues fans out blockLabelValues calls concurrently, applies the search filter,
// and sends each accepted value to ch with ctx-aware backpressure.
func (s *BucketStore) produceSearchLabelValues(
	ctx context.Context,
	req *storepb.SearchLabelValuesRequest,
	searchFilter *streaminglabelvalues.FilterChains,
	reqSeriesMatchers, reqBlockMatchers []*labels.Matcher,
	stats *safeQueryStats,
	resHints *storepb.StoreSearchResponseHints,
	ch chan<- mimirstorage.SearchResult,
) (annotations.Annotations, error) {
	var (
		g, gctx  = errgroup.WithContext(ctx)
		hintsMtx sync.Mutex
	)

	s.blockSet.filter(req.Start, req.End, reqBlockMatchers, func(b *bucketBlock) {
		hintsMtx.Lock()
		resHints.AddQueriedBlock(b.meta.ULID)
		hintsMtx.Unlock()

		// This index reader is here only to keep the block open inside the goroutine;
		// blockLabelValues creates its own reader with the correct labelValuesPostingsStrategy.
		indexr := b.indexReader(nil)

		g.Go(func() error {
			defer runutil.CloseWithLogOnErr(b.logger, indexr, "close block index reader")
			b.ensureIndexHeaderLoaded(gctx, stats)

			result, err := blockLabelValues(gctx, b, s.postingsStrategy, s.maxSeriesPerBatch, req.Label, reqSeriesMatchers, s.logger, stats)
			if err != nil {
				return errors.Wrapf(err, "block %s", b.meta.ULID)
			}

			var score float64
			var accepted bool
			for _, v := range result {

				if searchFilter != nil {
					accepted, score = searchFilter.Accept(v)
					if !accepted {
						continue
					}
				}
				select {
				case ch <- mimirstorage.SearchResult{Value: v, Score: score}:
				case <-gctx.Done():
					return gctx.Err()
				}
			}
			return nil
		})
	})

	if err := g.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, status.Error(codes.Canceled, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	return nil, nil
}

// streamStoreSearchResults batches iter results into StoreSearchResponse messages and sends them.
// The final message always carries response_hints. Warnings (if any) are sent in a trailing message.
// limit is used to cap the batch size: if a limit is set we'll never need more than that many results.
func streamStoreSearchResults(
	iter *mimirstorage.SearchValueSet,
	stream interface {
		Send(*storepb.StoreSearchResponse) error
	},
	resHints *storepb.StoreSearchResponseHints,
	limit int,
	batchSize int,
) error {
	if batchSize <= 0 {
		batchSize = 1024
	}
	if limit > 0 {
		batchSize = min(limit, batchSize)
	}
	batch := make([]*storepb.StoreSearchResult, batchSize)
	for k := range batch {
		batch[k] = &storepb.StoreSearchResult{}
	}

	idx := 0
	for iter.Next() {
		r := iter.At()
		*batch[idx] = storepb.StoreSearchResult{Value: r.Value, Score: r.Score}
		idx++
		if idx == len(batch) {
			if err := stream.Send(&storepb.StoreSearchResponse{Results: batch}); err != nil {
				return err
			}
			idx = 0
		}
	}

	if err := iter.Err(); err != nil {
		return err
	}

	// Final message: flush remaining results, attach hints and any warnings.
	finalResp := &storepb.StoreSearchResponse{ResponseHints: resHints}
	if idx > 0 {
		finalResp.Results = batch[:idx]
	}
	if warns := iter.Warnings(); len(warns) > 0 {
		finalResp.Warnings = make([]string, 0, len(warns))
		for _, w := range warns {
			finalResp.Warnings = append(finalResp.Warnings, w.Error())
		}
	}
	return stream.Send(finalResp)
}

// sortParamsFromFilter extracts sort parameters from a SearchFilter.
// Returns (0, 0) if sf is nil (no sorting).
func sortParamsFromFilter(sf *storepb.SearchFilter) (sortBy, sortOrder int) {
	if sf != nil {
		return int(sf.SortBy), int(sf.SortOrder)
	}
	return 0, 0
}

// buildSearchFilter converts a proto SearchFilter into a FilterChains ready for concurrent use.
// Returns nil if sf is nil or has no search terms.
func buildSearchFilter(sf *storepb.SearchFilter) (*streaminglabelvalues.FilterChains, error) {
	if sf == nil || len(sf.SearchTerms) == 0 {
		return nil, nil
	}
	return streaminglabelvalues.BuildFilterChains(sf.SearchTerms, sf.CaseInsensitive, sf.FuzzAlg, sf.FuzzThreshold)
}
