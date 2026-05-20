// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"sync"
	"time"

	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
)

const searchBatchSize = 256

// SearchLabelNames streams label names matching the wire search filter across
// all blocks in the request's time range. Implements storegatewaypb.StoreGatewayServer.
//
// Each block is read concurrently and filter/order/limit are applied per block
// (per-goroutine filter, since filters cache term runes lazily and are not
// concurrency-safe). The per-block SearchResultSets are then streamed through
// a pairwise k-way merge that respects the requested ordering, deduplicates
// across blocks, and stops after the request limit — without materialising
// the merged set. The IDs of every block we consulted are returned on the
// final batch as response_hints so the querier's consistency check can credit
// the blocks this store-gateway actually queried.
func (s *BucketStore) SearchLabelNames(req *storepb.SearchLabelNamesRequest, srv storegatewaypb.StoreGateway_SearchLabelNamesServer) error {
	ctx := srv.Context()

	matchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
	if err != nil {
		return status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request matchers").Error())
	}

	reqBlockMatchers, err := searchLabelNamesRequestBlockMatchers(req)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	params, err := storepbToParams(req.Filter)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	order := storepbToOrdering(req.Ordering)
	limit := int(req.Limit)

	stats := newSafeQueryStats()
	// TODO(streaming-search): metrics recorded here are not labelled by RPC,
	// so search load currently blends into label-{names,values} histograms.
	// Add an `rpc` label when this RPC becomes user-facing.
	defer s.recordLabelNamesCallResult(stats)
	defer s.recordRequestAmbientTime(stats, time.Now())

	g, gctx := errgroup.WithContext(ctx)

	var (
		setsMtx       sync.Mutex
		sets          []storage.SearchResultSet
		queriedBlocks []ulid.ULID
	)
	seriesLimiter := s.seriesLimiterFactory(s.metrics.queriesDropped.WithLabelValues("series"))

	s.blockSet.filter(req.Start, req.End, reqBlockMatchers, func(b *bucketBlock) {
		// indexReader is created here (outside the goroutine) to hold the block open.
		indexr := b.indexReader(s.postingsStrategy)
		blockID := b.meta.ULID
		g.Go(func() error {
			defer runutil.CloseWithLogOnErr(s.logger, indexr, "search label names")

			b.ensureIndexHeaderLoaded(gctx, stats)

			result, err := blockLabelNames(gctx, indexr, matchers, seriesLimiter, s.maxSeriesPerBatch, s.logger, stats)
			if err != nil {
				return errors.Wrapf(err, "block %s", blockID)
			}

			set, err := applyPerBlockSearchHints(result, params, order, limit)
			if err != nil {
				return errors.Wrapf(err, "block %s", blockID)
			}
			setsMtx.Lock()
			if set != nil {
				sets = append(sets, set)
			}
			queriedBlocks = append(queriedBlocks, blockID)
			setsMtx.Unlock()
			return nil
		})
	})

	if err := g.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			return status.Error(codes.Canceled, err.Error())
		}
		return status.Error(codes.Internal, err.Error())
	}

	merged := storage.MergeSearchResultSets(sets, &storage.SearchHints{OrderBy: order, Limit: limit})
	defer merged.Close()
	return streamBucketSearchResults(ctx, merged, queriedBlocks, srv.Send)
}

// SearchLabelValues streams label values for req.Label matching the wire search
// filter across all blocks in the request's time range. Implements storegatewaypb.StoreGatewayServer.
//
// See SearchLabelNames for the merge structure and response-hints contract.
func (s *BucketStore) SearchLabelValues(req *storepb.SearchLabelValuesRequest, srv storegatewaypb.StoreGateway_SearchLabelValuesServer) error {
	ctx := srv.Context()

	matchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
	if err != nil {
		return status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request matchers").Error())
	}

	reqBlockMatchers, err := searchLabelValuesRequestBlockMatchers(req)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	params, err := storepbToParams(req.Filter)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	order := storepbToOrdering(req.Ordering)
	limit := int(req.Limit)

	stats := newSafeQueryStats()
	// TODO(streaming-search): metrics recorded here are not labelled by RPC,
	// so search load currently blends into label-{names,values} histograms.
	// Add an `rpc` label when this RPC becomes user-facing.
	defer s.recordLabelValuesCallResult(stats)
	defer s.recordRequestAmbientTime(stats, time.Now())

	g, gctx := errgroup.WithContext(ctx)

	var (
		setsMtx       sync.Mutex
		sets          []storage.SearchResultSet
		queriedBlocks []ulid.ULID
	)

	s.blockSet.filter(req.Start, req.End, reqBlockMatchers, func(b *bucketBlock) {
		// indexr is intentionally unused below — blockLabelValues takes the
		// bucketBlock directly and constructs its own reader with the right
		// postingsSelectionStrategy. We still create one here (and defer its
		// Close) so b.pendingReaders is incremented for the duration of the
		// goroutine: that refcount keeps the block from being unloaded
		// mid-query. Same idiom as BucketStore.LabelValues (bucket.go:1544).
		indexr := b.indexReader(nil)
		blockID := b.meta.ULID
		g.Go(func() error {
			defer runutil.CloseWithLogOnErr(s.logger, indexr, "search label values")

			b.ensureIndexHeaderLoaded(gctx, stats)

			result, err := blockLabelValues(gctx, b, s.postingsStrategy, s.maxSeriesPerBatch, req.Label, matchers, s.logger, stats)
			if err != nil {
				return errors.Wrapf(err, "block %s", blockID)
			}

			set, err := applyPerBlockSearchHints(result, params, order, limit)
			if err != nil {
				return errors.Wrapf(err, "block %s", blockID)
			}
			setsMtx.Lock()
			if set != nil {
				sets = append(sets, set)
			}
			queriedBlocks = append(queriedBlocks, blockID)
			setsMtx.Unlock()
			return nil
		})
	})

	if err := g.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			return status.Error(codes.Canceled, err.Error())
		}
		return status.Error(codes.Internal, err.Error())
	}

	merged := storage.MergeSearchResultSets(sets, &storage.SearchHints{OrderBy: order, Limit: limit})
	defer merged.Close()
	return streamBucketSearchResults(ctx, merged, queriedBlocks, srv.Send)
}

// searchLabelNamesRequestBlockMatchers extracts the block_matchers hint from a
// SearchLabelNamesRequest. Returns nil when no hint is set so blockSet.filter
// skips per-block filtering.
func searchLabelNamesRequestBlockMatchers(req *storepb.SearchLabelNamesRequest) ([]*labels.Matcher, error) {
	if req.RequestHints == nil || len(req.RequestHints.BlockMatchers) == 0 {
		return nil, nil
	}
	bm, err := storepb.MatchersToPromMatchers(req.RequestHints.BlockMatchers...)
	if err != nil {
		return nil, errors.Wrap(err, "translate request hints labels matchers")
	}
	return bm, nil
}

// searchLabelValuesRequestBlockMatchers mirrors searchLabelNamesRequestBlockMatchers.
func searchLabelValuesRequestBlockMatchers(req *storepb.SearchLabelValuesRequest) ([]*labels.Matcher, error) {
	if req.RequestHints == nil || len(req.RequestHints.BlockMatchers) == 0 {
		return nil, nil
	}
	bm, err := storepb.MatchersToPromMatchers(req.RequestHints.BlockMatchers...)
	if err != nil {
		return nil, errors.Wrap(err, "translate request hints labels matchers")
	}
	return bm, nil
}

// applyPerBlockSearchHints builds a per-goroutine filter from params, then
// runs storage.ApplySearchHints over the block's raw values to apply the
// filter, ordering, and per-block limit. Returns a SearchResultSet wrapping
// the per-block results, or (nil, nil) if the block produced nothing useful.
//
// Filters wrap Prometheus matchers that lazily cache term runes on first
// Unicode candidate and are not safe for concurrent use, so every block-fan-out
// goroutine constructs its own filter rather than sharing one.
func applyPerBlockSearchHints(values []string, params *streaminglabelvalues.Params, order storage.Ordering, limit int) (storage.SearchResultSet, error) {
	if len(values) == 0 {
		return nil, nil
	}
	filter, err := streaminglabelvalues.BuildFilter(params)
	if err != nil {
		return nil, err
	}
	results := storage.ApplySearchHints(values, &storage.SearchHints{
		Filter:  filter,
		OrderBy: order,
		Limit:   limit,
	})
	if len(results) == 0 {
		return nil, nil
	}
	return storage.NewSearchResultSetFromSlice(results, nil), nil
}

// storepbToParams converts a wire SearchFilter into a validated
// streaminglabelvalues.Params via NewParams. A nil input returns (nil, nil).
func storepbToParams(wf *storepb.SearchFilter) (*streaminglabelvalues.Params, error) {
	if wf == nil {
		return nil, nil
	}
	alg := streaminglabelvalues.FuzzAlgSubsequence
	if wf.FuzzAlg == storepb.FUZZ_ALG_JARO_WINKLER {
		alg = streaminglabelvalues.FuzzAlgJaroWinkler
	}
	return streaminglabelvalues.NewParams(wf.Terms, !wf.CaseInsensitive, alg, int(wf.FuzzThreshold))
}

// storepbToOrdering maps the wire SearchOrdering enum onto storage.Ordering.
func storepbToOrdering(o storepb.SearchOrdering) storage.Ordering {
	switch o {
	case storepb.ORDER_BY_VALUE_DESC:
		return storage.OrderByValueDesc
	case storepb.ORDER_BY_SCORE_DESC:
		return storage.OrderByScoreDesc
	default:
		return storage.OrderByValueAsc
	}
}

// streamBucketSearchResults pulls from the merged SearchResultSet in batches
// of searchBatchSize and ships each batch over send. Any warnings accumulated
// by the merge and the response hints (queried block IDs) ride on the final
// batch. The stream context is checked at each batch boundary so a cancelled
// client stops the loop promptly.
//
// The trailer batch is always sent — even when results and warnings are both
// empty — so the querier's consistency check can credit the blocks this
// store-gateway actually queried via the response_hints field.
func streamBucketSearchResults(ctx context.Context, rs storage.SearchResultSet, queriedBlocks []ulid.ULID, send func(*storepb.SearchResultBatch) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	batch := &storepb.SearchResultBatch{Results: make([]storepb.SearchResultBatch_Result, 0, searchBatchSize)}
	for rs.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		r := rs.At()
		batch.Results = append(batch.Results, storepb.SearchResultBatch_Result{Value: r.Value, Score: r.Score})
		if len(batch.Results) >= searchBatchSize {
			if err := send(batch); err != nil {
				return err
			}
			batch = &storepb.SearchResultBatch{Results: make([]storepb.SearchResultBatch_Result, 0, searchBatchSize)}
		}
	}
	if err := rs.Err(); err != nil {
		return err
	}
	batch.Warnings = warningsToStrings(rs.Warnings())
	batch.ResponseHints = buildSearchResponseHints(queriedBlocks)
	return send(batch)
}

// buildSearchResponseHints builds the per-RPC SearchResponseHints. Returns nil
// if no blocks were queried so the wire field is omitted.
func buildSearchResponseHints(queriedBlocks []ulid.ULID) *storepb.SearchResponseHints {
	if len(queriedBlocks) == 0 {
		return nil
	}
	hints := &storepb.SearchResponseHints{QueriedBlocks: make([]storepb.Block, 0, len(queriedBlocks))}
	for _, id := range queriedBlocks {
		hints.QueriedBlocks = append(hints.QueriedBlocks, storepb.Block{Id: id.String()})
	}
	return hints
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
