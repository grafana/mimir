// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"sync"
	"time"

	"github.com/grafana/dskit/runutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mimirstorage "github.com/grafana/mimir/pkg/storage"
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
// the merged set.
func (s *BucketStore) SearchLabelNames(req *storepb.SearchLabelNamesRequest, srv storegatewaypb.StoreGateway_SearchLabelNamesServer) error {
	ctx := srv.Context()

	matchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
	if err != nil {
		return status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request matchers").Error())
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
		setsMtx sync.Mutex
		sets    []storage.SearchResultSet
	)
	seriesLimiter := s.seriesLimiterFactory(s.metrics.queriesDropped.WithLabelValues("series"))

	s.blockSet.filter(req.Start, req.End, nil, func(b *bucketBlock) {
		// indexReader is created here (outside the goroutine) to hold the block open.
		indexr := b.indexReader(s.postingsStrategy)
		g.Go(func() error {
			defer runutil.CloseWithLogOnErr(s.logger, indexr, "search label names")

			b.ensureIndexHeaderLoaded(gctx, stats)

			result, err := blockLabelNames(gctx, indexr, matchers, seriesLimiter, s.maxSeriesPerBatch, s.logger, stats)
			if err != nil {
				return errors.Wrapf(err, "block %s", b.meta.ULID)
			}

			set, err := applyPerBlockSearchHints(result, params, order, limit)
			if err != nil {
				return errors.Wrapf(err, "block %s", b.meta.ULID)
			}
			if set == nil {
				return nil
			}
			setsMtx.Lock()
			sets = append(sets, set)
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

	merged := mimirstorage.PairwiseMergeSearchSets(sets, order, limit)
	defer merged.Close()
	return streamBucketSearchResults(ctx, merged, srv.Send)
}

// SearchLabelValues streams label values for req.Label matching the wire search
// filter across all blocks in the request's time range. Implements storegatewaypb.StoreGatewayServer.
//
// See SearchLabelNames for the merge structure.
func (s *BucketStore) SearchLabelValues(req *storepb.SearchLabelValuesRequest, srv storegatewaypb.StoreGateway_SearchLabelValuesServer) error {
	ctx := srv.Context()

	matchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
	if err != nil {
		return status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request matchers").Error())
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
		setsMtx sync.Mutex
		sets    []storage.SearchResultSet
	)

	s.blockSet.filter(req.Start, req.End, nil, func(b *bucketBlock) {
		// indexReader is created here (outside the goroutine) to hold the block open.
		indexr := b.indexReader(nil)
		g.Go(func() error {
			defer runutil.CloseWithLogOnErr(s.logger, indexr, "search label values")

			b.ensureIndexHeaderLoaded(gctx, stats)

			result, err := blockLabelValues(gctx, b, s.postingsStrategy, s.maxSeriesPerBatch, req.Label, matchers, s.logger, stats)
			if err != nil {
				return errors.Wrapf(err, "block %s", b.meta.ULID)
			}

			set, err := applyPerBlockSearchHints(result, params, order, limit)
			if err != nil {
				return errors.Wrapf(err, "block %s", b.meta.ULID)
			}
			if set == nil {
				return nil
			}
			setsMtx.Lock()
			sets = append(sets, set)
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

	merged := mimirstorage.PairwiseMergeSearchSets(sets, order, limit)
	defer merged.Close()
	return streamBucketSearchResults(ctx, merged, srv.Send)
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
// by the merge ride on the final batch (or are sent alone in a trailer batch
// when no results were produced). The stream context is checked at each
// batch boundary so a cancelled client stops the loop promptly.
func streamBucketSearchResults(ctx context.Context, rs storage.SearchResultSet, send func(*storepb.SearchResultBatch) error) error {
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
	batch.Warnings = mimirstorage.WarningsToStrings(rs.Warnings())
	if len(batch.Results) > 0 || len(batch.Warnings) > 0 {
		return send(batch)
	}
	return nil
}

 
