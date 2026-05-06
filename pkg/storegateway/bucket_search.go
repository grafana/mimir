// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/grafana/dskit/runutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util"
)

const searchBatchSize = 256

// SearchLabelNames streams label names matching the wire search filter across
// all blocks in the request's time range. Implements storegatewaypb.StoreGatewayServer.
func (s *BucketStore) SearchLabelNames(req *storepb.SearchLabelNamesRequest, srv storegatewaypb.StoreGateway_SearchLabelNamesServer) error {
	ctx := srv.Context()

	matchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
	if err != nil {
		return status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request matchers").Error())
	}

	hints, err := buildBucketSearchHints(req.Filter, req.Ordering, req.Limit)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	stats := newSafeQueryStats()
	// TODO(streaming-search): metrics recorded here are not labelled by RPC,
	// so search load currently blends into label-{names,values} histograms.
	// Add an `rpc` label when this RPC becomes user-facing.
	defer s.recordLabelNamesCallResult(stats)
	defer s.recordRequestAmbientTime(stats, time.Now())

	g, gctx := errgroup.WithContext(ctx)

	var (
		setsMtx sync.Mutex
		sets    [][]string
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

			if len(result) > 0 {
				setsMtx.Lock()
				sets = append(sets, result)
				setsMtx.Unlock()
			}

			return nil
		})
	})

	if err := g.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			return status.Error(codes.Canceled, err.Error())
		}
		return status.Error(codes.Internal, err.Error())
	}

	// TODO(streaming-search): per-block label names are materialised into
	// memory before the merge/filter/limit step. For tenants with O(10M)
	// label-name cardinality this is an OOM hazard. The querier-side limit
	// pushdown (next PR) is expected to bound the worst case via tighter
	// per-store-gateway limits; if it cannot, revisit per-block early
	// termination here for OrderByValue{Asc,Desc}. OrderByScoreDesc cannot
	// be safely truncated per block.
	merged := util.MergeSlices(sets...)
	results := storage.ApplySearchHints(merged, hints)
	return streamBucketSearchResults(ctx, results, unfilteredTruncationWarning(merged, hints), srv.Send)
}

// SearchLabelValues streams label values for req.Label matching the wire search
// filter across all blocks in the request's time range. Implements storegatewaypb.StoreGatewayServer.
func (s *BucketStore) SearchLabelValues(req *storepb.SearchLabelValuesRequest, srv storegatewaypb.StoreGateway_SearchLabelValuesServer) error {
	ctx := srv.Context()

	matchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
	if err != nil {
		return status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request matchers").Error())
	}

	hints, err := buildBucketSearchHints(req.Filter, req.Ordering, req.Limit)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	stats := newSafeQueryStats()
	// TODO(streaming-search): metrics recorded here are not labelled by RPC,
	// so search load currently blends into label-{names,values} histograms.
	// Add an `rpc` label when this RPC becomes user-facing.
	defer s.recordLabelValuesCallResult(stats)
	defer s.recordRequestAmbientTime(stats, time.Now())

	g, gctx := errgroup.WithContext(ctx)

	var (
		setsMtx sync.Mutex
		sets    [][]string
	)

	s.blockSet.filter(req.Start, req.End, nil, func(b *bucketBlock) {
		// indexReader is created here (outside the goroutine) to hold the block open.
		// nil postingsStrategy mirrors BucketStore.LabelValues.
		indexr := b.indexReader(nil)
		g.Go(func() error {
			defer runutil.CloseWithLogOnErr(s.logger, indexr, "search label values")

			b.ensureIndexHeaderLoaded(gctx, stats)

			result, err := blockLabelValues(gctx, b, s.postingsStrategy, s.maxSeriesPerBatch, req.Label, matchers, s.logger, stats)
			if err != nil {
				return errors.Wrapf(err, "block %s", b.meta.ULID)
			}

			if len(result) > 0 {
				setsMtx.Lock()
				sets = append(sets, result)
				setsMtx.Unlock()
			}

			return nil
		})
	})

	if err := g.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			return status.Error(codes.Canceled, err.Error())
		}
		return status.Error(codes.Internal, err.Error())
	}

	// TODO(streaming-search): per-block label values are materialised into
	// memory before the merge/filter/limit step. For tenants with O(10M)
	// label-value cardinality on a hot label this is an OOM hazard. The
	// querier-side limit pushdown (next PR) is expected to bound the worst
	// case via tighter per-store-gateway limits; if it cannot, revisit
	// per-block early termination here for OrderByValue{Asc,Desc}.
	// OrderByScoreDesc cannot be safely truncated per block.
	merged := util.MergeSlices(sets...)
	results := storage.ApplySearchHints(merged, hints)
	return streamBucketSearchResults(ctx, results, unfilteredTruncationWarning(merged, hints), srv.Send)
}

// unfilteredTruncationWarning returns a one-element slice with a
// human-readable warning when ApplySearchHints will clip the merged result
// set due to the request limit AND no filter is configured. Returns nil in
// every other case so the wire field is omitted.
//
// When a filter is configured we cannot tell, without a second pass over
// merged, how many values would survive the filter — and ApplySearchHints
// applies the limit to the post-filter count. To stay honest we suppress
// the warning whenever a filter is present (false-negative) rather than
// emit a misleading message based on the pre-filter count. With no filter,
// the merged count IS the post-filter count and the warning is accurate.
//
// TODO(streaming-search): if user feedback wants reliable truncation
// notification on filtered searches, push a truncation signal upstream into
// storage.ApplySearchHints (vendored Prometheus change) instead of running
// the filter twice here on the hot path.
func unfilteredTruncationWarning(merged []string, hints *storage.SearchHints) []string {
	if hints == nil || hints.Limit <= 0 || hints.Filter != nil || len(merged) <= hints.Limit {
		return nil
	}
	return []string{fmt.Sprintf("results truncated: %d values matched, returning first %d", len(merged), hints.Limit)}
}

// buildBucketSearchHints converts the wire filter, ordering, and limit into a
// storage.SearchHints. Validation errors (delegated to BuildFilter) are
// returned for the caller to map to gRPC InvalidArgument.
func buildBucketSearchHints(wf *storepb.SearchFilter, ord storepb.SearchOrdering, limit int64) (*storage.SearchHints, error) {
	filter, err := streaminglabelvalues.BuildFilter(storepbToParams(wf))
	if err != nil {
		return nil, err
	}
	return &storage.SearchHints{
		Filter:  filter,
		OrderBy: storepbToOrdering(ord),
		Limit:   int(limit),
	}, nil
}

// storepbToParams converts a wire SearchFilter into the streaminglabelvalues.Params
// shape that BuildFilter consumes. A nil input returns nil.
func storepbToParams(wf *storepb.SearchFilter) *streaminglabelvalues.Params {
	if wf == nil {
		return nil
	}
	p := &streaminglabelvalues.Params{
		Terms:         wf.Terms,
		CaseSensitive: !wf.CaseInsensitive,
		FuzzThreshold: int(wf.FuzzThreshold),
	}
	switch wf.FuzzAlg {
	case storepb.FUZZ_ALG_JARO_WINKLER:
		p.FuzzAlg = streaminglabelvalues.FuzzAlgJaroWinkler
	default:
		p.FuzzAlg = streaminglabelvalues.FuzzAlgSubsequence
	}
	return p
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

// streamBucketSearchResults sends results in batches of searchBatchSize via
// send. Warnings, if any, ride on the final batch (or are sent alone in a
// trailer batch with no results when the result set is empty). The stream
// context is checked before iteration starts and at each batch boundary so a
// cancelled client stops the loop promptly.
func streamBucketSearchResults(ctx context.Context, results []storage.SearchResult, warnings []string, send func(*storepb.SearchResultBatch) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	batch := &storepb.SearchResultBatch{Results: make([]storepb.SearchResultBatch_Result, 0, searchBatchSize)}
	for _, r := range results {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch.Results = append(batch.Results, storepb.SearchResultBatch_Result{Value: r.Value, Score: r.Score})
		if len(batch.Results) >= searchBatchSize {
			if err := send(batch); err != nil {
				return err
			}
			batch = &storepb.SearchResultBatch{Results: make([]storepb.SearchResultBatch_Result, 0, searchBatchSize)}
		}
	}
	batch.Warnings = warnings
	if len(batch.Results) > 0 || len(batch.Warnings) > 0 {
		return send(batch)
	}
	return nil
}
