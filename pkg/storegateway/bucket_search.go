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

	merged := util.MergeSlices(sets...)
	results := storage.ApplySearchHints(merged, hints)
	return streamBucketSearchResults(results, srv.Send)
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

	merged := util.MergeSlices(sets...)
	results := storage.ApplySearchHints(merged, hints)
	return streamBucketSearchResults(results, srv.Send)
}

// buildBucketSearchHints converts the wire filter, ordering, and limit into a
// storage.SearchHints. Validation errors are returned for the caller to map to
// gRPC InvalidArgument.
func buildBucketSearchHints(wf *storepb.SearchFilter, ord storepb.SearchOrdering, limit int64) (*storage.SearchHints, error) {
	params := storepbToParams(wf)
	if err := params.Validate(); err != nil {
		return nil, err
	}
	filter, err := streaminglabelvalues.BuildFilter(params)
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
		CaseSensitive: wf.CaseSensitive,
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

// streamBucketSearchResults sends results in batches of searchBatchSize via send.
func streamBucketSearchResults(results []storage.SearchResult, send func(*storepb.SearchResultBatch) error) error {
	batch := &storepb.SearchResultBatch{Results: make([]storepb.SearchResultBatch_Result, 0, searchBatchSize)}
	for _, r := range results {
		batch.Results = append(batch.Results, storepb.SearchResultBatch_Result{Value: r.Value, Score: r.Score})
		if len(batch.Results) >= searchBatchSize {
			if err := send(batch); err != nil {
				return err
			}
			batch = &storepb.SearchResultBatch{Results: make([]storepb.SearchResultBatch_Result, 0, searchBatchSize)}
		}
	}
	if len(batch.Results) > 0 {
		return send(batch)
	}
	return nil
}
