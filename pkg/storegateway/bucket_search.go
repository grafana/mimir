// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/grafana/dskit/runutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util"
)

// SearchLabelNames implements the storegatewaypb.StoreGatewayServer interface.
// It is identical to LabelNames but applies the SearchFilter before the limit.
func (s *BucketStore) SearchLabelNames(ctx context.Context, req *storepb.SearchLabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	reqSeriesMatchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request labels matchers").Error())
	}

	var (
		stats          = newSafeQueryStats()
		resHints       = &storepb.LabelNamesResponseHints{}
		opaqueResHints = &hintspb.LabelNamesResponseHints{}
	)

	defer s.recordLabelNamesCallResult(stats)
	defer s.recordRequestAmbientTime(stats, time.Now())

	var reqBlockMatchers []*labels.Matcher
	if req.RequestHints != nil {
		reqBlockMatchers, err = storepb.MatchersToPromMatchers(req.RequestHints.BlockMatchers...)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request hints labels matchers").Error())
		}
	}

	defer s.recordBucketIndexDiscoveryDiff(ctx)

	g, gctx := errgroup.WithContext(ctx)

	searchFilter := buildSearchFilter(req.SearchFilter)

	var setsMtx sync.Mutex
	var sets [][]string
	var blocksQueriedByBlockMeta = make(map[blockQueriedMeta]int)
	seriesLimiter := s.seriesLimiterFactory(s.metrics.queriesDropped.WithLabelValues("series"))

	s.blockSet.filter(req.Start, req.End, reqBlockMatchers, func(b *bucketBlock) {
		resHints.AddQueriedBlock(b.meta.ULID)
		opaqueResHints.AddQueriedBlock(b.meta.ULID)

		blocksQueriedByBlockMeta[newBlockQueriedMeta(b.meta)]++

		// This indexReader is here to make sure its block is held open inside the goroutine below.
		indexr := b.indexReader(s.postingsStrategy)

		g.Go(func() error {
			defer runutil.CloseWithLogOnErr(s.logger, indexr, "label names")

			b.ensureIndexHeaderLoaded(gctx, stats)

			result, err := blockLabelNames(gctx, indexr, reqSeriesMatchers, seriesLimiter, s.maxSeriesPerBatch, s.logger, stats)
			if err != nil {
				return errors.Wrapf(err, "block %s", b.meta.ULID)
			}

			result = streaminglabelvalues.ApplyFilterChains(result, searchFilter)

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
			return nil, status.Error(codes.Canceled, err.Error())
		}

		return nil, status.Error(codes.Internal, err.Error())
	}

	stats.update(func(stats *queryStats) {
		stats.blocksQueried = len(sets)
		for sl, count := range blocksQueriedByBlockMeta {
			stats.blocksQueriedByBlockMeta[sl] = count
		}
	})

	anyHints, err := types.MarshalAny(opaqueResHints)
	if err != nil {
		return nil, status.Error(codes.Unknown, errors.Wrap(err, "marshal label names response hints").Error())
	}

	names := util.MergeSlices(sets...)
	names = applySearchSort(names, req.SearchFilter, searchFilter)
	if req.Limit > 0 && len(names) > int(req.Limit) {
		names = names[:req.Limit]
	}

	return &storepb.LabelNamesResponse{
		Names:         names,
		Hints:         anyHints,
		ResponseHints: resHints,
	}, nil
}

// SearchLabelValues implements the storegatewaypb.StoreGatewayServer interface.
// It is identical to LabelValues but applies the SearchFilter before the limit.
func (s *BucketStore) SearchLabelValues(ctx context.Context, req *storepb.SearchLabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	reqSeriesMatchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request labels matchers").Error())
	}

	stats := newSafeQueryStats()
	defer s.recordLabelValuesCallResult(stats)
	defer s.recordRequestAmbientTime(stats, time.Now())

	resHints := &storepb.LabelValuesResponseHints{}
	opaqueResHints := &hintspb.LabelValuesResponseHints{}

	g, gctx := errgroup.WithContext(ctx)

	var reqBlockMatchers []*labels.Matcher
	if req.RequestHints != nil {
		reqBlockMatchers, err = storepb.MatchersToPromMatchers(req.RequestHints.BlockMatchers...)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request hints labels matchers").Error())
		}
	}

	defer s.recordBucketIndexDiscoveryDiff(ctx)

	searchFilter := buildSearchFilter(req.SearchFilter)

	var setsMtx sync.Mutex
	var sets [][]string
	s.blockSet.filter(req.Start, req.End, reqBlockMatchers, func(b *bucketBlock) {
		resHints.AddQueriedBlock(b.meta.ULID)
		opaqueResHints.AddQueriedBlock(b.meta.ULID)

		// This index reader shouldn't be used for ExpandedPostings, since it doesn't have the correct strategy.
		// It's here only to make sure the block is held open inside the goroutine below.
		indexr := b.indexReader(nil)

		g.Go(func() error {
			defer runutil.CloseWithLogOnErr(b.logger, indexr, "close block index reader")

			b.ensureIndexHeaderLoaded(gctx, stats)

			result, err := blockLabelValues(gctx, b, s.postingsStrategy, s.maxSeriesPerBatch, req.Label, reqSeriesMatchers, s.logger, stats)
			if err != nil {
				return errors.Wrapf(err, "block %s", b.meta.ULID)
			}

			result = streaminglabelvalues.ApplyFilterChains(result, searchFilter)

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
			return nil, status.Error(codes.Canceled, err.Error())
		}

		return nil, status.Error(codes.Internal, err.Error())
	}

	anyHints, err := types.MarshalAny(opaqueResHints)
	if err != nil {
		return nil, status.Error(codes.Unknown, errors.Wrap(err, "marshal label values response hints").Error())
	}

	values := util.MergeSlices(sets...)
	values = applySearchSort(values, req.SearchFilter, searchFilter)
	if req.Limit > 0 && len(values) > int(req.Limit) {
		values = values[:req.Limit]
	}

	return &storepb.LabelValuesResponse{
		Values:        values,
		Hints:         anyHints,
		ResponseHints: resHints,
	}, nil
}

// buildSearchFilter converts a proto SearchFilter into a FilterChains ready for concurrent use.
// Returns nil if sf is nil or has no search terms.
func buildSearchFilter(sf *storepb.SearchFilter) *streaminglabelvalues.FilterChains {
	if sf == nil || len(sf.SearchTerms) == 0 {
		return nil
	}
	op := streaminglabelvalues.Or
	if sf.Operator == storepb.AND {
		op = streaminglabelvalues.And
	}
	return streaminglabelvalues.BuildFilterChains(sf.SearchTerms, sf.CaseInsensitive, op, sf.FuzzThreshold)
}

// applySearchSort sorts values according to the sort_by/sort_order fields in sf.
// For alpha-asc (default from MergeSlices) no reordering is needed.
// For alpha-desc the slice is reversed in-place.
// For score sort, ScoreAndSort is called using the pre-built filter.
// If sf is nil or sort_by is 0, values are returned unchanged.
func applySearchSort(values []string, sf *storepb.SearchFilter, filter *streaminglabelvalues.FilterChains) []string {
	if sf == nil || sf.SortBy == storepb.SORT_BY_NONE {
		return values
	}
	switch sf.SortBy {
	case storepb.SORT_BY_ALPHA:
		if sf.SortOrder == storepb.SORT_ORDER_DESC {
			slices.Reverse(values)
		}
	case storepb.SORT_BY_SCORE: // filter provides scoring; falls back to unchanged if no filter
		values = streaminglabelvalues.ScoreAndSort(values, filter, sf.SortOrder != storepb.SORT_ORDER_ASC)
	}
	return values
}
