// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// SortInRangeQueryWarning is a wrapper that delegates everything to its inner operator but
// emits a "sort is ineffective for range queries" warning annotation from Stats.
// It is used by the sort/sort_desc/sort_by_label/sort_by_label_desc factories when the query
// is a range query.
type SortInRangeQueryWarning struct {
	Inner              types.InstantVectorOperator
	expressionPosition posrange.PositionRange
}

var _ types.InstantVectorOperator = &SortInRangeQueryWarning{}

func NewSortInRangeQueryWarning(inner types.InstantVectorOperator, expressionPosition posrange.PositionRange) *SortInRangeQueryWarning {
	return &SortInRangeQueryWarning{
		Inner:              inner,
		expressionPosition: expressionPosition,
	}
}

func (s *SortInRangeQueryWarning) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	return s.Inner.SeriesMetadata(ctx, matchers)
}

func (s *SortInRangeQueryWarning) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	return s.Inner.NextSeries(ctx)
}

func (s *SortInRangeQueryWarning) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *SortInRangeQueryWarning) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return s.Inner.Prepare(ctx, params)
}

func (s *SortInRangeQueryWarning) AfterPrepare(ctx context.Context) error {
	return s.Inner.AfterPrepare(ctx)
}

func (s *SortInRangeQueryWarning) FinishedReading(ctx context.Context) error {
	return s.Inner.FinishedReading(ctx)
}

func (s *SortInRangeQueryWarning) Stats(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	stats, childAnnos, err := s.Inner.Stats(ctx)
	if err != nil {
		return nil, nil, err
	}

	childAnnos.Add(annotations.NewSortInRangeQueryWarning(s.expressionPosition))

	return stats, childAnnos, nil
}

func (s *SortInRangeQueryWarning) Close() {
	s.Inner.Close()
}
