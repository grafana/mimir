// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"slices"

	"github.com/facette/natsort"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type SortByLabel struct {
	inner                    types.InstantVectorOperator
	descending               bool
	labels                   []string
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	expressionPosition       posrange.PositionRange

	seriesIndex       int
	sortedMetadata    []types.SeriesMetadata
	originalPositions map[uint64]int
	buffer            *operators.InstantVectorOperatorBuffer
}

var _ types.InstantVectorOperator = &SortByLabel{}

func NewSortByLabel(
	inner types.InstantVectorOperator,
	descending bool,
	labels []string,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,

) *SortByLabel {
	return &SortByLabel{
		inner:                    inner,
		descending:               descending,
		labels:                   labels,
		memoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
	}
}

func (s *SortByLabel) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	innerMetadata, err := s.inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	// Create a mapping of each series to its original position before sorting.
	s.originalPositions = make(map[uint64]int, len(innerMetadata))
	for i, series := range innerMetadata {
		s.originalPositions[series.Labels.Hash()] = i
	}

	slices.SortFunc(innerMetadata, compareForLabels(s.labels, s.descending))

	s.sortedMetadata = innerMetadata
	s.buffer = operators.NewInstantVectorOperatorBuffer(s.inner, nil, len(innerMetadata), s.memoryConsumptionTracker)

	return innerMetadata, nil
}

func compareForLabels(lbls []string, descending bool) func(a, b types.SeriesMetadata) int {
	f := func(a, b types.SeriesMetadata) int {
		for _, lbl := range lbls {
			val1 := a.Labels.Get(lbl)
			val2 := b.Labels.Get(lbl)
			if val1 == val2 {
				continue
			}

			if natsort.Compare(val1, val2) {
				return -1
			}

			return +1
		}

		// If all labels provided as arguments were equal, sort by the full label set. This ensures a
		// consistent ordering and matches the behavior of the Prometheus engine.
		return labels.Compare(a.Labels, b.Labels)
	}

	if descending {
		return func(a, b types.SeriesMetadata) int {
			return -f(a, b)
		}
	}

	return f
}

func (s *SortByLabel) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if s.seriesIndex >= len(s.sortedMetadata) {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	// Get metadata for the series to return based on its sorted order
	meta := s.sortedMetadata[s.seriesIndex]
	// Get the original position in the stream of series so that we can
	// request it from buffer which buffers series from the inner operator
	// while they aren't needed.
	orig := s.originalPositions[meta.Labels.Hash()]

	data, err := s.buffer.GetSeries(ctx, []int{orig})
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	s.seriesIndex++
	return data[0], nil
}

func (s *SortByLabel) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *SortByLabel) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return s.inner.Prepare(ctx, params)
}

func (s *SortByLabel) Close() {
	s.inner.Close()
	s.buffer.Close()
}
