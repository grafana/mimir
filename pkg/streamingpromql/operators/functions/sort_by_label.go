// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"sort"

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

	seriesIndex     int
	originalIndexes []int
	buffer          *operators.InstantVectorOperatorBuffer
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

func (s *SortByLabel) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	innerMetadata, err := s.inner.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	// Store the original indexes of each piece of metadata and then sort them alongside
	// the metadata. This allows us to return series in sorted order when requesting them
	// from the inner operator by their original index.
	indexes, err := types.IntSlicePool.Get(len(innerMetadata), s.memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(innerMetadata); i++ {
		indexes = append(indexes, i)
	}

	sort.Sort(&sortableMetadata{
		metadata:   innerMetadata,
		indexes:    indexes,
		labels:     s.labels,
		descending: s.descending,
	})

	s.originalIndexes = indexes
	s.buffer = operators.NewInstantVectorOperatorBuffer(s.inner, nil, len(innerMetadata), s.memoryConsumptionTracker)

	return innerMetadata, nil
}

type sortableMetadata struct {
	metadata   []types.SeriesMetadata
	indexes    []int
	labels     []string
	descending bool
}

func (s *sortableMetadata) Len() int {
	return len(s.metadata)
}

func (s *sortableMetadata) Swap(i, j int) {
	s.metadata[i], s.metadata[j] = s.metadata[j], s.metadata[i]
	s.indexes[i], s.indexes[j] = s.indexes[j], s.indexes[i]
}

func (s *sortableMetadata) Less(i, j int) bool {
	res := s.lessInner(i, j)
	if s.descending {
		return !res
	}
	return res
}

func (s *sortableMetadata) lessInner(i, j int) bool {
	a := s.metadata[i]
	b := s.metadata[j]
	for _, lbl := range s.labels {
		val1 := a.Labels.Get(lbl)
		val2 := b.Labels.Get(lbl)
		if val1 == val2 {
			continue
		}

		if natsort.Compare(val1, val2) {
			return true
		}

		return false
	}

	// If all labels provided as arguments were equal, sort by the full label set. This ensures a
	// consistent ordering and matches the behavior of the Prometheus engine.
	return labels.Compare(a.Labels, b.Labels) < 0
}

func (s *SortByLabel) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if s.seriesIndex >= len(s.originalIndexes) {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	// Get the original position in the stream of series so that we can request it from
	// the buffer which buffers series from the inner operator while they aren't needed.
	originalIndex := s.originalIndexes[s.seriesIndex]

	data, err := s.buffer.GetSeries(ctx, []int{originalIndex})
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

func (s *SortByLabel) AfterPrepare(ctx context.Context) error {
	return s.inner.AfterPrepare(ctx)
}

func (s *SortByLabel) Finalize(ctx context.Context) error {
	return s.inner.Finalize(ctx)
}

func (s *SortByLabel) Close() {
	if s.buffer != nil {
		// Closing the buffer also closes its source operator which is our `inner`.
		s.buffer.Close()
	}
	// If the buffer hasn't been initialized yet, we still need to close `inner`
	// ourselves. It's safe to call Close on operators multiple times.
	s.inner.Close()
	types.IntSlicePool.Put(&s.originalIndexes, s.memoryConsumptionTracker)
}
