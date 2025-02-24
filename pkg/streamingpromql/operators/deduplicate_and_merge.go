// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"
	"errors"
	"slices"

	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

var errVectorContainsMetricsWithSameLabels = errors.New("vector cannot contain metrics with the same labelset")

type DeduplicateAndMerge struct {
	Inner                    types.InstantVectorOperator
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	// If true, there are definitely no duplicate series from the inner operator, so we can just
	// return them as-is.
	passthrough bool

	// If not in passthrough mode:
	groups [][]int // Each group contains the indices of the series from the inner operator that contribute to the final output series.
	buffer *InstantVectorOperatorBuffer
}

var _ types.InstantVectorOperator = &DeduplicateAndMerge{}

func NewDeduplicateAndMerge(inner types.InstantVectorOperator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) *DeduplicateAndMerge {
	return &DeduplicateAndMerge{Inner: inner, MemoryConsumptionTracker: memoryConsumptionTracker}
}

func (d *DeduplicateAndMerge) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	innerMetadata, err := d.Inner.SeriesMetadata(ctx)

	if err != nil {
		return nil, err
	}

	if !types.HasDuplicateSeries(innerMetadata) {
		// Common case: there are definitely no duplicate series, so we don't need to do anything special in this operator.
		// We can just return series as-is from the inner operator.
		d.passthrough = true

		return innerMetadata, nil
	}

	// We might have duplicates (or HasDuplicateSeries hit a hash collision). Determine the merged output series.
	groups, outputMetadata := d.computeOutputSeriesGroups(innerMetadata)
	d.groups = groups
	types.PutSeriesMetadataSlice(innerMetadata)

	d.buffer = NewInstantVectorOperatorBuffer(d.Inner, nil, d.MemoryConsumptionTracker)

	return outputMetadata, nil
}

func (d *DeduplicateAndMerge) computeOutputSeriesGroups(innerMetadata []types.SeriesMetadata) ([][]int, []types.SeriesMetadata) {
	// Why use a string, rather than the labels hash as a key here? This avoids any issues with hash collisions.
	outputGroupMap := map[string][]int{}

	// Why 1024 bytes? It's what labels.Labels.String() uses as a buffer size, so we use that as a sensible starting point too.
	labelBytes := make([]byte, 0, 1024)

	for seriesIdx, series := range innerMetadata {
		labelBytes = series.Labels.Bytes(labelBytes)
		g, groupExists := outputGroupMap[string(labelBytes)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if !groupExists {
			outputGroupMap[string(labelBytes)] = []int{seriesIdx}
		} else {
			outputGroupMap[string(labelBytes)] = append(g, seriesIdx)
		}
	}

	outputGroups := make([][]int, 0, len(outputGroupMap))
	for _, group := range outputGroupMap {
		outputGroups = append(outputGroups, group)
	}

	// Sort the groups so that the groups that can be completed earliest are returned earliest.
	slices.SortFunc(outputGroups, func(a, b []int) int {
		aLastIndex := a[len(a)-1]
		bLastIndex := b[len(b)-1]

		return aLastIndex - bLastIndex
	})

	// Now that we know which series we'll return, and in what order, create the list of output series.
	outputMetadata := types.GetSeriesMetadataSlice(len(outputGroups))

	for _, group := range outputGroups {
		outputMetadata = append(outputMetadata, innerMetadata[group[0]])
	}

	return outputGroups, outputMetadata
}

func (d *DeduplicateAndMerge) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if d.passthrough {
		return d.Inner.NextSeries(ctx)
	}

	if len(d.groups) == 0 {
		// We are done.
		return types.InstantVectorSeriesData{}, types.EOS
	}

	group := d.groups[0]
	d.groups = d.groups[1:]

	series, err := d.buffer.GetSeries(ctx, group)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	merged, conflict, err := MergeSeries(series, group, d.MemoryConsumptionTracker)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if conflict != nil {
		return types.InstantVectorSeriesData{}, errVectorContainsMetricsWithSameLabels
	}

	return merged, nil
}

func (d *DeduplicateAndMerge) ExpressionPosition() posrange.PositionRange {
	return d.Inner.ExpressionPosition()
}

func (d *DeduplicateAndMerge) Close() {
	d.Inner.Close()

	if d.buffer != nil {
		d.buffer.Close()
	}
}
