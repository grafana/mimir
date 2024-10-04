// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// AndBinaryOperation represents a logical 'and' between two vectors.
type AndBinaryOperation struct {
	Left                     types.InstantVectorOperator
	Right                    types.InstantVectorOperator
	VectorMatching           parser.VectorMatching
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	timeRange            types.QueryTimeRange
	expressionPosition   posrange.PositionRange
	leftSeriesGroups     []*andGroup
	rightSeriesGroups    []*andGroup
	nextRightSeriesIndex int
}

var _ types.InstantVectorOperator = &AndBinaryOperation{}

func NewAndBinaryOperation(
	left types.InstantVectorOperator,
	right types.InstantVectorOperator,
	vectorMatching parser.VectorMatching,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	timeRange types.QueryTimeRange,
	expressionPosition posrange.PositionRange,
) *AndBinaryOperation {
	return &AndBinaryOperation{
		Left:                     left,
		Right:                    right,
		VectorMatching:           vectorMatching,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		timeRange:                timeRange,
		expressionPosition:       expressionPosition,
	}
}

func (a *AndBinaryOperation) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	leftMetadata, err := a.Left.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	defer types.PutSeriesMetadataSlice(leftMetadata)

	if len(leftMetadata) == 0 {
		// We can't produce any series, we are done.
		return nil, nil
	}

	rightMetadata, err := a.Right.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	defer types.PutSeriesMetadataSlice(rightMetadata)

	if len(rightMetadata) == 0 {
		// We can't produce any series, we are done.
		return nil, nil
	}

	groupMap := map[string]*andGroup{}
	groupKeyFunc := vectorMatchingGroupKeyFunc(a.VectorMatching)

	// Iterate through the left-hand series, and create groups for each based on the matching labels.
	a.leftSeriesGroups = make([]*andGroup, 0, len(leftMetadata))

	for _, s := range leftMetadata {
		groupKey := groupKeyFunc(s.Labels)
		group, exists := groupMap[string(groupKey)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if !exists {
			group = &andGroup{lastRightSeriesIndex: -1}
			groupMap[string(groupKey)] = group
		}

		group.leftSeriesCount++
		a.leftSeriesGroups = append(a.leftSeriesGroups, group)
	}

	// Iterate through the right-hand series, and find groups for each based on the matching labels.
	a.rightSeriesGroups = make([]*andGroup, 0, len(rightMetadata))
	outputSeriesCount := 0

	for idx, s := range rightMetadata {
		groupKey := groupKeyFunc(s.Labels)
		group, exists := groupMap[string(groupKey)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if exists {
			if group.lastRightSeriesIndex == -1 {
				// First time a right-hand series has matched this group.
				// We'll return all left-hand series matching this group, so add the count to the running total of output series.
				outputSeriesCount += group.leftSeriesCount
			}

			group.lastRightSeriesIndex = idx
		}

		// Even if there is no matching group, we want to store a nil value here so we know to throw the series away when we read it later.
		a.rightSeriesGroups = append(a.rightSeriesGroups, group)
	}

	// Iterate through the left-hand series again, and build the list of output series based on those that matched at least one series on the right.
	outputSeries := types.GetSeriesMetadataSlice(outputSeriesCount)

	for seriesIdx, group := range a.leftSeriesGroups {
		if group.lastRightSeriesIndex == -1 {
			// This series doesn't match any series from the right side.
			// Discard the group.
			a.leftSeriesGroups[seriesIdx] = nil
		} else {
			outputSeries = append(outputSeries, leftMetadata[seriesIdx])
		}
	}

	return outputSeries, nil
}

func (a *AndBinaryOperation) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	for {
		if len(a.leftSeriesGroups) == 0 {
			// No more series to return.
			return types.InstantVectorSeriesData{}, types.EOS
		}

		thisSeriesGroup := a.leftSeriesGroups[0]
		a.leftSeriesGroups = a.leftSeriesGroups[1:]

		if thisSeriesGroup == nil {
			// This series from the left side has no matching series on the right side.
			// Read it, discard it, and move on to the next series.
			d, err := a.Left.NextSeries(ctx)
			if err != nil {
				return types.InstantVectorSeriesData{}, err
			}

			types.PutInstantVectorSeriesData(d, a.MemoryConsumptionTracker)
			continue
		}

		if err := a.readRightSideUntilGroupComplete(ctx, thisSeriesGroup); err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		// Only read the left series after we've finished reading right series, to minimise the number of series we're
		// holding in memory at once.
		// We deliberately don't return this data to the pool, as FilterLeftSeries reuses the slices.
		originalData, err := a.Left.NextSeries(ctx)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		filteredData, err := thisSeriesGroup.FilterLeftSeries(originalData, a.MemoryConsumptionTracker, a.timeRange)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		thisSeriesGroup.leftSeriesCount--

		if thisSeriesGroup.leftSeriesCount == 0 {
			// This is the last series for this group, return it to the pool.
			thisSeriesGroup.Close(a.MemoryConsumptionTracker)
		}

		return filteredData, nil
	}
}

// readRightSideUntilGroupComplete reads series from the right-hand side until all series for desiredGroup have been read.
func (a *AndBinaryOperation) readRightSideUntilGroupComplete(ctx context.Context, desiredGroup *andGroup) error {
	for a.nextRightSeriesIndex <= desiredGroup.lastRightSeriesIndex {
		groupForRightSeries := a.rightSeriesGroups[0]
		a.rightSeriesGroups = a.rightSeriesGroups[1:]

		data, err := a.Right.NextSeries(ctx)
		if err != nil {
			return err
		}

		if groupForRightSeries != nil {
			if err := groupForRightSeries.AccumulateRightSeriesPresence(data, a.MemoryConsumptionTracker, a.timeRange); err != nil {
				return err
			}
		}

		types.PutInstantVectorSeriesData(data, a.MemoryConsumptionTracker)
		a.nextRightSeriesIndex++
	}

	return nil
}

func (a *AndBinaryOperation) ExpressionPosition() posrange.PositionRange {
	return a.expressionPosition
}

func (a *AndBinaryOperation) Close() {
	a.Left.Close()
	a.Right.Close()
}

type andGroup struct {
	leftSeriesCount      int
	lastRightSeriesIndex int
	rightSamplePresence  []bool // FIXME: this would be a good candidate for a bitmap type
}

// AccumulateRightSeriesPresence records the presence of samples on the right-hand side.
func (g *andGroup) AccumulateRightSeriesPresence(data types.InstantVectorSeriesData, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, timeRange types.QueryTimeRange) error {
	if g.rightSamplePresence == nil {
		var err error
		g.rightSamplePresence, err = types.BoolSlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)

		if err != nil {
			return err
		}

		g.rightSamplePresence = g.rightSamplePresence[:timeRange.StepCount]
	}

	for _, p := range data.Floats {
		g.rightSamplePresence[timeRange.PointIndex(p.T)] = true
	}

	for _, p := range data.Histograms {
		g.rightSamplePresence[timeRange.PointIndex(p.T)] = true
	}

	return nil
}

// FilterLeftSeries returns leftData filtered based on samples seen for the right-hand side.
// The return value reuses the slices from leftData, and returns any unused slices to the pool.
func (g *andGroup) FilterLeftSeries(leftData types.InstantVectorSeriesData, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, timeRange types.QueryTimeRange) (types.InstantVectorSeriesData, error) {
	filteredData := types.InstantVectorSeriesData{}
	nextOutputFloatIndex := 0

	for _, p := range leftData.Floats {
		if !g.rightSamplePresence[timeRange.PointIndex(p.T)] {
			continue
		}

		leftData.Floats[nextOutputFloatIndex] = p
		nextOutputFloatIndex++
	}

	if nextOutputFloatIndex > 0 {
		// We have at least one output float point to return.
		filteredData.Floats = leftData.Floats[:nextOutputFloatIndex]
	} else {
		// We don't have any float points to return, return the original slice to the pool.
		types.FPointSlicePool.Put(leftData.Floats, memoryConsumptionTracker)
	}

	nextOutputHistogramIndex := 0

	for idx, p := range leftData.Histograms {
		if !g.rightSamplePresence[timeRange.PointIndex(p.T)] {
			continue
		}

		leftData.Histograms[nextOutputHistogramIndex] = p

		if idx > nextOutputHistogramIndex {
			// Remove the histogram from the original point to ensure that it's not mutated unexpectedly when the HPoint slice is reused.
			leftData.Histograms[idx].H = nil
		}

		nextOutputHistogramIndex++
	}

	if nextOutputHistogramIndex > 0 {
		// We have at least one output histogram point to return.
		filteredData.Histograms = leftData.Histograms[:nextOutputHistogramIndex]
	} else {
		// We don't have any histogram points to return, return the original slice to the pool.
		types.HPointSlicePool.Put(leftData.Histograms, memoryConsumptionTracker)
	}

	return filteredData, nil
}

func (g *andGroup) Close(memoryConsumptionTracker *limiting.MemoryConsumptionTracker) {
	types.BoolSlicePool.Put(g.rightSamplePresence, memoryConsumptionTracker)
}
