// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// AndUnlessBinaryOperation represents a logical 'and' or 'unless' between two vectors.
type AndUnlessBinaryOperation struct {
	Left                     types.InstantVectorOperator
	Right                    types.InstantVectorOperator
	VectorMatching           parser.VectorMatching
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
	IsUnless                 bool // If true, this operator represents an 'unless', if false, this operator represents an 'and'

	timeRange                  types.QueryTimeRange
	expressionPosition         posrange.PositionRange
	leftSeriesGroups           []*andGroup
	rightSeriesGroups          []*andGroup
	nextRightSeriesIndex       int
	lastRightSeriesIndexToRead int
	nextLeftSeriesIndex        int
	lastLeftSeriesIndexToRead  int
}

var _ types.InstantVectorOperator = &AndUnlessBinaryOperation{}

func NewAndUnlessBinaryOperation(
	left types.InstantVectorOperator,
	right types.InstantVectorOperator,
	vectorMatching parser.VectorMatching,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	isUnless bool,
	timeRange types.QueryTimeRange,
	expressionPosition posrange.PositionRange,
) *AndUnlessBinaryOperation {
	return &AndUnlessBinaryOperation{
		Left:                     left,
		Right:                    right,
		VectorMatching:           vectorMatching,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		IsUnless:                 isUnless,
		timeRange:                timeRange,
		expressionPosition:       expressionPosition,

		lastLeftSeriesIndexToRead:  -1,
		lastRightSeriesIndexToRead: -1,
	}
}

func (a *AndUnlessBinaryOperation) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	series, err := a.computeSeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	if a.lastLeftSeriesIndexToRead == -1 {
		// We're not going to read anything from the left side, close it now.
		if err := a.Left.Finalize(ctx); err != nil {
			return nil, err
		}

		a.Left.Close()
	}

	if a.lastRightSeriesIndexToRead == -1 {
		// We're not going to read anything from the right side, close it now.
		if err := a.Right.Finalize(ctx); err != nil {
			return nil, err
		}

		a.Right.Close()
	}

	return series, nil
}

func (a *AndUnlessBinaryOperation) computeSeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	leftMetadata, err := a.Left.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	if len(leftMetadata) == 0 {
		// We can't produce any series, we are done.
		types.SeriesMetadataSlicePool.Put(&leftMetadata, a.MemoryConsumptionTracker)
		return nil, nil
	}

	rightMetadata, err := a.Right.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	defer types.SeriesMetadataSlicePool.Put(&rightMetadata, a.MemoryConsumptionTracker)

	if len(rightMetadata) == 0 && !a.IsUnless {
		// We can't produce any series, we are done.
		types.SeriesMetadataSlicePool.Put(&leftMetadata, a.MemoryConsumptionTracker)
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

	for idx, s := range rightMetadata {
		groupKey := groupKeyFunc(s.Labels)
		group, exists := groupMap[string(groupKey)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if exists {
			group.lastRightSeriesIndex = idx
			a.lastRightSeriesIndexToRead = idx
		}

		// Even if there is no matching group, we want to store a nil value here so we know to throw the series away when we read it later.
		a.rightSeriesGroups = append(a.rightSeriesGroups, group)
	}

	if a.IsUnless {
		return a.computeUnlessSeriesMetadata(leftMetadata), nil
	}
	return a.computeAndSeriesMetadata(leftMetadata), nil
}

func (a *AndUnlessBinaryOperation) computeAndSeriesMetadata(leftMetadata []types.SeriesMetadata) []types.SeriesMetadata {
	// Iterate through the left-hand series again, and build the list of output series based on those that matched at least one series on the right.
	// It's safe to reuse the left metadata slice as we'll return series in the same order, and only ever return fewer series than the left operator produces.
	nextOutputSeriesIndex := 0

	for seriesIdx, group := range a.leftSeriesGroups {
		if group.lastRightSeriesIndex == -1 {
			// This series doesn't match any series from the right side.
			// Discard the group.
			a.leftSeriesGroups[seriesIdx] = nil
			a.MemoryConsumptionTracker.DecreaseMemoryConsumptionForLabels(leftMetadata[seriesIdx].Labels)
		} else {
			leftMetadata[nextOutputSeriesIndex] = leftMetadata[seriesIdx]
			nextOutputSeriesIndex++
			a.lastLeftSeriesIndexToRead = seriesIdx
		}
	}

	// Clear up labels that we don't need anymore.
	clear(leftMetadata[nextOutputSeriesIndex:])

	return leftMetadata[:nextOutputSeriesIndex]
}

func (a *AndUnlessBinaryOperation) computeUnlessSeriesMetadata(leftMetadata []types.SeriesMetadata) []types.SeriesMetadata {
	// Iterate through the left-hand series again, and remove references to any groups that don't match any series from the right side:
	// we can just return the left-hand series as-is if it does not match anything from the right side.
	for seriesIdx, group := range a.leftSeriesGroups {
		if group.lastRightSeriesIndex == -1 {
			a.leftSeriesGroups[seriesIdx] = nil
		}
	}

	a.lastLeftSeriesIndexToRead = len(leftMetadata) - 1

	return leftMetadata
}

func (a *AndUnlessBinaryOperation) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	d, err := a.computeNextSeries(ctx)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	// If we're done reading the left side, close it so it can release any resources as early as possible.
	// We do the same thing for the right side in readRightSideUntilGroupComplete.
	if a.nextLeftSeriesIndex > a.lastLeftSeriesIndexToRead {
		if err := a.Left.Finalize(ctx); err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		a.Left.Close()
	}

	return d, nil
}

func (a *AndUnlessBinaryOperation) computeNextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	for {
		if len(a.leftSeriesGroups) == 0 {
			// No more series to return.
			return types.InstantVectorSeriesData{}, types.EOS
		}

		thisSeriesGroup := a.leftSeriesGroups[0]
		a.leftSeriesGroups = a.leftSeriesGroups[1:]
		a.nextLeftSeriesIndex++

		if thisSeriesGroup == nil {
			// This series from the left side has no matching series on the right side.
			d, err := a.Left.NextSeries(ctx)
			if err != nil {
				return types.InstantVectorSeriesData{}, err
			}

			if a.IsUnless {
				// If this is an 'unless' operation, we should return the series as-is, as this series can't be filtered by anything on the right.
				return d, nil
			}

			// If this is an 'and' operation, we should discard it and move on to the next series, as this series can't contribute to the result.
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

		filteredData, err := thisSeriesGroup.FilterLeftSeries(originalData, a.MemoryConsumptionTracker, a.timeRange, a.IsUnless)
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
func (a *AndUnlessBinaryOperation) readRightSideUntilGroupComplete(ctx context.Context, desiredGroup *andGroup) error {
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

	// If we're done reading the right side, close it so it can release any resources as early as possible.
	if a.nextRightSeriesIndex > a.lastRightSeriesIndexToRead {
		if err := a.Right.Finalize(ctx); err != nil {
			return err
		}

		a.Right.Close()
	}

	return nil
}

func (a *AndUnlessBinaryOperation) ExpressionPosition() posrange.PositionRange {
	return a.expressionPosition
}

func (a *AndUnlessBinaryOperation) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if err := a.Left.Prepare(ctx, params); err != nil {
		return err
	}

	return a.Right.Prepare(ctx, params)
}

func (a *AndUnlessBinaryOperation) AfterPrepare(ctx context.Context) error {
	if err := a.Left.AfterPrepare(ctx); err != nil {
		return err
	}

	return a.Right.AfterPrepare(ctx)
}

func (a *AndUnlessBinaryOperation) Finalize(ctx context.Context) error {
	if err := a.Left.Finalize(ctx); err != nil {
		return err
	}

	return a.Right.Finalize(ctx)
}

func (a *AndUnlessBinaryOperation) Close() {
	a.Left.Close()
	a.Right.Close()

	for _, group := range a.leftSeriesGroups {
		if group == nil {
			continue
		}

		group.Close(a.MemoryConsumptionTracker)
	}

	a.leftSeriesGroups = nil

	// We don't need to explicitly close any groups in rightSeriesGroups, as they would have been closed above.
	a.rightSeriesGroups = nil
}

type andGroup struct {
	leftSeriesCount      int
	lastRightSeriesIndex int
	rightSamplePresence  []bool // FIXME: this would be a good candidate for a bitmap type
}

// AccumulateRightSeriesPresence records the presence of samples on the right-hand side.
func (g *andGroup) AccumulateRightSeriesPresence(data types.InstantVectorSeriesData, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, timeRange types.QueryTimeRange) error {
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
func (g *andGroup) FilterLeftSeries(leftData types.InstantVectorSeriesData, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, timeRange types.QueryTimeRange, isUnless bool) (types.InstantVectorSeriesData, error) {
	return filterSeries(leftData, g.rightSamplePresence, !isUnless, memoryConsumptionTracker, timeRange)
}

func (g *andGroup) Close(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	types.BoolSlicePool.Put(&g.rightSamplePresence, memoryConsumptionTracker)
}
