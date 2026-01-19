// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// OrBinaryOperation represents a logical 'or' between two vectors.
type OrBinaryOperation struct {
	Left                     types.InstantVectorOperator
	Right                    types.InstantVectorOperator
	VectorMatching           parser.VectorMatching
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	timeRange          types.QueryTimeRange
	expressionPosition posrange.PositionRange

	// If nextSeriesIsFromLeft is true, this operator will next return leftSeriesCount[0] series from the left side before
	// switching to returning series from the right side, and vice versa.
	//
	// For example, if nextSeriesIsFromLeft is true, and leftSeriesCount is [3, 5, 2], and rightSeriesCount is [1, 4], then
	// this operator will first return three series from the left, then 1 from the right, then 5 from the left, then 4 from
	// the right and finally 2 from the left.
	nextSeriesIsFromLeft bool
	leftSeriesCount      []int
	rightSeriesCount     []int

	// These will both be nil if we only have series from one side (ie. there are series on the left but not the right, or vice versa).
	leftSeriesGroups  []*orGroup
	rightSeriesGroups []*orGroup
}

var _ types.InstantVectorOperator = &OrBinaryOperation{}

func NewOrBinaryOperation(
	left types.InstantVectorOperator,
	right types.InstantVectorOperator,
	vectorMatching parser.VectorMatching,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	timeRange types.QueryTimeRange,
	expressionPosition posrange.PositionRange,
) types.InstantVectorOperator {
	return &OrBinaryOperation{
		Left:                     left,
		Right:                    right,
		VectorMatching:           vectorMatching,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		timeRange:                timeRange,
		expressionPosition:       expressionPosition,
	}
}

func (o *OrBinaryOperation) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	leftMetadata, err := o.Left.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	rightMetadata, err := o.Right.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	if len(leftMetadata) == 0 && len(rightMetadata) == 0 {
		// Nothing to return.
		types.SeriesMetadataSlicePool.Put(&leftMetadata, o.MemoryConsumptionTracker)
		types.SeriesMetadataSlicePool.Put(&rightMetadata, o.MemoryConsumptionTracker)

		if err := o.Left.Finalize(ctx); err != nil {
			return nil, err
		}

		o.Left.Close()

		if err := o.Right.Finalize(ctx); err != nil {
			return nil, err
		}

		o.Right.Close()

		return nil, nil
	}

	if len(leftMetadata) == 0 {
		// We can just return everything from the right side.
		o.nextSeriesIsFromLeft = false
		o.rightSeriesCount = []int{len(rightMetadata)}
		types.SeriesMetadataSlicePool.Put(&leftMetadata, o.MemoryConsumptionTracker)

		if err := o.Left.Finalize(ctx); err != nil {
			return nil, err
		}

		o.Left.Close()

		return rightMetadata, nil
	}

	if len(rightMetadata) == 0 {
		// We can just return everything from the left side.
		o.nextSeriesIsFromLeft = true
		o.leftSeriesCount = []int{len(leftMetadata)}
		types.SeriesMetadataSlicePool.Put(&rightMetadata, o.MemoryConsumptionTracker)

		if err := o.Right.Finalize(ctx); err != nil {
			return nil, err
		}

		o.Right.Close()

		return leftMetadata, nil
	}

	defer types.SeriesMetadataSlicePool.Put(&leftMetadata, o.MemoryConsumptionTracker)
	defer types.SeriesMetadataSlicePool.Put(&rightMetadata, o.MemoryConsumptionTracker)

	o.computeGroups(leftMetadata, rightMetadata)

	return o.computeSeriesOutputOrder(leftMetadata, rightMetadata)
}

func (o *OrBinaryOperation) computeGroups(leftMetadata []types.SeriesMetadata, rightMetadata []types.SeriesMetadata) {
	groupMap := map[string]*orGroup{}
	groupKeyFunc := vectorMatchingGroupKeyFunc(o.VectorMatching)

	// Iterate through the right-hand series, and create groups for each based on the matching labels.
	o.rightSeriesGroups = make([]*orGroup, 0, len(rightMetadata))

	for _, s := range rightMetadata {
		groupKey := groupKeyFunc(s.Labels)
		group, exists := groupMap[string(groupKey)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if !exists {
			group = &orGroup{lastLeftSeriesIndex: -1}
			groupMap[string(groupKey)] = group
		}

		group.rightSeriesCount++
		o.rightSeriesGroups = append(o.rightSeriesGroups, group)
	}

	// Iterate through the left-hand series, and find groups for each based on the matching labels.
	o.leftSeriesGroups = make([]*orGroup, 0, len(leftMetadata))

	for idx, s := range leftMetadata {
		groupKey := groupKeyFunc(s.Labels)
		group, exists := groupMap[string(groupKey)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if exists {
			group.lastLeftSeriesIndex = idx
		}

		// Even if there is no matching group, we want to store a nil value here so we know we don't need to store presence information later.
		o.leftSeriesGroups = append(o.leftSeriesGroups, group)
	}

	// Iterate through the right-hand series again, and remove any groups that don't match any series from the left.
	for idx, group := range o.rightSeriesGroups {
		if group.lastLeftSeriesIndex == -1 {
			o.rightSeriesGroups[idx] = nil
		}
	}
}

func (o *OrBinaryOperation) computeSeriesOutputOrder(leftMetadata []types.SeriesMetadata, rightMetadata []types.SeriesMetadata) ([]types.SeriesMetadata, error) {
	// The idea here is to determine the order we should return series in, returning series from the right side as soon as we've seen all
	// the series from the left that we need.
	//
	// We can return left series as soon as they're read, given they are returned unmodified (we just need to store sample presence
	// information so we can filter the corresponding right side series later on).
	//
	// We deliberately ignore the case where series on both sides have the same labels: this makes the logic here much simpler, and
	// we rely on DeduplicateAndMerge to merge series when required. This does come at a slight performance cost, so we could revisit this
	// in the future if profiles show this is problematic. DeduplicateAndMerge should never produce a conflict, as the filtering done here
	// should ensure there is only one value for each time step for each set of series with the same labels.
	//
	// A simpler version of this would be to just return all left side series first, then all right side series.
	// However, if we do that, we will always need to hold presence bitmaps for every group in memory until we've read all left side series.
	// By sorting the series so we return series from the right as soon as we've seen all of the corresponding series from the left, we
	// minimise the number of presence bitmaps we need to hold in memory at once, at the cost of potentially holding some intermediate
	// state on both sides.

	nextLeftSeriesToRead := 0
	series, err := types.SeriesMetadataSlicePool.Get(len(leftMetadata)+len(rightMetadata), o.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	for nextRightSeriesToRead, rightGroup := range o.rightSeriesGroups {
		lastSeriesFromLeft := false

		// Check if we need to advance through some left series first.
		if rightGroup != nil && rightGroup.lastLeftSeriesIndex >= nextLeftSeriesToRead {
			seriesCount := rightGroup.lastLeftSeriesIndex - nextLeftSeriesToRead + 1

			o.leftSeriesCount = append(o.leftSeriesCount, seriesCount)
			seriesToAppend := leftMetadata[nextLeftSeriesToRead : rightGroup.lastLeftSeriesIndex+1]
			series, err = types.AppendSeriesMetadata(o.MemoryConsumptionTracker, series, seriesToAppend...)
			if err != nil {
				return nil, err
			}

			nextLeftSeriesToRead += seriesCount

			if nextRightSeriesToRead == 0 {
				// The first series this operator will return is from the left.
				// Signal that to NextSeries.
				o.nextSeriesIsFromLeft = true
			}

			lastSeriesFromLeft = true
		}

		// If the last series was from the left, or if this is the first series from the right, start a new run of right series.
		if lastSeriesFromLeft || nextRightSeriesToRead == 0 {
			o.rightSeriesCount = append(o.rightSeriesCount, 1)
			lastSeriesFromLeft = false
		} else {
			o.rightSeriesCount[len(o.rightSeriesCount)-1]++
		}

		series, err = types.AppendSeriesMetadata(o.MemoryConsumptionTracker, series, rightMetadata[nextRightSeriesToRead])
		if err != nil {
			return nil, err
		}
	}

	// Check if there are any remaining series on the left side.
	if nextLeftSeriesToRead < len(leftMetadata) {
		seriesCount := len(leftMetadata) - nextLeftSeriesToRead
		series, err = types.AppendSeriesMetadata(o.MemoryConsumptionTracker, series, leftMetadata[nextLeftSeriesToRead:]...)
		if err != nil {
			return nil, err
		}

		o.leftSeriesCount = append(o.leftSeriesCount, seriesCount)
	}

	return series, nil
}

func (o *OrBinaryOperation) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if len(o.rightSeriesCount) == 0 && len(o.leftSeriesCount) == 0 {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	if o.nextSeriesIsFromLeft {
		return o.nextLeftSeries(ctx)
	}

	return o.nextRightSeries(ctx)
}

func (o *OrBinaryOperation) nextLeftSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	o.leftSeriesCount[0]--

	if o.leftSeriesCount[0] == 0 {
		o.nextSeriesIsFromLeft = false
		o.leftSeriesCount = o.leftSeriesCount[1:]
	}

	d, err := o.readNextLeftSeries(ctx)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if len(o.leftSeriesCount) == 0 {
		// No more series from left side remaining, close it.
		if err := o.Left.Finalize(ctx); err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		o.Left.Close()
	}

	return d, nil
}

func (o *OrBinaryOperation) readNextLeftSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	data, err := o.Left.NextSeries(ctx)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if o.leftSeriesGroups == nil {
		// If we only have series from the left side, then leftSeriesGroups and rightSeriesGroups will be nil and we have no filtering to do.
		return data, nil
	}

	group := o.leftSeriesGroups[0]
	o.leftSeriesGroups = o.leftSeriesGroups[1:]

	if group != nil {
		if err := group.AccumulateLeftSeriesPresence(data, o.MemoryConsumptionTracker, o.timeRange); err != nil {
			return types.InstantVectorSeriesData{}, err
		}
	}

	return data, nil
}

func (o *OrBinaryOperation) nextRightSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	o.rightSeriesCount[0]--

	if o.rightSeriesCount[0] == 0 {
		o.nextSeriesIsFromLeft = true
		o.rightSeriesCount = o.rightSeriesCount[1:]
	}

	d, err := o.readNextRightSeries(ctx)

	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if len(o.rightSeriesCount) == 0 {
		// No more series from right side remaining, close it after we read this next series.
		if err := o.Right.Finalize(ctx); err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		o.Right.Close()
	}

	return d, nil
}

func (o *OrBinaryOperation) readNextRightSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	data, err := o.Right.NextSeries(ctx) // We don't need to return this series to the pool: FilterRightSeries will handle that for us if needed.
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if o.rightSeriesGroups == nil {
		// If we only have series from the right side, then leftSeriesGroups and rightSeriesGroups will be nil and we have no filtering to do.
		return data, nil
	}

	group := o.rightSeriesGroups[0]
	o.rightSeriesGroups = o.rightSeriesGroups[1:]

	if group == nil {
		// This series matches nothing on the left side, we can return it as-is.
		return data, nil
	}

	data, err = group.FilterRightSeries(data, o.MemoryConsumptionTracker, o.timeRange)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	group.rightSeriesCount--
	if group.rightSeriesCount == 0 {
		// This is the last right series for the group, return it to the pool.
		group.Close(o.MemoryConsumptionTracker)
	}

	return data, nil
}

func (o *OrBinaryOperation) ExpressionPosition() posrange.PositionRange {
	return o.expressionPosition
}

func (o *OrBinaryOperation) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if err := o.Left.Prepare(ctx, params); err != nil {
		return err
	}

	return o.Right.Prepare(ctx, params)
}

func (o *OrBinaryOperation) AfterPrepare(ctx context.Context) error {
	if err := o.Left.AfterPrepare(ctx); err != nil {
		return err
	}

	return o.Right.AfterPrepare(ctx)
}

func (o *OrBinaryOperation) Finalize(ctx context.Context) error {
	if err := o.Left.Finalize(ctx); err != nil {
		return err
	}

	return o.Right.Finalize(ctx)
}

func (o *OrBinaryOperation) Close() {
	o.Left.Close()
	o.Right.Close()

	for _, g := range o.leftSeriesGroups {
		if g == nil {
			continue
		}

		g.Close(o.MemoryConsumptionTracker)
	}

	o.leftSeriesGroups = nil

	for _, g := range o.rightSeriesGroups {
		if g == nil {
			continue
		}

		g.Close(o.MemoryConsumptionTracker)
	}

	o.rightSeriesGroups = nil
}

type orGroup struct {
	lastLeftSeriesIndex int
	rightSeriesCount    int
	leftSamplePresence  []bool // FIXME: this would be a good candidate for a bitmap type
}

// AccumulateLeftSeriesPresence records the presence of samples on the left-hand side.
func (g *orGroup) AccumulateLeftSeriesPresence(data types.InstantVectorSeriesData, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, timeRange types.QueryTimeRange) error {
	if g.leftSamplePresence == nil {
		var err error
		g.leftSamplePresence, err = types.BoolSlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)

		if err != nil {
			return err
		}

		g.leftSamplePresence = g.leftSamplePresence[:timeRange.StepCount]
	}

	for _, p := range data.Floats {
		g.leftSamplePresence[timeRange.PointIndex(p.T)] = true
	}

	for _, p := range data.Histograms {
		g.leftSamplePresence[timeRange.PointIndex(p.T)] = true
	}

	return nil
}

// FilterRightSeries returns rightData filtered based on samples seen for the left-hand side.
// The return value reuses the slices from rightData, and returns any unused slices to the pool.
func (g *orGroup) FilterRightSeries(rightData types.InstantVectorSeriesData, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, timeRange types.QueryTimeRange) (types.InstantVectorSeriesData, error) {
	return filterSeries(rightData, g.leftSamplePresence, false, memoryConsumptionTracker, timeRange)
}

func (g *orGroup) Close(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	types.BoolSlicePool.Put(&g.leftSamplePresence, memoryConsumptionTracker)
}
