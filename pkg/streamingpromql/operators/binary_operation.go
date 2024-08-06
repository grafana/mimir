// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package operators

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// BinaryOperation represents a binary operation between instant vectors such as "<expr> + <expr>" or "<expr> - <expr>".
type BinaryOperation struct {
	Left                     types.InstantVectorOperator
	Right                    types.InstantVectorOperator
	Op                       parser.ItemType
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	VectorMatching parser.VectorMatching

	// We need to retain these so that NextSeries() can return an error message with the series labels when
	// multiple points match on a single side.
	// Note that we don't retain the output series metadata: if we need to return an error message, we can compute
	// the output series labels from these again.
	leftMetadata  []types.SeriesMetadata
	rightMetadata []types.SeriesMetadata

	remainingSeries []*binaryOperationOutputSeries
	leftBuffer      *binaryOperationSeriesBuffer
	rightBuffer     *binaryOperationSeriesBuffer
	leftIterator    types.InstantVectorSeriesDataIterator
	rightIterator   types.InstantVectorSeriesDataIterator
	opFunc          binaryOperationFunc

	expressionPosition posrange.PositionRange
}

var _ types.InstantVectorOperator = &BinaryOperation{}

type binaryOperationOutputSeries struct {
	leftSeriesIndices  []int
	rightSeriesIndices []int
}

// latestLeftSeries returns the index of the last series from the left source needed for this output series.
//
// It assumes that leftSeriesIndices is sorted in ascending order.
func (s binaryOperationOutputSeries) latestLeftSeries() int {
	return s.leftSeriesIndices[len(s.leftSeriesIndices)-1]
}

// latestRightSeries returns the index of the last series from the right source needed for this output series.
//
// It assumes that rightSeriesIndices is sorted in ascending order.
func (s binaryOperationOutputSeries) latestRightSeries() int {
	return s.rightSeriesIndices[len(s.rightSeriesIndices)-1]
}

func NewBinaryOperation(
	left types.InstantVectorOperator,
	right types.InstantVectorOperator,
	vectorMatching parser.VectorMatching,
	op parser.ItemType,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
) (*BinaryOperation, error) {
	opFunc := arithmeticOperationFuncs[op]
	if opFunc == nil {
		return nil, compat.NewNotSupportedError(fmt.Sprintf("binary expression with '%s'", op))
	}

	return &BinaryOperation{
		Left:                     left,
		Right:                    right,
		leftIterator:             types.InstantVectorSeriesDataIterator{},
		rightIterator:            types.InstantVectorSeriesDataIterator{},
		VectorMatching:           vectorMatching,
		Op:                       op,
		MemoryConsumptionTracker: memoryConsumptionTracker,

		opFunc:             opFunc,
		expressionPosition: expressionPosition,
	}, nil
}

func (b *BinaryOperation) ExpressionPosition() posrange.PositionRange {
	return b.expressionPosition
}

// SeriesMetadata returns the series expected to be produced by this operator.
//
// Note that it is possible that this method returns a series which will not have any points, as the
// list of possible output series is generated based solely on the series labels, not their data.
//
// For example, if this operator is for a range query with the expression "left_metric + right_metric", but
// left_metric has points at T=0 and T=1 in the query range, and right_metric has points at T=2 and T=3 in the
// query range, then SeriesMetadata will return a series, but NextSeries will return no points for that series.
//
// If this affects many series in the query, this may cause consuming operators to be less efficient, but in
// practice this rarely happens.
//
// (The alternative would be to compute the entire result here in SeriesMetadata and only return the series that
// contain points, but that would mean we'd need to hold the entire result in memory at once, which we want to
// avoid.)
func (b *BinaryOperation) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	if canProduceAnySeries, err := b.loadSeriesMetadata(ctx); err != nil {
		return nil, err
	} else if !canProduceAnySeries {
		return nil, nil
	}

	allMetadata, allSeries, leftSeriesUsed, rightSeriesUsed, err := b.computeOutputSeries()
	if err != nil {
		return nil, err
	}

	b.sortSeries(allMetadata, allSeries)
	b.remainingSeries = allSeries

	b.leftBuffer = newBinaryOperationSeriesBuffer(b.Left, leftSeriesUsed, b.MemoryConsumptionTracker)
	b.rightBuffer = newBinaryOperationSeriesBuffer(b.Right, rightSeriesUsed, b.MemoryConsumptionTracker)

	return allMetadata, nil
}

// loadSeriesMetadata loads series metadata from both sides of this operation.
// It returns false if one side returned no series and that means there is no way for this operation to return any series.
// (eg. if doing A + B and either A or B have no series, then there is no way for this operation to produce any series)
func (b *BinaryOperation) loadSeriesMetadata(ctx context.Context) (bool, error) {
	// We retain the series labels for later so we can use them to generate error messages.
	// We'll return them to the pool in Close().

	var err error
	b.leftMetadata, err = b.Left.SeriesMetadata(ctx)
	if err != nil {
		return false, err
	}

	if len(b.leftMetadata) == 0 {
		// FIXME: this is incorrect for 'or'
		// No series on left-hand side, we'll never have any output series.
		return false, nil
	}

	b.rightMetadata, err = b.Right.SeriesMetadata(ctx)
	if err != nil {
		return false, err
	}

	if len(b.rightMetadata) == 0 {
		// FIXME: this is incorrect for 'or' and 'unless'
		// No series on right-hand side, we'll never have any output series.
		return false, nil
	}

	return true, nil
}

// computeOutputSeries determines the possible output series from this operator.
// It assumes leftMetadata and rightMetadata have already been populated.
//
// It returns:
// - a list of all possible series this operator could return
// - a corresponding list of the source series for each output series
// - a list indicating which series from the left side are needed to compute the output
// - a list indicating which series from the right side are needed to compute the output
func (b *BinaryOperation) computeOutputSeries() ([]types.SeriesMetadata, []*binaryOperationOutputSeries, []bool, []bool, error) {
	labelsFunc := b.labelsFunc()
	outputSeriesMap := map[string]*binaryOperationOutputSeries{}

	// Use the smaller side to populate the map of possible output series first.
	// This should ensure we don't unnecessarily populate the output series map with series that will never match in most cases.
	// (It's possible that all the series on the larger side all belong to the same group, but this is expected to be rare.)
	// FIXME: this doesn't work as-is for 'unless'.
	smallerSide := b.leftMetadata
	largerSide := b.rightMetadata
	smallerSideIsLeftSide := len(b.leftMetadata) < len(b.rightMetadata)

	if !smallerSideIsLeftSide {
		smallerSide = b.rightMetadata
		largerSide = b.leftMetadata
	}

	for idx, s := range smallerSide {
		groupLabels := labelsFunc(s.Labels).String()
		series, exists := outputSeriesMap[groupLabels]

		if !exists {
			series = &binaryOperationOutputSeries{}
			outputSeriesMap[groupLabels] = series
		}

		if smallerSideIsLeftSide {
			series.leftSeriesIndices = append(series.leftSeriesIndices, idx)
		} else {
			series.rightSeriesIndices = append(series.rightSeriesIndices, idx)
		}
	}

	for idx, s := range largerSide {
		groupLabels := labelsFunc(s.Labels).String()

		if series, exists := outputSeriesMap[groupLabels]; exists {
			if smallerSideIsLeftSide {
				// Currently iterating through right side.
				series.rightSeriesIndices = append(series.rightSeriesIndices, idx)
			} else {
				series.leftSeriesIndices = append(series.leftSeriesIndices, idx)
			}
		}

		// FIXME: if this is an 'or' operation, then we need to create the right side even if the left doesn't exist (or vice-versa)
	}

	// Remove series that cannot produce samples.
	for seriesLabels, outputSeries := range outputSeriesMap {
		if len(outputSeries.leftSeriesIndices) == 0 || len(outputSeries.rightSeriesIndices) == 0 {
			// FIXME: this is incorrect for 'or' and 'unless'
			// No matching series on at least one side for this output series, so output series will have no samples. Remove it.
			delete(outputSeriesMap, seriesLabels)
		}
	}

	allMetadata := make([]types.SeriesMetadata, 0, len(outputSeriesMap))
	allSeries := make([]*binaryOperationOutputSeries, 0, len(outputSeriesMap))

	leftSeriesUsed, err := types.BoolSlicePool.Get(len(b.leftMetadata), b.MemoryConsumptionTracker)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	rightSeriesUsed, err := types.BoolSlicePool.Get(len(b.rightMetadata), b.MemoryConsumptionTracker)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	leftSeriesUsed = leftSeriesUsed[:len(b.leftMetadata)]
	rightSeriesUsed = rightSeriesUsed[:len(b.rightMetadata)]

	for _, outputSeries := range outputSeriesMap {
		firstSeriesLabels := b.leftMetadata[outputSeries.leftSeriesIndices[0]].Labels
		allMetadata = append(allMetadata, types.SeriesMetadata{Labels: labelsFunc(firstSeriesLabels)})
		allSeries = append(allSeries, outputSeries)

		for _, leftSeriesIndex := range outputSeries.leftSeriesIndices {
			leftSeriesUsed[leftSeriesIndex] = true
		}

		for _, rightSeriesIndex := range outputSeries.rightSeriesIndices {
			rightSeriesUsed[rightSeriesIndex] = true
		}
	}

	return allMetadata, allSeries, leftSeriesUsed, rightSeriesUsed, nil
}

// sortSeries sorts metadata and series in place to try to minimise the number of input series we'll need to buffer in memory.
//
// This is critical for minimising the memory consumption of this operator: if we choose a poor ordering of series,
// we'll need to buffer many input series in memory.
//
// At present, sortSeries uses a very basic heuristic to guess the best way to sort the output series, but we could make
// this more sophisticated in the future.
func (b *BinaryOperation) sortSeries(metadata []types.SeriesMetadata, series []*binaryOperationOutputSeries) {
	// For one-to-one matching, we assume that each output series takes one series from each side of the operator.
	// If this is true, then the best order is the one in which we read from the highest cardinality side in order.
	// If we do this, then in the worst case, we'll have to buffer the whole of the lower cardinality side.
	// (Compare this with sorting so that we read the lowest cardinality side in order: in the worst case, we'll have
	// to buffer the whole of the higher cardinality side.)
	//
	// FIXME: this is reasonable for one-to-one matching, but likely not for one-to-many / many-to-one.
	// For one-to-many / many-to-one, it would likely be best to buffer the side used for multiple output series (the "one" side),
	// as we'll need to retain these series for multiple output series anyway.

	var sortInterface sort.Interface

	if len(b.leftMetadata) < len(b.rightMetadata) {
		sortInterface = newFavourRightSideSorter(metadata, series)
	} else {
		sortInterface = newFavourLeftSideSorter(metadata, series)
	}

	sort.Sort(sortInterface)
}

type binaryOperationOutputSorter struct {
	metadata []types.SeriesMetadata
	series   []*binaryOperationOutputSeries
}

type favourLeftSideSorter struct {
	binaryOperationOutputSorter
}

func newFavourLeftSideSorter(metadata []types.SeriesMetadata, series []*binaryOperationOutputSeries) favourLeftSideSorter {
	return favourLeftSideSorter{binaryOperationOutputSorter{metadata, series}}
}

type favourRightSideSorter struct {
	binaryOperationOutputSorter
}

func newFavourRightSideSorter(metadata []types.SeriesMetadata, series []*binaryOperationOutputSeries) favourRightSideSorter {
	return favourRightSideSorter{binaryOperationOutputSorter{metadata, series}}
}

func (g binaryOperationOutputSorter) Len() int {
	return len(g.metadata)
}

func (g binaryOperationOutputSorter) Swap(i, j int) {
	g.metadata[i], g.metadata[j] = g.metadata[j], g.metadata[i]
	g.series[i], g.series[j] = g.series[j], g.series[i]
}

func (g favourLeftSideSorter) Less(i, j int) bool {
	iLeft := g.series[i].latestLeftSeries()
	jLeft := g.series[j].latestLeftSeries()
	if iLeft != jLeft {
		return iLeft < jLeft
	}

	return g.series[i].latestRightSeries() < g.series[j].latestRightSeries()
}

func (g favourRightSideSorter) Less(i, j int) bool {
	iRight := g.series[i].latestRightSeries()
	jRight := g.series[j].latestRightSeries()
	if iRight != jRight {
		return iRight < jRight
	}

	return g.series[i].latestLeftSeries() < g.series[j].latestLeftSeries()
}

// labelsFunc returns a function that computes the labels of the output group this series belongs to.
func (b *BinaryOperation) labelsFunc() func(labels.Labels) labels.Labels {
	lb := labels.NewBuilder(labels.EmptyLabels())

	if b.VectorMatching.On {
		return func(l labels.Labels) labels.Labels {
			lb.Reset(l)
			lb.Keep(b.VectorMatching.MatchingLabels...)
			return lb.Labels()
		}
	}

	return func(l labels.Labels) labels.Labels {
		lb.Reset(l)
		lb.Del(b.VectorMatching.MatchingLabels...)
		lb.Del(labels.MetricName)
		return lb.Labels()
	}
}

func (b *BinaryOperation) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if len(b.remainingSeries) == 0 {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	thisSeries := b.remainingSeries[0]
	b.remainingSeries = b.remainingSeries[1:]

	allLeftSeries, err := b.leftBuffer.getSeries(ctx, thisSeries.leftSeriesIndices)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	mergedLeftSide, err := b.mergeOneSide(allLeftSeries, thisSeries.leftSeriesIndices, b.leftMetadata, "left")
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	allRightSeries, err := b.rightBuffer.getSeries(ctx, thisSeries.rightSeriesIndices)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	mergedRightSide, err := b.mergeOneSide(allRightSeries, thisSeries.rightSeriesIndices, b.rightMetadata, "right")
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	return b.computeResult(mergedLeftSide, mergedRightSide)
}

// mergeOneSide exists to handle the case where one side of an output series has different source series at different time steps.
//
// For example, consider the query "left_side + on (env) right_side" with the following source data:
//
//	left_side{env="test", pod="a"} 1 2 _
//	left_side{env="test", pod="b"} _ _ 3
//	right_side{env="test"} 100 200 300
//
// mergeOneSide will take in both series for left_side and return a single series with the points [1, 2, 3].
//
// mergeOneSide is optimised for the case where there is only one source series, or the source series do not overlap, as in the example above.
//
// NOTE: mergeOneSide has the side-effect of re-ordering both data and sourceSeriesIndices.
//
// FIXME: for many-to-one / one-to-many matching, we could avoid re-merging each time for the side used multiple times
func (b *BinaryOperation) mergeOneSide(data []types.InstantVectorSeriesData, sourceSeriesIndices []int, sourceSeriesMetadata []types.SeriesMetadata, side string) (types.InstantVectorSeriesData, error) {
	if len(data) == 1 {
		// Fast path: if there's only one series on this side, there's no merging required.
		return data[0], nil
	}

	if len(data) == 0 {
		return types.InstantVectorSeriesData{}, nil
	}

	// Merge floats and histograms individually.
	// After which we check if there are any duplicate points in either the floats or histograms.

	ds := seriesForOneGroupSide{
		data:                data,
		sourceSeriesIndices: sourceSeriesIndices,
	}

	floats, err := b.mergeOneSideFloats(ds, sourceSeriesMetadata, side)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}
	histograms, err := b.mergeOneSideHistograms(ds, sourceSeriesMetadata, side)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	// Check for any conflicts between floats and histograms
	idxFloats, idxHistograms := 0, 0
	for idxFloats < len(floats) && idxHistograms < len(histograms) {
		if floats[idxFloats].T == histograms[idxHistograms].T {
			// Conflict found
			firstConflictingSeriesLabels := sourceSeriesMetadata[0].Labels
			groupLabels := b.labelsFunc()(firstConflictingSeriesLabels)

			return types.InstantVectorSeriesData{}, fmt.Errorf("found both float and histogram samples for the match group %s on the %s side of the operation at timestamp %s", groupLabels, side, timestamp.Time(floats[idxFloats].T).Format(time.RFC3339Nano))
		}
		if floats[idxFloats].T < histograms[idxHistograms].T {
			idxFloats++
		} else {
			idxHistograms++
		}
	}

	return types.InstantVectorSeriesData{Floats: floats, Histograms: histograms}, nil
}

func (b *BinaryOperation) mergeOneSideFloats(seriesGroupSide seriesForOneGroupSide, sourceSeriesMetadata []types.SeriesMetadata, side string) ([]promql.FPoint, error) {
	if len(seriesGroupSide.data) == 0 {
		return nil, nil
	}
	if len(seriesGroupSide.data) == 1 {
		return seriesGroupSide.data[0].Floats, nil
	}

	sort.Sort(floatSideSorter{side: seriesGroupSide})

	// After sorting, if the first series has no floats, we're done, as that means there are no floats in any series
	if len(seriesGroupSide.data[0].Floats) == 0 {
		return nil, nil
	}

	mergedSize := len(seriesGroupSide.data[0].Floats)
	remainingSeriesWithFloats := 1
	haveOverlaps := false

	for i := 0; i < len(seriesGroupSide.data)-1; i++ {
		first := seriesGroupSide.data[i]
		second := seriesGroupSide.data[i+1]

		// We're going to create a new slice, so return this one to the pool.
		// We must defer here, rather than at the end, as the merge loop below reslices Floats.
		// FIXME: this isn't correct for many-to-one / one-to-many matching - we'll need the series again (unless we store the result of the merge)
		defer types.FPointSlicePool.Put(second.Floats, b.MemoryConsumptionTracker)

		if len(second.Floats) == 0 {
			// We've reached the end of all series with floats.
			// However, continue iterating so we can return all of the slices.
			// (As they may have length 0, but a non-zero capacity).
			continue
		}

		mergedSize += len(second.Floats)
		remainingSeriesWithFloats++

		// Check if first overlaps with second.
		// InstantVectorSeriesData.Floats is required to be sorted in timestamp order, so if the last point
		// of the first series is before the first point of the second series, it cannot overlap.
		if first.Floats[len(first.Floats)-1].T >= second.Floats[0].T {
			haveOverlaps = true
		}
	}

	if remainingSeriesWithFloats == 1 {
		// No other series had any floats
		return seriesGroupSide.data[0].Floats, nil
	}

	// We're going to create a new slice, so return this one to the pool.
	// We'll return the other slices in the for loop below.
	// We must defer here, rather than at the end, as the merge loop below reslices Floats.
	// FIXME: this isn't correct for many-to-one / one-to-many matching - we'll need the series again (unless we store the result of the merge)
	defer types.FPointSlicePool.Put(seriesGroupSide.data[0].Floats, b.MemoryConsumptionTracker)

	// Re-slice the ds.data with just the series with floats to make the rest of our job easier
	// Because we aren't re-sorting here it doesn't matter that ds.sourceSeriesIndices remains longer.
	data := seriesGroupSide.data[:remainingSeriesWithFloats]

	output, err := types.FPointSlicePool.Get(mergedSize, b.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	if !haveOverlaps {
		// Fast path: no overlaps, so we can just concatenate the slices together, and there's no
		// need to check for conflicts either.
		for _, d := range data {
			output = append(output, d.Floats...)
		}

		return output, nil
	}

	// Slow path: there are overlaps, so we need to merge slices together and check for conflicts as we go.
	// We don't expect to have many series here, so something like a loser tree is likely unnecessary.
	for {
		if remainingSeriesWithFloats == 1 {
			// Only one series left, just copy remaining points.
			for _, d := range data {
				if len(d.Floats) > 0 {
					output = append(output, d.Floats...)
					return output, nil
				}
			}
		}

		nextT := int64(math.MaxInt64)
		sourceSeriesIndexInData := -1

		for seriesIndexInData, d := range data {
			if len(d.Floats) == 0 {
				continue
			}

			nextPointInSeries := d.Floats[0]
			if nextPointInSeries.T == nextT {
				// Another series has a point with the same timestamp. We have a conflict.
				firstConflictingSeriesLabels := sourceSeriesMetadata[seriesGroupSide.sourceSeriesIndices[sourceSeriesIndexInData]].Labels
				secondConflictingSeriesLabels := sourceSeriesMetadata[seriesGroupSide.sourceSeriesIndices[seriesIndexInData]].Labels
				groupLabels := b.labelsFunc()(firstConflictingSeriesLabels)

				return nil, fmt.Errorf("found duplicate series for the match group %s on the %s side of the operation at timestamp %s: %s and %s", groupLabels, side, timestamp.Time(nextT).Format(time.RFC3339Nano), firstConflictingSeriesLabels, secondConflictingSeriesLabels)
			}

			if d.Floats[0].T < nextT {
				nextT = d.Floats[0].T
				sourceSeriesIndexInData = seriesIndexInData
			}
		}

		output = append(output, data[sourceSeriesIndexInData].Floats[0])
		data[sourceSeriesIndexInData].Floats = data[sourceSeriesIndexInData].Floats[1:]

		if len(data[sourceSeriesIndexInData].Floats) == 0 {
			remainingSeriesWithFloats--
		}
	}
}

func (b *BinaryOperation) mergeOneSideHistograms(seriesGroupSide seriesForOneGroupSide, sourceSeriesMetadata []types.SeriesMetadata, side string) ([]promql.HPoint, error) {
	if len(seriesGroupSide.data) == 0 {
		return nil, nil
	}
	if len(seriesGroupSide.data) == 1 {
		return seriesGroupSide.data[0].Histograms, nil
	}

	sort.Sort(histogramSideSorter{side: seriesGroupSide})

	// After sorting, if the first series has no histograms, we're done, as that means there are no histograms in any series
	if len(seriesGroupSide.data[0].Histograms) == 0 {
		return nil, nil
	}

	mergedSize := len(seriesGroupSide.data[0].Histograms)
	remainingSeriesWithHistograms := 1
	haveOverlaps := false

	for i := 0; i < len(seriesGroupSide.data)-1; i++ {
		first := seriesGroupSide.data[i]
		second := seriesGroupSide.data[i+1]

		// We're going to create a new slice, so return this one to the pool.
		// We must defer here, rather than at the end, as the merge loop below reslices Histograms.
		// FIXME: this isn't correct for many-to-one / one-to-many matching - we'll need the series again (unless we store the result of the merge)
		defer types.HPointSlicePool.Put(second.Histograms, b.MemoryConsumptionTracker)

		if len(second.Histograms) == 0 {
			// We've reached the end of all series with histograms.
			// However, continue iterating so we can return all of the slices.
			// (As they may have length 0, but a non-zero capacity).
			continue
		}
		mergedSize += len(second.Histograms)
		remainingSeriesWithHistograms++

		// Check if first overlaps with second.
		// InstantVectorSeriesData.Histograms is required to be sorted in timestamp order, so if the last point
		// of the first series is before the first point of the second series, it cannot overlap.
		if first.Histograms[len(first.Histograms)-1].T >= second.Histograms[0].T {
			haveOverlaps = true
		}
	}

	if remainingSeriesWithHistograms == 1 {
		// No other series had any histograms
		return seriesGroupSide.data[0].Histograms, nil
	}

	// We're going to create a new slice, so return this one to the pool.
	// We'll return the other slices in the for loop below.
	// We must defer here, rather than at the end, as the merge loop below reslices Histograms.
	// FIXME: this isn't correct for many-to-one / one-to-many matching - we'll need the series again (unless we store the result of the merge)
	defer types.HPointSlicePool.Put(seriesGroupSide.data[0].Histograms, b.MemoryConsumptionTracker)

	// Re-slice data with just the series with histograms to make the rest of our job easier
	// Because we aren't re-sorting here it doesn't matter that ds.sourceSeriesIndices remains longer.
	data := seriesGroupSide.data[:remainingSeriesWithHistograms]

	output, err := types.HPointSlicePool.Get(mergedSize, b.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	if !haveOverlaps {
		// Fast path: no overlaps, so we can just concatenate the slices together, and there's no
		// need to check for conflicts either.
		for _, d := range data {
			output = append(output, d.Histograms...)
		}

		return output, nil
	}

	// Slow path: there are overlaps, so we need to merge slices together and check for conflicts as we go.
	// We don't expect to have many series here, so something like a loser tree is likely unnecessary.
	for {
		if remainingSeriesWithHistograms == 1 {
			// Only one series left, just copy remaining points.
			for _, d := range data {
				if len(d.Histograms) > 0 {
					output = append(output, d.Histograms...)
					return output, nil
				}
			}
		}

		nextT := int64(math.MaxInt64)
		sourceSeriesIndexInData := -1

		for seriesIndexInData, d := range data {
			if len(d.Histograms) == 0 {
				continue
			}

			nextPointInSeries := d.Histograms[0]
			if nextPointInSeries.T == nextT {
				// Another series has a point with the same timestamp. We have a conflict.
				firstConflictingSeriesLabels := sourceSeriesMetadata[seriesGroupSide.sourceSeriesIndices[sourceSeriesIndexInData]].Labels
				secondConflictingSeriesLabels := sourceSeriesMetadata[seriesGroupSide.sourceSeriesIndices[seriesIndexInData]].Labels
				groupLabels := b.labelsFunc()(firstConflictingSeriesLabels)

				return nil, fmt.Errorf("found duplicate series for the match group %s on the %s side of the operation at timestamp %s: %s and %s", groupLabels, side, timestamp.Time(nextT).Format(time.RFC3339Nano), firstConflictingSeriesLabels, secondConflictingSeriesLabels)
			}

			if d.Histograms[0].T < nextT {
				nextT = d.Histograms[0].T
				sourceSeriesIndexInData = seriesIndexInData
			}
		}

		output = append(output, data[sourceSeriesIndexInData].Histograms[0])
		data[sourceSeriesIndexInData].Histograms = data[sourceSeriesIndexInData].Histograms[1:]

		if len(data[sourceSeriesIndexInData].Histograms) == 0 {
			remainingSeriesWithHistograms--
		}
	}
}

func (b *BinaryOperation) computeResult(left types.InstantVectorSeriesData, right types.InstantVectorSeriesData) (types.InstantVectorSeriesData, error) {
	var fPoints []promql.FPoint
	var hPoints []promql.HPoint

	// For one-to-one matching for arithmetic operators, we'll never produce more points than the smaller input side.
	// Because floats and histograms can be multiplied together, we use the sum of both the float and histogram points.
	// We also don't know if the output will be exclusively floats or histograms, so we'll use the same size slice for both.
	// We only assign the slices once we see the associated point type so it shouldn't be common that we allocate both.
	//
	// FIXME: this is not safe to do for one-to-many, many-to-one or many-to-many matching, as we may need the input series for later output series.
	canReturnLeftFPointSlice, canReturnLeftHPointSlice, canReturnRightFPointSlice, canReturnRightHPointSlice := true, true, true, true
	leftPoints := len(left.Floats) + len(left.Histograms)
	rightPoints := len(right.Floats) + len(right.Histograms)
	maxPoints := max(leftPoints, rightPoints)

	// We cannot re-use any slices when the series contain a mix of floats and histograms.
	// Consider the following, where f is a float at a particular step, and h is a histogram.
	// load 5m
	//   series1 f f f h h
	//   series2 h h f f h
	// eval range from 0 to 25m step 5m series1 * series2
	//   {}      h h f h f
	// We can fit the resulting 3 histograms into series2 existing slice. However, the second
	// last step (index 3) produces a histogram which would be stored over the existing histogram
	// at the end of series2 (also index 3).
	// It should be pretty uncommon that metric contains both histograms and floats, so we will
	// accept the cost of a new slice.
	mixedPoints := len(left.Floats) > 0 && len(left.Histograms) > 0 || len(right.Floats) > 0 && len(right.Histograms) > 0

	prepareFSlice := func() error {
		if !mixedPoints && maxPoints <= cap(left.Floats) && cap(left.Floats) < cap(right.Floats) {
			// Can fit output in left side, and the left side is smaller than the right
			canReturnLeftFPointSlice = false
			fPoints = left.Floats[:0]
		} else if !mixedPoints && maxPoints <= cap(right.Floats) {
			// Can otherwise fit in the right side
			canReturnRightFPointSlice = false
			fPoints = right.Floats[:0]
		}
		// Either we have mixed points or we can't fit in either left or right side, so create a new slice
		var err error
		if fPoints, err = types.FPointSlicePool.Get(maxPoints, b.MemoryConsumptionTracker); err != nil {
			return err
		}
		return nil
	}

	prepareHSlice := func() error {
		if !mixedPoints && maxPoints <= cap(left.Histograms) && cap(left.Histograms) < cap(right.Histograms) {
			// Can fit output in left side, and the left side is smaller than the right
			canReturnLeftHPointSlice = false
			hPoints = left.Histograms[:0]
		} else if !mixedPoints && maxPoints <= cap(right.Histograms) {
			// Can otherwise fit in the right side
			canReturnRightHPointSlice = false
			hPoints = right.Histograms[:0]
		}
		// Either we have mixed points or we can't fit in either left or right side, so create a new slice
		var err error
		if hPoints, err = types.HPointSlicePool.Get(maxPoints, b.MemoryConsumptionTracker); err != nil {
			return err
		}
		return nil
	}

	b.leftIterator.Reset(left)
	b.rightIterator.Reset(right)

	// Get first sample from left and right
	lT, lF, lH, lOk := b.leftIterator.Next()
	rT, rF, rH, rOk := b.rightIterator.Next()
	// Continue iterating until we exhaust either the LHS or RHS
	// denoted by lOk or rOk being false.
	for lOk && rOk {
		if lT == rT {
			// Timestamps match at this step
			resultFloat, resultHist, ok, err := b.opFunc(lF, rF, lH, rH)
			if err != nil {
				return types.InstantVectorSeriesData{}, err
			}
			if ok {
				if resultHist != nil {
					if hPoints == nil {
						if err = prepareHSlice(); err != nil {
							return types.InstantVectorSeriesData{}, err
						}
					}
					hPoints = append(hPoints, promql.HPoint{
						H: resultHist,
						T: lT,
					})
				} else {
					if fPoints == nil {
						if err = prepareFSlice(); err != nil {
							return types.InstantVectorSeriesData{}, err
						}
					}
					fPoints = append(fPoints, promql.FPoint{
						F: resultFloat,
						T: lT,
					})
				}
			}
		}
		// Move the iterator with the lower timestamp, or both if equal
		if lT == rT {
			lT, lF, lH, lOk = b.leftIterator.Next()
			rT, rF, rH, rOk = b.rightIterator.Next()
		} else if lT < rT {
			lT, lF, lH, lOk = b.leftIterator.Next()
		} else {
			rT, rF, rH, rOk = b.rightIterator.Next()
		}
	}

	// Cleanup the unused slices.
	if canReturnLeftFPointSlice {
		types.FPointSlicePool.Put(left.Floats, b.MemoryConsumptionTracker)
	}
	if canReturnLeftHPointSlice {
		types.HPointSlicePool.Put(left.Histograms, b.MemoryConsumptionTracker)
	}
	if canReturnRightFPointSlice {
		types.FPointSlicePool.Put(right.Floats, b.MemoryConsumptionTracker)
	}
	if canReturnRightHPointSlice {
		types.HPointSlicePool.Put(right.Histograms, b.MemoryConsumptionTracker)
	}

	return types.InstantVectorSeriesData{
		Floats:     fPoints,
		Histograms: hPoints,
	}, nil
}

func (b *BinaryOperation) Close() {
	b.Left.Close()
	b.Right.Close()

	if b.leftMetadata != nil {
		types.PutSeriesMetadataSlice(b.leftMetadata)
	}

	if b.rightMetadata != nil {
		types.PutSeriesMetadataSlice(b.rightMetadata)
	}

	if b.leftBuffer != nil {
		b.leftBuffer.close()
	}

	if b.rightBuffer != nil {
		b.rightBuffer.close()
	}
}

// binaryOperationSeriesBuffer buffers series data until it is needed by BinaryOperation.
//
// For example, if the source operator produces series in order A, B, C, but their corresponding output series from the
// binary operation are in order B, A, C, binaryOperationSeriesBuffer will buffer the data for series A while series B is
// produced, then return series A when needed.
type binaryOperationSeriesBuffer struct {
	source          types.InstantVectorOperator
	nextIndexToRead int

	// If seriesUsed[i] == true, then the series at index i is needed for this operation and should be buffered if not used immediately.
	// If seriesUsed[i] == false, then the series at index i is never used and can be immediately discarded.
	// FIXME: could use a bitmap here to save some memory
	seriesUsed []bool

	memoryConsumptionTracker *limiting.MemoryConsumptionTracker

	// Stores series read but required for later series.
	buffer map[int]types.InstantVectorSeriesData

	// Reused to avoid allocating on every call to getSeries.
	output []types.InstantVectorSeriesData
}

func newBinaryOperationSeriesBuffer(source types.InstantVectorOperator, seriesUsed []bool, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) *binaryOperationSeriesBuffer {
	return &binaryOperationSeriesBuffer{
		source:                   source,
		seriesUsed:               seriesUsed,
		memoryConsumptionTracker: memoryConsumptionTracker,
		buffer:                   map[int]types.InstantVectorSeriesData{},
	}
}

// getSeries returns the data for the series in seriesIndices.
// The returned slice is only safe to use until getSeries is called again.
// seriesIndices should be sorted in ascending order to avoid unnecessary buffering.
func (b *binaryOperationSeriesBuffer) getSeries(ctx context.Context, seriesIndices []int) ([]types.InstantVectorSeriesData, error) {
	if cap(b.output) < len(seriesIndices) {
		b.output = make([]types.InstantVectorSeriesData, len(seriesIndices))
	}

	b.output = b.output[:len(seriesIndices)]

	for i, seriesIndex := range seriesIndices {
		d, err := b.getSingleSeries(ctx, seriesIndex)

		if err != nil {
			return nil, err
		}

		b.output[i] = d
	}

	return b.output, nil
}

func (b *binaryOperationSeriesBuffer) getSingleSeries(ctx context.Context, seriesIndex int) (types.InstantVectorSeriesData, error) {
	for seriesIndex > b.nextIndexToRead {
		d, err := b.source.NextSeries(ctx)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		if b.seriesUsed[b.nextIndexToRead] {
			// We need this series later, but not right now. Store it for later.
			b.buffer[b.nextIndexToRead] = d
		} else {
			// We don't need this series at all, return the slice to the pool now.
			types.PutInstantVectorSeriesData(d, b.memoryConsumptionTracker)
		}

		b.nextIndexToRead++
	}

	if seriesIndex == b.nextIndexToRead {
		// Don't bother buffering data if we can return it directly.
		b.nextIndexToRead++
		return b.source.NextSeries(ctx)
	}

	d := b.buffer[seriesIndex]
	delete(b.buffer, seriesIndex)

	return d, nil
}

func (b *binaryOperationSeriesBuffer) close() {
	if b.seriesUsed != nil {
		types.BoolSlicePool.Put(b.seriesUsed, b.memoryConsumptionTracker)
	}
}

type binaryOperationFunc func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, error)

// FIXME(jhesketh): Investigate avoiding copying histograms for binary ops.
var arithmeticOperationFuncs = map[parser.ItemType]binaryOperationFunc{
	parser.ADD: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, error) {
		if hlhs != nil && hrhs != nil {
			res, err := hlhs.Copy().Add(hrhs)
			if err != nil {
				return 0, nil, false, err
			}
			return 0, res.Compact(0), true, nil
		}
		return lhs + rhs, nil, true, nil
	},
	parser.SUB: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, error) {
		if hlhs != nil && hrhs != nil {
			res, err := hlhs.Copy().Sub(hrhs)
			if err != nil {
				return 0, nil, false, err
			}
			return 0, res.Compact(0), true, nil
		}
		return lhs - rhs, nil, true, nil
	},
	parser.MUL: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, error) {
		if hlhs != nil && hrhs == nil {
			return 0, hlhs.Copy().Mul(rhs), true, nil
		}
		if hlhs == nil && hrhs != nil {
			return 0, hrhs.Copy().Mul(lhs), true, nil
		}
		return lhs * rhs, nil, true, nil
	},
	parser.DIV: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, error) {
		if hlhs != nil && hrhs == nil {
			return 0, hlhs.Copy().Div(rhs), true, nil
		}
		return lhs / rhs, nil, true, nil
	},
	parser.POW: func(lhs, rhs float64, _, _ *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, error) {
		return math.Pow(lhs, rhs), nil, true, nil
	},
	parser.MOD: func(lhs, rhs float64, _, _ *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, error) {
		return math.Mod(lhs, rhs), nil, true, nil
	},
	parser.ATAN2: func(lhs, rhs float64, _, _ *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, error) {
		return math.Atan2(lhs, rhs), nil, true, nil
	},
}

// seriesForOneGroupSide couples data and sourceSeriesIndices for use by mergeOneSide.
// Specifically it is used for histogramSideSorter and floatSideSorter which providing a sorting interface
// (sort.Interface) which will sort both data and sourceSeriesIndices at the same time to retain a mapping
// of their indexes on either the present float timestamps or histogram timestamps.
type seriesForOneGroupSide struct {
	data                []types.InstantVectorSeriesData
	sourceSeriesIndices []int
}

// floatSideSorter sorts side by the timestamp of the first float point in each series.
// It maintains the order of both data and sourceSeriesIndices.
type floatSideSorter struct {
	side seriesForOneGroupSide
}

func (f floatSideSorter) Len() int {
	return len(f.side.data)
}

func (f floatSideSorter) Swap(i, j int) {
	f.side.data[i], f.side.data[j] = f.side.data[j], f.side.data[i]
	f.side.sourceSeriesIndices[i], f.side.sourceSeriesIndices[j] = f.side.sourceSeriesIndices[j], f.side.sourceSeriesIndices[i]
}

func (f floatSideSorter) Less(i, j int) bool {
	if len(f.side.data[i].Floats) == 0 {
		return false
	}
	if len(f.side.data[j].Floats) == 0 {
		return true
	}
	return f.side.data[i].Floats[0].T < f.side.data[j].Floats[0].T
}

// histogramSideSorter sorts side by the timestamp of the first histogram point in each series.
// It maintains the order of both data and sourceSeriesIndices.
type histogramSideSorter struct {
	side seriesForOneGroupSide
}

func (h histogramSideSorter) Len() int {
	return len(h.side.data)
}

func (h histogramSideSorter) Swap(i, j int) {
	h.side.data[i], h.side.data[j] = h.side.data[j], h.side.data[i]
	h.side.sourceSeriesIndices[i], h.side.sourceSeriesIndices[j] = h.side.sourceSeriesIndices[j], h.side.sourceSeriesIndices[i]
}

func (h histogramSideSorter) Less(i, j int) bool {
	if len(h.side.data[i].Histograms) == 0 {
		return false
	}
	if len(h.side.data[j].Histograms) == 0 {
		return true
	}
	return h.side.data[i].Histograms[0].T < h.side.data[j].Histograms[0].T
}
