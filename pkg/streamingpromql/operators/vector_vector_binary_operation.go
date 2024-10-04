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
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// VectorVectorBinaryOperation represents a binary operation between instant vectors such as "<expr> + <expr>" or "<expr> - <expr>".
type VectorVectorBinaryOperation struct {
	Left                     types.InstantVectorOperator
	Right                    types.InstantVectorOperator
	Op                       parser.ItemType
	ReturnBool               bool
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	VectorMatching parser.VectorMatching

	// We need to retain these so that NextSeries() can return an error message with the series labels when
	// multiple points match on a single side.
	// Note that we don't retain the output series metadata: if we need to return an error message, we can compute
	// the output series labels from these again.
	leftMetadata  []types.SeriesMetadata
	rightMetadata []types.SeriesMetadata

	remainingSeries []*binaryOperationOutputSeries
	leftBuffer      *InstantVectorOperatorBuffer
	rightBuffer     *InstantVectorOperatorBuffer
	leftIterator    types.InstantVectorSeriesDataIterator
	rightIterator   types.InstantVectorSeriesDataIterator
	opFunc          binaryOperationFunc

	expressionPosition posrange.PositionRange
	emitAnnotation     functions.EmitAnnotationFunc
}

var _ types.InstantVectorOperator = &VectorVectorBinaryOperation{}

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

func NewVectorVectorBinaryOperation(
	left types.InstantVectorOperator,
	right types.InstantVectorOperator,
	vectorMatching parser.VectorMatching,
	op parser.ItemType,
	returnBool bool,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
) (*VectorVectorBinaryOperation, error) {
	b := &VectorVectorBinaryOperation{
		Left:                     left,
		Right:                    right,
		leftIterator:             types.InstantVectorSeriesDataIterator{},
		rightIterator:            types.InstantVectorSeriesDataIterator{},
		VectorMatching:           vectorMatching,
		Op:                       op,
		ReturnBool:               returnBool,
		MemoryConsumptionTracker: memoryConsumptionTracker,

		expressionPosition: expressionPosition,
	}

	if returnBool {
		b.opFunc = boolComparisonOperationFuncs[op]
	} else {
		b.opFunc = arithmeticAndComparisonOperationFuncs[op]
	}

	if b.opFunc == nil {
		return nil, compat.NewNotSupportedError(fmt.Sprintf("binary expression with '%s'", op))
	}

	b.emitAnnotation = func(generator functions.AnnotationGenerator) {
		annotations.Add(generator("", expressionPosition))
	}

	return b, nil
}

func (b *VectorVectorBinaryOperation) ExpressionPosition() posrange.PositionRange {
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
func (b *VectorVectorBinaryOperation) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
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

	b.leftBuffer = NewInstantVectorOperatorBuffer(b.Left, leftSeriesUsed, b.MemoryConsumptionTracker)
	b.rightBuffer = NewInstantVectorOperatorBuffer(b.Right, rightSeriesUsed, b.MemoryConsumptionTracker)

	return allMetadata, nil
}

// loadSeriesMetadata loads series metadata from both sides of this operation.
// It returns false if one side returned no series and that means there is no way for this operation to return any series.
// (eg. if doing A + B and either A or B have no series, then there is no way for this operation to produce any series)
func (b *VectorVectorBinaryOperation) loadSeriesMetadata(ctx context.Context) (bool, error) {
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
func (b *VectorVectorBinaryOperation) computeOutputSeries() ([]types.SeriesMetadata, []*binaryOperationOutputSeries, []bool, []bool, error) {
	labelsFunc := b.groupLabelsFunc()
	groupKeyFunc := vectorMatchingGroupKeyFunc(b.VectorMatching)
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
		groupKey := groupKeyFunc(s.Labels)
		series, exists := outputSeriesMap[string(groupKey)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if !exists {
			series = &binaryOperationOutputSeries{}
			outputSeriesMap[string(groupKey)] = series
		}

		if smallerSideIsLeftSide {
			series.leftSeriesIndices = append(series.leftSeriesIndices, idx)
		} else {
			series.rightSeriesIndices = append(series.rightSeriesIndices, idx)
		}
	}

	for idx, s := range largerSide {
		groupKey := groupKeyFunc(s.Labels)

		// Important: don't extract the string(...) call below - passing it directly allows us to avoid allocating it.
		if series, exists := outputSeriesMap[string(groupKey)]; exists {
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
func (b *VectorVectorBinaryOperation) sortSeries(metadata []types.SeriesMetadata, series []*binaryOperationOutputSeries) {
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

// groupLabelsFunc returns a function that computes the labels of the output group this series belongs to.
func (b *VectorVectorBinaryOperation) groupLabelsFunc() func(labels.Labels) labels.Labels {
	lb := labels.NewBuilder(labels.EmptyLabels())

	if b.VectorMatching.On {
		return func(l labels.Labels) labels.Labels {
			lb.Reset(l)
			lb.Keep(b.VectorMatching.MatchingLabels...)
			return lb.Labels()
		}
	}

	if b.Op.IsComparisonOperator() && !b.ReturnBool {
		// If this is a comparison operator, we want to retain the metric name, as the comparison acts like a filter.
		return func(l labels.Labels) labels.Labels {
			lb.Reset(l)
			lb.Del(b.VectorMatching.MatchingLabels...)
			return lb.Labels()
		}
	}

	return func(l labels.Labels) labels.Labels {
		lb.Reset(l)
		lb.Del(labels.MetricName)
		lb.Del(b.VectorMatching.MatchingLabels...)
		return lb.Labels()
	}
}

func (b *VectorVectorBinaryOperation) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if len(b.remainingSeries) == 0 {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	thisSeries := b.remainingSeries[0]
	b.remainingSeries = b.remainingSeries[1:]

	allLeftSeries, err := b.leftBuffer.GetSeries(ctx, thisSeries.leftSeriesIndices)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	mergedLeftSide, err := b.mergeOneSide(allLeftSeries, thisSeries.leftSeriesIndices, b.leftMetadata, "left")
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	allRightSeries, err := b.rightBuffer.GetSeries(ctx, thisSeries.rightSeriesIndices)
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
// NOTE: mergeOneSide has the side effect of re-ordering both data and sourceSeriesIndices.
//
// FIXME: for many-to-one / one-to-many matching, we could avoid re-merging each time for the side used multiple times
func (b *VectorVectorBinaryOperation) mergeOneSide(data []types.InstantVectorSeriesData, sourceSeriesIndices []int, sourceSeriesMetadata []types.SeriesMetadata, side string) (types.InstantVectorSeriesData, error) {
	merged, conflict, err := MergeSeries(data, sourceSeriesIndices, b.MemoryConsumptionTracker)

	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if conflict != nil {
		return types.InstantVectorSeriesData{}, b.mergeConflictToError(conflict, sourceSeriesMetadata, side)
	}

	return merged, nil
}

func (b *VectorVectorBinaryOperation) mergeConflictToError(conflict *MergeConflict, sourceSeriesMetadata []types.SeriesMetadata, side string) error {
	firstConflictingSeriesLabels := sourceSeriesMetadata[conflict.firstConflictingSeriesIndex].Labels
	groupLabels := b.groupLabelsFunc()(firstConflictingSeriesLabels)

	if conflict.secondConflictingSeriesIndex == -1 {
		return fmt.Errorf(
			"found %s for the match group %s on the %s side of the operation at timestamp %s",
			conflict.description,
			groupLabels,
			side,
			timestamp.Time(conflict.timestamp).Format(time.RFC3339Nano),
		)
	}

	secondConflictingSeriesLabels := sourceSeriesMetadata[conflict.secondConflictingSeriesIndex].Labels

	return fmt.Errorf(
		"found %s for the match group %s on the %s side of the operation at timestamp %s: %s and %s",
		conflict.description,
		groupLabels,
		side,
		timestamp.Time(conflict.timestamp).Format(time.RFC3339Nano),
		firstConflictingSeriesLabels,
		secondConflictingSeriesLabels,
	)
}

func (b *VectorVectorBinaryOperation) computeResult(left types.InstantVectorSeriesData, right types.InstantVectorSeriesData) (types.InstantVectorSeriesData, error) {
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
			return nil
		}
		if !mixedPoints && maxPoints <= cap(right.Floats) {
			// Can otherwise fit in the right side
			canReturnRightFPointSlice = false
			fPoints = right.Floats[:0]
			return nil
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
			return nil
		}
		if !mixedPoints && maxPoints <= cap(right.Histograms) {
			// Can otherwise fit in the right side
			canReturnRightHPointSlice = false
			hPoints = right.Histograms[:0]
			return nil
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
				err = functions.NativeHistogramErrorToAnnotation(err, b.emitAnnotation)
				if err == nil {
					// Error was converted to an annotation, continue without emitting a sample here.
					ok = false
				} else {
					return types.InstantVectorSeriesData{}, err
				}
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

func (b *VectorVectorBinaryOperation) Close() {
	b.Left.Close()
	b.Right.Close()

	if b.leftMetadata != nil {
		types.PutSeriesMetadataSlice(b.leftMetadata)
	}

	if b.rightMetadata != nil {
		types.PutSeriesMetadataSlice(b.rightMetadata)
	}

	if b.leftBuffer != nil {
		b.leftBuffer.Close()
	}

	if b.rightBuffer != nil {
		b.rightBuffer.Close()
	}
}

type binaryOperationFunc func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, error)

// FIXME(jhesketh): Investigate avoiding copying histograms for binary ops.
var arithmeticAndComparisonOperationFuncs = map[parser.ItemType]binaryOperationFunc{
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
	parser.EQLC: func(lhs, rhs float64, _, _ *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, error) {
		if lhs == rhs {
			return lhs, nil, true, nil
		}

		return 0, nil, false, nil
	},
	parser.NEQ: func(lhs, rhs float64, _, _ *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, error) {
		if lhs != rhs {
			return lhs, nil, true, nil
		}

		return 0, nil, false, nil
	},
	parser.LTE: func(lhs, rhs float64, _, _ *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, error) {
		if lhs <= rhs {
			return lhs, nil, true, nil
		}

		return 0, nil, false, nil
	},
	parser.LSS: func(lhs, rhs float64, _, _ *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, error) {
		if lhs < rhs {
			return lhs, nil, true, nil
		}

		return 0, nil, false, nil
	},
	parser.GTE: func(lhs, rhs float64, _, _ *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, error) {
		if lhs >= rhs {
			return lhs, nil, true, nil
		}

		return 0, nil, false, nil
	},
	parser.GTR: func(lhs, rhs float64, _, _ *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, error) {
		if lhs > rhs {
			return lhs, nil, true, nil
		}

		return 0, nil, false, nil
	},
}

var boolComparisonOperationFuncs = map[parser.ItemType]binaryOperationFunc{
	parser.EQLC: func(lhs, rhs float64, _, _ *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, error) {
		if lhs == rhs {
			return 1, nil, true, nil
		}

		return 0, nil, true, nil
	},
	parser.NEQ: func(lhs, rhs float64, _, _ *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, error) {
		if lhs != rhs {
			return 1, nil, true, nil
		}

		return 0, nil, true, nil
	},
	parser.LTE: func(lhs, rhs float64, _, _ *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, error) {
		if lhs <= rhs {
			return 1, nil, true, nil
		}

		return 0, nil, true, nil
	},
	parser.LSS: func(lhs, rhs float64, _, _ *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, error) {
		if lhs < rhs {
			return 1, nil, true, nil
		}

		return 0, nil, true, nil
	},
	parser.GTE: func(lhs, rhs float64, _, _ *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, error) {
		if lhs >= rhs {
			return 1, nil, true, nil
		}

		return 0, nil, true, nil
	},
	parser.GTR: func(lhs, rhs float64, _, _ *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, error) {
		if lhs > rhs {
			return 1, nil, true, nil
		}

		return 0, nil, true, nil
	},
}
