// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"fmt"
	"slices"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// vectorMatchingGroupKeyFunc returns a function that computes the grouping key of the output group a series belongs to.
//
// The return value from the function is valid until it is called again.
func vectorMatchingGroupKeyFunc(vectorMatching parser.VectorMatching) func(labels.Labels) []byte {
	buf := make([]byte, 0, 1024)

	if vectorMatching.On {
		slices.Sort(vectorMatching.MatchingLabels)

		return func(l labels.Labels) []byte {
			buf = l.BytesWithLabels(buf, vectorMatching.MatchingLabels...)
			return buf
		}
	}

	if len(vectorMatching.MatchingLabels) == 0 {
		// Fast path for common case for expressions like "a + b" with no 'on' or 'without' labels.
		return func(l labels.Labels) []byte {
			buf = l.BytesWithoutLabels(buf, labels.MetricName)
			return buf
		}
	}

	lbls := make([]string, 0, len(vectorMatching.MatchingLabels)+1)
	lbls = append(lbls, labels.MetricName)
	lbls = append(lbls, vectorMatching.MatchingLabels...)
	slices.Sort(lbls)

	return func(l labels.Labels) []byte {
		buf = l.BytesWithoutLabels(buf, lbls...)
		return buf
	}
}

// vectorMatchingGroupLabelsFunc returns a function that computes the labels of the output group a series belongs to.
func groupLabelsFunc(vectorMatching parser.VectorMatching, op parser.ItemType, returnBool bool) func(labels.Labels) labels.Labels {
	lb := labels.NewBuilder(labels.EmptyLabels())

	if vectorMatching.On {
		return func(l labels.Labels) labels.Labels {
			lb.Reset(l)
			lb.Keep(vectorMatching.MatchingLabels...)
			return lb.Labels()
		}
	}

	if op.IsComparisonOperator() && !returnBool {
		// If this is a comparison operator, we want to retain the metric name, as the comparison acts like a filter.
		return func(l labels.Labels) labels.Labels {
			lb.Reset(l)
			lb.Del(vectorMatching.MatchingLabels...)
			return lb.Labels()
		}
	}

	return func(l labels.Labels) labels.Labels {
		lb.Reset(l)
		lb.Del(labels.MetricName)
		lb.Del(vectorMatching.MatchingLabels...)
		return lb.Labels()
	}
}

// filterSeries returns data filtered based on the mask provided.
//
// mask is expected to contain one value for each time step in the query time range.
// Samples in data where mask has value desiredMaskValue are returned.
//
// The return value reuses the slices from data, and returns any unused slices to the pool.
func filterSeries(data types.InstantVectorSeriesData, mask []bool, desiredMaskValue bool, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, timeRange types.QueryTimeRange) (types.InstantVectorSeriesData, error) {
	filteredData := types.InstantVectorSeriesData{}
	nextOutputFloatIndex := 0

	for _, p := range data.Floats {
		if mask[timeRange.PointIndex(p.T)] != desiredMaskValue {
			continue
		}

		data.Floats[nextOutputFloatIndex] = p
		nextOutputFloatIndex++
	}

	if nextOutputFloatIndex > 0 {
		// We have at least one output float point to return.
		filteredData.Floats = data.Floats[:nextOutputFloatIndex]
	} else {
		// We don't have any float points to return, return the original slice to the pool.
		types.FPointSlicePool.Put(data.Floats, memoryConsumptionTracker)
	}

	nextOutputHistogramIndex := 0

	for idx, p := range data.Histograms {
		if mask[timeRange.PointIndex(p.T)] != desiredMaskValue {
			continue
		}

		data.Histograms[nextOutputHistogramIndex] = p

		if idx > nextOutputHistogramIndex {
			// Remove the histogram from the original point to ensure that it's not mutated unexpectedly when the HPoint slice is reused.
			data.Histograms[idx].H = nil
		}

		nextOutputHistogramIndex++
	}

	if nextOutputHistogramIndex > 0 {
		// We have at least one output histogram point to return.
		filteredData.Histograms = data.Histograms[:nextOutputHistogramIndex]
	} else {
		// We don't have any histogram points to return, return the original slice to the pool.
		types.HPointSlicePool.Put(data.Histograms, memoryConsumptionTracker)
	}

	return filteredData, nil
}

// emitIncompatibleTypesAnnotation adds an annotation to a given the presence of histograms on the left (lH) and right (rH) sides of op.
// If lH is nil, this indicates that the left side was a float, and similarly for the right side and rH.
// If lH is not nil, this indicates that the left side was a histogram, and similarly for the right side and rH.
func emitIncompatibleTypesAnnotation(a *annotations.Annotations, op parser.ItemType, lH *histogram.FloatHistogram, rH *histogram.FloatHistogram, expressionPosition posrange.PositionRange) {
	a.Add(annotations.NewIncompatibleTypesInBinOpInfo(sampleTypeDescription(lH), op.String(), sampleTypeDescription(rH), expressionPosition))
}

func sampleTypeDescription(h *histogram.FloatHistogram) string {
	if h == nil {
		return "float"
	}

	return "histogram"
}

type vectorVectorBinaryOperationEvaluator struct {
	op                       parser.ItemType
	opFunc                   binaryOperationFunc
	leftIterator             types.InstantVectorSeriesDataIterator
	rightIterator            types.InstantVectorSeriesDataIterator
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker
	annotations              *annotations.Annotations
	expressionPosition       posrange.PositionRange
}

func newVectorVectorBinaryOperationEvaluator(
	op parser.ItemType,
	returnBool bool,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
) (vectorVectorBinaryOperationEvaluator, error) {
	e := vectorVectorBinaryOperationEvaluator{
		op:                       op,
		opFunc:                   nil,
		memoryConsumptionTracker: memoryConsumptionTracker,
		annotations:              annotations,
		expressionPosition:       expressionPosition,
	}

	if returnBool {
		e.opFunc = boolComparisonOperationFuncs[op]
	} else {
		e.opFunc = arithmeticAndComparisonOperationFuncs[op]
	}

	if e.opFunc == nil {
		return vectorVectorBinaryOperationEvaluator{}, compat.NewNotSupportedError(fmt.Sprintf("binary expression with '%s'", op))
	}

	return e, nil

}

func (e *vectorVectorBinaryOperationEvaluator) computeResult(left types.InstantVectorSeriesData, right types.InstantVectorSeriesData, takeOwnershipOfLeft bool, takeOwnershipOfRight bool) (types.InstantVectorSeriesData, error) {
	var fPoints []promql.FPoint
	var hPoints []promql.HPoint

	// For arithmetic and comparison operators, we'll never produce more points than the smaller input side.
	// Because floats and histograms can be multiplied together, we use the sum of both the float and histogram points.
	// We also don't know if the output will be exclusively floats or histograms, so we'll use the same size slice for both.
	// We only assign the slices once we see the associated point type so it shouldn't be common that we allocate both.
	canReturnLeftFPointSlice, canReturnLeftHPointSlice, canReturnRightFPointSlice, canReturnRightHPointSlice := takeOwnershipOfLeft, takeOwnershipOfLeft, takeOwnershipOfRight, takeOwnershipOfRight
	leftPoints := len(left.Floats) + len(left.Histograms)
	rightPoints := len(right.Floats) + len(right.Histograms)
	minPoints := min(leftPoints, rightPoints)

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
	mixedPoints := (len(left.Floats) > 0 && len(left.Histograms) > 0) || (len(right.Floats) > 0 && len(right.Histograms) > 0)

	prepareFSlice := func() error {
		canFitInLeftSide := minPoints <= cap(left.Floats)
		leftSideIsSmaller := cap(left.Floats) < cap(right.Floats)
		safeToReuseLeftSide := !mixedPoints && canFitInLeftSide && takeOwnershipOfLeft
		canFitInRightSide := minPoints <= cap(right.Floats)
		safeToReuseRightSide := !mixedPoints && canFitInRightSide && takeOwnershipOfRight

		if safeToReuseLeftSide && (leftSideIsSmaller || !safeToReuseRightSide) {
			canReturnLeftFPointSlice = false
			fPoints = left.Floats[:0]
			return nil
		}

		if safeToReuseRightSide {
			canReturnRightFPointSlice = false
			fPoints = right.Floats[:0]
			return nil
		}

		// We can't reuse either existing slice, so create a new one.
		var err error
		if fPoints, err = types.FPointSlicePool.Get(minPoints, e.memoryConsumptionTracker); err != nil {
			return err
		}
		return nil
	}

	prepareHSlice := func() error {
		canFitInLeftSide := minPoints <= cap(left.Histograms)
		leftSideIsSmaller := cap(left.Histograms) < cap(right.Histograms)
		safeToReuseLeftSide := !mixedPoints && canFitInLeftSide && takeOwnershipOfLeft
		canFitInRightSide := minPoints <= cap(right.Histograms)
		safeToReuseRightSide := !mixedPoints && canFitInRightSide && takeOwnershipOfRight

		if safeToReuseLeftSide && (leftSideIsSmaller || !safeToReuseRightSide) {
			canReturnLeftHPointSlice = false
			hPoints = left.Histograms[:0]
			return nil
		}

		if safeToReuseRightSide {
			canReturnRightHPointSlice = false
			hPoints = right.Histograms[:0]
			return nil
		}

		// We can't reuse either existing slice, so create a new one.
		var err error
		if hPoints, err = types.HPointSlicePool.Get(minPoints, e.memoryConsumptionTracker); err != nil {
			return err
		}
		return nil
	}

	e.leftIterator.Reset(left)
	e.rightIterator.Reset(right)

	// Get first sample from left and right
	lT, lF, lH, lOk := e.leftIterator.Next()
	rT, rF, rH, rOk := e.rightIterator.Next()
	// Continue iterating until we exhaust either the LHS or RHS
	// denoted by lOk or rOk being false.
	for lOk && rOk {
		if lT == rT {
			// We have samples on both sides at this timestep.
			resultFloat, resultHist, keep, valid, err := e.opFunc(lF, rF, lH, rH)

			if err != nil {
				err = functions.NativeHistogramErrorToAnnotation(err, e.emitAnnotation)
				if err != nil {
					return types.InstantVectorSeriesData{}, err
				}

				// Else: error was converted to an annotation, continue without emitting a sample here.
				keep = false
			}

			if !valid {
				emitIncompatibleTypesAnnotation(e.annotations, e.op, lH, rH, e.expressionPosition)
			}

			if keep {
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

		// Advance the iterator with the lower timestamp, or both if equal
		if lT == rT {
			lT, lF, lH, lOk = e.leftIterator.Next()
			rT, rF, rH, rOk = e.rightIterator.Next()
		} else if lT < rT {
			lT, lF, lH, lOk = e.leftIterator.Next()
		} else {
			rT, rF, rH, rOk = e.rightIterator.Next()
		}
	}

	// Cleanup the unused slices.
	if canReturnLeftFPointSlice {
		types.FPointSlicePool.Put(left.Floats, e.memoryConsumptionTracker)
	}
	if canReturnLeftHPointSlice {
		types.HPointSlicePool.Put(left.Histograms, e.memoryConsumptionTracker)
	}
	if canReturnRightFPointSlice {
		types.FPointSlicePool.Put(right.Floats, e.memoryConsumptionTracker)
	}
	if canReturnRightHPointSlice {
		types.HPointSlicePool.Put(right.Histograms, e.memoryConsumptionTracker)
	}

	return types.InstantVectorSeriesData{
		Floats:     fPoints,
		Histograms: hPoints,
	}, nil
}

func (e *vectorVectorBinaryOperationEvaluator) emitAnnotation(generator types.AnnotationGenerator) {
	e.annotations.Add(generator("", e.expressionPosition))
}
