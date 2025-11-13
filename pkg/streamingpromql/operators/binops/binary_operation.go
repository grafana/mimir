// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/grafana/regexp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
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
			buf = l.BytesWithoutLabels(buf, model.MetricNameLabel)
			return buf
		}
	}

	lbls := make([]string, 0, len(vectorMatching.MatchingLabels)+1)
	lbls = append(lbls, model.MetricNameLabel)
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
		lbls := vectorMatching.MatchingLabels

		// We never want to include __name__, even if it's explicitly mentioned in on(...).
		// See https://github.com/prometheus/prometheus/issues/16631.
		if i := slices.Index(vectorMatching.MatchingLabels, model.MetricNameLabel); i != -1 {
			lbls = make([]string, 0, len(vectorMatching.MatchingLabels)-1)
			lbls = append(lbls, vectorMatching.MatchingLabels[:i]...)
			lbls = append(lbls, vectorMatching.MatchingLabels[i+1:]...)
		}

		return func(l labels.Labels) labels.Labels {
			lb.Reset(l)
			lb.Keep(lbls...)
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
		lb.Del(model.MetricNameLabel)
		lb.Del(vectorMatching.MatchingLabels...)
		return lb.Labels()
	}
}

func formatConflictError(
	firstConflictingSeriesIndex int,
	secondConflictingSeriesIndex int,
	description string,
	ts int64,
	sourceSeriesMetadata []types.SeriesMetadata,
	side string,
	vectorMatching parser.VectorMatching,
	op parser.ItemType,
	returnBool bool,
) error {
	firstConflictingSeriesLabels := sourceSeriesMetadata[firstConflictingSeriesIndex].Labels
	groupLabels := groupLabelsFunc(vectorMatching, op, returnBool)(firstConflictingSeriesLabels)

	if secondConflictingSeriesIndex == -1 {
		return fmt.Errorf(
			"found %s for the match group %s on the %s side of the operation at timestamp %s",
			description,
			groupLabels,
			side,
			timestamp.Time(ts).Format(time.RFC3339Nano),
		)
	}

	secondConflictingSeriesLabels := sourceSeriesMetadata[secondConflictingSeriesIndex].Labels

	return fmt.Errorf(
		"found %s for the match group %s on the %s side of the operation at timestamp %s: %s and %s",
		description,
		groupLabels,
		side,
		timestamp.Time(ts).Format(time.RFC3339Nano),
		firstConflictingSeriesLabels,
		secondConflictingSeriesLabels,
	)
}

// filterSeries returns data filtered based on the mask provided.
//
// mask is expected to contain one value for each time step in the query time range.
// Samples in data where mask has value desiredMaskValue are returned.
//
// The return value reuses the slices from data, and returns any unused slices to the pool.
func filterSeries(data types.InstantVectorSeriesData, mask []bool, desiredMaskValue bool, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, timeRange types.QueryTimeRange) (types.InstantVectorSeriesData, error) {
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
		types.FPointSlicePool.Put(&data.Floats, memoryConsumptionTracker)
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
		types.HPointSlicePool.Put(&data.Histograms, memoryConsumptionTracker)
	}

	return filteredData, nil
}

// emitIncompatibleTypesAnnotation adds an annotation to a given the presence of histograms on the left (lH) and right (rH) sides of op.
// If lH is nil, this indicates that the left side was a float, and similarly for the right side and rH.
// If lH is not nil, this indicates that the left side was a histogram, and similarly for the right side and rH.
func emitIncompatibleTypesAnnotation(a *annotations.Annotations, op parser.ItemType, lH *histogram.FloatHistogram, rH *histogram.FloatHistogram, expressionPosition posrange.PositionRange) {
	a.Add(annotations.NewIncompatibleTypesInBinOpInfo(sampleTypeDescription(lH), op.String(), sampleTypeDescription(rH), expressionPosition))
}

func emitIncompatibleBucketLayoutAnnotation(a *annotations.Annotations, op parser.ItemType, expressionPosition posrange.PositionRange) {
	a.Add(annotations.NewIncompatibleBucketLayoutInBinOpWarning(op.String(), expressionPosition))
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
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	annotations              *annotations.Annotations
	expressionPosition       posrange.PositionRange
	emitAnnotation           types.EmitAnnotationFunc
}

func newVectorVectorBinaryOperationEvaluator(
	op parser.ItemType,
	returnBool bool,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
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

	e.emitAnnotation = func(generator types.AnnotationGenerator) {
		e.annotations.Add(generator("", e.expressionPosition))
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
	lT, lF, lH, lHIndex, lOk := e.leftIterator.Next()
	rT, rF, rH, rHIndex, rOk := e.rightIterator.Next()

	appendHistogram := func(t int64, h *histogram.FloatHistogram) error {
		if hPoints == nil {
			if err := prepareHSlice(); err != nil {
				return err
			}
		}

		// Check if we're reusing the FloatHistogram from either side.
		// If so, remove it so that it is not modified when the slice is reused.
		if h == lH {
			left.Histograms[lHIndex].H = nil
		}

		if h == rH {
			right.Histograms[rHIndex].H = nil
		}

		hPoints = append(hPoints, promql.HPoint{
			H: h,
			T: t,
		})

		return nil
	}

	appendFloat := func(t int64, f float64) error {
		if fPoints == nil {
			if err := prepareFSlice(); err != nil {
				return err
			}
		}

		fPoints = append(fPoints, promql.FPoint{
			F: f,
			T: t,
		})

		return nil
	}

	appendNextSample := func() error {
		resultFloat, resultHist, keep, valid, err := e.opFunc(lF, rF, lH, rH, takeOwnershipOfLeft, takeOwnershipOfRight, e.emitAnnotation)

		if err != nil {
			if errors.Is(err, histogram.ErrHistogramsIncompatibleSchema) {
				emitIncompatibleBucketLayoutAnnotation(e.annotations, e.op, e.expressionPosition)
				err = nil
			}

			if err != nil {
				return err
			}

			// Else: error was converted to an annotation, continue without emitting a sample here.
			keep = false
		}

		if !valid {
			emitIncompatibleTypesAnnotation(e.annotations, e.op, lH, rH, e.expressionPosition)
		}

		if !keep {
			return nil
		}

		if resultHist != nil {
			return appendHistogram(lT, resultHist)
		}

		return appendFloat(lT, resultFloat)
	}

	// Continue iterating until we exhaust either the LHS or RHS
	// denoted by lOk or rOk being false.
	for lOk && rOk {
		if lT == rT {
			// We have samples on both sides at this timestep.
			if err := appendNextSample(); err != nil {
				return types.InstantVectorSeriesData{}, err
			}
		}

		// Advance the iterator with the lower timestamp, or both if equal
		if lT == rT {
			lT, lF, lH, lHIndex, lOk = e.leftIterator.Next()
			rT, rF, rH, rHIndex, rOk = e.rightIterator.Next()
		} else if lT < rT {
			lT, lF, lH, lHIndex, lOk = e.leftIterator.Next()
		} else {
			rT, rF, rH, rHIndex, rOk = e.rightIterator.Next()
		}
	}

	// Cleanup the unused slices.
	if canReturnLeftFPointSlice {
		types.FPointSlicePool.Put(&left.Floats, e.memoryConsumptionTracker)
	}
	if canReturnLeftHPointSlice {
		types.HPointSlicePool.Put(&left.Histograms, e.memoryConsumptionTracker)
	}
	if canReturnRightFPointSlice {
		types.FPointSlicePool.Put(&right.Floats, e.memoryConsumptionTracker)
	}
	if canReturnRightHPointSlice {
		types.HPointSlicePool.Put(&right.Histograms, e.memoryConsumptionTracker)
	}

	return types.InstantVectorSeriesData{
		Floats:     fPoints,
		Histograms: hPoints,
	}, nil
}

type binaryOperationFunc func(
	lF, rF float64,
	lH, rH *histogram.FloatHistogram,
	canMutateLeft, canMutateRight bool,
	emitAnnotation types.EmitAnnotationFunc,
) (f float64, h *histogram.FloatHistogram, keep bool, valid bool, err error)

var arithmeticAndComparisonOperationFuncs = map[parser.ItemType]binaryOperationFunc{
	parser.ADD: func(lF, rF float64, lH, rH *histogram.FloatHistogram, canMutateLeft, canMutateRight bool, emitAnnotation types.EmitAnnotationFunc) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if lH == nil && rH == nil {
			return lF + rF, nil, true, true, nil
		}

		if lH != nil && rH != nil {
			var res *histogram.FloatHistogram
			var nhcbBoundsReconciled bool
			var err error

			if canMutateLeft {
				res, _, nhcbBoundsReconciled, err = lH.Add(rH)
			} else if canMutateRight {
				res, _, nhcbBoundsReconciled, err = rH.Add(lH)
			} else {
				res, _, nhcbBoundsReconciled, err = lH.Copy().Add(rH)
			}

			if err != nil {
				return 0, nil, false, true, err
			}

			if nhcbBoundsReconciled {
				emitAnnotation(func(_ string, pos posrange.PositionRange) error {
					return annotations.NewMismatchedCustomBucketsHistogramsInfo(pos, annotations.HistogramAdd)
				})
			}

			return 0, res.Compact(0), true, true, nil
		}

		return 0, nil, false, false, nil
	},
	parser.SUB: func(lF, rF float64, lH, rH *histogram.FloatHistogram, canMutateLeft, canMutateRight bool, emitAnnotation types.EmitAnnotationFunc) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if lH == nil && rH == nil {
			return lF - rF, nil, true, true, nil
		}

		if lH != nil && rH != nil {
			var res *histogram.FloatHistogram
			var nhcbBoundsReconciled bool
			var err error

			if canMutateLeft {
				res, _, nhcbBoundsReconciled, err = lH.Sub(rH)
			} else if canMutateRight {
				res, _, nhcbBoundsReconciled, err = rH.Mul(-1).Add(lH)
			} else {
				res, _, nhcbBoundsReconciled, err = lH.Copy().Sub(rH)
			}

			if err != nil {
				return 0, nil, false, true, err
			}

			if nhcbBoundsReconciled {
				emitAnnotation(func(_ string, pos posrange.PositionRange) error {
					return annotations.NewMismatchedCustomBucketsHistogramsInfo(pos, annotations.HistogramSub)
				})
			}

			return 0, res.Compact(0), true, true, nil
		}

		return 0, nil, false, false, nil
	},
	parser.MUL: func(lF, rF float64, lH, rH *histogram.FloatHistogram, canMutateLeft, canMutateRight bool, _ types.EmitAnnotationFunc) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if lH == nil && rH == nil {
			return lF * rF, nil, true, true, nil
		}

		if lH != nil && rH == nil {
			if !canMutateLeft {
				lH = lH.Copy()
			}

			return 0, lH.Mul(rF), true, true, nil
		}

		if lH == nil && rH != nil {
			if !canMutateRight {
				rH = rH.Copy()
			}

			return 0, rH.Mul(lF), true, true, nil
		}

		return 0, nil, false, false, nil
	},
	parser.DIV: func(lF, rF float64, lH, rH *histogram.FloatHistogram, canMutateLeft, _ bool, _ types.EmitAnnotationFunc) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if lH == nil && rH == nil {
			return lF / rF, nil, true, true, nil
		}

		if lH != nil && rH == nil {
			if !canMutateLeft {
				lH = lH.Copy()
			}

			return 0, lH.Div(rF), true, true, nil
		}

		return 0, nil, false, false, nil
	},
	parser.POW: func(lF, rF float64, lH, rH *histogram.FloatHistogram, _, _ bool, _ types.EmitAnnotationFunc) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if lH == nil && rH == nil {
			return math.Pow(lF, rF), nil, true, true, nil
		}

		return 0, nil, false, false, nil
	},
	parser.MOD: func(lF, rF float64, lH, rH *histogram.FloatHistogram, _, _ bool, _ types.EmitAnnotationFunc) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if lH == nil && rH == nil {
			return math.Mod(lF, rF), nil, true, true, nil
		}

		return 0, nil, false, false, nil
	},
	parser.ATAN2: func(lF, rF float64, lH, rH *histogram.FloatHistogram, _, _ bool, _ types.EmitAnnotationFunc) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if lH == nil && rH == nil {
			return math.Atan2(lF, rF), nil, true, true, nil
		}

		return 0, nil, false, false, nil
	},
	parser.EQLC: func(lF, rF float64, lH, rH *histogram.FloatHistogram, canMutateLeft, canMutateRight bool, _ types.EmitAnnotationFunc) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if lH == nil && rH == nil {
			if lF == rF {
				return lF, nil, true, true, nil
			}

			return 0, nil, false, true, nil
		}

		if lH != nil && rH != nil {
			if lH.Equals(rH) {
				var res *histogram.FloatHistogram

				if canMutateLeft {
					res = lH
				} else if canMutateRight {
					res = rH
				} else {
					res = lH.Copy()
				}

				return 0, res, true, true, nil
			}

			return 0, nil, false, true, nil
		}

		return 0, nil, false, false, nil
	},
	parser.NEQ: func(lF, rF float64, lH, rH *histogram.FloatHistogram, canMutateLeft, _ bool, _ types.EmitAnnotationFunc) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if lH == nil && rH == nil {
			if lF != rF {
				return lF, nil, true, true, nil
			}

			return 0, nil, false, true, nil
		}

		if lH != nil && rH != nil {
			if !lH.Equals(rH) {
				if !canMutateLeft {
					lH = lH.Copy()
				}

				return 0, lH, true, true, nil
			}

			return 0, nil, false, true, nil
		}

		return lF, lH, false, false, nil
	},
	parser.LTE: func(lF, rF float64, lH, rH *histogram.FloatHistogram, _, _ bool, _ types.EmitAnnotationFunc) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if lH != nil || rH != nil {
			return 0, nil, false, false, nil
		}

		if lF <= rF {
			return lF, nil, true, true, nil
		}

		return 0, nil, false, true, nil
	},
	parser.LSS: func(lF, rF float64, lH, rH *histogram.FloatHistogram, _, _ bool, _ types.EmitAnnotationFunc) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if lH != nil || rH != nil {
			return 0, nil, false, false, nil
		}

		if lF < rF {
			return lF, nil, true, true, nil
		}

		return 0, nil, false, true, nil
	},
	parser.GTE: func(lF, rF float64, lH, rH *histogram.FloatHistogram, _, _ bool, _ types.EmitAnnotationFunc) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if lH != nil || rH != nil {
			return 0, nil, false, false, nil
		}

		if lF >= rF {
			return lF, nil, true, true, nil
		}

		return 0, nil, false, true, nil
	},
	parser.GTR: func(lF, rF float64, lH, rH *histogram.FloatHistogram, _, _ bool, _ types.EmitAnnotationFunc) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if lH != nil || rH != nil {
			return 0, nil, false, false, nil
		}

		if lF > rF {
			return lF, nil, true, true, nil
		}

		return 0, nil, false, true, nil
	},
}

var boolComparisonOperationFuncs = map[parser.ItemType]binaryOperationFunc{
	parser.EQLC: func(lF, rF float64, lH, rH *histogram.FloatHistogram, _, _ bool, _ types.EmitAnnotationFunc) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if lH == nil && rH == nil {
			if lF == rF {
				return 1, nil, true, true, nil
			}

			return 0, nil, true, true, nil
		}

		if lH != nil && rH != nil {
			if lH.Equals(rH) {
				return 1, nil, true, true, nil
			}

			return 0, nil, true, true, nil
		}

		return 0, nil, false, false, nil
	},
	parser.NEQ: func(lF, rF float64, lH, rH *histogram.FloatHistogram, _, _ bool, _ types.EmitAnnotationFunc) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if lH == nil && rH == nil {
			if lF != rF {
				return 1, nil, true, true, nil
			}

			return 0, nil, true, true, nil
		}

		if lH != nil && rH != nil {
			if !lH.Equals(rH) {
				return 1, nil, true, true, nil
			}

			return 0, nil, true, true, nil
		}

		return 0, nil, false, false, nil
	},
	parser.LTE: func(lF, rF float64, lH, rH *histogram.FloatHistogram, _, _ bool, _ types.EmitAnnotationFunc) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if lH != nil || rH != nil {
			return 0, nil, false, false, nil
		}

		if lF <= rF {
			return 1, nil, true, true, nil
		}

		return 0, nil, true, true, nil
	},
	parser.LSS: func(lF, rF float64, lH, rH *histogram.FloatHistogram, _, _ bool, _ types.EmitAnnotationFunc) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if lH != nil || rH != nil {
			return 0, nil, false, false, nil
		}

		if lF < rF {
			return 1, nil, true, true, nil
		}

		return 0, nil, true, true, nil
	},
	parser.GTE: func(lF, rF float64, lH, rH *histogram.FloatHistogram, _, _ bool, _ types.EmitAnnotationFunc) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if lH != nil || rH != nil {
			return 0, nil, false, false, nil
		}

		if lF >= rF {
			return 1, nil, true, true, nil
		}

		return 0, nil, true, true, nil
	},
	parser.GTR: func(lF, rF float64, lH, rH *histogram.FloatHistogram, _, _ bool, _ types.EmitAnnotationFunc) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if lH != nil || rH != nil {
			return 0, nil, false, false, nil
		}

		if lF > rF {
			return 1, nil, true, true, nil
		}

		return 0, nil, true, true, nil
	},
}

// Hints are hints that can be applied to binary operations to avoid doing unnecessary work.
type Hints struct {
	Include []string
}

const (
	maxHintMatcherValues = 64
)

// BuildMatchers builds matchers to limit the data selected on one side of binary operation
// based on the series returned by the other side and QueryHints as determined by the query
// planner. If there are more than a hard-coded maximum number of values for the hinted labels
// matchers for that label are skipped.
func BuildMatchers(metadata []types.SeriesMetadata, hints *Hints) types.Matchers {
	var matchers []types.Matcher

	for _, label := range hints.Include {
		values := getUniqueLabelValues(metadata, label, maxHintMatcherValues)

		if len(values) > 0 {
			ordered := make([]string, 0, len(values))
			for k := range maps.Keys(values) {
				ordered = append(ordered, regexp.QuoteMeta(k))
			}

			// It's important that the values we're matching against for each matcher are in the
			// same order because we deduplicate matchers before passing them to a queryable.
			slices.Sort(ordered)

			matchers = append(matchers, types.Matcher{
				Type:  labels.MatchRegexp,
				Name:  label,
				Value: strings.Join(ordered, "|"),
			})
		}
	}

	return matchers
}

func getUniqueLabelValues(metadata []types.SeriesMetadata, label string, maxValues int) map[string]struct{} {
	values := make(map[string]struct{})

	for _, series := range metadata {
		// Stop getting values from each series if we're past the max number of
		// values that we'll include in a matcher. In this case, we can't use the
		// values collected so far to build a matcher.
		if len(values) >= maxValues {
			return map[string]struct{}{}
		}

		val := series.Labels.Get(label)
		if val != "" {
			values[val] = struct{}{}
		}
	}

	return values
}
