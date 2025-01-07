// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package binops

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// OneToOneVectorVectorBinaryOperation represents a one-to-one binary operation between instant vectors such as "<expr> + <expr>" or "<expr> - <expr>".
// One-to-many and many-to-one binary operations between instant vectors are not supported.
type OneToOneVectorVectorBinaryOperation struct {
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

	remainingSeries []*oneToOneBinaryOperationOutputSeries
	leftBuffer      *operators.InstantVectorOperatorBuffer
	rightBuffer     *operators.InstantVectorOperatorBuffer
	evaluator       vectorVectorBinaryOperationEvaluator

	expressionPosition posrange.PositionRange
	annotations        *annotations.Annotations
}

var _ types.InstantVectorOperator = &OneToOneVectorVectorBinaryOperation{}

type oneToOneBinaryOperationOutputSeries struct {
	leftSeriesIndices  []int
	rightSeriesIndices []int
}

// latestLeftSeries returns the index of the last series from the left source needed for this output series.
//
// It assumes that leftSeriesIndices is sorted in ascending order.
func (s oneToOneBinaryOperationOutputSeries) latestLeftSeries() int {
	return s.leftSeriesIndices[len(s.leftSeriesIndices)-1]
}

// latestRightSeries returns the index of the last series from the right source needed for this output series.
//
// It assumes that rightSeriesIndices is sorted in ascending order.
func (s oneToOneBinaryOperationOutputSeries) latestRightSeries() int {
	return s.rightSeriesIndices[len(s.rightSeriesIndices)-1]
}

func NewOneToOneVectorVectorBinaryOperation(
	left types.InstantVectorOperator,
	right types.InstantVectorOperator,
	vectorMatching parser.VectorMatching,
	op parser.ItemType,
	returnBool bool,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
) (*OneToOneVectorVectorBinaryOperation, error) {
	e, err := newVectorVectorBinaryOperationEvaluator(op, returnBool, memoryConsumptionTracker, annotations, expressionPosition)
	if err != nil {
		return nil, err
	}

	b := &OneToOneVectorVectorBinaryOperation{
		Left:                     left,
		Right:                    right,
		VectorMatching:           vectorMatching,
		Op:                       op,
		ReturnBool:               returnBool,
		MemoryConsumptionTracker: memoryConsumptionTracker,

		evaluator:          e,
		expressionPosition: expressionPosition,
		annotations:        annotations,
	}

	return b, nil
}

func (b *OneToOneVectorVectorBinaryOperation) ExpressionPosition() posrange.PositionRange {
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
func (b *OneToOneVectorVectorBinaryOperation) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
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

	b.leftBuffer = operators.NewInstantVectorOperatorBuffer(b.Left, leftSeriesUsed, b.MemoryConsumptionTracker)
	b.rightBuffer = operators.NewInstantVectorOperatorBuffer(b.Right, rightSeriesUsed, b.MemoryConsumptionTracker)

	return allMetadata, nil
}

// loadSeriesMetadata loads series metadata from both sides of this operation.
// It returns false if one side returned no series and that means there is no way for this operation to return any series.
// (eg. if doing A + B and either A or B have no series, then there is no way for this operation to produce any series)
func (b *OneToOneVectorVectorBinaryOperation) loadSeriesMetadata(ctx context.Context) (bool, error) {
	// We retain the series labels for later so we can use them to generate error messages.
	// We'll return them to the pool in Close().

	var err error
	b.leftMetadata, err = b.Left.SeriesMetadata(ctx)
	if err != nil {
		return false, err
	}

	if len(b.leftMetadata) == 0 {
		// No series on left-hand side, we'll never have any output series.
		return false, nil
	}

	b.rightMetadata, err = b.Right.SeriesMetadata(ctx)
	if err != nil {
		return false, err
	}

	if len(b.rightMetadata) == 0 {
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
func (b *OneToOneVectorVectorBinaryOperation) computeOutputSeries() ([]types.SeriesMetadata, []*oneToOneBinaryOperationOutputSeries, []bool, []bool, error) {
	labelsFunc := groupLabelsFunc(b.VectorMatching, b.Op, b.ReturnBool)
	groupKeyFunc := vectorMatchingGroupKeyFunc(b.VectorMatching)
	outputSeriesMap := map[string]*oneToOneBinaryOperationOutputSeries{}

	// Use the smaller side to populate the map of possible output series first.
	// This should ensure we don't unnecessarily populate the output series map with series that will never match in most cases.
	// (It's possible that all the series on the larger side all belong to the same group, but this is expected to be rare.)
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
			series = &oneToOneBinaryOperationOutputSeries{}
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
	}

	// Remove series that cannot produce samples.
	for seriesLabels, outputSeries := range outputSeriesMap {
		if len(outputSeries.leftSeriesIndices) == 0 || len(outputSeries.rightSeriesIndices) == 0 {
			// No matching series on at least one side for this output series, so output series will have no samples. Remove it.
			delete(outputSeriesMap, seriesLabels)
		}
	}

	allMetadata := types.GetSeriesMetadataSlice(len(outputSeriesMap))
	allSeries := make([]*oneToOneBinaryOperationOutputSeries, 0, len(outputSeriesMap))

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
func (b *OneToOneVectorVectorBinaryOperation) sortSeries(metadata []types.SeriesMetadata, series []*oneToOneBinaryOperationOutputSeries) {
	// For one-to-one matching, we assume that each output series takes one series from each side of the operator.
	// If this is true, then the best order is the one in which we read from the highest cardinality side in order.
	// If we do this, then in the worst case, we'll have to buffer the whole of the lower cardinality side.
	// (Compare this with sorting so that we read the lowest cardinality side in order: in the worst case, we'll have
	// to buffer the whole of the higher cardinality side.)
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
	series   []*oneToOneBinaryOperationOutputSeries
}

type favourLeftSideSorter struct {
	binaryOperationOutputSorter
}

func newFavourLeftSideSorter(metadata []types.SeriesMetadata, series []*oneToOneBinaryOperationOutputSeries) favourLeftSideSorter {
	return favourLeftSideSorter{binaryOperationOutputSorter{metadata, series}}
}

type favourRightSideSorter struct {
	binaryOperationOutputSorter
}

func newFavourRightSideSorter(metadata []types.SeriesMetadata, series []*oneToOneBinaryOperationOutputSeries) favourRightSideSorter {
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

func (b *OneToOneVectorVectorBinaryOperation) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if len(b.remainingSeries) == 0 {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	thisSeries := b.remainingSeries[0]
	b.remainingSeries = b.remainingSeries[1:]

	allLeftSeries, err := b.leftBuffer.GetSeries(ctx, thisSeries.leftSeriesIndices)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	mergedLeftSide, err := b.mergeSingleSide(allLeftSeries, thisSeries.leftSeriesIndices, b.leftMetadata, "left")
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	allRightSeries, err := b.rightBuffer.GetSeries(ctx, thisSeries.rightSeriesIndices)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	mergedRightSide, err := b.mergeSingleSide(allRightSeries, thisSeries.rightSeriesIndices, b.rightMetadata, "right")
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	return b.evaluator.computeResult(mergedLeftSide, mergedRightSide, true, true)
}

// mergeSingleSide exists to handle the case where one side of an output series has different source series at different time steps.
//
// For example, consider the query "left_side + on (env) right_side" with the following source data:
//
//	left_side{env="test", pod="a"} 1 2 _
//	left_side{env="test", pod="b"} _ _ 3
//	right_side{env="test"} 100 200 300
//
// mergeSingleSide will take in both series for left_side and return a single series with the points [1, 2, 3].
//
// mergeSingleSide is optimised for the case where there is only one source series, or the source series do not overlap, as in the example above.
//
// mergeSingleSide has the side effect of re-ordering both data and sourceSeriesIndices.
func (b *OneToOneVectorVectorBinaryOperation) mergeSingleSide(data []types.InstantVectorSeriesData, sourceSeriesIndices []int, sourceSeriesMetadata []types.SeriesMetadata, side string) (types.InstantVectorSeriesData, error) {
	merged, conflict, err := operators.MergeSeries(data, sourceSeriesIndices, b.MemoryConsumptionTracker)

	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if conflict != nil {
		return types.InstantVectorSeriesData{}, b.mergeConflictToError(conflict, sourceSeriesMetadata, side)
	}

	return merged, nil
}

func (b *OneToOneVectorVectorBinaryOperation) mergeConflictToError(conflict *operators.MergeConflict, sourceSeriesMetadata []types.SeriesMetadata, side string) error {
	firstConflictingSeriesLabels := sourceSeriesMetadata[conflict.FirstConflictingSeriesIndex].Labels
	groupLabels := groupLabelsFunc(b.VectorMatching, b.Op, b.ReturnBool)(firstConflictingSeriesLabels)

	if conflict.SecondConflictingSeriesIndex == -1 {
		return fmt.Errorf(
			"found %s for the match group %s on the %s side of the operation at timestamp %s",
			conflict.Description,
			groupLabels,
			side,
			timestamp.Time(conflict.Timestamp).Format(time.RFC3339Nano),
		)
	}

	secondConflictingSeriesLabels := sourceSeriesMetadata[conflict.SecondConflictingSeriesIndex].Labels

	return fmt.Errorf(
		"found %s for the match group %s on the %s side of the operation at timestamp %s: %s and %s",
		conflict.Description,
		groupLabels,
		side,
		timestamp.Time(conflict.Timestamp).Format(time.RFC3339Nano),
		firstConflictingSeriesLabels,
		secondConflictingSeriesLabels,
	)
}

func (b *OneToOneVectorVectorBinaryOperation) Close() {
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

type binaryOperationFunc func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (f float64, h *histogram.FloatHistogram, keep bool, valid bool, err error)

// FIXME(jhesketh): Investigate avoiding copying histograms for binary ops.
// We would need nil-out the retained FloatHistogram instances in their original HPoint slices, to avoid them being modified when the slice is returned to the pool.
var arithmeticAndComparisonOperationFuncs = map[parser.ItemType]binaryOperationFunc{
	parser.ADD: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if hlhs == nil && hrhs == nil {
			return lhs + rhs, nil, true, true, nil
		}

		if hlhs != nil && hrhs != nil {
			res, err := hlhs.Copy().Add(hrhs)
			if err != nil {
				return 0, nil, false, true, err
			}
			return 0, res.Compact(0), true, true, nil
		}

		return 0, nil, false, false, nil
	},
	parser.SUB: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if hlhs == nil && hrhs == nil {
			return lhs - rhs, nil, true, true, nil
		}

		if hlhs != nil && hrhs != nil {
			res, err := hlhs.Copy().Sub(hrhs)
			if err != nil {
				return 0, nil, false, true, err
			}
			return 0, res.Compact(0), true, true, nil
		}

		return 0, nil, false, false, nil
	},
	parser.MUL: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if hlhs == nil && hrhs == nil {
			return lhs * rhs, nil, true, true, nil
		}

		if hlhs != nil && hrhs == nil {
			return 0, hlhs.Copy().Mul(rhs), true, true, nil
		}

		if hlhs == nil && hrhs != nil {
			return 0, hrhs.Copy().Mul(lhs), true, true, nil
		}

		return 0, nil, false, false, nil
	},
	parser.DIV: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if hlhs == nil && hrhs == nil {
			return lhs / rhs, nil, true, true, nil
		}

		if hlhs != nil && hrhs == nil {
			return 0, hlhs.Copy().Div(rhs), true, true, nil
		}

		return 0, nil, false, false, nil
	},
	parser.POW: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if hlhs == nil && hrhs == nil {
			return math.Pow(lhs, rhs), nil, true, true, nil
		}

		return 0, nil, false, false, nil
	},
	parser.MOD: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if hlhs == nil && hrhs == nil {
			return math.Mod(lhs, rhs), nil, true, true, nil
		}

		return 0, nil, false, false, nil
	},
	parser.ATAN2: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if hlhs == nil && hrhs == nil {
			return math.Atan2(lhs, rhs), nil, true, true, nil
		}

		return 0, nil, false, false, nil
	},
	parser.EQLC: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if hlhs == nil && hrhs == nil {
			if lhs == rhs {
				return lhs, nil, true, true, nil
			}

			return 0, nil, false, true, nil
		}

		if hlhs != nil && hrhs != nil {
			if hlhs.Equals(hrhs) {
				return 0, hlhs.Copy(), true, true, nil
			}

			return 0, nil, false, true, nil
		}

		return 0, nil, false, false, nil
	},
	parser.NEQ: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if hlhs == nil && hrhs == nil {
			if lhs != rhs {
				return lhs, nil, true, true, nil
			}

			return 0, nil, false, true, nil
		}

		if hlhs != nil && hrhs != nil {
			if !hlhs.Equals(hrhs) {
				return 0, hlhs.Copy(), true, true, nil
			}

			return 0, nil, false, true, nil
		}

		return lhs, hlhs, false, false, nil
	},
	parser.LTE: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if hlhs != nil || hrhs != nil {
			return 0, nil, false, false, nil
		}

		if lhs <= rhs {
			return lhs, nil, true, true, nil
		}

		return 0, nil, false, true, nil
	},
	parser.LSS: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if hlhs != nil || hrhs != nil {
			return 0, nil, false, false, nil
		}

		if lhs < rhs {
			return lhs, nil, true, true, nil
		}

		return 0, nil, false, true, nil
	},
	parser.GTE: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if hlhs != nil || hrhs != nil {
			return 0, nil, false, false, nil
		}

		if lhs >= rhs {
			return lhs, nil, true, true, nil
		}

		return 0, nil, false, true, nil
	},
	parser.GTR: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if hlhs != nil || hrhs != nil {
			return 0, nil, false, false, nil
		}

		if lhs > rhs {
			return lhs, nil, true, true, nil
		}

		return 0, nil, false, true, nil
	},
}

var boolComparisonOperationFuncs = map[parser.ItemType]binaryOperationFunc{
	parser.EQLC: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if hlhs == nil && hrhs == nil {
			if lhs == rhs {
				return 1, nil, true, true, nil
			}

			return 0, nil, true, true, nil
		}

		if hlhs != nil && hrhs != nil {
			if hlhs.Equals(hrhs) {
				return 1, nil, true, true, nil
			}

			return 0, nil, true, true, nil
		}

		return 0, nil, false, false, nil
	},
	parser.NEQ: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if hlhs == nil && hrhs == nil {
			if lhs != rhs {
				return 1, nil, true, true, nil
			}

			return 0, nil, true, true, nil
		}

		if hlhs != nil && hrhs != nil {
			if !hlhs.Equals(hrhs) {
				return 1, nil, true, true, nil
			}

			return 0, nil, true, true, nil
		}

		return 0, nil, false, false, nil
	},
	parser.LTE: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if hlhs != nil || hrhs != nil {
			return 0, nil, false, false, nil
		}

		if lhs <= rhs {
			return 1, nil, true, true, nil
		}

		return 0, nil, true, true, nil
	},
	parser.LSS: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if hlhs != nil || hrhs != nil {
			return 0, nil, false, false, nil
		}

		if lhs < rhs {
			return 1, nil, true, true, nil
		}

		return 0, nil, true, true, nil
	},
	parser.GTE: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if hlhs != nil || hrhs != nil {
			return 0, nil, false, false, nil
		}

		if lhs >= rhs {
			return 1, nil, true, true, nil
		}

		return 0, nil, true, true, nil
	},
	parser.GTR: func(lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
		if hlhs != nil || hrhs != nil {
			return 0, nil, false, false, nil
		}

		if lhs > rhs {
			return 1, nil, true, true, nil
		}

		return 0, nil, true, true, nil
	},
}
