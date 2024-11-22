// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// VectorScalarBinaryOperation represents a binary operation between an instant vector and a scalar such as "<expr> + 2" or "3 * <expr>".
type VectorScalarBinaryOperation struct {
	Scalar                   types.ScalarOperator
	Vector                   types.InstantVectorOperator
	ScalarIsLeftSide         bool
	Op                       parser.ItemType
	ReturnBool               bool
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	timeRange types.QueryTimeRange
	opFunc    vectorScalarBinaryOperationFunc

	expressionPosition posrange.PositionRange
	annotations        *annotations.Annotations
	scalarData         types.ScalarData
	vectorIterator     types.InstantVectorSeriesDataIterator
}

type vectorScalarBinaryOperationFunc func(scalar float64, vectorF float64, vectorH *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error)

func NewVectorScalarBinaryOperation(
	scalar types.ScalarOperator,
	vector types.InstantVectorOperator,
	scalarIsLeftSide bool,
	op parser.ItemType,
	returnBool bool,
	timeRange types.QueryTimeRange,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
) (*VectorScalarBinaryOperation, error) {
	var f binaryOperationFunc

	if returnBool {
		f = boolComparisonOperationFuncs[op]
	} else {
		f = arithmeticAndComparisonOperationFuncs[op]
	}

	if f == nil {
		return nil, compat.NewNotSupportedError(fmt.Sprintf("binary expression with '%s'", op))
	}

	b := &VectorScalarBinaryOperation{
		Scalar:                   scalar,
		Vector:                   vector,
		ScalarIsLeftSide:         scalarIsLeftSide,
		Op:                       op,
		ReturnBool:               returnBool,
		MemoryConsumptionTracker: memoryConsumptionTracker,

		timeRange:          timeRange,
		annotations:        annotations,
		expressionPosition: expressionPosition,
	}

	if !b.ScalarIsLeftSide {
		b.opFunc = func(scalar float64, vectorF float64, vectorH *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
			return f(vectorF, scalar, vectorH, nil)
		}
	} else if op.IsComparisonOperator() && !returnBool {
		b.opFunc = func(scalar float64, vectorF float64, vectorH *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
			_, _, keep, valid, err := f(scalar, vectorF, nil, vectorH)

			// We always want to return the value from the vector when we're doing a filter-style comparison.
			return vectorF, vectorH, keep, valid, err
		}
	} else {
		b.opFunc = func(scalar float64, vectorF float64, vectorH *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, bool, error) {
			return f(scalar, vectorF, nil, vectorH)
		}
	}

	return b, nil
}

func (v *VectorScalarBinaryOperation) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	// Get the scalar values once, now, rather than having to do this later in NextSeries.
	var err error
	v.scalarData, err = v.Scalar.GetValues(ctx)
	if err != nil {
		return nil, err
	}

	metadata, err := v.Vector.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	if !v.Op.IsComparisonOperator() || v.ReturnBool {
		// We don't need to do deduplication and merging of series in this operator: we expect that this operator
		// is wrapped in a DeduplicateAndMerge.
		metadata, err = functions.DropSeriesName.Func(metadata, v.MemoryConsumptionTracker)
		if err != nil {
			return nil, err
		}
	}

	return metadata, nil
}

func (v *VectorScalarBinaryOperation) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	series, err := v.Vector.NextSeries(ctx)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	returnInputFPointSlice := true
	returnInputHPointSlice := true

	// We cannot re-use any slices when the series contains a mix of floats and histograms.
	// For example, imagine the series has [histogram, histogram, float, float, histogram] and we're performing the
	// operation "2 + series".
	// Histograms are treated as 0, so each input point produces a float.
	// If we reuse the input series' FPoint slice, we'll overwrite the later float points while processing the earlier
	// histogram points.
	// This shouldn't happen often, so we don't mind the cost of allocating a new slice in this case.
	// It should be pretty uncommon that metric contains both histograms and floats, so we will accept the cost of a new
	// slice.
	haveMixedFloatsAndHistograms := len(series.Histograms) > 0 && len(series.Floats) > 0
	pointCount := len(series.Histograms) + len(series.Floats)

	var fPoints []promql.FPoint
	var hPoints []promql.HPoint

	prepareFPointSlice := func() error {
		if haveMixedFloatsAndHistograms || cap(series.Floats) < pointCount {
			// We have to get a new slice.
			var err error
			fPoints, err = types.FPointSlicePool.Get(pointCount, v.MemoryConsumptionTracker)
			return err
		}

		// We can reuse the existing slice.
		returnInputFPointSlice = false
		fPoints = series.Floats[:0]
		return nil
	}

	prepareHPointSlice := func() error {
		if haveMixedFloatsAndHistograms || cap(series.Histograms) < pointCount {
			// We have to get a new slice.
			var err error
			hPoints, err = types.HPointSlicePool.Get(pointCount, v.MemoryConsumptionTracker)
			return err
		}

		// We can reuse the existing slice.
		returnInputHPointSlice = false
		hPoints = series.Histograms[:0]
		return nil
	}

	v.vectorIterator.Reset(series)

	for {
		t, vectorF, vectorH, keep := v.vectorIterator.Next()

		if !keep {
			// We are done.
			break
		}

		scalarIdx := (t - v.timeRange.StartT) / v.timeRange.IntervalMilliseconds // Scalars always have a value at every step, so we can just compute the index of the corresponding scalar value from the timestamp.
		scalarValue := v.scalarData.Samples[scalarIdx].F

		f, h, keep, valid, err := v.opFunc(scalarValue, vectorF, vectorH)
		if err != nil {
			err = functions.NativeHistogramErrorToAnnotation(err, v.emitAnnotation)
			if err == nil {
				// Error was converted to an annotation, continue without emitting a sample here.
				continue
			}

			return types.InstantVectorSeriesData{}, err
		}

		if !valid {
			if v.ScalarIsLeftSide {
				emitIncompatibleTypesAnnotation(v.annotations, v.Op, nil, vectorH, v.expressionPosition)
			} else {
				emitIncompatibleTypesAnnotation(v.annotations, v.Op, vectorH, nil, v.expressionPosition)
			}
		}

		if !keep {
			continue
		}

		if h != nil {
			if hPoints == nil {
				// First histogram for this series, get a slice for it.
				if err := prepareHPointSlice(); err != nil {
					return types.InstantVectorSeriesData{}, err
				}
			}

			hPoints = append(hPoints, promql.HPoint{T: t, H: h})
		} else {
			// We have a float value.
			if fPoints == nil {
				// First float for this series, get a slice for it.
				if err := prepareFPointSlice(); err != nil {
					return types.InstantVectorSeriesData{}, err
				}
			}

			fPoints = append(fPoints, promql.FPoint{T: t, F: f})
		}
	}

	if returnInputFPointSlice {
		types.FPointSlicePool.Put(series.Floats, v.MemoryConsumptionTracker)
	}

	if returnInputHPointSlice {
		types.HPointSlicePool.Put(series.Histograms, v.MemoryConsumptionTracker)
	}

	return types.InstantVectorSeriesData{
		Floats:     fPoints,
		Histograms: hPoints,
	}, nil
}

func (v *VectorScalarBinaryOperation) ExpressionPosition() posrange.PositionRange {
	return v.expressionPosition
}

func (v *VectorScalarBinaryOperation) Close() {
	v.Scalar.Close()
	v.Vector.Close()
	types.FPointSlicePool.Put(v.scalarData.Samples, v.MemoryConsumptionTracker)
}

func (v *VectorScalarBinaryOperation) emitAnnotation(generator types.AnnotationGenerator) {
	v.annotations.Add(generator("", v.expressionPosition))
}

var _ types.InstantVectorOperator = &VectorScalarBinaryOperation{}
