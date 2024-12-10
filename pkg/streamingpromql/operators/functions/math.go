// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"math"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

var Abs = FloatTransformationDropHistogramsFunc(math.Abs)
var Acos = FloatTransformationDropHistogramsFunc(math.Acos)
var Acosh = FloatTransformationDropHistogramsFunc(math.Acosh)
var Asin = FloatTransformationDropHistogramsFunc(math.Asin)
var Asinh = FloatTransformationDropHistogramsFunc(math.Asinh)
var Atan = FloatTransformationDropHistogramsFunc(math.Atan)
var Atanh = FloatTransformationDropHistogramsFunc(math.Atanh)
var Ceil = FloatTransformationDropHistogramsFunc(math.Ceil)
var Cos = FloatTransformationDropHistogramsFunc(math.Cos)
var Cosh = FloatTransformationDropHistogramsFunc(math.Cosh)
var Exp = FloatTransformationDropHistogramsFunc(math.Exp)
var Floor = FloatTransformationDropHistogramsFunc(math.Floor)
var Ln = FloatTransformationDropHistogramsFunc(math.Log)
var Log10 = FloatTransformationDropHistogramsFunc(math.Log10)
var Log2 = FloatTransformationDropHistogramsFunc(math.Log2)
var Sin = FloatTransformationDropHistogramsFunc(math.Sin)
var Sinh = FloatTransformationDropHistogramsFunc(math.Sinh)
var Sqrt = FloatTransformationDropHistogramsFunc(math.Sqrt)
var Tan = FloatTransformationDropHistogramsFunc(math.Tan)
var Tanh = FloatTransformationDropHistogramsFunc(math.Tanh)

var Deg = FloatTransformationDropHistogramsFunc(func(f float64) float64 {
	return f * 180 / math.Pi
})

var Rad = FloatTransformationDropHistogramsFunc(func(f float64) float64 {
	return f * math.Pi / 180
})

var Sgn = FloatTransformationDropHistogramsFunc(func(f float64) float64 {
	if f < 0 {
		return -1
	}

	if f > 0 {
		return 1
	}

	// This behaviour is undocumented, but if f is +/- NaN, Prometheus' engine returns that value.
	// Otherwise, if the value is 0, we should return 0.
	return f
})

var UnaryNegation InstantVectorSeriesFunction = func(seriesData types.InstantVectorSeriesData, _ []types.ScalarData, _ types.QueryTimeRange, _ *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
	for i := range seriesData.Floats {
		seriesData.Floats[i].F = -seriesData.Floats[i].F
	}

	for i := range seriesData.Histograms {
		seriesData.Histograms[i].H.Mul(-1) // Mul modifies the histogram in-place, so we don't need to do anything with the result here.
	}

	return seriesData, nil
}

var Clamp InstantVectorSeriesFunction = func(seriesData types.InstantVectorSeriesData, scalarArgsData []types.ScalarData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
	outputIdx := 0
	minArg := scalarArgsData[0]
	maxArg := scalarArgsData[1]

	for _, data := range seriesData.Floats {
		// Scalars are guaranteed to have a point for each step in the query.
		idx := timeRange.PointIndex(data.T)
		minVal := minArg.Samples[idx].F
		maxVal := maxArg.Samples[idx].F

		if maxVal < minVal {
			// Drop this point as there is no valid answer
			continue
		}
		// We reuse the existing FPoint slice in place
		seriesData.Floats[outputIdx].T = data.T
		seriesData.Floats[outputIdx].F = max(minVal, min(maxVal, data.F))
		outputIdx++
	}
	seriesData.Floats = seriesData.Floats[:outputIdx]
	// Histograms are dropped from clamp
	types.HPointSlicePool.Put(seriesData.Histograms, memoryConsumptionTracker)
	seriesData.Histograms = nil
	return seriesData, nil
}

func ClampMinMaxFactory(isMin bool) InstantVectorSeriesFunction {
	clampFunc := func(val, f float64) float64 {
		return min(val, f)
	}
	if isMin {
		clampFunc = func(val, f float64) float64 {
			return max(val, f)
		}
	}

	return func(seriesData types.InstantVectorSeriesData, scalarArgsData []types.ScalarData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
		clampTo := scalarArgsData[0]

		for step, data := range seriesData.Floats {
			// Scalars are guaranteed to have a point for each step in the query.
			idx := timeRange.PointIndex(data.T)
			val := clampTo.Samples[idx].F

			// We reuse the existing FPoint slice in place
			seriesData.Floats[step].F = clampFunc(val, data.F)
		}
		// Histograms are dropped from clamp
		types.HPointSlicePool.Put(seriesData.Histograms, memoryConsumptionTracker)
		seriesData.Histograms = nil
		return seriesData, nil
	}
}

// round returns a number rounded to toNearest.
// Ties are solved by rounding up.
var Round InstantVectorSeriesFunction = func(seriesData types.InstantVectorSeriesData, scalarArgsData []types.ScalarData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
	toNearest := scalarArgsData[0]

	for step, data := range seriesData.Floats {
		// Scalars are guaranteed to have a point for each step in the query.
		idx := timeRange.PointIndex(data.T)

		// Invert as it seems to cause fewer floating point accuracy issues.
		toNearestInverse := 1.0 / toNearest.Samples[idx].F

		// We reuse the existing FPoint slice in place
		seriesData.Floats[step].F = math.Floor(data.F*toNearestInverse+0.5) / toNearestInverse
	}
	// Histograms are dropped from Round
	types.HPointSlicePool.Put(seriesData.Histograms, memoryConsumptionTracker)
	seriesData.Histograms = nil
	return seriesData, nil
}
