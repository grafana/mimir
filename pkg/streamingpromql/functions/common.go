// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"fmt"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// SeriesMetadataFunction is a function to operate on the metadata across series.
type SeriesMetadataFunction func(seriesMetadata []types.SeriesMetadata, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) ([]types.SeriesMetadata, error)

func DropSeriesName(seriesMetadata []types.SeriesMetadata, _ *limiting.MemoryConsumptionTracker) ([]types.SeriesMetadata, error) {
	for i := range seriesMetadata {
		seriesMetadata[i].Labels = seriesMetadata[i].Labels.DropMetricName()
	}

	if hasDuplicate, example := types.HasDuplicateSeries(seriesMetadata); hasDuplicate {
		types.PutSeriesMetadataSlice(seriesMetadata)
		return nil, fmt.Errorf("vector cannot contain metrics with the same labelset: multiple series with labels %v", example.String())
	}

	return seriesMetadata, nil
}

// InstantVectorFunction is a function that takes in a instant vector and produces an instant vector.
type InstantVectorFunction func(seriesData types.InstantVectorSeriesData, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error)

// floatTransformationFunc is not needed elsewhere, so it is not exported yet
func floatTransformationFunc(transform func(f float64) float64) InstantVectorFunction {
	return func(seriesData types.InstantVectorSeriesData, _ *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
		for i := range seriesData.Floats {
			seriesData.Floats[i].F = transform(seriesData.Floats[i].F)
		}
		return seriesData, nil
	}
}

func FloatTransformationDropHistogramsFunc(transform func(f float64) float64) InstantVectorFunction {
	ft := floatTransformationFunc(transform)
	return func(seriesData types.InstantVectorSeriesData, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
		// Functions that do not explicitly mention native histograms in their documentation will ignore histogram samples.
		// https://prometheus.io/docs/prometheus/latest/querying/functions
		types.HPointSlicePool.Put(seriesData.Histograms, memoryConsumptionTracker)
		seriesData.Histograms = nil
		return ft(seriesData, memoryConsumptionTracker)
	}
}

func PassthroughData(seriesData types.InstantVectorSeriesData, _ *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
	return seriesData, nil
}

// RangeVectorStepFunction is a function that operates on a range vector step.
//
// floatBuffer and histogramBuffer will contain the values for the step range.
// They may also contain values outside of the step, so step.RangeEnd needs to be considered.
//
// Parameters:
//   - step: the range vector step data to be processed.
//   - rangeSeconds: the duration of the range in seconds.
//   - floatBuffer: a ring buffer containing float values for the step range.
//   - histogramBuffer: a ring buffer containing histogram values for the step range.
//   - emitAnnotation: a callback function to emit an annotation for the current series.
//
// Returns:
//   - hasFloat bool: a boolean indicating if a float value is present.
//   - f float64: the float value.
//   - h *histogram.FloatHistogram: nil if no histogram is present.
//   - err error.
type RangeVectorStepFunction func(
	step types.RangeVectorStepData,
	rangeSeconds float64,
	floatBuffer *types.FPointRingBuffer,
	histogramBuffer *types.HPointRingBuffer,
	emitAnnotation EmitAnnotationFunc,
) (f float64, hasFloat bool, h *histogram.FloatHistogram, err error)

// EmitAnnotationFunc is a function that emits the annotation created by generator.
type EmitAnnotationFunc func(generator AnnotationGenerator)

// AnnotationGenerator is a function that returns an annotation for the given metric name and expression position.
type AnnotationGenerator func(metricName string, expressionPosition posrange.PositionRange) error

// RangeVectorSeriesValidationFunction is a function that is called after a series is completed for a function over a range vector.
type RangeVectorSeriesValidationFunction func(seriesData types.InstantVectorSeriesData, metricName string, emitAnnotation EmitAnnotationFunc)

// RangeVectorSeriesValidationFunctionFactory is a factory function that returns a RangeVectorSeriesValidationFunction
type RangeVectorSeriesValidationFunctionFactory func() RangeVectorSeriesValidationFunction

type FunctionOverRangeVector struct {
	// StepFunc is the function that computes an output sample for a single step.
	StepFunc RangeVectorStepFunction

	// SeriesValidationFuncFactory is the function that creates a validator for a complete series, emitting any annotations
	// for that series.
	//
	// A new validator instance is created for each instance of this range vector function (ie. a validator is not shared
	// between queries or between different invocations of the same range vector function in a single query). A single
	// instance of a range vector function can then implement optimisations such as skipping checks for repeated metric
	// names.
	//
	// SeriesValidationFuncFactory can be nil, in which case no validation is performed.
	SeriesValidationFuncFactory RangeVectorSeriesValidationFunctionFactory

	// SeriesMetadataFunc is the function that computes the output series for this function based on the given input series.
	//
	// If SeriesMetadataFunc is nil, the input series are used as-is.
	SeriesMetadataFunc SeriesMetadataFunction

	// NeedsSeriesNamesForAnnotations indicates that this function uses the names of input series when emitting annotations.
	NeedsSeriesNamesForAnnotations bool
}
