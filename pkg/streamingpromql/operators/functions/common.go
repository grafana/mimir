// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"github.com/prometheus/prometheus/model/histogram"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// SeriesMetadataFunction is a function to operate on the metadata across series.
type SeriesMetadataFunction func(seriesMetadata []types.SeriesMetadata, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) ([]types.SeriesMetadata, error)

// DropSeriesName is a series metadata function that removes the __name__ label from all series.
var DropSeriesName = SeriesMetadataFunctionDefinition{
	Func: func(seriesMetadata []types.SeriesMetadata, _ *limiting.MemoryConsumptionTracker) ([]types.SeriesMetadata, error) {
		for i := range seriesMetadata {
			seriesMetadata[i].Labels = seriesMetadata[i].Labels.DropMetricName()
		}

		return seriesMetadata, nil
	},
	NeedsSeriesDeduplication: true,
}

// InstantVectorSeriesFunction is a function that takes in an instant vector and produces an instant vector.
type InstantVectorSeriesFunction func(seriesData types.InstantVectorSeriesData, scalarArgsData []types.ScalarData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error)

// floatTransformationFunc is not needed elsewhere, so it is not exported yet
func floatTransformationFunc(transform func(f float64) float64) InstantVectorSeriesFunction {
	return func(seriesData types.InstantVectorSeriesData, _ []types.ScalarData, _ types.QueryTimeRange, _ *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
		for i := range seriesData.Floats {
			seriesData.Floats[i].F = transform(seriesData.Floats[i].F)
		}
		return seriesData, nil
	}
}

func FloatTransformationDropHistogramsFunc(transform func(f float64) float64) InstantVectorSeriesFunction {
	ft := floatTransformationFunc(transform)
	return func(seriesData types.InstantVectorSeriesData, _ []types.ScalarData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
		// Functions that do not explicitly mention native histograms in their documentation will ignore histogram samples.
		// https://prometheus.io/docs/prometheus/latest/querying/functions
		types.HPointSlicePool.Put(seriesData.Histograms, memoryConsumptionTracker)
		seriesData.Histograms = nil
		return ft(seriesData, nil, timeRange, memoryConsumptionTracker)
	}
}

func PassthroughData(seriesData types.InstantVectorSeriesData, _ []types.ScalarData, _ types.QueryTimeRange, _ *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
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
	step *types.RangeVectorStepData,
	rangeSeconds float64,
	scalarArgsData []types.ScalarData,
	timeRange types.QueryTimeRange,
	emitAnnotation types.EmitAnnotationFunc,
) (f float64, hasFloat bool, h *histogram.FloatHistogram, err error)

// RangeVectorSeriesValidationFunction is a function that is called after a series is completed for a function over a range vector.
type RangeVectorSeriesValidationFunction func(seriesData types.InstantVectorSeriesData, metricName string, emitAnnotation types.EmitAnnotationFunc)

// RangeVectorSeriesValidationFunctionFactory is a factory function that returns a RangeVectorSeriesValidationFunction
type RangeVectorSeriesValidationFunctionFactory func() RangeVectorSeriesValidationFunction

type FunctionOverInstantVectorDefinition struct {
	// SeriesDataFunc is the function that computes an output series for a single input series.
	SeriesDataFunc InstantVectorSeriesFunction

	// SeriesMetadataFunction is the function that computes the output series for this function based on the given input series.
	//
	// If SeriesMetadataFunction.Func is nil, the input series are used as-is.
	SeriesMetadataFunction SeriesMetadataFunctionDefinition
}

type FunctionOverRangeVectorDefinition struct {
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

	// SeriesMetadataFunction is the function that computes the output series for this function based on the given input series.
	//
	// If SeriesMetadataFunction.Func is nil, the input series are used as-is.
	SeriesMetadataFunction SeriesMetadataFunctionDefinition

	// NeedsSeriesNamesForAnnotations indicates that this function uses the names of input series when emitting annotations.
	NeedsSeriesNamesForAnnotations bool
}

type SeriesMetadataFunctionDefinition struct {
	// Func is the function that computes the output series for this function based on the given input series.
	//
	// If Func is nil, the input series are used as-is.
	Func SeriesMetadataFunction

	// NeedsSeriesDeduplication enables deduplication and merging of output series with the same labels.
	//
	// This should be set to true if Func modifies the input series labels in such a way that duplicates may be
	// present in the output series labels (eg. dropping a label).
	NeedsSeriesDeduplication bool
}
