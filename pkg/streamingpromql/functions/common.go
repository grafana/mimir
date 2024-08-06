// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"github.com/prometheus/prometheus/model/histogram"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// SeriesMetadataFunction is a function to operate on the metadata across series.
type SeriesMetadataFunction func(seriesMetadata []types.SeriesMetadata, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) ([]types.SeriesMetadata, error)

func DropSeriesName(seriesMetadata []types.SeriesMetadata, _ *limiting.MemoryConsumptionTracker) ([]types.SeriesMetadata, error) {
	for i := range seriesMetadata {
		seriesMetadata[i].Labels = seriesMetadata[i].Labels.DropMetricName()
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

func Passthrough(seriesData types.InstantVectorSeriesData, _ *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
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
//
// Returns:
//   - hasFloat bool: a boolean indicating if a float value is present.
//   - f float64: the float value.
//   - h *histogram.FloatHistogram: nil if no histogram is present.
//   - err error.
type RangeVectorStepFunction func(step types.RangeVectorStepData, rangeSeconds float64, floatBuffer *types.FPointRingBuffer, histogramBuffer *types.HPointRingBuffer) (f float64, hasFloat bool, h *histogram.FloatHistogram, err error)
