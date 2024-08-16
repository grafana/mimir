// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestDropSeriesName_HappyPath(t *testing.T) {
	seriesMetadata := []types.SeriesMetadata{
		{Labels: labels.FromStrings("__name__", "metric_name", "label1", "value1")},
		{Labels: labels.FromStrings("__name__", "another_metric", "label2", "value2")},
	}

	expected := []types.SeriesMetadata{
		{Labels: labels.FromStrings("label1", "value1")},
		{Labels: labels.FromStrings("label2", "value2")},
	}

	modifiedMetadata, err := DropSeriesName(seriesMetadata, limiting.NewMemoryConsumptionTracker(0, nil))
	require.NoError(t, err)
	require.Equal(t, expected, modifiedMetadata)
}

func TestDropSeriesName_DuplicateSeries(t *testing.T) {
	seriesMetadata := []types.SeriesMetadata{
		{Labels: labels.FromStrings("__name__", "metric_name", "label1", "value1")},
		{Labels: labels.FromStrings("__name__", "another_metric", "label1", "value1")},
	}

	_, err := DropSeriesName(seriesMetadata, limiting.NewMemoryConsumptionTracker(0, nil))
	require.EqualError(t, err, `vector cannot contain metrics with the same labelset: multiple series with labels {label1="value1"}`)
}

func TestFloatTransformationFunc(t *testing.T) {
	transform := func(f float64) float64 { return f * 2 }
	transformFunc := floatTransformationFunc(transform)

	seriesData := types.InstantVectorSeriesData{
		Floats: []promql.FPoint{
			{F: 1.0},
			{F: 2.5},
		},
		Histograms: []promql.HPoint{
			{H: &histogram.FloatHistogram{Count: 1, Sum: 2}},
		},
	}

	expected := types.InstantVectorSeriesData{
		Floats: []promql.FPoint{
			{F: 2.0},
			{F: 5.0},
		},
		Histograms: []promql.HPoint{
			{H: &histogram.FloatHistogram{Count: 1, Sum: 2}},
		},
	}

	modifiedSeriesData, err := transformFunc(seriesData, limiting.NewMemoryConsumptionTracker(0, nil))
	require.NoError(t, err)
	require.Equal(t, expected, modifiedSeriesData)
}

func TestFloatTransformationDropHistogramsFunc(t *testing.T) {
	transform := func(f float64) float64 { return f * 2 }
	transformFunc := FloatTransformationDropHistogramsFunc(transform)

	seriesData := types.InstantVectorSeriesData{
		Floats: []promql.FPoint{
			{F: 1.0},
			{F: 2.5},
		},
		Histograms: []promql.HPoint{
			{H: &histogram.FloatHistogram{Count: 1, Sum: 2}},
		},
	}

	expected := types.InstantVectorSeriesData{
		Floats: []promql.FPoint{
			{F: 2.0},
			{F: 5.0},
		},
		Histograms: nil, // Histograms should be dropped
	}

	modifiedSeriesData, err := transformFunc(seriesData, limiting.NewMemoryConsumptionTracker(0, nil))
	require.NoError(t, err)
	require.Equal(t, expected, modifiedSeriesData)
}
