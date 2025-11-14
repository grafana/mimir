// SPDX-License-Identifier: AGPL-3.0-only

package aggregations

import (
	"context"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestCountValues_GroupLabelling(t *testing.T) {
	testCases := map[string]struct {
		grouping                    []string
		without                     bool
		inputSeries                 labels.Labels
		expectedOutputSeries        labels.Labels
		overrideExpectedOutputBytes []byte
	}{
		"grouping to a single series": {
			grouping:                    []string{},
			inputSeries:                 labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod"),
			expectedOutputSeries:        labels.FromStrings("value", "123"),
			overrideExpectedOutputBytes: []byte{}, // Special case for grouping to a single series.
		},

		// Grouping with 'by'
		"grouping with 'by', single grouping label, input has only metric name": {
			grouping:             []string{"env"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric"),
			expectedOutputSeries: labels.FromStrings("value", "123"),
		},
		"grouping with 'by', single grouping label, input does not have grouping label": {
			grouping:             []string{"env"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "foo", "bar"),
			expectedOutputSeries: labels.FromStrings("value", "123"),
		},
		"grouping with 'by', single grouping label, input does have grouping label": {
			grouping:             []string{"env"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "foo", "bar"),
			expectedOutputSeries: labels.FromStrings("env", "prod", "value", "123"),
		},
		"grouping with 'by', multiple grouping labels, input has only metric name": {
			grouping:             []string{"cluster", "env"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric"),
			expectedOutputSeries: labels.FromStrings("value", "123"),
		},
		"grouping with 'by', multiple grouping labels, input does not have any grouping labels": {
			grouping:             []string{"cluster", "env"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "foo", "bar"),
			expectedOutputSeries: labels.FromStrings("value", "123"),
		},
		"grouping with 'by', multiple grouping labels, input has some grouping labels": {
			grouping:             []string{"cluster", "env"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "foo", "bar"),
			expectedOutputSeries: labels.FromStrings("env", "prod", "value", "123"),
		},
		"grouping with 'by', multiple grouping labels, input has superset of grouping labels": {
			grouping:             []string{"cluster", "env"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1", "foo", "bar"),
			expectedOutputSeries: labels.FromStrings("env", "prod", "cluster", "cluster-1", "value", "123"),
		},
		"grouping with 'by', multiple grouping labels, input has all grouping labels and no others": {
			grouping:             []string{"cluster", "env"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1"),
			expectedOutputSeries: labels.FromStrings("env", "prod", "cluster", "cluster-1", "value", "123"),
		},
		"grouping with 'by', unsorted grouping labels": {
			grouping:             []string{"env", "cluster"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1", "foo", "bar"),
			expectedOutputSeries: labels.FromStrings("env", "prod", "cluster", "cluster-1", "value", "123"),
		},
		"grouping with 'by', some grouping labels sort after value label": {
			grouping:             []string{"env", "cluster", "z-label"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1", "foo", "bar", "z-label", "z-value"),
			expectedOutputSeries: labels.FromStrings("env", "prod", "cluster", "cluster-1", "value", "123", "z-label", "z-value"),
		},
		"grouping with 'by', grouping labels include __name__": {
			grouping:             []string{"cluster", "env", "__name__"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1", "foo", "bar"),
			expectedOutputSeries: labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1", "value", "123"),
		},

		// Grouping with 'without'
		"grouping with 'without', single grouping label, input has only metric name": {
			grouping:             []string{"env"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric"),
			expectedOutputSeries: labels.FromStrings("value", "123"),
		},
		"grouping with 'without', single grouping label, input does not have grouping label": {
			grouping:             []string{"env"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "a-label", "a-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "f-label", "f-value", "value", "123"),
		},
		"grouping with 'without', single grouping label, input does have grouping label": {
			grouping:             []string{"env"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "a-label", "a-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "f-label", "f-value", "value", "123"),
		},
		"grouping with 'without', multiple grouping labels, input has only metric name": {
			grouping:             []string{"cluster", "env"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric"),
			expectedOutputSeries: labels.FromStrings("value", "123"),
		},
		"grouping with 'without', multiple grouping labels, input does not have any grouping labels": {
			grouping:             []string{"cluster", "env"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value", "value", "123"),
		},
		"grouping with 'without', multiple grouping labels, input has some grouping labels": {
			grouping:             []string{"cluster", "env"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value", "value", "123"),
		},
		"grouping with 'without', multiple grouping labels, input has superset of grouping labels": {
			grouping:             []string{"cluster", "env"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value", "value", "123"),
		},
		"grouping with 'without', multiple grouping labels, input has all grouping labels and no others": {
			grouping:             []string{"cluster", "env"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1"),
			expectedOutputSeries: labels.FromStrings("value", "123"),
		},
		"grouping with 'without', unsorted grouping labels": {
			grouping:             []string{"env", "cluster"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value", "value", "123"),
		},
		"grouping with 'without', some grouping labels sort after value label": {
			grouping:             []string{"env", "cluster", "z-label"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value", "z-label", "z-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value", "value", "123"),
		},
		"grouping with 'without', grouping labels include __name__": {
			grouping:             []string{"cluster", "env", "__name__"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value", "value", "123"),
		},
		"grouping with 'without', grouping labels include duplicates": {
			grouping:             []string{"cluster", "env", "cluster"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value", "value", "123"),
		},
		"grouping with 'without', grouping labels include label that sorts before __name__": {
			grouping:             []string{"cluster", "env", "__aaa__"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "__aaa__", "foo", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value", "value", "123"),
		},

		"grouping with 'without', multiple grouping labels, input has all grouping labels and value label": {
			grouping:             []string{"cluster", "env"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1", "value", "foo"),
			expectedOutputSeries: labels.FromStrings("value", "123"),
		},
		"grouping with 'without', multiple grouping labels including value label, input has all grouping labels and value label": {
			grouping:             []string{"cluster", "env", "value"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1", "value", "foo"),
			expectedOutputSeries: labels.FromStrings("value", "123"),
		},
		"grouping with 'without', multiple grouping labels including value label, input has all grouping labels and not value label": {
			grouping:             []string{"cluster", "env", "value"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1"),
			expectedOutputSeries: labels.FromStrings("value", "123"),
		},

		"grouping with 'by', multiple grouping labels, input has all grouping labels and value label": {
			grouping:             []string{"cluster", "env"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1", "value", "foo"),
			expectedOutputSeries: labels.FromStrings("env", "prod", "cluster", "cluster-1", "value", "123"),
		},
		"grouping with 'by', multiple grouping labels including value label, input has all grouping labels and value label": {
			grouping:             []string{"cluster", "env", "value"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1", "value", "foo"),
			expectedOutputSeries: labels.FromStrings("env", "prod", "cluster", "cluster-1", "value", "123"),
		},
		"grouping with 'by', multiple grouping labels including value label, input has all grouping labels and not value label": {
			grouping:             []string{"cluster", "env", "value"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1"),
			expectedOutputSeries: labels.FromStrings("env", "prod", "cluster", "cluster-1", "value", "123"),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
			floats, err := types.FPointSlicePool.Get(1, memoryConsumptionTracker)
			require.NoError(t, err)
			floats = append(floats, promql.FPoint{T: 0, F: 123})

			inner := &operators.TestOperator{
				Series: []labels.Labels{testCase.inputSeries},
				Data: []types.InstantVectorSeriesData{
					{Floats: floats},
				},
				MemoryConsumptionTracker: memoryConsumptionTracker,
			}

			labelName := operators.NewStringLiteral("value", posrange.PositionRange{})
			aggregator := NewCountValues(inner, labelName, types.NewInstantQueryTimeRange(timestamp.Time(0)), testCase.grouping, testCase.without, memoryConsumptionTracker, posrange.PositionRange{})

			metadata, err := aggregator.SeriesMetadata(context.Background(), nil)
			require.NoError(t, err)

			require.Equal(t, testutils.LabelsToSeriesMetadata([]labels.Labels{testCase.expectedOutputSeries}), metadata)
		})
	}
}
