// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type mockStringOperator struct {
	value string
}

func (m mockStringOperator) GetValue() string {
	return m.value
}

func (m mockStringOperator) ExpressionPosition() posrange.PositionRange {
	return posrange.PositionRange{}
}

func (m mockStringOperator) Close() {}

func (m mockStringOperator) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return nil
}

func TestLabelJoinFactory(t *testing.T) {
	testCases := []struct {
		name           string
		dst            string
		separator      string
		srcLabels      []string
		inputSeries    []types.SeriesMetadata
		expectedSeries []types.SeriesMetadata
		expectedError  string
	}{
		{
			name:      "basic join with two source labels",
			dst:       "joined",
			separator: "-",
			srcLabels: []string{"label1", "label2"},
			inputSeries: []types.SeriesMetadata{
				{Labels: labels.FromStrings("__name__", "metric", "label1", "value1", "label2", "value2")},
			},
			expectedSeries: []types.SeriesMetadata{
				{Labels: labels.FromStrings("__name__", "metric", "label1", "value1", "label2", "value2", "joined", "value1-value2")},
			},
		},
		{
			name:      "join with empty separator",
			dst:       "joined",
			separator: "",
			srcLabels: []string{"label1", "label2"},
			inputSeries: []types.SeriesMetadata{
				{Labels: labels.FromStrings("label1", "value1", "label2", "value2")},
			},
			expectedSeries: []types.SeriesMetadata{
				{Labels: labels.FromStrings("label1", "value1", "label2", "value2", "joined", "value1value2")},
			},
		},
		{
			name:      "join with missing source label",
			dst:       "joined",
			separator: "-",
			srcLabels: []string{"label1", "missing", "label2"},
			inputSeries: []types.SeriesMetadata{
				{Labels: labels.FromStrings("label1", "value1", "label2", "value2")},
			},
			expectedSeries: []types.SeriesMetadata{
				{Labels: labels.FromStrings("label1", "value1", "label2", "value2", "joined", "value1--value2")},
			},
		},
		{
			name:      "join single source label",
			dst:       "joined",
			separator: "-",
			srcLabels: []string{"label1"},
			inputSeries: []types.SeriesMetadata{
				{Labels: labels.FromStrings("label1", "value1")},
			},
			expectedSeries: []types.SeriesMetadata{
				{Labels: labels.FromStrings("label1", "value1", "joined", "value1")},
			},
		},
		{
			name:      "join multiple series",
			dst:       "joined",
			separator: "-",
			srcLabels: []string{"instance", "job"},
			inputSeries: []types.SeriesMetadata{
				{Labels: labels.FromStrings("instance", "host1", "job", "prometheus")},
				{Labels: labels.FromStrings("instance", "host2", "job", "grafana")},
			},
			expectedSeries: []types.SeriesMetadata{
				{Labels: labels.FromStrings("instance", "host1", "job", "prometheus", "joined", "host1-prometheus")},
				{Labels: labels.FromStrings("instance", "host2", "job", "grafana", "joined", "host2-grafana")},
			},
		},
		{
			name:      "overwrite existing destination label",
			dst:       "existing",
			separator: "-",
			srcLabels: []string{"label1", "label2"},
			inputSeries: []types.SeriesMetadata{
				{Labels: labels.FromStrings("label1", "value1", "label2", "value2", "existing", "old_value")},
			},
			expectedSeries: []types.SeriesMetadata{
				{Labels: labels.FromStrings("label1", "value1", "label2", "value2", "existing", "value1-value2")},
			},
		},
		{
			name:           "empty input",
			dst:            "joined",
			separator:      "-",
			srcLabels:      []string{"label1"},
			inputSeries:    []types.SeriesMetadata{},
			expectedSeries: []types.SeriesMetadata{},
		},
		{
			name:          "empty destination label name",
			dst:           "",
			separator:     "-",
			srcLabels:     []string{"label1"},
			expectedError: "invalid destination label name in label_join(): ",
		},
		{
			name:          "empty source label name",
			dst:           "joined",
			separator:     "-",
			srcLabels:     []string{"valid", ""},
			expectedError: "invalid source label name in label_join(): ",
		},
		{
			name:          "destination label name with invalid UTF-8",
			dst:           "\x80",
			separator:     "-",
			srcLabels:     []string{"label1"},
			expectedError: "invalid destination label name in label_join(): \x80",
		},
		{
			name:          "source label name with invalid UTF-8",
			dst:           "joined",
			separator:     "-",
			srcLabels:     []string{"valid", "\x80"},
			expectedError: "invalid source label name in label_join(): \x80",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dstOp := mockStringOperator{value: tc.dst}
			separatorOp := mockStringOperator{value: tc.separator}
			srcOps := make([]types.StringOperator, len(tc.srcLabels))
			for i, label := range tc.srcLabels {
				srcOps[i] = mockStringOperator{value: label}
			}

			labelJoinFunc := LabelJoinFactory(dstOp, separatorOp, srcOps)

			tracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
			for _, series := range tc.inputSeries {
				err := tracker.IncreaseMemoryConsumptionForLabels(series.Labels)
				require.NoError(t, err)
			}

			result, err := labelJoinFunc(tc.inputSeries, tracker)
			if tc.expectedError != "" {
				require.EqualError(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedSeries, result)
			}
		})
	}
}
