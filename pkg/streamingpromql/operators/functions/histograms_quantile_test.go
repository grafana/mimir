// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
)

// Most of the functionality of the histogram operator is tested through the test scripts in
// pkg/streamingpromql/testdata.
//
// The output sorting behaviour is impossible to test through these scripts, so we instead test it here.

func TestHistogramQuantileFunction_ReturnsGroupsFinishedFirstEarliest(t *testing.T) {
	testCases := map[string]struct {
		inputSeries               []labels.Labels
		expectedOutputSeriesOrder []labels.Labels
	}{
		"empty input": {
			inputSeries:               []labels.Labels{},
			expectedOutputSeriesOrder: []labels.Labels{},
		},
		"classic histogram": {
			inputSeries: []labels.Labels{
				labels.FromStrings("__name__", "series", "le", "1"),
				labels.FromStrings("__name__", "series", "le", "2"),
				labels.FromStrings("__name__", "series", "le", "4"),
				labels.FromStrings("__name__", "series", "le", "8"),
				labels.FromStrings("__name__", "series", "le", "16"),
				labels.FromStrings("__name__", "series", "le", "+Inf"),
			},
			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("le", "1"),
				labels.FromStrings("le", "2"),
				labels.FromStrings("le", "4"),
				labels.FromStrings("le", "8"),
				labels.FromStrings("le", "16"),
				labels.EmptyLabels(),
				labels.FromStrings("le", "+Inf"),
			},
		},
		"series without le": {
			inputSeries: []labels.Labels{
				labels.FromStrings("__name__", "series", "abc", "1"),
				labels.FromStrings("__name__", "series", "abc", "2"),
			},
			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("abc", "1"),
				labels.FromStrings("abc", "2"),
			},
		},
		"classic histogram and series with same name": {
			inputSeries: []labels.Labels{
				labels.FromStrings("__name__", "series", "le", "1"),
				labels.FromStrings("__name__", "series", "le", "2"),
				labels.FromStrings("__name__", "series", "le", "4"),
				labels.FromStrings("__name__", "series", "le", "8"),
				labels.FromStrings("__name__", "series", "le", "16"),
				labels.FromStrings("__name__", "series", "le", "+Inf"),
				labels.FromStrings("__name__", "series"),
			},
			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("le", "1"),
				labels.FromStrings("le", "2"),
				labels.FromStrings("le", "4"),
				labels.FromStrings("le", "8"),
				labels.FromStrings("le", "16"),
				labels.FromStrings("le", "+Inf"),
				labels.EmptyLabels(),
			},
		},
		"multiple classic histograms with interleaved series": {
			inputSeries: []labels.Labels{
				labels.FromStrings("label", "A", "le", "1"),
				labels.FromStrings("label", "B", "le", "1"),
				labels.FromStrings("label", "A", "le", "2"),
				labels.FromStrings("label", "B", "le", "2"),
				labels.FromStrings("label", "A", "le", "4"),
				labels.FromStrings("label", "B", "le", "4"),
				labels.FromStrings("label", "A", "le", "8"),
				labels.FromStrings("label", "A", "le", "16"),
				labels.FromStrings("label", "A", "le", "+Inf"),
				labels.FromStrings("label", "B", "le", "8"),
				labels.FromStrings("label", "B", "le", "16"),
				labels.FromStrings("label", "B", "le", "+Inf"),
			},
			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("label", "A", "le", "1"),
				labels.FromStrings("label", "B", "le", "1"),
				labels.FromStrings("label", "A", "le", "2"),
				labels.FromStrings("label", "B", "le", "2"),
				labels.FromStrings("label", "A", "le", "4"),
				labels.FromStrings("label", "B", "le", "4"),
				labels.FromStrings("label", "A", "le", "8"),
				labels.FromStrings("label", "A", "le", "16"),
				labels.FromStrings("label", "A"),
				labels.FromStrings("label", "A", "le", "+Inf"),
				labels.FromStrings("label", "B", "le", "8"),
				labels.FromStrings("label", "B", "le", "16"),
				labels.FromStrings("label", "B"),
				labels.FromStrings("label", "B", "le", "+Inf"),
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			hOp := &HistogramQuantileFunction{
				phArg:                  &testScalarOperator{},
				inner:                  &operators.TestOperator{Series: testCase.inputSeries},
				innerSeriesMetricNames: &operators.MetricNames{},
			}

			outputSeries, err := hOp.SeriesMetadata(context.Background())
			require.NoError(t, err)

			require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedOutputSeriesOrder), outputSeries)
		})
	}
}
