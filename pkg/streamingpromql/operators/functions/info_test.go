// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"slices"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestFilterInfoInnerMatchers(t *testing.T) {
	matchers := types.Matchers{
		{Type: labels.MatchRegexp, Name: model.MetricNameLabel, Value: "metric_total"},
		{Type: labels.MatchRegexp, Name: "cluster", Value: "one"},
		{Type: labels.MatchRegexp, Name: "data", Value: "info"},
	}

	testCases := map[string]struct {
		dataLabelMatchers types.Matchers
		expected          types.Matchers
	}{
		"unconstrained data labels": {
			dataLabelMatchers: types.Matchers{{Type: labels.MatchEqual, Name: model.MetricNameLabel, Value: "target_info"}},
			expected:          nil,
		},
		"explicit data label": {
			dataLabelMatchers: types.Matchers{
				{Type: labels.MatchEqual, Name: model.MetricNameLabel, Value: "target_info"},
				{Type: labels.MatchRegexp, Name: "data", Value: ".+"},
			},
			expected: types.Matchers{
				{Type: labels.MatchRegexp, Name: model.MetricNameLabel, Value: "metric_total"},
				{Type: labels.MatchRegexp, Name: "cluster", Value: "one"},
			},
		},
		"multiple explicit data labels": {
			dataLabelMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "cluster", Value: ".+"},
				{Type: labels.MatchRegexp, Name: "data", Value: ".+"},
			},
			expected: types.Matchers{{Type: labels.MatchRegexp, Name: model.MetricNameLabel, Value: "metric_total"}},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, testCase.expected, filterInfoInnerMatchers(matchers, testCase.dataLabelMatchers))
		})
	}
}

func TestInfoGroupWalker(t *testing.T) {
	targetInfo := labels.FromStrings("__name__", "target_info", "instance", "a", "job", "1", "env", "prod")
	updatedTargetInfo := labels.FromStrings("__name__", "target_info", "instance", "a", "job", "1", "env", "staging")
	buildInfo := labels.FromStrings("__name__", "build_info", "instance", "a", "job", "1", "version", "1")

	// samples returns info series samples at the given timestamps, with original sample timestamps (in seconds, as
	// returned by the info selector) of t/1000 - lag.
	samples := func(lag int64, ts ...int64) []promql.FPoint {
		points := make([]promql.FPoint, 0, len(ts))
		for _, t := range ts {
			points = append(points, promql.FPoint{T: t, F: float64(t/1000 - lag)})
		}
		return points
	}

	type step struct {
		t         int64
		changed   bool
		labelSets []labels.Labels // Only checked if changed.
	}

	testCases := map[string]struct {
		series        []infoSeries
		metricCount   int
		expectedSteps []step
		expectedError string
	}{
		"one series": {
			series:      []infoSeries{{labels: targetInfo, metricIndex: 0, floats: samples(0, 1000, 2000, 3000)}},
			metricCount: 1,
			expectedSteps: []step{
				{t: 1000, changed: true, labelSets: []labels.Labels{targetInfo}},
				{t: 2000},
				{t: 3000},
			},
		},
		"one series with a gap": {
			series:      []infoSeries{{labels: targetInfo, metricIndex: 0, floats: samples(0, 1000, 5000)}},
			metricCount: 1,
			expectedSteps: []step{
				{t: 1000, changed: true, labelSets: []labels.Labels{targetInfo}},
				{t: 5000},
			},
		},
		"series of the same info metric at the same timestamps: newest original timestamp is used": {
			series: []infoSeries{
				{labels: targetInfo, metricIndex: 0, floats: samples(1, 1000, 2000, 3000)},
				{labels: updatedTargetInfo, metricIndex: 0, floats: samples(0, 2000, 3000, 4000)},
			},
			metricCount: 1,
			expectedSteps: []step{
				{t: 1000, changed: true, labelSets: []labels.Labels{targetInfo}},
				{t: 2000, changed: true, labelSets: []labels.Labels{updatedTargetInfo}},
				{t: 3000},
				{t: 4000},
			},
		},
		"series of the same info metric with the same original timestamp": {
			series: []infoSeries{
				{labels: targetInfo, metricIndex: 0, floats: samples(0, 1000, 2000)},
				{labels: updatedTargetInfo, metricIndex: 0, floats: samples(0, 2000)},
			},
			metricCount: 1,
			expectedSteps: []step{
				{t: 1000, changed: true, labelSets: []labels.Labels{targetInfo}},
			},
			expectedError: `found duplicate series for info metric: existing {__name__="target_info", env="prod", instance="a", job="1"}, new {__name__="target_info", env="staging", instance="a", job="1"}, @ 2000 (1970-01-01T00:00:02Z)`,
		},
		"series of different info metrics are used together": {
			series: []infoSeries{
				{labels: targetInfo, metricIndex: 0, floats: samples(0, 1000, 2000)},
				{labels: buildInfo, metricIndex: 1, floats: samples(0, 2000, 3000)},
			},
			metricCount: 2,
			expectedSteps: []step{
				{t: 1000, changed: true, labelSets: []labels.Labels{targetInfo}},
				{t: 2000, changed: true, labelSets: []labels.Labels{targetInfo, buildInfo}},
				{t: 3000, changed: true, labelSets: []labels.Labels{buildInfo}},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			var walker infoGroupWalker
			walker.reset(&infoSignature{series: testCase.series}, testCase.metricCount)

			var actualSteps []step
			for {
				ts, changed, ok, err := walker.next()
				if err != nil {
					require.EqualError(t, err, testCase.expectedError)
					break
				}
				if !ok {
					require.Empty(t, testCase.expectedError)
					break
				}

				s := step{t: ts, changed: changed}
				if changed {
					s.labelSets = slices.Clone(walker.labelSets)
					require.Equal(t, makeLabelSetsHash(s.labelSets), walker.hash)
				}
				actualSteps = append(actualSteps, s)
			}

			require.Equal(t, testCase.expectedSteps, actualSteps)
		})
	}
}

func TestInfoGroupLookup(t *testing.T) {
	targetInfo := labels.FromStrings("__name__", "target_info", "instance", "a", "job", "1", "env", "prod")
	updatedTargetInfo := labels.FromStrings("__name__", "target_info", "instance", "a", "job", "1", "env", "staging")
	signature := &infoSignature{series: []infoSeries{
		{labels: targetInfo, metricIndex: 0, floats: []promql.FPoint{{T: 1000, F: 0}, {T: 2000, F: 1}}},
		{labels: updatedTargetInfo, metricIndex: 0, floats: []promql.FPoint{{T: 2000, F: 2}, {T: 4000, F: 4}}},
	}}

	const targetInfoHashID, updatedTargetInfoHashID labelSetsHashID = 1, 2
	lookup := infoGroupLookup{hashIDs: map[string]labelSetsHashID{
		makeLabelSetsHash([]labels.Labels{targetInfo}):        targetInfoHashID,
		makeLabelSetsHash([]labels.Labels{updatedTargetInfo}): updatedTargetInfoHashID,
	}}

	expected := map[int64]labelSetsHashID{
		0:    innerSeriesHashID,
		1000: targetInfoHashID,
		2000: updatedTargetInfoHashID,
		3000: innerSeriesHashID,
		4000: updatedTargetInfoHashID,
		5000: innerSeriesHashID,
	}

	// Look up every timestamp, and a subset of timestamps, as NextSeries does for inner series with gaps.
	for _, timestamps := range [][]int64{{0, 1000, 2000, 3000, 4000, 5000}, {0, 3000, 4000}, {4000}} {
		require.NoError(t, lookup.reset(signature, 1))
		for _, ts := range timestamps {
			hashID, err := lookup.at(ts)
			require.NoError(t, err)
			require.Equal(t, expected[ts], hashID, "timestamp %d", ts)
		}
	}

	// A nil signature means no info series can enrich the inner series.
	require.NoError(t, lookup.reset(nil, 1))
	hashID, err := lookup.at(1000)
	require.NoError(t, err)
	require.Equal(t, innerSeriesHashID, hashID)
}
