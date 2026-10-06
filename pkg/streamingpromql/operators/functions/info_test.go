// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
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

func TestInfoFunction_Signature(t *testing.T) {
	testCases := map[string]struct {
		input    labels.Labels
		expected labels.Labels
	}{
		"metric name and other labels are not included": {
			input:    labels.FromStrings("__name__", "metric", "env", "prod", "instance", "a", "job", "1", "zone", "z"),
			expected: labels.FromStrings("instance", "a", "job", "1"),
		},
		"only instance": {
			input:    labels.FromStrings("__name__", "metric", "instance", "a"),
			expected: labels.FromStrings("instance", "a"),
		},
		"only job": {
			input:    labels.FromStrings("__name__", "metric", "job", "1"),
			expected: labels.FromStrings("job", "1"),
		},
		"no identifying labels": {
			input:    labels.FromStrings("__name__", "metric", "env", "prod"),
			expected: labels.EmptyLabels(),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			f := &InfoFunction{sigBuf: make([]byte, 0, types.LabelBytesBufferSize)}
			// Signatures are used as map keys, so compare them as strings.
			require.Equal(t, string(testCase.expected.Bytes(nil)), string(f.signature(testCase.input)))
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

func TestInfoSignature_CompleteAndInfoGroupLookup(t *testing.T) {
	targetInfo := labels.FromStrings("__name__", "target_info", "instance", "a", "job", "1", "env", "prod")
	updatedTargetInfo := labels.FromStrings("__name__", "target_info", "instance", "a", "job", "1", "env", "staging")
	buildInfo := labels.FromStrings("__name__", "build_info", "instance", "a", "job", "1", "version", "1")

	type series struct {
		labels      labels.Labels
		metricIndex infoMetricIndex
		lag         int64 // Original sample timestamps are t/1000 - lag seconds.
		timestamps  []int64
	}

	testCases := map[string]struct {
		series      []series
		metricCount int
		// expected is the group at each step of the query, from 0 to 6000: nil if inner series are not enriched.
		expected map[int64][]labels.Labels
	}{
		"no info series with samples": {
			metricCount: 1,
			expected:    map[int64][]labels.Labels{},
		},
		"one series present at consecutive steps": {
			series:      []series{{labels: targetInfo, timestamps: []int64{1000, 2000, 3000}}},
			metricCount: 1,
			expected: map[int64][]labels.Labels{
				1000: {targetInfo},
				2000: {targetInfo},
				3000: {targetInfo},
			},
		},
		"one series with a gap": {
			series:      []series{{labels: targetInfo, timestamps: []int64{1000, 2000, 5000}}},
			metricCount: 1,
			expected: map[int64][]labels.Labels{
				1000: {targetInfo},
				2000: {targetInfo},
				5000: {targetInfo},
			},
		},
		"series of the same info metric replacing each other": {
			series: []series{
				{labels: targetInfo, lag: 1, timestamps: []int64{1000, 2000}},
				{labels: updatedTargetInfo, timestamps: []int64{2000, 3000}},
			},
			metricCount: 1,
			expected: map[int64][]labels.Labels{
				1000: {targetInfo},
				2000: {updatedTargetInfo},
				3000: {updatedTargetInfo},
			},
		},
		"series of different info metrics": {
			series: []series{
				{labels: targetInfo, timestamps: []int64{1000, 2000, 3000}},
				{labels: buildInfo, metricIndex: 1, timestamps: []int64{2000, 4000}},
			},
			metricCount: 2,
			expected: map[int64][]labels.Labels{
				1000: {targetInfo},
				2000: {targetInfo, buildInfo},
				3000: {targetInfo},
				4000: {buildInfo},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			tracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
			signature := &infoSignature{}
			for _, s := range testCase.series {
				floats, err := types.FPointSlicePool.Get(len(s.timestamps), tracker)
				require.NoError(t, err)
				for _, ts := range s.timestamps {
					floats = append(floats, promql.FPoint{T: ts, F: float64(ts/1000 - s.lag)})
				}
				signature.series = append(signature.series, infoSeries{labels: s.labels, metricIndex: s.metricIndex, floats: floats})
			}

			hashIDs := map[string]labelSetsHashID{innerSeriesKey: innerSeriesHashID}
			var walker infoGroupWalker
			// The query has a step of one second.
			require.NoError(t, signature.complete(&walker, testCase.metricCount, time.Second.Milliseconds(), hashIDs, tracker))

			// The samples are not needed after the transitions have been found.
			require.Nil(t, signature.series)
			require.Zero(t, tracker.CurrentEstimatedMemoryConsumptionBytes())

			expectedHashID := func(ts int64) labelSetsHashID {
				group, exists := testCase.expected[ts]
				if !exists {
					return innerSeriesHashID
				}
				hashID, exists := hashIDs[makeLabelSetsHash(group)]
				require.True(t, exists, "group at timestamp %d not interned", ts)
				return hashID
			}

			for _, group := range testCase.expected {
				require.Contains(t, signature.labelSetsByHash, makeLabelSetsHash(group))
			}

			// Look up every step, and subsets of steps, as NextSeries does for inner series with gaps.
			var lookup infoGroupLookup
			for _, timestamps := range [][]int64{{0, 1000, 2000, 3000, 4000, 5000, 6000}, {0, 3000, 4000}, {2000, 6000}, {5000}} {
				lookup.reset(signature)
				for _, ts := range timestamps {
					require.Equal(t, expectedHashID(ts), lookup.at(ts), "timestamp %d", ts)
				}
			}
		})
	}

	t.Run("nil signature", func(t *testing.T) {
		var lookup infoGroupLookup
		lookup.reset(nil)
		require.Equal(t, innerSeriesHashID, lookup.at(1000))
	})
}
