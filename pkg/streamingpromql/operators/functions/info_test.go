// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
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

func TestFinalizeInfoSeriesGroups(t *testing.T) {
	targetInfo := labels.FromStrings("__name__", "target_info", "instance", "a", "job", "1", "env", "prod")
	buildInfo := labels.FromStrings("__name__", "build_info", "instance", "a", "job", "1", "version", "1")
	updatedTargetInfo := labels.FromStrings("__name__", "target_info", "instance", "a", "job", "1", "env", "staging")

	f := &InfoFunction{
		sigTimestamps: map[int64]map[string]labelSetsHashID{
			0: {},
			1: {},
			2: {},
		},
		labelSets: make(map[string]map[string][]labels.Labels),
	}

	var groups infoSeriesGroups
	firstGroupID := groups.addGroup(targetInfo)
	groups.addToGroup(firstGroupID, buildInfo)
	reorderedGroupID := groups.addGroup(buildInfo)
	groups.addToGroup(reorderedGroupID, targetInfo)
	updatedGroupID := groups.addGroup(updatedTargetInfo)
	groups.addToGroup(updatedGroupID, buildInfo)
	f.sigTimestamps[0]["signature"] = firstGroupID
	f.sigTimestamps[1]["signature"] = reorderedGroupID
	f.sigTimestamps[2]["signature"] = updatedGroupID
	f.finalizeInfoSeriesGroups(&groups)

	firstGroupHashID := f.sigTimestamps[0]["signature"]
	reorderedGroupHashID := f.sigTimestamps[1]["signature"]
	updatedGroupHashID := f.sigTimestamps[2]["signature"]

	require.Equal(t, firstGroupHashID, reorderedGroupHashID)
	require.NotEqual(t, firstGroupHashID, updatedGroupHashID)
	require.NotEqual(t, innerSeriesHashID, firstGroupHashID)
	require.Equal(t, innerSeriesKey, f.labelSetsHashesByID[innerSeriesHashID])
	require.Len(t, f.labelSetsHashesByID, 3)
	require.Len(t, f.labelSets["signature"], 2)

	storedResults := map[string]types.InstantVectorSeriesData{
		innerSeriesKey:                          {},
		f.labelSetsHashesByID[firstGroupHashID]: {},
	}

	_, hash, skip, err := f.getSplitResult(0, "signature", storedResults, map[string]int{f.labelSetsHashesByID[firstGroupHashID]: 0}, 0, 0)
	require.NoError(t, err)
	require.False(t, skip)
	require.Equal(t, f.labelSetsHashesByID[firstGroupHashID], hash)

	_, hash, skip, err = f.getSplitResult(3, "signature", storedResults, map[string]int{innerSeriesKey: 0}, 0, 0)
	require.NoError(t, err)
	require.False(t, skip)
	require.Equal(t, innerSeriesKey, hash)
}
