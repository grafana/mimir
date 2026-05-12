// SPDX-License-Identifier: AGPL-3.0-only

package rangevectorsplitting

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestSplittingCacheKey_RoundTrip(t *testing.T) {
	node := &core.MatrixSelector{MatrixSelectorDetails: &core.MatrixSelectorDetails{
		Matchers: []*core.LabelMatcher{
			{Name: "__name__", Type: labels.MatchEqual, Value: "metric"},
			{Name: "env", Type: labels.MatchEqual, Value: "prod"},
		},
		Range: time.Hour,
	}}
	params := &planning.QueryParameters{
		OriginalExpression:       "some_query{env=\"prod\"}[1h]",
		TimeRange:                types.QueryTimeRange{StartT: 100, EndT: 200, IntervalMilliseconds: 1, StepCount: 1},
		EnableDelayedNameRemoval: true,
		LookbackDelta:            5 * time.Minute,
	}

	key, err := SplittingCacheKey(node, params)
	require.NoError(t, err)

	var encoded planning.EncodedQueryPlan
	require.NoError(t, encoded.Unmarshal([]byte(key)))

	require.Len(t, encoded.Nodes, 1)
	require.Equal(t, planning.NODE_TYPE_MATRIX_SELECTOR, encoded.Nodes[0].NodeType)

	decodedNodes, err := encoded.DecodeNodes(encoded.RootNode)
	require.NoError(t, err)
	require.Len(t, decodedNodes, 1)
	require.Equal(t, node, decodedNodes[0])

	require.Equal(t, params.EnableDelayedNameRemoval, encoded.EnableDelayedNameRemoval)
	require.Equal(t, params.LookbackDelta, encoded.LookbackDelta)
	require.Equal(t, "", encoded.OriginalExpression)
	zeroTimeRange := types.QueryTimeRange{}
	require.Equal(t, zeroTimeRange.Encode(), encoded.TimeRange)
}

func TestSplittingCacheKey_FieldSensitivity(t *testing.T) {
	matrixSelectorA := func() *core.MatrixSelector {
		return &core.MatrixSelector{MatrixSelectorDetails: &core.MatrixSelectorDetails{
			Matchers: []*core.LabelMatcher{{Name: "__name__", Type: labels.MatchEqual, Value: "a"}},
			Range:    time.Hour,
		}}
	}

	baseNode := matrixSelectorA()
	baseParams := &planning.QueryParameters{}
	baseKey, err := SplittingCacheKey(baseNode, baseParams)
	require.NoError(t, err)
	require.NotEmpty(t, baseKey)

	testCases := map[string]struct {
		node     planning.SplitNode
		params   *planning.QueryParameters
		shareKey bool // true: same cache key as baseKey; false: different
	}{
		"identical node and params share the cache key": {
			node:     matrixSelectorA(),
			params:   &planning.QueryParameters{},
			shareKey: true,
		},
		"different matchers produce a different cache key": {
			node: &core.MatrixSelector{MatrixSelectorDetails: &core.MatrixSelectorDetails{
				Matchers: []*core.LabelMatcher{{Name: "__name__", Type: labels.MatchEqual, Value: "b"}},
				Range:    time.Hour,
			}},
			params:   &planning.QueryParameters{},
			shareKey: false,
		},
		"different range produces a different cache key": {
			node: &core.MatrixSelector{MatrixSelectorDetails: &core.MatrixSelectorDetails{
				Matchers: []*core.LabelMatcher{{Name: "__name__", Type: labels.MatchEqual, Value: "a"}},
				Range:    2 * time.Hour,
			}},
			params:   &planning.QueryParameters{},
			shareKey: false,
		},
		"different EnableDelayedNameRemoval produces a different cache key": {
			node:     matrixSelectorA(),
			params:   &planning.QueryParameters{EnableDelayedNameRemoval: true},
			shareKey: false,
		},
		"different LookbackDelta produces a different cache key": {
			node:     matrixSelectorA(),
			params:   &planning.QueryParameters{LookbackDelta: 5 * time.Minute},
			shareKey: false,
		},
		"TimeRange does not affect the cache key": {
			node:     matrixSelectorA(),
			params:   &planning.QueryParameters{TimeRange: types.QueryTimeRange{StartT: 1, EndT: 2, IntervalMilliseconds: 1, StepCount: 1}},
			shareKey: true,
		},
		"OriginalExpression does not affect the cache key": {
			node:     matrixSelectorA(),
			params:   &planning.QueryParameters{OriginalExpression: "some_query"},
			shareKey: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			key, err := SplittingCacheKey(tc.node, tc.params)
			require.NoError(t, err)
			if tc.shareKey {
				require.Equal(t, baseKey, key)
			} else {
				require.NotEqual(t, baseKey, key)
			}
		})
	}
}

func TestSplittingCacheKey_DoesNotMutateCallersQueryParameters(t *testing.T) {
	node := &core.MatrixSelector{MatrixSelectorDetails: &core.MatrixSelectorDetails{
		Matchers: []*core.LabelMatcher{{Name: "__name__", Type: labels.MatchEqual, Value: "a"}},
		Range:    time.Hour,
	}}
	params := &planning.QueryParameters{
		OriginalExpression:       "foo",
		TimeRange:                types.QueryTimeRange{StartT: 10, EndT: 20},
		EnableDelayedNameRemoval: true,
		LookbackDelta:            5 * time.Minute,
	}
	before := *params
	_, err := SplittingCacheKey(node, params)
	require.NoError(t, err)
	require.Equal(t, before, *params)
}
