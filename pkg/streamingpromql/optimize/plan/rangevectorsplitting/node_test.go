// SPDX-License-Identifier: AGPL-3.0-only

package rangevectorsplitting

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions"
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
	require.NoError(t, encoded.Unmarshal(key))

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
		node                planning.SplitNode
		params              *planning.QueryParameters
		expectSameKeyAsBase bool
	}{
		"identical node and params share the cache key": {
			node:                matrixSelectorA(),
			params:              &planning.QueryParameters{},
			expectSameKeyAsBase: true,
		},
		"different matchers produce a different cache key": {
			node: &core.MatrixSelector{MatrixSelectorDetails: &core.MatrixSelectorDetails{
				Matchers: []*core.LabelMatcher{{Name: "__name__", Type: labels.MatchEqual, Value: "b"}},
				Range:    time.Hour,
			}},
			params:              &planning.QueryParameters{},
			expectSameKeyAsBase: false,
		},
		"different range produces a different cache key": {
			node: &core.MatrixSelector{MatrixSelectorDetails: &core.MatrixSelectorDetails{
				Matchers: []*core.LabelMatcher{{Name: "__name__", Type: labels.MatchEqual, Value: "a"}},
				Range:    2 * time.Hour,
			}},
			params:              &planning.QueryParameters{},
			expectSameKeyAsBase: false,
		},
		"different EnableDelayedNameRemoval produces a different cache key": {
			node:                matrixSelectorA(),
			params:              &planning.QueryParameters{EnableDelayedNameRemoval: true},
			expectSameKeyAsBase: false,
		},
		"different LookbackDelta produces a different cache key": {
			node:                matrixSelectorA(),
			params:              &planning.QueryParameters{LookbackDelta: 5 * time.Minute},
			expectSameKeyAsBase: false,
		},
		"TimeRange does not affect the cache key": {
			node:                matrixSelectorA(),
			params:              &planning.QueryParameters{TimeRange: types.QueryTimeRange{StartT: 1, EndT: 2, IntervalMilliseconds: 1, StepCount: 1}},
			expectSameKeyAsBase: true,
		},
		"OriginalExpression does not affect the cache key": {
			node:                matrixSelectorA(),
			params:              &planning.QueryParameters{OriginalExpression: "some_query"},
			expectSameKeyAsBase: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			key, err := SplittingCacheKey(tc.node, tc.params)
			require.NoError(t, err)
			if tc.expectSameKeyAsBase {
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

type staticLimits struct {
	oooWindow time.Duration
}

func (l staticLimits) GetMaxOutOfOrderTimeWindow(context.Context) (time.Duration, error) {
	return l.oooWindow, nil
}

// TestMaterializer_computeRanges verifies that the split ranges are computed correctly at materialize time, including
// the fallback reasons that depend on runtime state (the out-of-order window and current time).
func TestMaterializer_computeRanges(t *testing.T) {
	hourInMs := int64(time.Hour / time.Millisecond)
	minuteInMs := int64(time.Minute / time.Millisecond)

	// A fixed "now" of 100h keeps the out-of-order threshold well after all the timestamps used below, unless a test
	// deliberately configures an out-of-order window.
	fixedNow := timestamp.Time(100 * hourInMs)

	newMaterializer := func(oooWindow time.Duration) *Materializer {
		return NewMaterializer(true, 2*time.Hour, staticLimits{oooWindow: oooWindow}, func() time.Time { return fixedNow }, nil, nil, nil)
	}

	// inner builds an inner matrix selector with the given range and offset.
	inner := func(rng, offset time.Duration) *core.MatrixSelector {
		return &core.MatrixSelector{MatrixSelectorDetails: &core.MatrixSelectorDetails{
			Range:  rng,
			Offset: offset,
		}}
	}

	instantAt := func(evalTimeMs int64) types.QueryTimeRange {
		return types.NewInstantQueryTimeRange(timestamp.Time(evalTimeMs))
	}

	t.Run("splits a 5h range into cacheable blocks", func(t *testing.T) {
		m := newMaterializer(0)
		ranges, notApplied, err := m.computeRanges(context.Background(), inner(5*time.Hour, 0), instantAt(6*hourInMs))
		require.NoError(t, err)
		require.Empty(t, notApplied)
		require.Equal(t, []Range{
			{Start: 1 * hourInMs, End: 2*hourInMs - 1, Cacheable: false},
			{Start: 2*hourInMs - 1, End: 4*hourInMs - 1, Cacheable: true},
			{Start: 4*hourInMs - 1, End: 6*hourInMs - 1, Cacheable: true},
			{Start: 6*hourInMs - 1, End: 6 * hourInMs, Cacheable: false},
		}, ranges)
	})

	t.Run("falls back when there is no complete cacheable block", func(t *testing.T) {
		m := newMaterializer(0)
		// Query at 4h30m with 3h range and 31m offset -> data-time (59m, 3h59m]. First aligned boundary is 2h, and
		// 2h + 2h = 4h > 3h59m, so no complete block fits.
		ranges, notApplied, err := m.computeRanges(context.Background(), inner(3*time.Hour, 31*time.Minute), instantAt(4*hourInMs+30*minuteInMs))
		require.NoError(t, err)
		require.Equal(t, "no_complete_cache_block", notApplied)
		require.Nil(t, ranges)
	})

	t.Run("falls back when every block is within the out-of-order window", func(t *testing.T) {
		// With now=100h, a 99h out-of-order window puts the threshold at 1h, before the first aligned block boundary
		// (2h-1ms), so no block can be cached.
		m := newMaterializer(99 * time.Hour)
		ranges, notApplied, err := m.computeRanges(context.Background(), inner(5*time.Hour, 0), instantAt(6*hourInMs))
		require.NoError(t, err)
		require.Equal(t, "no_cacheable_blocks_after_ooo_filter", notApplied)
		require.Nil(t, ranges)
	})

	t.Run("marks all ranges uncacheable when caching is disabled", func(t *testing.T) {
		m := newMaterializer(0)
		ctx := requestoptions.ContextWithOptions(context.Background(), requestoptions.Options{CacheDisabled: true})
		ranges, notApplied, err := m.computeRanges(ctx, inner(5*time.Hour, 0), instantAt(6*hourInMs))
		require.NoError(t, err)
		require.Empty(t, notApplied)
		require.NotEmpty(t, ranges)
		for _, r := range ranges {
			require.False(t, r.Cacheable, "expected all ranges to be uncacheable when caching is disabled")
		}
	})
}
