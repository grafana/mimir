// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/costattribution/costattributionmodel"
)

func TestNewActiveTracker(t *testing.T) {
	testCases := map[string]struct {
		costAttributionLabels costattributionmodel.Labels
		expectedErr           error
	}{
		"happy case single label": {
			costAttributionLabels: costattributionmodel.Labels{{Input: "good_label", Output: "good_label"}},
			expectedErr:           nil,
		},
		"happy case multiple labels": {
			costAttributionLabels: costattributionmodel.Labels{
				{Input: "good_label", Output: "good_label"},
				{Input: "another_good_label", Output: "another_good_label"},
			},
			expectedErr: nil,
		},
		"incorrect label name causes an error single label": {
			costAttributionLabels: costattributionmodel.Labels{{Input: "__bad_label__", Output: "__bad_label__"}},
			expectedErr:           fmt.Errorf(`failed to create an active series tracker for tenant tenant-1: descriptor Desc{fqName: "cortex_ingester_attributed_active_series", help: "The total number of active series per user and attribution.", constLabels: {}, variableLabels: {__bad_label__,tenant}} is invalid: "__bad_label__" is not a valid label name for metric "cortex_ingester_attributed_active_series"`),
		},
		"incorrect label name causes an error multiple labels": {
			costAttributionLabels: costattributionmodel.Labels{
				{Input: "good_label", Output: "good_label"},
				{Input: "__bad_label__", Output: "__bad_label__"},
			},
			expectedErr: fmt.Errorf(`failed to create an active series tracker for tenant tenant-1: descriptor Desc{fqName: "cortex_ingester_attributed_active_series", help: "The total number of active series per user and attribution.", constLabels: {}, variableLabels: {good_label,__bad_label__,tenant}} is invalid: "__bad_label__" is not a valid label name for metric "cortex_ingester_attributed_active_series"`),
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			ast, astErr := NewActiveSeriesTracker("tenant-1", testCase.costAttributionLabels, 10, 1*time.Minute, log.NewNopLogger())
			if testCase.expectedErr == nil {
				require.NoError(t, astErr)
				require.NotNil(t, ast)
			} else {
				require.Error(t, astErr)
				require.EqualError(t, astErr, testCase.expectedErr.Error())
				require.Nil(t, ast)
			}
		})
	}
}

func TestActiveTracker_hasSameLabels(t *testing.T) {
	manager, _, _ := newTestManager()
	ast := manager.ActiveSeriesTracker("user1")
	assert.True(t, ast.hasSameLabels(costattributionmodel.Labels{{Input: "team", Output: "my_team"}}), "Expected cost attribution labels mismatch")
}

func TestActiveTracker_IncrementDecrement(t *testing.T) {
	manager, _, _ := newTestManager()
	ast := manager.ActiveSeriesTracker("user3")
	lbls1 := labels.FromStrings("department", "foo", "service", "bar")
	lbls2 := labels.FromStrings("department", "bar", "service", "baz")
	lbls3 := labels.FromStrings("department", "baz", "service", "foo")
	lbls4 := labels.FromStrings("department", "baz", "service", "foo4")
	lbls5 := labels.FromStrings("department", "baz5", "service", "foo")

	ast.Increment(lbls1, time.Unix(1, 0), -1)
	assert.True(t, ast.overflowSince.IsZero(), "First observation, should not overflow")
	assert.Equal(t, 1, len(ast.observed))

	ast.Decrement(lbls1, -1)
	assert.True(t, ast.overflowSince.IsZero(), "First observation decremented, should not overflow")
	assert.Equal(t, 0, len(ast.observed), "First observation decremented, should be removed since it reached 0")

	ast.Increment(lbls1, time.Unix(2, 0), -1)
	ast.Increment(lbls2, time.Unix(2, 0), -1)
	assert.True(t, ast.overflowSince.IsZero(), "Second observation, should not overflow")
	assert.Equal(t, 2, len(ast.observed))

	ast.Increment(lbls3, time.Unix(3, 0), -1)
	assert.Equal(t, time.Unix(3, 0), ast.overflowSince, "Third observation, should overflow but still tracked in observed.")
	assert.Equal(t, 3, len(ast.observed))
	assert.Zero(t, ast.overflowCounter.activeSeries.Load(), "Overflow counter should be zero because we tracked in observed.")

	ast.Increment(lbls4, time.Unix(4, 0), -1)
	assert.Equal(t, time.Unix(3, 0), ast.overflowSince, "Fourth observation, should stay overflow and still tracked in observed.")
	assert.Equal(t, 4, len(ast.observed))
	assert.Zero(t, ast.overflowCounter.activeSeries.Load(), "Overflow counter should be zero because we tracked in observed.")

	ast.Increment(lbls5, time.Unix(4, 0), -1)
	assert.Equal(t, time.Unix(3, 0), ast.overflowSince, "Fifth observation, should not be tracked in observed anymore, should go into overflow counter.")
	assert.Equal(t, 4, len(ast.observed))
	assert.Equal(t, int64(1), ast.overflowCounter.activeSeries.Load(), "Overflow counter should track the fifth series.")
}

func TestActiveTracker_Concurrency(t *testing.T) {
	m, _, costAttributionReg := newTestManager()
	ast := m.ActiveSeriesTracker("user1")

	var wg sync.WaitGroup
	var i int64
	for i = 0; i < 100; i++ {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			lbls := labels.FromStrings("team", string(rune('A'+(i%26))))
			ast.Increment(lbls, time.Unix(i, 0), 2)
		}(i)
	}
	wg.Wait()

	// Verify no data races or inconsistencies
	assert.True(t, len(ast.observed) > 0, "Observed set should not be empty after concurrent updates")
	assert.LessOrEqual(t, len(ast.observed), trackedSeriesFactor*ast.maxCardinality, "Observed count should not exceed max cardinality multiplied by trackedSeriesFactor")
	assert.False(t, ast.overflowSince.IsZero(), "Expected state to be Overflow")

	expectedMetrics := `
	# HELP cortex_ingester_attributed_active_native_histogram_buckets The total number of active native histogram buckets per user and attribution.
    # TYPE cortex_ingester_attributed_active_native_histogram_buckets gauge
    cortex_ingester_attributed_active_native_histogram_buckets{my_team="__overflow__",tenant="user1",tracker="cost-attribution"} 200
    # HELP cortex_ingester_attributed_active_native_histogram_series The total number of active native histogram series per user and attribution.
    # TYPE cortex_ingester_attributed_active_native_histogram_series gauge
    cortex_ingester_attributed_active_native_histogram_series{my_team="__overflow__",tenant="user1",tracker="cost-attribution"} 100
    # HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
    # TYPE cortex_ingester_attributed_active_series gauge
	cortex_ingester_attributed_active_series{my_team="__overflow__",tenant="user1",tracker="cost-attribution"} 100
`
	assert.NoError(t, testutil.GatherAndCompare(costAttributionReg,
		strings.NewReader(expectedMetrics),
		"cortex_ingester_attributed_active_series",
		"cortex_ingester_attributed_active_native_histogram_series",
		"cortex_ingester_attributed_active_native_histogram_buckets"),
	)

	for i = 0; i < 100; i++ {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			lbls := labels.FromStrings("team", string(rune('A'+(i%26))))
			ast.Decrement(lbls, -1)
		}(i)
	}
	wg.Wait()

	assert.Equal(t, 0, len(ast.observed), "Observed set should be empty after all decrements")
	assert.False(t, ast.overflowSince.IsZero(), "Expected state still to be Overflow")
}
