// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"strings"
	"sync"
	"testing"
	"time"

	model "github.com/grafana/mimir/pkg/costattribution/model"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
)

func TestActiveTracker_hasSameLabels(t *testing.T) {
	manager, _, _ := newTestManager()
	ast := manager.ActiveSeriesTracker("user1")
	assert.True(t, ast.hasSameLabels([]model.Label{{Input: "team", Output: ""}}), "Expected cost attribution labels mismatch")
}

func TestActiveTracker_IncrementDecrement(t *testing.T) {
	manager, _, _ := newTestManager()
	ast := manager.ActiveSeriesTracker("user3")
	lbls1 := labels.FromStrings("department", "foo", "service", "bar")
	lbls2 := labels.FromStrings("department", "bar", "service", "baz")
	lbls3 := labels.FromStrings("department", "baz", "service", "foo")

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
	assert.Equal(t, time.Unix(3, 0), ast.overflowSince, "Third observation, should overflow")
	assert.Equal(t, 2, len(ast.observed))

	ast.Increment(lbls3, time.Unix(4, 0), -1)
	assert.Equal(t, time.Unix(3, 0), ast.overflowSince, "Fourth observation, should stay overflow")
	assert.Equal(t, 2, len(ast.observed))
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
	assert.LessOrEqual(t, len(ast.observed), ast.maxCardinality, "Observed count should not exceed max cardinality")
	assert.False(t, ast.overflowSince.IsZero(), "Expected state to be Overflow")

	expectedMetrics := `
	# HELP cortex_ingester_attributed_active_native_histogram_buckets The total number of active native histogram buckets per user and attribution.
    # TYPE cortex_ingester_attributed_active_native_histogram_buckets gauge
    cortex_ingester_attributed_active_native_histogram_buckets{team="__overflow__",tenant="user1",tracker="cost-attribution"} 200
    # HELP cortex_ingester_attributed_active_native_histogram_series The total number of active native histogram series per user and attribution.
    # TYPE cortex_ingester_attributed_active_native_histogram_series gauge
    cortex_ingester_attributed_active_native_histogram_series{team="__overflow__",tenant="user1",tracker="cost-attribution"} 100
    # HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
    # TYPE cortex_ingester_attributed_active_series gauge
	cortex_ingester_attributed_active_series{team="__overflow__",tenant="user1",tracker="cost-attribution"} 100
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
