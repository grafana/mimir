// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
)

func TestActiveTracker_hasSameLabels(t *testing.T) {
	ast := newTestManager().ActiveSeriesTracker("user1")
	assert.True(t, ast.hasSameLabels([]string{"team"}), "Expected cost attribution labels mismatch")
}

func TestActiveTracker_updateCounters(t *testing.T) {
	ast := newTestManager().ActiveSeriesTracker("user3")
	lbls1 := labels.FromStrings("department", "foo", "service", "bar")
	lbls2 := labels.FromStrings("department", "bar", "service", "baz")
	lbls3 := labels.FromStrings("department", "baz", "service", "foo")

	ast.Increment(lbls1, time.Unix(1, 0))
	assert.Equal(t, int64(0), ast.overflowSince.Load(), "First observation, should not overflow")

	ast.Decrement(lbls1)
	assert.Equal(t, int64(0), ast.overflowSince.Load(), "First observation decremented, should not overflow")
	assert.Equal(t, 0, len(ast.observed), "First observation decremented, should be removed since it reached 0")

	ast.Increment(lbls1, time.Unix(2, 0))
	ast.Increment(lbls2, time.Unix(2, 0))
	assert.Equal(t, int64(0), ast.overflowSince.Load(), "Second observation, should not overflow")

	ast.Increment(lbls3, time.Unix(3, 0))
	assert.Equal(t, int64(3), ast.overflowSince.Load(), "Third observation, should overflow")

	ast.Increment(lbls3, time.Unix(4, 0))
	assert.Equal(t, int64(3), ast.overflowSince.Load(), "Fourth observation, should stay overflow")
}

func TestActiveTracker_Concurrency(t *testing.T) {
	m := newTestManager()
	ast := m.ActiveSeriesTracker("user1")

	var wg sync.WaitGroup
	var i int64
	for i = 0; i < 100; i++ {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			lbls := labels.FromStrings("team", string(rune('A'+(i%26))))
			ast.Increment(lbls, time.Unix(i, 0))
		}(i)
	}
	wg.Wait()

	// Verify no data races or inconsistencies
	assert.True(t, len(ast.observed) > 0, "Observed set should not be empty after concurrent updates")
	assert.LessOrEqual(t, len(ast.observed), ast.maxCardinality, "Observed count should not exceed max cardinality")
	assert.NotEqual(t, 0, ast.overflowSince.Load(), "Expected state to be Overflow")

	expectedMetrics := `
    # HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
    # TYPE cortex_ingester_attributed_active_series gauge
	cortex_ingester_attributed_active_series{team="__overflow__",tenant="user1",tracker="cost-attribution"} 100
`
	assert.NoError(t, testutil.GatherAndCompare(m.reg, strings.NewReader(expectedMetrics), "cortex_ingester_attributed_active_series"))

	for i = 0; i < 100; i++ {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			lbls := labels.FromStrings("team", string(rune('A'+(i%26))))
			ast.Decrement(lbls)
		}(i)
	}
	wg.Wait()

	assert.Equal(t, 0, len(ast.observed), "Observed set should be empty after all decrements")
	assert.NotEqual(t, 0, ast.overflowSince.Load(), "Expected state still to be Overflow")
}
