// SPDX-License-Identifier: AGPL-3.0-only

package aggregations

import (
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestCounterResets(t *testing.T) {
	testCases := map[string]struct {
		hints    []histogram.CounterResetHint
		expected bool
	}{
		"consecutive NotCounterReset": {
			hints:    []histogram.CounterResetHint{histogram.NotCounterReset, histogram.NotCounterReset},
			expected: false,
		},
		"consecutive UnknownCounterReset": {
			hints:    []histogram.CounterResetHint{histogram.UnknownCounterReset, histogram.UnknownCounterReset},
			expected: false,
		},
		"consecutive CounterReset": {
			hints:    []histogram.CounterResetHint{histogram.CounterReset, histogram.CounterReset},
			expected: false,
		},
		"consecutive GaugeType": {
			hints:    []histogram.CounterResetHint{histogram.GaugeType, histogram.GaugeType},
			expected: false,
		},
		"NotCounterReset + UnknownCounterReset": {
			hints:    []histogram.CounterResetHint{histogram.NotCounterReset, histogram.UnknownCounterReset},
			expected: false,
		},
		"NotCounterReset + GaugeType": {
			hints:    []histogram.CounterResetHint{histogram.NotCounterReset, histogram.GaugeType},
			expected: false,
		},
		"NotCounterReset + CounterReset": {
			hints:    []histogram.CounterResetHint{histogram.NotCounterReset, histogram.CounterReset},
			expected: true,
		},
		"CounterReset + GaugeType": {
			hints:    []histogram.CounterResetHint{histogram.CounterReset, histogram.GaugeType},
			expected: false,
		},
		"CounterReset + UnknownCounterReset": {
			hints:    []histogram.CounterResetHint{histogram.CounterReset, histogram.UnknownCounterReset},
			expected: false,
		},
		"GaugeType + UnknownCounterReset": {
			hints:    []histogram.CounterResetHint{histogram.GaugeType, histogram.UnknownCounterReset},
			expected: false,
		},
		"NotCounterReset, UnknownCounterReset, CounterReset": {
			hints:    []histogram.CounterResetHint{histogram.NotCounterReset, histogram.UnknownCounterReset, histogram.CounterReset},
			expected: true,
		},
		"NotCounterReset, GaugeType, CounterReset multiple": {
			hints:    []histogram.CounterResetHint{histogram.NotCounterReset, histogram.GaugeType, histogram.CounterReset},
			expected: true,
		},
		"NotCounterReset + GaugeType across multiple": {
			hints:    []histogram.CounterResetHint{histogram.NotCounterReset, histogram.UnknownCounterReset, histogram.GaugeType},
			expected: false,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			tracker, err := newHistogramCounterResetTracker(1, limiter.NewUnlimitedMemoryConsumptionTracker(t.Context()))
			require.NoError(t, err)

			tracker.init(0, testCase.hints[0])
			if len(testCase.hints) > 2 {
				hints := testCase.hints[1 : len(testCase.hints)-2]
				for _, hint := range hints {
					_ = tracker.checkCounterResetConflicts(0, hint)
				}
			}
			require.Equal(t, testCase.expected, tracker.checkCounterResetConflicts(0, testCase.hints[len(testCase.hints)-1]))

			// repeat in the opposite order
			slices.Reverse(testCase.hints)
			tracker, err = newHistogramCounterResetTracker(1, limiter.NewUnlimitedMemoryConsumptionTracker(t.Context()))
			require.NoError(t, err)

			tracker.init(0, testCase.hints[0])
			if len(testCase.hints) > 2 {
				hints := testCase.hints[1 : len(testCase.hints)-2]
				for _, hint := range hints {
					_ = tracker.checkCounterResetConflicts(0, hint)
				}
			}
			require.Equal(t, testCase.expected, tracker.checkCounterResetConflicts(0, testCase.hints[len(testCase.hints)-1]))
		})
	}
}

func TestCounterResetsSeries(t *testing.T) {
	tracker, err := newHistogramCounterResetTracker(2, limiter.NewUnlimitedMemoryConsumptionTracker(t.Context()))
	require.NoError(t, err)
	tracker.init(0, histogram.UnknownCounterReset)
	tracker.init(1, histogram.NotCounterReset)
	require.False(t, tracker.checkCounterResetConflicts(0, histogram.NotCounterReset))
	require.True(t, tracker.checkCounterResetConflicts(1, histogram.CounterReset))
	require.True(t, tracker.checkCounterResetConflicts(0, histogram.CounterReset))
	require.False(t, tracker.checkCounterResetConflicts(1, histogram.NotCounterReset))
}
