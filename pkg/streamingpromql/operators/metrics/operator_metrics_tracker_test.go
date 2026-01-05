// SPDX-License-Identifier: AGPL-3.0-only

package operatormetrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestNewStepInvariantExpressionMetricsTrackerNodes(t *testing.T) {
	tracker := NewOperatorMetricsTracker(prometheus.NewRegistry())
	require.Same(t, tracker.StepInvariantTracker.nodes, tracker.StepInvariantTracker.NodeCounter())

	require.Equal(t, float64(0), testutil.ToFloat64(tracker.StepInvariantTracker.nodes))
	tracker.StepInvariantTracker.OnStepInvariantNodeObserved()
	require.Equal(t, float64(1), testutil.ToFloat64(tracker.StepInvariantTracker.nodes))
}

func TestNewStepInvariantExpressionMetricsTrackerFloatPoints(t *testing.T) {
	tracker := NewOperatorMetricsTracker(prometheus.NewRegistry())

	// Passing in a step count <= 1 has no effect
	require.Equal(t, float64(0), testutil.ToFloat64(tracker.StepInvariantTracker.Counter(FPoint)))
	tracker.StepInvariantTracker.OnStepInvariantStepsSaved(FPoint, 0)
	require.Equal(t, float64(0), testutil.ToFloat64(tracker.StepInvariantTracker.Counter(FPoint)))
	tracker.StepInvariantTracker.OnStepInvariantStepsSaved(FPoint, 1)
	require.Equal(t, float64(0), testutil.ToFloat64(tracker.StepInvariantTracker.Counter(FPoint)))

	tracker.StepInvariantTracker.OnStepInvariantStepsSaved(FPoint, 2)
	require.Equal(t, float64(1), testutil.ToFloat64(tracker.StepInvariantTracker.Counter(FPoint)))
	tracker.StepInvariantTracker.OnStepInvariantStepsSaved(FPoint, 12)
	require.Equal(t, float64(1+11), testutil.ToFloat64(tracker.StepInvariantTracker.Counter(FPoint)))
}

func TestNewStepInvariantExpressionMetricsTrackerHistogramPoints(t *testing.T) {
	tracker := NewOperatorMetricsTracker(prometheus.NewRegistry())

	// Passing in a step count <= 1 has no effect
	require.Equal(t, float64(0), testutil.ToFloat64(tracker.StepInvariantTracker.Counter(HPoint)))
	tracker.StepInvariantTracker.OnStepInvariantStepsSaved(HPoint, 0)
	require.Equal(t, float64(0), testutil.ToFloat64(tracker.StepInvariantTracker.Counter(HPoint)))
	tracker.StepInvariantTracker.OnStepInvariantStepsSaved(HPoint, 1)
	require.Equal(t, float64(0), testutil.ToFloat64(tracker.StepInvariantTracker.Counter(HPoint)))

	tracker.StepInvariantTracker.OnStepInvariantStepsSaved(HPoint, 2)
	require.Equal(t, float64(1), testutil.ToFloat64(tracker.StepInvariantTracker.Counter(HPoint)))
	tracker.StepInvariantTracker.OnStepInvariantStepsSaved(HPoint, 12)
	require.Equal(t, float64(1+11), testutil.ToFloat64(tracker.StepInvariantTracker.Counter(HPoint)))
}
