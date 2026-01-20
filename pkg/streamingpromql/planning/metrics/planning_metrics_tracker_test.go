// SPDX-License-Identifier: AGPL-3.0-only

package planningmetrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestNewStepInvariantExpressionMetricsTrackerNodes(t *testing.T) {
	tracker := NewMetricsTracker(prometheus.NewRegistry())
	require.Same(t, tracker.StepInvariantTracker.nodes, tracker.StepInvariantTracker.NodesCounter())

	require.Equal(t, float64(0), testutil.ToFloat64(tracker.StepInvariantTracker.nodes))
	require.Equal(t, float64(0), testutil.ToFloat64(tracker.StepInvariantTracker.steps))

	tracker.StepInvariantTracker.OnStepInvariantExpressionAdded(10)
	require.Equal(t, float64(1), testutil.ToFloat64(tracker.StepInvariantTracker.nodes))
	require.Equal(t, float64(9), testutil.ToFloat64(tracker.StepInvariantTracker.steps))

	tracker.StepInvariantTracker.OnStepInvariantExpressionAdded(5)
	require.Equal(t, float64(2), testutil.ToFloat64(tracker.StepInvariantTracker.nodes))
	require.Equal(t, float64(13), testutil.ToFloat64(tracker.StepInvariantTracker.steps))
}

func TestNewStepInvariantExpressionMetricsTrackerNodesNotEnoughSteps(t *testing.T) {
	tracker := NewMetricsTracker(prometheus.NewRegistry())
	require.Same(t, tracker.StepInvariantTracker.nodes, tracker.StepInvariantTracker.NodesCounter())

	require.Equal(t, float64(0), testutil.ToFloat64(tracker.StepInvariantTracker.nodes))
	require.Equal(t, float64(0), testutil.ToFloat64(tracker.StepInvariantTracker.steps))

	tracker.StepInvariantTracker.OnStepInvariantExpressionAdded(0)
	require.Equal(t, float64(0), testutil.ToFloat64(tracker.StepInvariantTracker.nodes))
	require.Equal(t, float64(0), testutil.ToFloat64(tracker.StepInvariantTracker.steps))

	tracker.StepInvariantTracker.OnStepInvariantExpressionAdded(1)
	require.Equal(t, float64(0), testutil.ToFloat64(tracker.StepInvariantTracker.nodes))
	require.Equal(t, float64(0), testutil.ToFloat64(tracker.StepInvariantTracker.steps))
}
