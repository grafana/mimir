// SPDX-License-Identifier: AGPL-3.0-only

package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestNewStepInvariantExpressionMetricsTrackerNodes(t *testing.T) {
	tracker := NewMetricsTracker(prometheus.NewRegistry())

	require.Equal(t, float64(0), testutil.ToFloat64(tracker.StepInvariantTracker.nodes))
	require.Equal(t, float64(0), testutil.ToFloat64(tracker.StepInvariantTracker.steps))

	tracker.StepInvariantTracker.OnStepInvariantExpressionAdded(10)
	require.Equal(t, float64(1), testutil.ToFloat64(tracker.StepInvariantTracker.nodes))
	require.Equal(t, float64(9), testutil.ToFloat64(tracker.StepInvariantTracker.steps))

	tracker.StepInvariantTracker.OnStepInvariantExpressionAdded(5)
	require.Equal(t, float64(2), testutil.ToFloat64(tracker.StepInvariantTracker.nodes))
	require.Equal(t, float64(13), testutil.ToFloat64(tracker.StepInvariantTracker.steps))
}
