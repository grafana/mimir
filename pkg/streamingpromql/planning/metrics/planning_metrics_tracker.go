// SPDX-License-Identifier: AGPL-3.0-only

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// MetricsTracker holds metrics trackers used by the query planner.
type MetricsTracker struct {
	StepInvariantTracker *StepInvariantExpressionMetricsTracker
}

func NewMetricsTracker(reg prometheus.Registerer) *MetricsTracker {
	return &MetricsTracker{
		StepInvariantTracker: newStepInvariantExpressionMetricsTracker(reg),
	}
}

type StepInvariantExpressionMetricsTracker struct {
	// total number of step invariant expression nodes created in planning
	nodes prometheus.Counter

	// total number of steps which have been saved - the number of steps which did not need to be re-evaluated
	steps prometheus.Counter
}

// OnStepInvariantExpressionAdded is called when a step invariant expression planning node is added to a plan.
// It increments the nodes counter by 1, and the steps saved counter is incremented by the given stepCount-1.
// It is the caller's responsibility to ensure that the step count is > 1.
func (t *StepInvariantExpressionMetricsTracker) OnStepInvariantExpressionAdded(stepCount int) {
	t.nodes.Inc()
	t.steps.Add(float64(stepCount - 1))
}

func newStepInvariantExpressionMetricsTracker(reg prometheus.Registerer) *StepInvariantExpressionMetricsTracker {
	return &StepInvariantExpressionMetricsTracker{
		nodes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_step_invariant_nodes_total",
			Help: "Total number of step invariant nodes.",
		}),
		steps: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_step_invariant_steps_saved_total",
			Help: "Total number of steps which were saved from being evaluated due to step invariant handling.",
		}),
	}
}
