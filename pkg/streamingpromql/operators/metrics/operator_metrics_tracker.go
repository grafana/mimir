package operatormetrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// MetricsTracker holds metrics trackers used by operators.
// These trackers can be passed into operators to elicit metrics
// during their lifecycle.
type MetricsTracker struct {
	StepInvariantTracker *StepInvariantExpressionMetricsTracker
}

func NewOperatorMetricsTracker(reg prometheus.Registerer) *MetricsTracker {
	return &MetricsTracker{
		StepInvariantTracker: newStepInvariantExpressionMetricsTracker(reg),
	}
}

type StepInvariantPointType int

const (
	FPoint StepInvariantPointType = iota
	HPoint StepInvariantPointType = iota
)

var stepInvariantPointTypeLabels = []string{
	"fpoint", "hpoint",
}

type StepInvariantExpressionMetricsTracker struct {
	// total number of observed step invariant nodes
	nodes prometheus.Counter

	// total number of points which were saved from being retrieved from the inner operation
	points *prometheus.CounterVec
}

// OnStepInvariantNodeObserved is called when a step invariant node is observed.
// It increments the nodes counter by 1.
func (t *StepInvariantExpressionMetricsTracker) OnStepInvariantNodeObserved() {
	t.nodes.Inc()
}

// NodeCounter returns the counter tracking step invariant nodes.
// This is provided for unit tests and allowing the underlying counter to remain unexported.
func (t *StepInvariantExpressionMetricsTracker) NodeCounter() prometheus.Counter {
	return t.nodes
}

// Counter returns the counter tracking step invariant point savings for a given type.
// This is provided for unit tests and allowing the underlying counter to remain unexported.
func (t *StepInvariantExpressionMetricsTracker) Counter(pointType StepInvariantPointType) prometheus.Counter {
	return t.points.WithLabelValues(stepInvariantPointTypeLabels[pointType])
}

// OnStepInvariantStepsSaved increments the counter related to tracking step invariant point savings for the given point type.
// The actual counter will be incremented by stepCount-1, reflecting that the first step loaded data.
// If the stepCount is less than or equal to 1 then this function is a no-op.
func (t *StepInvariantExpressionMetricsTracker) OnStepInvariantStepsSaved(pointType StepInvariantPointType, stepCount int) {
	if stepCount <= 1 {
		return
	}
	t.points.WithLabelValues(stepInvariantPointTypeLabels[pointType]).Add(float64(stepCount - 1))
}

func newStepInvariantExpressionMetricsTracker(reg prometheus.Registerer) *StepInvariantExpressionMetricsTracker {
	return &StepInvariantExpressionMetricsTracker{
		nodes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_step_invariant_nodes_total",
			Help: "Total number of step invariant nodes.",
		}),
		points: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_step_invariant_points_saved_total",
			Help: "Total number of samples which were saved from being queried / loaded due to step invariant handling.",
		}, []string{"type"}),
	}
}
