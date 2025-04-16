// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"math"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	complexityThreshold = 1
)

// dynamicStep is a MetricsQueryHandler that change the step of a query dynamically
// based on the complexity of the query.
type dynamicStep struct {
	next   MetricsQueryHandler
	logger log.Logger

	dynamicStepUsageCount prometheus.CounterVec
	complexityThreshold   float64
}

func newDynamicStepMiddleware(complexityThreshold float64, logger log.Logger, registerer prometheus.Registerer) MetricsQueryMiddleware {
	dynamicStepUsageCount := promauto.With(registerer).NewCounterVec(
		prometheus.CounterOpts{
			Name: "mimir_query_frontend_dynamic_step_usage_count",
			Help: "Number of queries whose step has been adjusted dynamically.",
		},
		[]string{"query"},
	)
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &dynamicStep{
			next:   next,
			logger: logger,

			dynamicStepUsageCount: *dynamicStepUsageCount,
			complexityThreshold:   complexityThreshold,
		}
	})
}

// Do change the step of a query dynamically based on the complexity of the query in the request.
func (d *dynamicStep) Do(ctx context.Context, request MetricsQueryRequest) (Response, error) {

	// Get the cardinality of the query

	hints := request.GetHints()

	if hints == nil || hints.GetCardinalityEstimate() == nil {
		level.Debug(d.logger).Log("msg", "no cardinality estimation required for dynamic step. Skipping it")
		return d.next.Do(ctx, request)
	}

	cardinality := hints.GetCardinalityEstimate().EstimatedSeriesCount

	// Get the step of the query
	step := request.GetStep()

	newStep := d.getNewStep(cardinality, step)

	level.Debug(d.logger).Log("msg", "query step adjustment", "step", step, "new_step", newStep, "cardinality", cardinality)
	if newStep > step {
		level.Warn(d.logger).Log("msg", "query step adjusted", "step", step, "new_step", newStep, "cardinality", cardinality)
		request.SetStep(newStep)
		d.dynamicStepUsageCount.With(
			prometheus.Labels{
				"query": request.GetQuery(),
			}).Inc()
	}

	return d.next.Do(ctx, request)
}

// getNewStep returns the new step based on the complexity of the query and the current step.
func (d *dynamicStep) getNewStep(cardinality uint64, step int64) int64 {

	complexity := float64(cardinality) / float64(step)

	if complexity > d.complexityThreshold {
		// Calculate the new step based on the complexity
		step = 1000 * int64(10*math.Pow(complexity/d.complexityThreshold, 1.2))
	}
	return step
}
