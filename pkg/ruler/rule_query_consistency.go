// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const alertForStateMetricName = "ALERTS_FOR_STATE"

// WrapQueryFuncWithReadConsistency wraps rules.QueryFunc with a function that injects strong read consistency
// requirement in the context if the query is originated from a rule which depends on other rules in the same
// rule group.
func WrapQueryFuncWithReadConsistency(fn rules.QueryFunc, logger log.Logger) rules.QueryFunc {
	return func(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {

		// Get details about the rule.
		detail := rules.FromOriginContext(ctx)

		// If the rule has dependencies then we should enforce strong read consistency,
		// otherwise we leave it empty to have Mimir falling back to the per-tenant default.
		if !detail.NoDependencyRules {
			spanLog := spanlogger.FromContext(ctx, logger)
			spanLog.SetTag("read_consistency", api.ReadConsistencyStrong)
			spanLog.DebugLog("msg", "forced strong read consistency because the rule depends on other rules in the same rule group")

			ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)
		}

		return fn(ctx, qs, t)
	}
}

// WrapQueryableWithReadConsistency wraps storage.Queryable with a queryable that injects strong read consistency
// requirement in the context for any request matching ALERTS_FOR_STATE metric name.
//
// The ALERTS_FOR_STATE metric is used to restore the state of a firing alert each time a rule Group is started.
// In case of Mimir, it could happen for example when the ruler starts, or rule groups are resharded among rulers.
//
// When querying the ALERTS_FOR_STATE, ruler requires strong consistency in order to ensure we restore the state
// from the last evaluation. Without such guarantee, the ruler may query a previous state.
func WrapQueryableWithReadConsistency(q storage.Queryable, logger log.Logger) storage.Queryable {
	return &readConsistencyQueryable{
		next:   q,
		logger: logger,
	}
}

type readConsistencyQueryable struct {
	next   storage.Queryable
	logger log.Logger
}

// Querier implements storage.Queryable.
func (q *readConsistencyQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	querier, err := q.next.Querier(mint, maxt)
	if err != nil {
		return querier, err
	}

	return &readConsistencyQuerier{next: querier, logger: q.logger}, nil
}

type readConsistencyQuerier struct {
	next   storage.Querier
	logger log.Logger
}

// Select implements storage.Querier.
func (q *readConsistencyQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	ctx = forceStrongReadConsistencyIfQueryingAlertsForStateMetric(ctx, matchers, q.logger)

	return q.next.Select(ctx, sortSeries, hints, matchers...)
}

// LabelValues implements storage.Querier.
func (q *readConsistencyQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	ctx = forceStrongReadConsistencyIfQueryingAlertsForStateMetric(ctx, matchers, q.logger)

	return q.next.LabelValues(ctx, name, hints, matchers...)
}

// LabelNames implements storage.Querier.
func (q *readConsistencyQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	ctx = forceStrongReadConsistencyIfQueryingAlertsForStateMetric(ctx, matchers, q.logger)

	return q.next.LabelNames(ctx, hints, matchers...)
}

// Close implements storage.Querier.
func (q *readConsistencyQuerier) Close() error {
	return q.next.Close()
}

// isQueryingAlertsForStateMetric checks whether the input metricName or matchers match the
// ALERTS_FOR_STATE metric.
func isQueryingAlertsForStateMetric(metricName string, matchers ...*labels.Matcher) bool {
	if metricName == alertForStateMetricName {
		return true
	}

	for _, matcher := range matchers {
		if matcher.Name == labels.MetricName && matcher.Matches(alertForStateMetricName) {
			return true
		}
	}

	return false
}

// forceStrongReadConsistencyIfQueryingAlertsForStateMetric enforces strong read consistency if from matchers we
// detect that the query is querying the ALERTS_FOR_STATE metric, otherwise we leave it empty to have Mimir falling
// back to the per-tenant default.
func forceStrongReadConsistencyIfQueryingAlertsForStateMetric(ctx context.Context, matchers []*labels.Matcher, logger log.Logger) context.Context {
	if isQueryingAlertsForStateMetric("", matchers...) {
		spanLog := spanlogger.FromContext(ctx, logger)
		spanLog.SetTag("read_consistency", api.ReadConsistencyStrong)
		spanLog.DebugLog("msg", fmt.Sprintf("forced strong read consistency because %s metric has been queried", alertForStateMetricName))

		ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)
	}

	return ctx
}
