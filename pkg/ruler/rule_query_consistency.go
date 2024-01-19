// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/querier/api"
)

// WrapQueryFuncWithReadConsistency wraps rules.QueryFunc with a function that injects strong read consistency
// requirement in the context if the query is originated from a rule which depends on other rules in the same
// rule group.
func WrapQueryFuncWithReadConsistency(fn rules.QueryFunc) rules.QueryFunc {
	return func(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {
		// Get details about the rule.
		detail := rules.FromOriginContext(ctx)

		// If the rule has dependencies then we should enforce strong read consistency,
		// otherwise we'll fallback to the per-tenant default.
		if !detail.NoDependencyRules {
			ctx = api.ContextWithReadConsistency(ctx, api.ReadConsistencyStrong)
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
func WrapQueryableWithReadConsistency(q storage.Queryable) storage.Queryable {
	return &readConsistencyQueryable{next: q}
}

type readConsistencyQueryable struct {
	next storage.Queryable
}

// Querier implements storage.Queryable.
func (q *readConsistencyQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	querier, err := q.next.Querier(mint, maxt)
	if err != nil {
		return querier, err
	}

	return &readConsistencyQuerier{next: querier}, nil
}

type readConsistencyQuerier struct {
	next storage.Querier
}

// Select implements storage.Querier.
func (q *readConsistencyQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	// Enforce strong read consistency if it's querying the ALERTS_FOR_STATE metric, otherwise
	// fallback to the default.
	if isQueryingAlertsForStateMetric("", matchers...) {
		ctx = api.ContextWithReadConsistency(ctx, api.ReadConsistencyStrong)
	}

	return q.next.Select(ctx, sortSeries, hints, matchers...)
}

// LabelValues implements storage.Querier.
func (q *readConsistencyQuerier) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	// Enforce strong read consistency if it's querying the ALERTS_FOR_STATE metric, otherwise
	// fallback to the default.
	if isQueryingAlertsForStateMetric(name, matchers...) {
		ctx = api.ContextWithReadConsistency(ctx, api.ReadConsistencyStrong)
	}

	return q.next.LabelValues(ctx, name, matchers...)
}

// LabelNames implements storage.Querier.
func (q *readConsistencyQuerier) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	// Enforce strong read consistency if it's querying the ALERTS_FOR_STATE metric, otherwise
	// fallback to the default.
	if isQueryingAlertsForStateMetric("", matchers...) {
		ctx = api.ContextWithReadConsistency(ctx, api.ReadConsistencyStrong)
	}

	return q.next.LabelNames(ctx, matchers...)
}

// Close implements storage.Querier.
func (q *readConsistencyQuerier) Close() error {
	return q.next.Close()
}

// isQueryingAlertsForStateMetric checks whether the input metricName or matchers match the
// ALERTS_FOR_STATE metric.
func isQueryingAlertsForStateMetric(metricName string, matchers ...*labels.Matcher) bool {
	const alertForStateMetricName = "ALERTS_FOR_STATE"

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
