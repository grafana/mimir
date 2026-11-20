// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const alertForStateMetricName = "ALERTS_FOR_STATE"

// WrapQueryFuncWithReadConsistency wraps rules.QueryFunc with a function that injects strong read consistency
// requirement in the context if the query is originated from a rule which depends on other rules in the same
// rule group.
func WrapQueryFuncWithReadConsistency(fn rules.QueryFunc, limits RulesLimits, userID string, logger log.Logger) rules.QueryFunc {
	return func(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {

		// Get details about the rule and tenant's settings.
		detail := rules.FromOriginContext(ctx)
		maxConsistencyDelay := limits.RulerEvaluationConsistencyMaxDelay(userID)

		if !detail.NoDependencyRules {
			// If the rule has dependencies then we should enforce strong read consistency.
			spanLog := spanlogger.FromContext(ctx, logger)
			spanLog.SetTag("read_consistency", api.ReadConsistencyStrong)
			spanLog.DebugLog("msg", "forced strong read consistency because the rule depends on other rules in the same rule group")

			ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)
		} else if maxConsistencyDelay > 0 {
			// The rule doesn't request strong read consistency, but the tenant has a max delay enforcement
			// configured for eventually consistent rule evaluations. We intentionally don't set the consistency level
			// because the default consistency level could have been overridden on a per-tenant basis, so
			// we just let the query run with whatever consistency level it was configured for the tenant.
			spanLog := spanlogger.FromContext(ctx, logger)
			spanLog.SetTag("read_consistency_max_delay", maxConsistencyDelay)
			spanLog.DebugLog("msg", "configured read consistency max delay")

			ctx = api.ContextWithReadConsistencyMaxDelay(ctx, maxConsistencyDelay)
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

	set := q.next.Select(ctx, sortSeries, hints, matchers...)
	if isQueryingAlertsForStateMetric("", matchers...) {
		return bufferAlertsForStateSeries(ctx, set)
	}

	return set
}

// bufferAlertsForStateSeries eagerly reads the input series set, in order, and returns a series set
// whose series can be iterated in any order and any number of times.
//
// The series returned by a Mimir queryable may be backed by the ingesters chunks stream, which requires
// series to be consumed in the same order they're returned. However, rules.Group.RestoreForState retains
// all the series and then iterates only the ones matching an active alert, in the (random) order of the active
// alerts map. Reading a series out of order fails and the activation state of the alert is lost.
//
// RestoreForState only looks at the last float sample of each series, so that is the only one retained
// to keep memory bounded. The queryable wrapped by this function is only used for restoring the alerts state.
func bufferAlertsForStateSeries(ctx context.Context, set storage.SeriesSet) storage.SeriesSet {
	var series []storage.Series

	for set.Next() {
		if err := ctx.Err(); err != nil {
			return storage.ErrSeriesSet(err)
		}

		s := set.At()
		// Copy the labels: they may be backed by a buffer that is reused once the stream moves on.
		lbls := s.Labels().Copy()

		var floats []promql.FPoint
		it := s.Iterator(nil)
		// Drain the whole series, even past a non-float sample, so the next series can be read from the stream.
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
			if vt != chunkenc.ValFloat {
				continue
			}
			t, v := it.At()
			// Replace the previous sample, so that at most one is retained.
			floats = append(floats[:0], promql.FPoint{T: t, F: v})
		}
		if err := it.Err(); err != nil {
			return storage.ErrSeriesSet(err)
		}

		series = append(series, promql.NewStorageSeries(promql.Series{Metric: lbls, Floats: floats}))
	}
	if err := set.Err(); err != nil {
		return storage.ErrSeriesSet(err)
	}

	return &bufferedSeriesSet{series: series, pos: -1, warnings: set.Warnings()}
}

type bufferedSeriesSet struct {
	series   []storage.Series
	pos      int
	warnings annotations.Annotations
}

func (s *bufferedSeriesSet) Next() bool {
	s.pos++
	return s.pos < len(s.series)
}

func (s *bufferedSeriesSet) At() storage.Series { return s.series[s.pos] }

func (s *bufferedSeriesSet) Err() error { return nil }

func (s *bufferedSeriesSet) Warnings() annotations.Annotations { return s.warnings }

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
		if matcher.Name == model.MetricNameLabel && matcher.Matches(alertForStateMetricName) {
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
