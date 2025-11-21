// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestWrapQueryFuncWithReadConsistency(t *testing.T) {
	const userID = "user-1"

	runWrappedFunc := func(ctx context.Context, limits RulesLimits) (hasConsistencyLevel bool, consistencyLevel string, hasConsistencyMaxDelay bool, consistencyMaxDelay time.Duration, err error) {
		orig := func(ctx context.Context, _ string, _ time.Time) (promql.Vector, error) {
			consistencyLevel, hasConsistencyLevel = api.ReadConsistencyLevelFromContext(ctx)
			consistencyMaxDelay, hasConsistencyMaxDelay = api.ReadConsistencyMaxDelayFromContext(ctx)
			return promql.Vector{}, nil
		}

		_, _ = WrapQueryFuncWithReadConsistency(orig, limits, userID, log.NewNopLogger())(ctx, "", time.Now())
		return
	}

	t.Run("should inject strong read consistency if the rule detail is missing in the context", func(t *testing.T) {
		for _, configuredMaxDelay := range []time.Duration{0, time.Minute} {
			t.Run(fmt.Sprintf("configured consistency max delay: %s", configuredMaxDelay.String()), func(t *testing.T) {
				limits := validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
					defaults.RulerEvaluationConsistencyMaxDelay = model.Duration(configuredMaxDelay)
				})

				hasConsistencyLevel, consistencyLevel, hasConsistencyMaxDelay, _, _ := runWrappedFunc(context.Background(), limits)
				assert.True(t, hasConsistencyLevel)
				assert.Equal(t, api.ReadConsistencyStrong, consistencyLevel)
				assert.False(t, hasConsistencyMaxDelay)
			})
		}
	})

	t.Run("should inject strong read consistency if it's unknown whether the rule has dependencies", func(t *testing.T) {
		for _, configuredMaxDelay := range []time.Duration{0, time.Minute} {
			t.Run(fmt.Sprintf("configured consistency max delay: %s", configuredMaxDelay.String()), func(t *testing.T) {
				var (
					r   = rules.NewRecordingRule("", &parser.StringLiteral{}, labels.New())
					ctx = rules.NewOriginContext(context.Background(), rules.NewRuleDetail(r))
				)

				limits := validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
					defaults.RulerEvaluationConsistencyMaxDelay = model.Duration(configuredMaxDelay)
				})

				hasConsistencyLevel, consistencyLevel, hasConsistencyMaxDelay, _, _ := runWrappedFunc(ctx, limits)
				assert.True(t, hasConsistencyLevel)
				assert.Equal(t, api.ReadConsistencyStrong, consistencyLevel)
				assert.False(t, hasConsistencyMaxDelay)
			})
		}
	})

	t.Run("should inject strong read consistency if the rule has dependencies", func(t *testing.T) {
		for _, configuredMaxDelay := range []time.Duration{0, time.Minute} {
			t.Run(fmt.Sprintf("configured consistency max delay: %s", configuredMaxDelay.String()), func(t *testing.T) {
				limits := validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
					defaults.RulerEvaluationConsistencyMaxDelay = model.Duration(configuredMaxDelay)
				})

				r := rules.NewRecordingRule("", &parser.StringLiteral{}, labels.New())
				r.SetDependencyRules([]rules.Rule{rules.NewRecordingRule("other", &parser.StringLiteral{}, labels.New())})

				ctx := rules.NewOriginContext(context.Background(), rules.NewRuleDetail(r))
				hasConsistencyLevel, consistencyLevel, hasConsistencyMaxDelay, _, _ := runWrappedFunc(ctx, limits)
				assert.True(t, hasConsistencyLevel)
				assert.Equal(t, api.ReadConsistencyStrong, consistencyLevel)
				assert.False(t, hasConsistencyMaxDelay)
			})
		}
	})

	t.Run("should not inject read consistency level if the rule has no dependencies, to let run with the per-tenant default", func(t *testing.T) {
		r := rules.NewRecordingRule("", &parser.StringLiteral{}, labels.New())
		r.SetDependencyRules([]rules.Rule{})

		ctx := rules.NewOriginContext(context.Background(), rules.NewRuleDetail(r))
		hasConsistencyLevel, _, hasConsistencyMaxDelay, _, _ := runWrappedFunc(ctx, validation.MockDefaultOverrides())
		assert.False(t, hasConsistencyLevel)
		assert.False(t, hasConsistencyMaxDelay)
	})

	t.Run("should inject read consistency max delay if the rule has no dependencies, and max delay has been configured", func(t *testing.T) {
		limits := validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
			defaults.RulerEvaluationConsistencyMaxDelay = model.Duration(time.Minute)
		})

		r := rules.NewRecordingRule("", &parser.StringLiteral{}, labels.New())
		r.SetDependencyRules([]rules.Rule{})

		ctx := rules.NewOriginContext(context.Background(), rules.NewRuleDetail(r))
		hasConsistencyLevel, _, hasConsistencyMaxDelay, consistencyMaxDelay, _ := runWrappedFunc(ctx, limits)
		assert.False(t, hasConsistencyLevel)
		assert.True(t, hasConsistencyMaxDelay)
		assert.Equal(t, time.Minute, consistencyMaxDelay)
	})
}

func TestWrapQueryableWithReadConsistency(t *testing.T) {
	runWrappedSelect := func(matchers ...*labels.Matcher) (hasReadConsistency bool, readConsistencyLevel string) {
		querier := newQuerierMock()
		querier.selectFunc = func(ctx context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
			readConsistencyLevel, hasReadConsistency = api.ReadConsistencyLevelFromContext(ctx)
			return storage.EmptySeriesSet()
		}

		wrappedQueryable := WrapQueryableWithReadConsistency(&storage.MockQueryable{MockQuerier: querier}, log.NewNopLogger())
		wrapperQuerier, err := wrappedQueryable.Querier(math.MinInt64, math.MaxInt64)
		require.NoError(t, err)

		wrapperQuerier.Select(context.Background(), false, nil, matchers...)
		return
	}

	t.Run("should inject strong read consistency if querying ALERTS_FOR_STATE", func(t *testing.T) {
		hasReadConsistency, readConsistencyLevel := runWrappedSelect(
			labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "ALERTS_FOR_STATE"),
			labels.MustNewMatcher(labels.MatchEqual, "alertname", "my_test_alert"),
		)

		assert.True(t, hasReadConsistency)
		assert.Equal(t, api.ReadConsistencyStrong, readConsistencyLevel)
	})

	t.Run("should not inject read consistency level if not querying ALERTS_FOR_STATE", func(t *testing.T) {
		hasReadConsistency, _ := runWrappedSelect(
			labels.MustNewMatcher(labels.MatchEqual, "alertname", "my_test_alert"),
		)

		assert.False(t, hasReadConsistency)
	})
}

func TestIsQueryingAlertsForStateMetric(t *testing.T) {
	assert.False(t, isQueryingAlertsForStateMetric(""))
	assert.False(t, isQueryingAlertsForStateMetric("test"))
	assert.False(t, isQueryingAlertsForStateMetric("ALERTS"))
	assert.True(t, isQueryingAlertsForStateMetric("ALERTS_FOR_STATE"))

	assert.False(t, isQueryingAlertsForStateMetric("", labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test")))
	assert.False(t, isQueryingAlertsForStateMetric("", labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "ALERTS")))
	assert.True(t, isQueryingAlertsForStateMetric("", labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "ALERTS_FOR_STATE")))
	assert.True(t, isQueryingAlertsForStateMetric("", labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, "ALERTS_.*")))
}

type querierMock struct {
	storage.Querier

	selectFunc func(_ context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet
}

func newQuerierMock() *querierMock {
	return &querierMock{
		Querier: storage.NoopQuerier(),
	}
}

func (m *querierMock) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if m.selectFunc != nil {
		return m.selectFunc(ctx, sortSeries, hints, matchers...)
	}

	return storage.EmptySeriesSet()
}
