// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"errors"
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
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
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

func TestWrapQueryableWithReadConsistency_AlertsForStateStreamOrder(t *testing.T) {
	alertsForState := labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, alertForStateMetricName)

	newQuerier := func(stream *orderedStreamSeriesSet) storage.Querier {
		querier := newQuerierMock()
		querier.selectFunc = func(context.Context, bool, *storage.SelectHints, ...*labels.Matcher) storage.SeriesSet {
			return stream
		}

		wrappedQueryable := WrapQueryableWithReadConsistency(&storage.MockQueryable{MockQuerier: querier}, log.NewNopLogger())
		wrappedQuerier, err := wrappedQueryable.Querier(math.MinInt64, math.MaxInt64)
		require.NoError(t, err)
		return wrappedQuerier
	}

	t.Run("series of ALERTS_FOR_STATE can be iterated in any order, like RestoreForState does", func(t *testing.T) {
		stream := newOrderedStreamSeriesSet(
			[]labels.Labels{labels.FromStrings("instance", "a"), labels.FromStrings("instance", "b"), labels.FromStrings("instance", "c")},
			[][]float64{{1, 2}, {3, 4}, {5, 6}},
		)

		set := newQuerier(stream).Select(context.Background(), true, nil, alertsForState)

		// Retain all the series first, then consume them in reverse order (the first series does not
		// have a matching active alert, so it is never read before the others).
		var series []storage.Series
		for set.Next() {
			series = append(series, set.At())
		}
		require.NoError(t, set.Err())
		require.Len(t, series, 3)

		expected := []float64{2, 4, 6}
		for i := len(series) - 1; i >= 0; i-- {
			it := series[i].Iterator(nil)
			var last float64
			for it.Next() == chunkenc.ValFloat {
				_, last = it.At()
			}
			require.NoError(t, it.Err())
			assert.Equal(t, expected[i], last)
		}
	})

	t.Run("an error reading the stream is returned by the series set", func(t *testing.T) {
		stream := newOrderedStreamSeriesSet(
			[]labels.Labels{labels.FromStrings("instance", "a"), labels.FromStrings("instance", "b")},
			[][]float64{{1}, {2}},
		)
		stream.failAt = 1

		set := newQuerier(stream).Select(context.Background(), true, nil, alertsForState)
		for set.Next() {
		}
		require.ErrorContains(t, set.Err(), "stream failed")
	})

	t.Run("other series are not buffered", func(t *testing.T) {
		stream := newOrderedStreamSeriesSet(
			[]labels.Labels{labels.FromStrings("instance", "a"), labels.FromStrings("instance", "b")},
			[][]float64{{1}, {2}},
		)

		set := newQuerier(stream).Select(context.Background(), true, nil, labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "up"))
		require.True(t, set.Next())
		assert.Zero(t, stream.nextToRead, "the underlying stream must not be consumed")
	})
}

// orderedStreamSeriesSet mimics the ingester chunks stream: the iterator of series N can
// only be consumed once the iterators of all the previous series have been consumed.
type orderedStreamSeriesSet struct {
	lbls   []labels.Labels
	values [][]float64
	pos    int

	nextToRead int
	failAt     int // Index of the series whose consumption fails, or -1.
}

func newOrderedStreamSeriesSet(lbls []labels.Labels, values [][]float64) *orderedStreamSeriesSet {
	return &orderedStreamSeriesSet{lbls: lbls, values: values, pos: -1, failAt: -1}
}

func (s *orderedStreamSeriesSet) Next() bool {
	s.pos++
	return s.pos < len(s.lbls)
}

func (s *orderedStreamSeriesSet) At() storage.Series {
	return &orderedStreamSeries{set: s, idx: s.pos}
}

func (s *orderedStreamSeriesSet) Err() error                        { return nil }
func (s *orderedStreamSeriesSet) Warnings() annotations.Annotations { return nil }

type orderedStreamSeries struct {
	set *orderedStreamSeriesSet
	idx int
}

func (s *orderedStreamSeries) Labels() labels.Labels { return s.set.lbls[s.idx] }

func (s *orderedStreamSeries) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	var err error
	switch {
	case s.idx == s.set.failAt:
		err = errors.New("stream failed")
	case s.idx != s.set.nextToRead:
		err = fmt.Errorf("attempted to read series at index %d from ingester chunks stream, but the stream has series with index %d", s.idx, s.set.nextToRead)
	default:
		s.set.nextToRead++
	}
	if err != nil {
		return &errIterator{err: err}
	}

	floats := make([]promql.FPoint, 0, len(s.set.values[s.idx]))
	for i, v := range s.set.values[s.idx] {
		floats = append(floats, promql.FPoint{T: int64(i + 1), F: v})
	}
	return promql.NewStorageSeries(promql.Series{Metric: s.Labels(), Floats: floats}).Iterator(nil)
}

type errIterator struct {
	chunkenc.Iterator
	err error
}

func (e *errIterator) Next() chunkenc.ValueType { return chunkenc.ValNone }
func (e *errIterator) Err() error               { return e.err }

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
