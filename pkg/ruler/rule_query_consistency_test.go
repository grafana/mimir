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
	"github.com/prometheus/prometheus/model/histogram"
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

func TestBufferAlertsForStateSeries(t *testing.T) {
	collect := func(t *testing.T, set storage.SeriesSet) (lbls []labels.Labels, floats [][]promql.FPoint, hists [][]promql.HPoint) {
		t.Helper()
		for set.Next() {
			s := set.At()
			lbls = append(lbls, s.Labels())
			var fs []promql.FPoint
			var hs []promql.HPoint
			it := s.Iterator(nil)
			for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
				switch vt {
				case chunkenc.ValFloat:
					ts, v := it.At()
					fs = append(fs, promql.FPoint{T: ts, F: v})
				case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
					ts, h := it.AtFloatHistogram(nil)
					hs = append(hs, promql.HPoint{T: ts, H: h})
				}
			}
			require.NoError(t, it.Err())
			floats = append(floats, fs)
			hists = append(hists, hs)
		}
		require.NoError(t, set.Err())
		return
	}

	newSeries := func(instance string, fs []promql.FPoint, hs []promql.HPoint) storage.Series {
		return promql.NewStorageSeries(promql.Series{Metric: labels.FromStrings("instance", instance), Floats: fs, Histograms: hs})
	}

	t.Run("empty series set", func(t *testing.T) {
		set := bufferAlertsForStateSeries(context.Background(), storage.EmptySeriesSet())
		lbls, _, _ := collect(t, set)
		assert.Empty(t, lbls)
	})

	t.Run("series without samples is kept with no samples", func(t *testing.T) {
		set := bufferAlertsForStateSeries(context.Background(), &listSeriesSet{series: []storage.Series{newSeries("a", nil, nil)}})
		lbls, floats, _ := collect(t, set)
		require.Len(t, lbls, 1)
		assert.Equal(t, labels.FromStrings("instance", "a"), lbls[0])
		assert.Empty(t, floats[0])
	})

	t.Run("only the last float sample is kept", func(t *testing.T) {
		set := bufferAlertsForStateSeries(context.Background(), &listSeriesSet{series: []storage.Series{
			newSeries("a", []promql.FPoint{{T: 1, F: 10}, {T: 2, F: 20}, {T: 3, F: 30}}, nil),
			newSeries("b", []promql.FPoint{{T: 5, F: 50}}, nil),
		}})
		_, floats, _ := collect(t, set)
		require.Len(t, floats, 2)
		assert.Equal(t, []promql.FPoint{{T: 3, F: 30}}, floats[0])
		assert.Equal(t, []promql.FPoint{{T: 5, F: 50}}, floats[1])
	})

	t.Run("histogram samples are dropped and the last float sample is kept, even if followed by a histogram", func(t *testing.T) {
		fh := &histogram.FloatHistogram{Count: 3, Sum: 6, Schema: 0, ZeroThreshold: 0.001}
		set := bufferAlertsForStateSeries(context.Background(), &listSeriesSet{series: []storage.Series{
			// Float at T=1,2 and a histogram at T=3 (after the last float).
			newSeries("a", []promql.FPoint{{T: 1, F: 10}, {T: 2, F: 20}}, []promql.HPoint{{T: 3, H: fh}}),
			// Histogram only.
			newSeries("b", nil, []promql.HPoint{{T: 1, H: fh}, {T: 2, H: fh}}),
			// Plain float, after the histogram series: must still be readable.
			newSeries("c", []promql.FPoint{{T: 1, F: 7}}, nil),
			// Histogram first, then floats: samples after the histogram must not be lost.
			newSeries("d", []promql.FPoint{{T: 2, F: 8}, {T: 3, F: 9}}, []promql.HPoint{{T: 1, H: fh}}),
		}})
		lbls, floats, hists := collect(t, set)
		require.Len(t, lbls, 4)
		assert.Equal(t, []promql.FPoint{{T: 2, F: 20}}, floats[0])
		assert.Empty(t, floats[1])
		assert.Equal(t, []promql.FPoint{{T: 1, F: 7}}, floats[2])
		assert.Equal(t, []promql.FPoint{{T: 3, F: 9}}, floats[3])
		for i, h := range hists {
			assert.Empty(t, h, "series %d", i)
		}
	})

	t.Run("series are fully drained so the stream can be read in order", func(t *testing.T) {
		stream := newOrderedStreamSeriesSet(
			[]labels.Labels{labels.FromStrings("instance", "a"), labels.FromStrings("instance", "b")},
			[][]float64{{1, 2}, {3, 4}},
		)
		set := bufferAlertsForStateSeries(context.Background(), stream)
		_, floats, _ := collect(t, set)
		require.Len(t, floats, 2)
		assert.Equal(t, []promql.FPoint{{T: 2, F: 2}}, floats[0])
		assert.Equal(t, []promql.FPoint{{T: 2, F: 4}}, floats[1])
		assert.Equal(t, 2, stream.nextToRead)
	})

	t.Run("context cancelled mid-iteration returns the context error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		set := &listSeriesSet{
			series: []storage.Series{
				newSeries("a", []promql.FPoint{{T: 1, F: 1}}, nil),
				newSeries("b", []promql.FPoint{{T: 1, F: 2}}, nil),
				newSeries("c", []promql.FPoint{{T: 1, F: 3}}, nil),
			},
			onAt: func(idx int) {
				if idx == 1 {
					cancel()
				}
			},
		}

		buffered := bufferAlertsForStateSeries(ctx, set)
		assert.False(t, buffered.Next())
		assert.ErrorIs(t, buffered.Err(), context.Canceled)
		assert.Equal(t, 1, set.lastAt, "iteration must stop at the first check after cancellation")
	})

	t.Run("an error from the input set is returned", func(t *testing.T) {
		set := &listSeriesSet{series: []storage.Series{newSeries("a", []promql.FPoint{{T: 1, F: 1}}, nil)}, err: errors.New("boom")}
		buffered := bufferAlertsForStateSeries(context.Background(), set)
		assert.False(t, buffered.Next())
		assert.ErrorContains(t, buffered.Err(), "boom")
	})

	t.Run("labels are copied, so reusing the input labels buffer does not alter buffered series", func(t *testing.T) {
		// Same-size values so that the overwritten buffer is reused in place.
		set := &reusedLabelsSeriesSet{values: []string{"aaa", "bbb", "ccc"}, pos: -1}
		buffered := bufferAlertsForStateSeries(context.Background(), set)

		lbls, floats, _ := collect(t, buffered)
		require.Len(t, lbls, 3)
		assert.Equal(t, labels.FromStrings("instance", "aaa"), lbls[0])
		assert.Equal(t, labels.FromStrings("instance", "bbb"), lbls[1])
		assert.Equal(t, labels.FromStrings("instance", "ccc"), lbls[2])
		assert.Equal(t, []promql.FPoint{{T: 1, F: 1}}, floats[0])

		// Mutating the input's labels after buffering must not change the result either.
		set.overwrite("zzz")
		assert.Equal(t, labels.FromStrings("instance", "ccc"), lbls[2])
		assert.Equal(t, labels.FromStrings("instance", "aaa"), lbls[0])
	})
}

// listSeriesSet is a SeriesSet over a list of series, with hooks to inject errors and observe iteration.
type listSeriesSet struct {
	series []storage.Series
	pos    int // Index of the next series to return (starts at 0).
	lastAt int
	onAt   func(idx int)
	err    error
}

func (s *listSeriesSet) Next() bool {
	if s.pos >= len(s.series) {
		return false
	}
	s.pos++
	return true
}

func (s *listSeriesSet) At() storage.Series {
	s.lastAt = s.pos - 1
	if s.onAt != nil {
		s.onAt(s.lastAt)
	}
	return s.series[s.lastAt]
}

func (s *listSeriesSet) Err() error                        { return s.err }
func (s *listSeriesSet) Warnings() annotations.Annotations { return nil }

// reusedLabelsSeriesSet returns series whose labels share a single buffer overwritten by each Next(),
// like a stream that reuses memory while reading.
type reusedLabelsSeriesSet struct {
	values  []string
	pos     int
	current labels.Labels
	builder labels.ScratchBuilder
}

func (s *reusedLabelsSeriesSet) overwrite(v string) {
	s.builder.Reset()
	s.builder.Add("instance", v)
	s.builder.Overwrite(&s.current)
}

func (s *reusedLabelsSeriesSet) Next() bool {
	s.pos++
	if s.pos >= len(s.values) {
		return false
	}
	s.overwrite(s.values[s.pos])
	return true
}

func (s *reusedLabelsSeriesSet) At() storage.Series { return &reusedLabelsSeries{set: s} }
func (s *reusedLabelsSeriesSet) Err() error         { return nil }
func (s *reusedLabelsSeriesSet) Warnings() annotations.Annotations {
	return nil
}

type reusedLabelsSeries struct{ set *reusedLabelsSeriesSet }

// Labels returns the shared, reused labels.
func (s *reusedLabelsSeries) Labels() labels.Labels { return s.set.current }
func (s *reusedLabelsSeries) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return promql.NewStorageSeries(promql.Series{Floats: []promql.FPoint{{T: 1, F: 1}}}).Iterator(nil)
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
