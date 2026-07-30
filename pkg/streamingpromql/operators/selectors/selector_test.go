// SPDX-License-Identifier: AGPL-3.0-only

package selectors

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestSeriesList_BasicListOperations(t *testing.T) {
	list := newSeriesList(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))
	require.Equal(t, 0, list.Len())

	series1 := mockSeries{labels.FromStrings("series", "1")}
	series2 := mockSeries{labels.FromStrings("series", "2")}
	series3 := mockSeries{labels.FromStrings("series", "3")}
	series4 := mockSeries{labels.FromStrings("series", "4")}

	list.Add(series1)
	requireSeriesListContents(t, list, series1)

	list.Add(series2)
	list.Add(series3)
	list.Add(series4)
	requireSeriesListContents(t, list, series1, series2, series3, series4)

	require.Equal(t, series1, list.Pop())
	require.Equal(t, series2, list.Pop())
	require.Equal(t, series3, list.Pop())
	require.Equal(t, series4, list.Pop())

	list.Close()
}

func TestSeriesList_OperationsNearBatchBoundaries(t *testing.T) {
	cases := []int{
		seriesBatchSize - 1,
		seriesBatchSize,
		seriesBatchSize + 1,
		(seriesBatchSize * 2) - 1,
		seriesBatchSize * 2,
		(seriesBatchSize * 2) + 1,
	}

	ctx := context.Background()

	for _, seriesCount := range cases {
		t.Run(fmt.Sprintf("N=%v", seriesCount), func(t *testing.T) {
			list := newSeriesList(limiter.NewUnlimitedMemoryConsumptionTracker(ctx))

			seriesAdded := make([]storage.Series, 0, seriesCount)

			for i := 0; i < seriesCount; i++ {
				s := mockSeries{labels.FromStrings("series", strconv.Itoa(i))}
				list.Add(s)
				seriesAdded = append(seriesAdded, s)
			}

			requireSeriesListContents(t, list, seriesAdded...)

			seriesPopped := make([]storage.Series, 0, seriesCount)

			for i := 0; i < seriesCount; i++ {
				seriesPopped = append(seriesPopped, list.Pop())
			}

			require.Equal(t, seriesAdded, seriesPopped)
		})
	}
}

func requireSeriesListContents(t *testing.T, list *seriesList, series ...storage.Series) {
	require.Equal(t, len(series), list.Len())

	expectedMetadata := make([]types.SeriesMetadata, 0, len(series))
	for _, s := range series {
		expectedMetadata = append(expectedMetadata, types.SeriesMetadata{Labels: s.Labels()})
	}

	metadata, err := list.ToSeriesMetadata()
	require.NoError(t, err)

	require.Equal(t, expectedMetadata, metadata)
}

type mockSeries struct {
	labels labels.Labels
}

func (m mockSeries) Labels() labels.Labels {
	return m.labels
}

func (m mockSeries) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	panic("mockSeries: Iterator() not supported")
}

func TestSelector_QueryRanges(t *testing.T) {
	start := time.Date(2024, 12, 11, 3, 12, 45, 0, time.UTC)
	end := start.Add(time.Hour)
	timeRange := types.NewRangeQueryTimeRange(start, end, time.Minute)
	ctx := context.Background()

	t.Run("instant vector selector", func(t *testing.T) {
		queryable := &mockQueryable{}
		lookbackDelta := 5 * time.Minute
		matchers := types.Matchers{{Type: labels.MatchRegexp, Name: "env", Value: "prod"}}
		s := &Selector{
			Queryable:                queryable,
			TimeRange:                timeRange,
			LookbackDelta:            lookbackDelta,
			Matchers:                 matchers,
			MemoryConsumptionTracker: limiter.NewUnlimitedMemoryConsumptionTracker(ctx),
		}

		_, err := s.SeriesMetadata(ctx, nil)
		require.NoError(t, err)

		expectedMinT := timestamp.FromTime(start.Add(-lookbackDelta).Add(time.Millisecond)) // Add a millisecond to exclude the beginning of the range.
		expectedMaxT := timestamp.FromTime(end)
		requireMinAndMaxTimes(t, queryable, expectedMinT, expectedMaxT)
		requireMatchers(t, queryable, matchers)
	})

	t.Run("instant vector selector with runtime matchers", func(t *testing.T) {
		queryable := &mockQueryable{}
		lookbackDelta := 5 * time.Minute
		matchers := types.Matchers{
			{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			{Type: labels.MatchRegexp, Name: "region", Value: "us-east-1"},
		}
		s := &Selector{
			Queryable:                queryable,
			TimeRange:                timeRange,
			LookbackDelta:            lookbackDelta,
			Matchers:                 matchers,
			MemoryConsumptionTracker: limiter.NewUnlimitedMemoryConsumptionTracker(ctx),
		}

		runtimeMatchers := types.Matchers{
			{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			{Type: labels.MatchRegexp, Name: "container", Value: "querier"},
		}
		_, err := s.SeriesMetadata(ctx, runtimeMatchers)
		require.NoError(t, err)

		expectedMinT := timestamp.FromTime(start.Add(-lookbackDelta).Add(time.Millisecond)) // Add a millisecond to exclude the beginning of the range.
		expectedMaxT := timestamp.FromTime(end)
		requireMinAndMaxTimes(t, queryable, expectedMinT, expectedMaxT)
		requireMatchers(t, queryable, types.Matchers{
			{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			{Type: labels.MatchRegexp, Name: "region", Value: "us-east-1"},
			{Type: labels.MatchRegexp, Name: "container", Value: "querier"},
		})
	})

	t.Run("range vector selector", func(t *testing.T) {
		queryable := &mockQueryable{}
		selectorRange := 15 * time.Minute
		matchers := types.Matchers{{Type: labels.MatchRegexp, Name: "env", Value: "prod"}}
		s := &Selector{
			Queryable:                queryable,
			TimeRange:                timeRange,
			Range:                    selectorRange,
			Matchers:                 matchers,
			MemoryConsumptionTracker: limiter.NewUnlimitedMemoryConsumptionTracker(ctx),
		}

		_, err := s.SeriesMetadata(ctx, nil)
		require.NoError(t, err)

		expectedMinT := timestamp.FromTime(start.Add(-selectorRange).Add(time.Millisecond)) // Add a millisecond to exclude the beginning of the range.
		expectedMaxT := timestamp.FromTime(end)
		requireMinAndMaxTimes(t, queryable, expectedMinT, expectedMaxT)
		requireMatchers(t, queryable, matchers)
	})

	t.Run("range vector selector with runtime matchers", func(t *testing.T) {
		queryable := &mockQueryable{}
		selectorRange := 15 * time.Minute
		matchers := types.Matchers{
			{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			{Type: labels.MatchRegexp, Name: "region", Value: "us-east-1"},
		}
		s := &Selector{
			Queryable:                queryable,
			TimeRange:                timeRange,
			Range:                    selectorRange,
			Matchers:                 matchers,
			MemoryConsumptionTracker: limiter.NewUnlimitedMemoryConsumptionTracker(ctx),
		}

		runtimeMatchers := types.Matchers{
			{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			{Type: labels.MatchRegexp, Name: "container", Value: "querier"},
		}
		_, err := s.SeriesMetadata(ctx, runtimeMatchers)
		require.NoError(t, err)

		expectedMinT := timestamp.FromTime(start.Add(-selectorRange).Add(time.Millisecond)) // Add a millisecond to exclude the beginning of the range.
		expectedMaxT := timestamp.FromTime(end)
		requireMinAndMaxTimes(t, queryable, expectedMinT, expectedMaxT)
		requireMatchers(t, queryable, types.Matchers{
			{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			{Type: labels.MatchRegexp, Name: "region", Value: "us-east-1"},
			{Type: labels.MatchRegexp, Name: "container", Value: "querier"},
		})
	})
}

func TestSelector_ReportsCardinality(t *testing.T) {
	start := time.Date(2024, 12, 11, 3, 12, 45, 0, time.UTC)
	end := start.Add(time.Hour)
	timeRange := types.NewRangeQueryTimeRange(start, end, time.Minute)
	lookbackDelta := 5 * time.Minute

	series := []labels.Labels{
		labels.FromStrings("__name__", "foo", "env", "prod", "instance", "a"),
		labels.FromStrings("__name__", "foo", "env", "prod", "instance", "b"),
		labels.FromStrings("__name__", "foo", "env", "dev", "instance", "c"),
	}

	baseMatchers := types.Matchers{{Type: labels.MatchEqual, Name: "__name__", Value: "foo"}}
	expectedMinT, expectedMaxT := ComputeQueriedTimeRange(timeRange, nil, 0, 0, lookbackDelta, false, false)

	t.Run("without subsets", func(t *testing.T) {
		qs, ctx := stats.ContextWithEmptyStats(context.Background())
		s := &Selector{
			Queryable:                &sliceQueryable{series: series},
			TimeRange:                timeRange,
			LookbackDelta:            lookbackDelta,
			Matchers:                 baseMatchers,
			MemoryConsumptionTracker: limiter.NewUnlimitedMemoryConsumptionTracker(ctx),
		}

		_, err := s.SeriesMetadata(ctx, nil)
		require.NoError(t, err)

		require.Equal(t, []stats.SelectorCardinality{
			{
				Matchers:    []stats.LabelMatcher{{Type: int32(labels.MatchEqual), Name: "__name__", Value: "foo"}},
				MinT:        expectedMinT,
				MaxT:        expectedMaxT,
				SeriesCount: 3,
			},
		}, qs.LoadSelectorCardinalities())
	})

	t.Run("with subsets", func(t *testing.T) {
		qs, ctx := stats.ContextWithEmptyStats(context.Background())
		s := &Selector{
			Queryable:     &sliceQueryable{series: series},
			TimeRange:     timeRange,
			LookbackDelta: lookbackDelta,
			Matchers:      baseMatchers,
			Subsets: []Subset{
				{Filter: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "env", "prod")}},
				{Filter: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "env", "dev")}},
			},
			MemoryConsumptionTracker: limiter.NewUnlimitedMemoryConsumptionTracker(ctx),
		}
		defer s.Close()

		_, err := s.SeriesMetadata(ctx, nil)
		require.NoError(t, err)

		require.Equal(t, []stats.SelectorCardinality{
			{
				Matchers:    []stats.LabelMatcher{{Type: int32(labels.MatchEqual), Name: "__name__", Value: "foo"}},
				MinT:        expectedMinT,
				MaxT:        expectedMaxT,
				SeriesCount: 3,
			},
			{
				Matchers:    []stats.LabelMatcher{{Type: int32(labels.MatchEqual), Name: "__name__", Value: "foo"}, {Type: int32(labels.MatchEqual), Name: "env", Value: "prod"}},
				MinT:        expectedMinT,
				MaxT:        expectedMaxT,
				SeriesCount: 2,
			},
			{
				Matchers:    []stats.LabelMatcher{{Type: int32(labels.MatchEqual), Name: "__name__", Value: "foo"}, {Type: int32(labels.MatchEqual), Name: "env", Value: "dev"}},
				MinT:        expectedMinT,
				MaxT:        expectedMaxT,
				SeriesCount: 1,
			},
		}, qs.LoadSelectorCardinalities())
	})

	t.Run("no stats in context", func(t *testing.T) {
		s := &Selector{
			Queryable:                &sliceQueryable{series: series},
			TimeRange:                timeRange,
			LookbackDelta:            lookbackDelta,
			Matchers:                 baseMatchers,
			MemoryConsumptionTracker: limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()),
		}

		// Should not panic when there are no stats in the context.
		_, err := s.SeriesMetadata(context.Background(), nil)
		require.NoError(t, err)
	})
}

func requireMinAndMaxTimes(t *testing.T, queryable *mockQueryable, expectedMinT, expectedMaxT int64) {
	require.Equal(t, expectedMinT, queryable.mint)
	require.Equal(t, expectedMaxT, queryable.maxt)
	require.Equal(t, expectedMinT, queryable.hints.Start)
	require.Equal(t, expectedMaxT, queryable.hints.End)
}

func requireMatchers(t *testing.T, queryable *mockQueryable, expected types.Matchers) {
	observed := make([]types.Matcher, 0, len(queryable.matchers))
	for _, m := range queryable.matchers {
		observed = append(observed, types.Matcher{
			Type:  m.Type,
			Name:  m.Name,
			Value: m.Value,
		})
	}

	require.ElementsMatch(t, expected, observed)
}

type mockQueryable struct {
	mint, maxt int64
	hints      *storage.SelectHints
	matchers   []*labels.Matcher
}

func (m *mockQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	m.mint = mint
	m.maxt = maxt

	return &mockQuerier{m}, nil
}

type mockQuerier struct {
	q *mockQueryable
}

func (m *mockQuerier) Select(_ context.Context, _ bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	m.q.hints = hints
	m.q.matchers = matchers

	return storage.EmptySeriesSet()
}

func (m *mockQuerier) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	panic("not supported")
}

func (m *mockQuerier) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	panic("not supported")
}

func (m *mockQuerier) Close() error {
	panic("not supported")
}

// sliceQueryable is a storage.Queryable that returns a fixed set of series (identified by their labels).
type sliceQueryable struct {
	series []labels.Labels
}

func (q *sliceQueryable) Querier(int64, int64) (storage.Querier, error) {
	return &sliceQuerier{series: q.series}, nil
}

type sliceQuerier struct {
	series []labels.Labels
}

func (q *sliceQuerier) Select(_ context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
	s := make([]storage.Series, 0, len(q.series))
	for _, l := range q.series {
		s = append(s, mockSeries{l})
	}

	return &sliceSeriesSet{series: s}
}

func (q *sliceQuerier) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	panic("not supported")
}

func (q *sliceQuerier) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	panic("not supported")
}

func (q *sliceQuerier) Close() error {
	return nil
}

type sliceSeriesSet struct {
	series []storage.Series
	idx    int
}

func (s *sliceSeriesSet) Next() bool {
	if s.idx >= len(s.series) {
		return false
	}
	s.idx++
	return true
}

func (s *sliceSeriesSet) At() storage.Series                { return s.series[s.idx-1] }
func (s *sliceSeriesSet) Err() error                        { return nil }
func (s *sliceSeriesSet) Warnings() annotations.Annotations { return nil }
