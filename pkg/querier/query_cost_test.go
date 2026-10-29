// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/util/promqlext"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestQueryCostEstimateDistributedStorage(t *testing.T) {
	for _, tc := range []struct {
		name       string
		maxLength  time.Duration
		maxSeries  int
		wantSeries int64
		wantStart  int64
		warning    string
	}{
		{name: "complete", wantSeries: 3, wantStart: 300001},
		{name: "time range truncated", maxLength: 2 * time.Minute, wantSeries: 2, wantStart: 480000, warning: "results may be incomplete because store.max-labels-query-length restricted the series time range to 2m0s"},
		{name: "series truncated", maxSeries: 1, wantSeries: 2, wantStart: 300001, warning: NewMaxLimitError(0, 1, validation.MaxSeriesQueryLimitFlag).Error()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg Config
			flagext.DefaultValues(&cfg)
			cfg.QueryStoreAfter = 0
			limits := defaultLimitsConfig()
			limits.QueryIngestersWithin = 0
			limits.MaxLabelsQueryLength = model.Duration(tc.maxLength)
			limits.MaxSeriesQueryLimit = tc.maxSeries
			hintsSeen := make(chan storage.SelectHints, 2)
			backend := func(names ...string) storage.Queryable {
				return storage.QueryableFunc(func(int64, int64) (storage.Querier, error) {
					return &storage.MockQuerier{SelectMockFunction: func(_ bool, hints *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
						hintsSeen <- *hints
						var ls []labels.Labels
						for _, name := range names {
							if name == "old" && hints.Start > 400000 {
								continue
							}
							ls = append(ls, labels.FromStrings("__name__", "up", "instance", name))
						}
						if hints.Limit > 0 && len(ls) > hints.Limit {
							ls = ls[:hints.Limit]
						}
						var warnings annotations.Annotations
						warnings.Add(errors.New("storage warning"))
						return series.NewSeriesSetWithWarnings(series.LabelsToSeriesSet(ls), warnings)
					}}, nil
				})
			}
			q := newMultiQueryable(cfg, backend("old", "shared"), backend("shared", "store"), validation.NewOverrides(limits, nil), stats.NewQueryMetrics(prometheus.NewRegistry()), log.NewNopLogger())
			ctx := user.InjectOrgID(t.Context(), "tenant-a")
			estimate, warnings, err := promql.EstimateCost(ctx, q, promqlext.NewPromQLParser(), "rate(up[5m])", time.Unix(600, 0), time.Unix(600, 0), 0, 5*time.Minute, time.Minute, time.Minute)
			require.NoError(t, err)
			require.Equal(t, promql.CostEstimate{SeriesTouched: tc.wantSeries, SamplesRead: tc.wantSeries * 6}, estimate)
			require.Contains(t, warnings, "storage warning")
			if tc.warning != "" {
				require.Contains(t, warnings, tc.warning)
				require.Len(t, warnings, 3)
			} else {
				require.Len(t, warnings, 2)
			}
			for range 2 {
				hints := <-hintsSeen
				require.Equal(t, "series", hints.Func)
				require.Equal(t, tc.wantStart, hints.Start)
				require.Equal(t, int64(600000), hints.End)
				require.Equal(t, tc.maxSeries, hints.Limit)
			}
		})
	}
}

func TestMultiQuerierSeriesSelectStreams(t *testing.T) {
	for _, terminalErr := range []error{nil, errors.New("storage failed after enumeration")} {
		t.Run(fmt.Sprint(terminalErr), func(t *testing.T) {
			left := &costCountingSeriesSet{SeriesSet: series.LabelsToSeriesSet([]labels.Labels{
				labels.FromStrings("instance", "a"), labels.FromStrings("instance", "b"),
			}), terminalErr: terminalErr}
			var warnings annotations.Annotations
			warnings.Add(errors.New("empty storage warning"))
			right := &costCountingSeriesSet{SeriesSet: series.NewSeriesSetWithWarnings(storage.EmptySeriesSet(), warnings)}
			backend := func(set storage.SeriesSet) storage.Queryable {
				return storage.QueryableFunc(func(int64, int64) (storage.Querier, error) {
					return &storage.MockQuerier{SelectMockFunction: func(bool, *storage.SelectHints, ...*labels.Matcher) storage.SeriesSet { return set }}, nil
				})
			}
			limits := defaultLimitsConfig()
			limits.QueryIngestersWithin = 0
			q, err := newMultiQueryable(Config{}, backend(left), backend(right), validation.NewOverrides(limits, nil), stats.NewQueryMetrics(prometheus.NewRegistry()), log.NewNopLogger()).Querier(0, 600000)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, q.Close()) })
			set := q.Select(user.InjectOrgID(t.Context(), "tenant-a"), false, &storage.SelectHints{Func: "series", Start: 0, End: 600000})
			require.Equal(t, 1, left.nextCalls, "Select must not drain the storage series")
			require.Equal(t, 1, right.nextCalls)
			var got []string
			for set.Next() {
				got = append(got, set.At().Labels().Get("instance"))
			}
			require.Equal(t, []string{"a", "b"}, got)
			require.ErrorIs(t, set.Err(), terminalErr)
			require.Contains(t, set.Warnings(), "empty storage warning")
		})
	}
}

func TestQueryCostEstimateStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(user.InjectOrgID(t.Context(), "tenant-a"))
	defer cancel()
	ls := make([]labels.Labels, 1000)
	for i := range ls {
		ls[i] = labels.FromStrings("instance", fmt.Sprintf("%04d", i))
	}
	set := &costCountingSeriesSet{SeriesSet: series.LabelsToSeriesSet(ls)}
	set.beforeNext = func() {
		if set.nextCalls == 2 {
			cancel()
		}
	}
	backend := func(set storage.SeriesSet) storage.Queryable {
		return storage.QueryableFunc(func(int64, int64) (storage.Querier, error) {
			return &storage.MockQuerier{SelectMockFunction: func(bool, *storage.SelectHints, ...*labels.Matcher) storage.SeriesSet { return set }}, nil
		})
	}
	limits := defaultLimitsConfig()
	limits.QueryIngestersWithin = 0
	q := newMultiQueryable(Config{}, backend(set), backend(storage.EmptySeriesSet()), validation.NewOverrides(limits, nil), stats.NewQueryMetrics(prometheus.NewRegistry()), log.NewNopLogger())
	estimate, _, err := promql.EstimateCost(ctx, q, promqlext.NewPromQLParser(), "up", time.Unix(600, 0), time.Unix(600, 0), 0, 5*time.Minute, time.Minute, time.Minute)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, promql.CostEstimate{}, estimate)
	require.Equal(t, 2, set.nextCalls, "cancellation must stop enumeration without draining storage")
}

type costCountingSeriesSet struct {
	storage.SeriesSet
	nextCalls   int
	exhausted   bool
	terminalErr error
	beforeNext  func()
}

func (s *costCountingSeriesSet) Next() bool {
	s.nextCalls++
	if s.beforeNext != nil {
		s.beforeNext()
	}
	if !s.SeriesSet.Next() {
		s.exhausted = true
		return false
	}
	return true
}

func (s *costCountingSeriesSet) Err() error {
	if s.exhausted {
		return s.terminalErr
	}
	return nil
}

func BenchmarkQueryCostSeriesMerge(b *testing.B) {
	for _, size := range []int{1000, 100000} {
		ls := make([]labels.Labels, size)
		for i := range ls {
			ls[i] = labels.FromStrings("instance", fmt.Sprintf("%06d", i))
		}
		for _, streaming := range []bool{false, true} {
			b.Run(fmt.Sprintf("%d/streaming=%t", size, streaming), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					sets := []storage.SeriesSet{series.LabelsToSeriesSet(ls), series.LabelsToSeriesSet(ls)}
					var merged storage.SeriesSet
					if streaming {
						merged = storage.NewMergeSeriesSet(sets, 0, storage.ChainedSeriesMerge)
					} else {
						merged = (&multiQuerier{}).mergeSeriesSets(sets)
					}
					for merged.Next() {
					}
					if err := merged.Err(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
