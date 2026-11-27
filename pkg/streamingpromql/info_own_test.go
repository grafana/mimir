// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/querier/stats"
)

// fakeOwnResources is the native target_info querier's side of info()'s own mode, for info()'s tests: whether own mode
// applies, the lookup by series' answer, and what info() asked and counted.
type fakeOwnResources struct {
	ownMode     bool
	unavailable bool
	resources   querierapi.NativeSeriesResources

	lookups       [][]*labels.Matcher
	observed      map[string]int
	released      int
	ownModeSelect []bool
}

type ownResourcesQueryable struct {
	storage.Queryable
	fake *fakeOwnResources
}

func (q ownResourcesQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	querier, err := q.Queryable.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return ownResourcesQuerier{Querier: querier, fake: q.fake}, nil
}

type ownResourcesQuerier struct {
	storage.Querier
	fake *fakeOwnResources
}

func (q ownResourcesQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	q.fake.ownModeSelect = append(q.fake.ownModeSelect, querierapi.NativeInfoOwnModeFromContext(ctx))
	return q.Querier.Select(ctx, sortSeries, hints, matchers...)
}

func (q ownResourcesQuerier) NativeInfoOwnMode(context.Context) bool { return q.fake.ownMode }

func (q ownResourcesQuerier) NativeSeriesResources(_ context.Context, _ *storage.SelectHints, matchers ...*labels.Matcher) (querierapi.NativeSeriesResources, bool, error) {
	q.fake.lookups = append(q.fake.lookups, matchers)
	if q.fake.unavailable {
		return querierapi.NativeSeriesResources{}, false, nil
	}
	res := q.fake.resources
	res.Release = func() { q.fake.released++ }
	return res, true, nil
}

func (q ownResourcesQuerier) ObserveNativeInfoInnerSeries(source string, count int) {
	if q.fake.observed == nil {
		q.fake.observed = map[string]int{}
	}
	q.fake.observed[source] += count
}

// info()'s own mode: each inner series that a lookup by series finds takes its own resource's versions, standing in
// for target_info from the lookup's coverage start on, and every other inner series takes the stored target_info, by
// the join. Every case runs a range query from 0 to 20m at a 1m step, with the 5m lookback.
func TestInfoFunction_OwnMode(t *testing.T) {
	store := promqltest.LoadedStorage(t, `
		load 1m
			up{job="a", instance="x"} 1x20
			up{job="b", instance="y"} 1x20
			up_too{job="a", instance="x"} 1x20
			target_info{job="a", instance="x", host_name="stored-a"} 1x20
			target_info{job="b", instance="y", host_name="stored-b"} 1x20
			build_info{job="a", instance="x", version="1"} 1x20
	`)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	upA := labels.FromStrings("__name__", "up", "instance", "x", "job", "a")
	targetInfoA := func(host string) labels.Labels {
		return labels.FromStrings("__name__", "target_info", "host_name", host, "instance", "x", "job", "a")
	}
	minute := time.Minute.Milliseconds()
	// a's own resource, from versions of hosts over [from, to] in minutes.
	ownA := func(cutover int64, versions ...querierapi.NativeSeriesResourceVersion) querierapi.NativeSeriesResources {
		return querierapi.NativeSeriesResources{CoverageStart: cutover, Series: []querierapi.NativeSeriesResource{{Labels: upA, Versions: versions}}}
	}
	version := func(fromMinute, toMinute int64, host string) querierapi.NativeSeriesResourceVersion {
		return querierapi.NativeSeriesResourceVersion{MinTime: fromMinute * minute, MaxTime: toMinute*minute - 1, Labels: targetInfoA(host)}
	}
	// Its last version lasts until its newest sample, at 20m.
	last := func(fromMinute int64, host string) querierapi.NativeSeriesResourceVersion {
		return querierapi.NativeSeriesResourceVersion{MinTime: fromMinute * minute, MaxTime: 20 * minute, Labels: targetInfoA(host)}
	}

	// The last run's per-query stats.
	var lastStats *stats.SafeStats
	run := func(t *testing.T, fake *fakeOwnResources, query string) map[string][]int64 {
		opts := NewTestEngineOpts()
		opts.InfoSeriesQueryable = func(q storage.Queryable) storage.Queryable { return ownResourcesQueryable{Queryable: q, fake: fake} }
		planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
		require.NoError(t, err)
		engine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
		require.NoError(t, err)
		queryStats, ctx := stats.ContextWithEmptyStats(t.Context())
		lastStats = queryStats
		q, err := engine.NewRangeQuery(ctx, store, nil, query, time.UnixMilli(0), time.UnixMilli(20*minute), time.Minute)
		require.NoError(t, err)
		defer q.Close()
		res := q.Exec(ctx)
		require.NoError(t, res.Err)
		matrix, err := res.Matrix()
		require.NoError(t, err)
		return stepsByLabels(matrix)
	}
	steps := func(fromMinute, toMinute int64) []int64 {
		var out []int64
		for m := fromMinute; m <= toMinute; m++ {
			out = append(out, m*minute)
		}
		return out
	}
	// info() keeps the inner series' name.
	enriched := func(job, instance, host string, extra ...string) string {
		return labels.FromStrings(append([]string{"__name__", "up", "host_name", host, "instance", instance, "job", job}, extra...)...).String()
	}

	t.Run("own resources the stored target_info's give the join's answer", func(t *testing.T) {
		fake := &fakeOwnResources{ownMode: true, resources: ownA(math.MinInt64, last(0, "stored-a"))}
		own := run(t, fake, `info(up)`)
		// The query's stats count the inner series by what enriched them, which the native metadata prober reads.
		require.Equal(t, uint64(1), lastStats.LoadNativeInfoOwnSeries())
		require.Equal(t, uint64(1), lastStats.LoadNativeInfoJoinSeries())
		require.Equal(t, run(t, &fakeOwnResources{}, `info(up)`), own)
		require.Len(t, fake.lookups, 1)
		require.Equal(t, map[string]int{querierapi.NativeInfoSourceOwn: 1, querierapi.NativeInfoSourceNotReturned: 1}, fake.observed)
		require.Equal(t, 1, fake.released, "the lookup's answers are released once")
		require.NotContains(t, fake.ownModeSelect, false, "every select of info series reads the stored series only")
	})

	t.Run("a version change, and the series the lookup didn't return taking the join", func(t *testing.T) {
		got := run(t, &fakeOwnResources{ownMode: true, resources: ownA(math.MinInt64, version(0, 10, "own-1"), last(10, "own-2"))}, `info(up)`)
		require.Equal(t, map[string][]int64{
			enriched("a", "x", "own-1"):    steps(0, 9),
			enriched("a", "x", "own-2"):    steps(10, 20),
			enriched("b", "y", "stored-b"): steps(0, 20),
		}, got)
	})

	t.Run("a newer version the data-label matchers reject leaves the older one's tail", func(t *testing.T) {
		got := run(t, &fakeOwnResources{ownMode: true, resources: ownA(math.MinInt64, version(0, 10, "own-1"), last(10, "own-2"))}, `info(up, {host_name="own-1"})`)
		// own-1 ends at 10m less a millisecond, and is visible for the lookback after.
		require.Equal(t, map[string][]int64{enriched("a", "x", "own-1"): steps(0, 14)}, got)
	})

	t.Run("before the cutover the stored target_info, and its bridge after it", func(t *testing.T) {
		// The resources cover from 10m on, and a's own version starts at 12m: before 10m, the stored target_info; at
		// 10m and 11m, its last sample before the cutover, within the lookback; from 12m, the own version, and the
		// stored target_info's samples after the cutover never count.
		got := run(t, &fakeOwnResources{ownMode: true, resources: ownA(10*minute, last(12, "own-1"))}, `info(up)`)
		require.Equal(t, steps(0, 11), got[enriched("a", "x", "stored-a")])
		require.Equal(t, steps(12, 20), got[enriched("a", "x", "own-1")])
	})

	t.Run("a version that ends before the cutover doesn't count after it", func(t *testing.T) {
		// own-1 ends at 9m, before the cutover at 10m, and the version after it is unknown: after the cutover, the
		// stored target_info's last sample before it bridges a lookback, as today's native path has it, and then
		// nothing enriches the series.
		unknown := querierapi.NativeSeriesResourceVersion{MinTime: 9 * minute, MaxTime: 20 * minute, Labels: labels.EmptyLabels()}
		got := run(t, &fakeOwnResources{ownMode: true, resources: ownA(10*minute, version(0, 9, "own-1"), unknown)}, `info(up)`)
		require.Equal(t, steps(0, 13), got[enriched("a", "x", "stored-a")])
		require.Equal(t, steps(14, 20), got[upA.String()])
		require.NotContains(t, got, enriched("a", "x", "own-1"))
	})

	t.Run("an offset shifts the version change", func(t *testing.T) {
		// The data-label selector evaluates step T at T less the offset, so own-2, from 10m, is in force from step 15m.
		got := run(t, &fakeOwnResources{ownMode: true, resources: ownA(math.MinInt64, version(0, 10, "own-1"), last(10, "own-2"))}, `info(up offset 5m)`)
		require.Equal(t, steps(5, 14), got[enriched("a", "x", "own-1")])
		require.Equal(t, steps(15, 20), got[enriched("a", "x", "own-2")])
	})

	t.Run("an own series and a joined one of the same target", func(t *testing.T) {
		// up takes its own resource and build_info joins it; up_too, which the lookup didn't return, takes the join:
		// both views of the target use build_info's samples, which go back to the pool once.
		got := run(t, &fakeOwnResources{ownMode: true, resources: ownA(math.MinInt64, last(0, "own-1"))}, `info({__name__=~"up|up_too", job="a"}, {__name__=~".+_info"})`)
		require.Equal(t, steps(0, 20), got[enriched("a", "x", "own-1", "version", "1")])
		require.Equal(t, steps(0, 20), got[labels.FromStrings("__name__", "up_too", "host_name", "stored-a", "instance", "x", "job", "a", "version", "1").String()])
	})

	t.Run("another info metric joins an own series", func(t *testing.T) {
		got := run(t, &fakeOwnResources{ownMode: true, resources: ownA(math.MinInt64, last(0, "own-1"))}, `info(up, {__name__=~".+_info"})`)
		require.Equal(t, steps(0, 20), got[enriched("a", "x", "own-1", "version", "1")])
	})

	for name, tc := range map[string]struct {
		query  string
		fake   *fakeOwnResources
		source string
	}{
		"a shape that doesn't qualify takes the join":              {query: `info(sum by (job, instance) (up))`, source: querierapi.NativeInfoSourceShape},
		"a data-label selector without target_info takes the join": {query: `info(up, {__name__="build_info"})`, source: querierapi.NativeInfoSourceNotTargetInfo},
		"an unavailable lookup takes the join":                     {query: `info(up)`, fake: &fakeOwnResources{ownMode: true, unavailable: true, resources: ownA(math.MinInt64, last(0, "own-1"))}, source: querierapi.NativeInfoSourceUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			fake := tc.fake
			if fake == nil {
				fake = &fakeOwnResources{ownMode: true, resources: ownA(math.MinInt64, last(0, "own-1"))}
			}
			require.Equal(t, run(t, &fakeOwnResources{}, tc.query), run(t, fake, tc.query))
			require.Equal(t, map[string]int{tc.source: 2}, fake.observed)
		})
	}

	t.Run("own mode off makes no lookup", func(t *testing.T) {
		fake := &fakeOwnResources{resources: ownA(math.MinInt64, last(0, "own-1"))}
		run(t, fake, `info(up)`)
		require.Empty(t, fake.lookups)
		require.NotContains(t, fake.ownModeSelect, true)
	})
}

// Two stored target_info series of one job and instance, with samples at the same times, make the join fail; an own
// series never reads the stored target_info after the cutover, so with the whole range after it, it takes its own.
func TestInfoFunction_OwnModeCollidingStoredTargetInfo(t *testing.T) {
	store := promqltest.LoadedStorage(t, `
		load 1m
			up{job="a", instance="x"} 1x20
			target_info{job="a", instance="x", host_name="one"} 1x20
			target_info{job="a", instance="x", host_name="two"} 1x20
	`)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	upA := labels.FromStrings("__name__", "up", "instance", "x", "job", "a")
	own := labels.FromStrings("__name__", "target_info", "host_name", "own", "instance", "x", "job", "a")
	fake := &fakeOwnResources{ownMode: true, resources: querierapi.NativeSeriesResources{
		CoverageStart: math.MinInt64,
		Series:        []querierapi.NativeSeriesResource{{Labels: upA, Versions: []querierapi.NativeSeriesResourceVersion{{MinTime: 0, MaxTime: 20 * time.Minute.Milliseconds(), Labels: own}}}},
	}}
	query := func(fake *fakeOwnResources) (promql.Matrix, error) {
		opts := NewTestEngineOpts()
		opts.InfoSeriesQueryable = func(q storage.Queryable) storage.Queryable { return ownResourcesQueryable{Queryable: q, fake: fake} }
		planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
		require.NoError(t, err)
		engine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
		require.NoError(t, err)
		q, err := engine.NewRangeQuery(t.Context(), store, nil, `info(up)`, time.UnixMilli(0), time.UnixMilli(20*time.Minute.Milliseconds()), time.Minute)
		require.NoError(t, err)
		defer q.Close()
		res := q.Exec(t.Context())
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Matrix()
	}

	_, err := query(&fakeOwnResources{})
	require.ErrorContains(t, err, "found duplicate series for info metric", "the join")
	matrix, err := query(fake)
	require.NoError(t, err)
	require.Len(t, matrix, 1)
	require.Equal(t, "own", matrix[0].Metric.Get("host_name"))

	// In own mode, an inner series the lookup didn't return takes the join, which fails as the join does, and the
	// failed query gives back every info series' samples once: own mode holds them, not its signatures.
	_, err = query(&fakeOwnResources{ownMode: true, resources: querierapi.NativeSeriesResources{CoverageStart: math.MinInt64}})
	require.ErrorContains(t, err, "found duplicate series for info metric", "own mode's join")
}

func stepsByLabels(matrix promql.Matrix) map[string][]int64 {
	out := map[string][]int64{}
	for _, s := range matrix {
		for _, p := range s.Floats {
			out[s.Metric.String()] = append(out[s.Metric.String()], p.T)
		}
	}
	return out
}
