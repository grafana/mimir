// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/tools/headcount/cardgen"
	"github.com/grafana/mimir/tools/headcount/model"
)

func testConfig(t *testing.T) model.Config {
	t.Helper()
	p, err := cardgen.LoadProfile("live", 7)
	require.NoError(t, err)
	cfg := p.Population
	cfg.MetricNames = 50 // Keep the test population small.
	return cfg
}

func TestShiftConfig_SameLabelsShiftedIntervals(t *testing.T) {
	cfg := testConfig(t)
	orig, err := model.New(cfg)
	require.NoError(t, err)

	start := time.Date(2026, 10, 16, 8, 0, 0, 0, time.UTC)
	shifted := shiftConfig(cfg, start)
	require.Equal(t, start, shifted.Start)
	require.Equal(t, cfg.End.Sub(cfg.Start), shifted.End.Sub(shifted.Start), "window length must be kept")

	moved, err := model.New(shifted)
	require.NoError(t, err)
	require.Len(t, moved.Series, len(orig.Series))

	delta := start.Sub(cfg.Start).Milliseconds()
	for i := range orig.Series {
		require.Equal(t, orig.Series[i].Labels, moved.Series[i].Labels)
		require.Len(t, moved.Series[i].Intervals, len(orig.Series[i].Intervals))
		for j, iv := range orig.Series[i].Intervals {
			require.Equal(t, model.Interval{Start: iv.Start + delta, End: iv.End + delta}, moved.Series[i].Intervals[j])
		}
	}

	// The truth of a window moves with it.
	all := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "__name__", ".+")}
	lo, hi := cfg.Start.Add(time.Hour).UnixMilli(), cfg.Start.Add(time.Hour+5*time.Minute).UnixMilli()
	require.Equal(t, orig.Truth(all, lo, hi), moved.Truth(all, lo+delta, hi+delta))
}

func TestAlignDownAndStepTimes(t *testing.T) {
	step := 15 * time.Second
	tm := time.Date(2026, 10, 16, 8, 0, 44, 123, time.UTC)
	got := alignDown(tm, step)
	require.Equal(t, time.Date(2026, 10, 16, 8, 0, 30, 0, time.UTC), got)
	require.Equal(t, got, alignDown(got, step), "aligned time must be a fixed point")

	steps := stepTimes(got, got.Add(time.Minute), step)
	require.Len(t, steps, 4)
	for i, s := range steps {
		require.Equal(t, got.Add(time.Duration(i)*step).UnixMilli(), s)
		require.Zero(t, s%step.Milliseconds(), "every grid point is a multiple of the step")
	}
}

func TestLiveAt(t *testing.T) {
	series := []model.Series{
		{Labels: labels.FromStrings("__name__", "a"), Intervals: []model.Interval{{Start: 0, End: 100}}},
		{Labels: labels.FromStrings("__name__", "b"), Intervals: []model.Interval{{Start: 100, End: 200}}},
		{Labels: labels.FromStrings("__name__", "c"), Intervals: []model.Interval{{Start: 0, End: 50}, {Start: 150, End: 200}}}, // Gap in [50, 150).
	}
	require.Equal(t, []int{0, 2}, liveAt(series, 0))
	require.Equal(t, []int{0}, liveAt(series, 60), "c is in its gap")
	require.Equal(t, []int{0}, liveAt(series, 99))
	require.Equal(t, []int{1}, liveAt(series, 100), "a's interval end is exclusive, b's start inclusive")
	require.Equal(t, []int{1, 2}, liveAt(series, 150))
	require.Empty(t, liveAt(series, 200))
}

func TestLiveAt_ChurnShowsUpOverGrid(t *testing.T) {
	cfg := testConfig(t)
	cfg.MetricNames = 20
	m, err := model.New(cfg)
	require.NoError(t, err)

	// Union of series live at any grid point of the first hour exceeds
	// the number live at the first grid point when churn is on, and every
	// churned series pushed at some point must be live in the model then.
	seen := map[int]bool{}
	first := len(liveAt(m.Series, cfg.Start.UnixMilli()))
	for _, s := range stepTimes(cfg.Start, cfg.Start.Add(time.Hour), 15*time.Second) {
		for _, i := range liveAt(m.Series, s) {
			seen[i] = true
			require.True(t, m.Series[i].Live(s, s+1))
		}
	}
	require.Greater(t, len(seen), first, "15-minute churn must add series within an hour")
}

func TestSampledTruth_MatchesTruthOnLongIntervals(t *testing.T) {
	cfg := testConfig(t)
	m, err := model.New(cfg)
	require.NoError(t, err)
	all := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "__name__", ".+")}
	keep := func(model.Series) bool { return true }
	step := 15 * time.Second
	for _, off := range []time.Duration{10 * time.Minute, 3 * time.Hour, 26 * time.Hour} {
		hi := alignDown(cfg.Start.Add(off), step).UnixMilli()
		lo := hi - (5 * time.Minute).Milliseconds() + 1 // (hi-5m, hi], as PromQL's lookback selects.
		require.Equal(t, m.Truth(all, lo, hi+1), sampledTruth(m.Series, keep, step, lo, hi), "offset %s", off)
	}
}

func TestSampledTruth_SkipsIntervalWithoutGridPoint(t *testing.T) {
	step := int64(15_000)
	series := []model.Series{
		{Intervals: []model.Interval{{Start: 3*step + 1_000, End: 3*step + 2_000}}}, // Between grid points: never sampled.
		{Intervals: []model.Interval{{Start: 3*step + 1_000, End: 4*step + 1}}},     // Contains grid point 4*step.
	}
	keep := func(model.Series) bool { return true }
	lo, hi := 0*step, 10*step
	require.Equal(t, 1, sampledTruth(series, keep, 15*time.Second, lo, hi))
	m := model.Series.Live
	require.True(t, m(series[0], lo, hi+1), "model.Truth would count the unsampled series")
}

func TestBuildRequest_RoundTrip(t *testing.T) {
	series := []model.Series{
		{Labels: labels.FromStrings("__name__", "a", "instance", "i0")},
		{Labels: labels.FromStrings("__name__", "b", "instance", "i1", "pod", "p")},
		{Labels: labels.FromStrings("__name__", "c")},
	}
	req, body, err := buildRequest(series, []int{0, 2}, 1_700_000_000_000, 42)
	require.NoError(t, err)
	require.Len(t, req.Timeseries, 2)

	raw, err := snappy.Decode(nil, body)
	require.NoError(t, err)
	var got mimirpb.WriteRequest
	require.NoError(t, got.Unmarshal(raw))
	require.Len(t, got.Timeseries, 2)
	for i, want := range []labels.Labels{series[0].Labels, series[2].Labels} {
		ts := got.Timeseries[i]
		require.Equal(t, want, mimirpb.FromLabelAdaptersToLabels(ts.Labels))
		require.Equal(t, []mimirpb.Sample{{TimestampMs: 1_700_000_000_000, Value: 42}}, ts.Samples)
	}
}
