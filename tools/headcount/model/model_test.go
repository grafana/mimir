// SPDX-License-Identifier: AGPL-3.0-only

package model

import (
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func testConfig() Config {
	return Config{
		Seed:        1,
		Start:       time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC),
		End:         time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC), // 4 days
		MetricNames: 50,
		SeriesZipfS: 1.2,
		SeriesFloor: 1,
		SeriesCap:   200,
		SpikeMetric: -1,
	}
}

func TestConfig_Validate(t *testing.T) {
	base := testConfig()

	valid := base
	require.NoError(t, valid.Validate())

	cases := map[string]func(*Config){
		"end before start":      func(c *Config) { c.End = c.Start.Add(-time.Hour) },
		"no metric names":       func(c *Config) { c.MetricNames = 0 },
		"zipf exponent too low": func(c *Config) { c.SeriesZipfS = 1 },
		"zero series floor":     func(c *Config) { c.SeriesFloor = 0 },
		"cap below floor":       func(c *Config) { c.SeriesCap = c.SeriesFloor - 1 },
		"spike index out of range": func(c *Config) {
			c.SpikeMetric = c.MetricNames
		},
		"peak below base": func(c *Config) {
			c.SpikeMetric, c.SpikeBaseValues, c.SpikePeakValues = 0, 10, 5
		},
		"gap fraction without duration": func(c *Config) { c.GapFraction = 0.1 },
		"churn fraction without period": func(c *Config) { c.ChurnFraction = 0.1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := base
			mutate(&c)
			require.Error(t, c.Validate())
		})
	}
}

func TestNew_Deterministic(t *testing.T) {
	cfg := testConfig()

	a, err := New(cfg)
	require.NoError(t, err)
	b, err := New(cfg)
	require.NoError(t, err)

	require.True(t, reflect.DeepEqual(a.Series, b.Series), "same seed must produce an identical population")
}

func TestNew_SeriesCountWithinBounds(t *testing.T) {
	cfg := testConfig()
	m, err := New(cfg)
	require.NoError(t, err)

	byName := map[string]int{}
	for _, s := range m.Series {
		byName[s.Labels.Get("__name__")]++
	}
	require.Len(t, byName, cfg.MetricNames, "every configured metric name must produce at least one series")
	for name, count := range byName {
		require.GreaterOrEqual(t, count, int(cfg.SeriesFloor), "metric %s", name)
		require.LessOrEqual(t, count, int(cfg.SeriesCap), "metric %s", name)
	}
}

func TestNew_ChurnKeepsConcurrentCardinalityStable(t *testing.T) {
	cfg := testConfig()
	cfg.ChurnFraction = 0.5
	cfg.ChurnPeriod = 24 * time.Hour // one generation per day, 4 generations over the 4-day range.

	m, err := New(cfg)
	require.NoError(t, err)

	// The total row count grows with the number of churn generations...
	require.Greater(t, len(m.Series), cfg.MetricNames*int(cfg.SeriesFloor))

	// ...but a one-hour window near the start sees only the concurrently-live
	// series: at most one member of each churn chain, plus every static series.
	windowStart := cfg.startMillis()
	windowEnd := windowStart + time.Hour.Milliseconds()
	byName := map[string]int{}
	for _, s := range m.Series {
		if s.Live(windowStart, windowEnd) {
			byName[s.Labels.Get("__name__")]++
		}
	}
	for name, count := range byName {
		require.LessOrEqual(t, count, int(cfg.SeriesCap), "metric %s: concurrent cardinality must respect the configured cap even with churn", name)
	}
}

func TestNew_SpikeMetric(t *testing.T) {
	cfg := testConfig()
	cfg.SpikeMetric = 0
	cfg.SpikeDay = 1
	cfg.SpikeBaseValues = 10
	cfg.SpikePeakValues = 1000

	m, err := New(cfg)
	require.NoError(t, err)

	dayMs := (24 * time.Hour).Milliseconds()
	beforeSpike := cfg.startMillis()
	duringSpike := cfg.startMillis() + dayMs + dayMs/2
	afterSpike := cfg.startMillis() + 3*dayMs

	spikeName := m.Series[0].Labels.Get("__name__") // metric index 0 is the configured spike metric.
	nameMatcher := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", spikeName)}
	require.Equal(t, cfg.SpikeBaseValues, m.Truth(nameMatcher, beforeSpike, beforeSpike+1))
	require.Equal(t, cfg.SpikePeakValues, m.Truth(nameMatcher, duringSpike, duringSpike+1))
	require.Equal(t, cfg.SpikeBaseValues, m.Truth(nameMatcher, afterSpike, afterSpike+1))
}

func TestApplyGaps_SplitsAnInterval(t *testing.T) {
	cfg := testConfig()
	cfg.GapFraction = 1 // every series gets a gap, so the test isn't flaky.
	cfg.GapDuration = time.Hour

	m, err := New(cfg)
	require.NoError(t, err)

	var gapped int
	for _, s := range m.Series {
		if len(s.Intervals) > 1 {
			gapped++
			mid := s.Intervals[0].End // gap starts right where the first interval ends.
			require.False(t, s.Live(mid, mid+cfg.GapDuration.Milliseconds()), "the gap window must not be live")
		}
	}
	require.Greater(t, gapped, 0, "at least one series must have been split by a gap")
}

func TestApplyStaleness_EndsSeriesEarly(t *testing.T) {
	cfg := testConfig()
	cfg.StaleFraction = 1 // every series goes stale, so the test isn't flaky.

	m, err := New(cfg)
	require.NoError(t, err)

	for _, s := range m.Series {
		last := s.Intervals[len(s.Intervals)-1]
		require.Less(t, last.End, cfg.endMillis(), "%s must end before the model's End", s.Labels)
	}
}

func TestTruth_MatchesLabelSelectors(t *testing.T) {
	cfg := testConfig()
	m, err := New(cfg)
	require.NoError(t, err)

	var wantName string
	for _, s := range m.Series {
		wantName = s.Labels.Get("__name__")
		break
	}

	matcher := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", wantName)}
	got := m.Truth(matcher, cfg.startMillis(), cfg.endMillis())

	want := 0
	for _, s := range m.Series {
		if s.Labels.Get("__name__") == wantName {
			want++
		}
	}
	require.Equal(t, want, got)
}
