// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

// BenchmarkEngineLabelValuesOverChurn looks up label values like dashboard variables do, over six hours of a tenant
// whose pods churn: each generation of series is live for an hour and a half, then idle, so most series a lookup sees
// are in cold blocks, from idle eviction and from compaction, and a lookup of the last hour cuts through them.
func BenchmarkEngineLabelValuesOverChurn(b *testing.B) {
	const (
		live        = 50_000
		generations = 4
		intervalMs  = 60_000
		rounds      = 360
		generation  = rounds / generations
	)
	e, err := OpenEngine(b.TempDir(), "tenant", EngineOptions{Shards: 8, TimelyCompaction: true, IdleEvictionMs: 1_800_000})
	require.NoError(b, err)
	b.Cleanup(func() { _ = e.Close() })
	for round := range rounds {
		app := e.Appender(context.Background())
		// Series of the generation before stay live for a while after the next starts, like a rollout.
		first := max(0, (round-10)/generation*live)
		for n := first; n < (round/generation+1)*live; n++ {
			ts, v := realisticSample(n, round, intervalMs)
			_, err := app.Append(0, realisticLabels(n), 0, ts, v, nil, nil, storage.AppendV2Options{})
			require.NoError(b, err)
		}
		require.NoError(b, app.Commit())
		if round%30 == 29 {
			require.NoError(b, e.Compact(context.Background()))
		}
	}
	require.Less(b, e.NumSeries(), uint64(live*generations/2), "most series left the head")
	const end = rounds * intervalMs
	for _, lookup := range []struct {
		name     string
		label    string
		mint     int64
		matchers []*promlabels.Matcher
	}{
		{"absent label matcher, last hour", "job", end - 3_600_000, []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchNotEqual, "source", "other")}},
		{"absent label matcher, pods, last hour", "pod", end - 3_600_000, []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchNotEqual, "source", "other")}},
		{"name regex, all", "job", 0, []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", "metric_1|metric_2")}},
		{"name regex, instances, all", "instance", 0, []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", "metric_1|metric_2")}},
		{"name prefix regex, last hour", "namespace", end - 3_600_000, []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", "metric_1.*")}},
		{"name regex and job, last hour", "pod", end - 3_600_000, []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", "metric_1|metric_2"), promlabels.MustNewMatcher(promlabels.MatchEqual, "job", "job-1")}},
		{"namespace regex, last hour", "pod", end - 3_600_000, []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchRegexp, "namespace", "ns-1|ns-2")}},
	} {
		b.Run(lookup.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				q, err := e.Querier(lookup.mint, end)
				require.NoError(b, err)
				_, _, err = q.LabelValues(context.Background(), lookup.label, nil, lookup.matchers...)
				require.NoError(b, err)
				_ = q.Close()
			}
		})
	}
}

func TestEngineColdLabelValuesWithMatchersOfAbsentLabels(t *testing.T) {
	e, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	app := e.Appender(t.Context())
	for n := range 20 {
		_, err := app.Append(0, promlabels.FromStrings("__name__", "up", "job", "job-"+string(rune('a'+n%4))), 0, 0, 1, nil, nil, storage.AppendV2Options{})
		require.NoError(t, err)
	}
	// Keeps the head six hours ahead, so compaction moves the other series out of it.
	_, err = app.Append(0, promlabels.FromStrings("__name__", "fresh"), 0, 6*3_600_000, 1, nil, nil, storage.AppendV2Options{})
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.NoError(t, e.Compact(t.Context()))
	require.Equal(t, uint64(1), e.NumSeries(), "the series left the head")
	all := []string{"job-a", "job-b", "job-c", "job-d"}
	for _, tc := range []struct {
		matcher  *promlabels.Matcher
		expected []string
	}{
		{promlabels.MustNewMatcher(promlabels.MatchNotEqual, "source", "other"), all},
		{promlabels.MustNewMatcher(promlabels.MatchRegexp, "source", ".*"), all},
		{promlabels.MustNewMatcher(promlabels.MatchEqual, "source", ""), all},
		{promlabels.MustNewMatcher(promlabels.MatchEqual, "source", "other"), nil},
		{promlabels.MustNewMatcher(promlabels.MatchRegexp, "source", ".+"), nil},
		{promlabels.MustNewMatcher(promlabels.MatchNotEqual, "source", ""), nil},
	} {
		t.Run(tc.matcher.String(), func(t *testing.T) {
			q, err := e.Querier(0, 3_600_000)
			require.NoError(t, err)
			defer q.Close()
			values, _, err := q.LabelValues(t.Context(), "job", nil, tc.matcher, promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "up"))
			require.NoError(t, err)
			if tc.expected == nil {
				require.Empty(t, values)
				return
			}
			require.Equal(t, tc.expected, values)
		})
	}
}
