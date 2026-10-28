// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// TestEngineLabelLookupsWithoutMatchersMatchPrometheus checks the cached label lookups against the
// TSDB while series are added and compacted out of the head between them.
func TestEngineLabelLookupsWithoutMatchersMatchPrometheus(t *testing.T) {
	ctx := context.Background()
	heads := [2]headUnderTest{openPrometheus(t, t.TempDir(), differentialOptions{}, nil), openEngine(t, t.TempDir(), differentialOptions{}, nil)}
	t.Cleanup(func() {
		for _, head := range heads {
			require.NoError(t, head.Close())
		}
	})
	appendSeries := func(first, last int, ts int64) {
		for _, head := range heads {
			app := head.Appender(ctx)
			for n := first; n < last; n++ {
				lset := promlabels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%5), "job", fmt.Sprintf("job-%d", n%3), "pod", fmt.Sprintf("pod-%d", n))
				if n%7 == 0 {
					lset = promlabels.FromStrings("__name__", "sparse", "extra", fmt.Sprintf("value-%d", n), "pod", fmt.Sprintf("pod-%d", n))
				}
				_, err := app.Append(0, lset, 0, ts, 1, nil, nil, storage.AppendV2Options{})
				require.NoError(t, err)
			}
			require.NoError(t, app.Commit())
		}
	}
	check := func(mint, maxt int64) {
		t.Helper()
		var names, values [2][]string
		for index, head := range heads {
			q, err := head.Querier(mint, maxt)
			require.NoError(t, err)
			names[index], _, err = q.LabelNames(ctx, nil)
			require.NoError(t, err)
			for _, label := range []string{"__name__", "job", "pod", "extra", "missing"} {
				got, _, err := q.LabelValues(ctx, label, nil)
				require.NoError(t, err)
				values[index] = append(values[index], label+"="+fmt.Sprint(got))
			}
			require.NoError(t, q.Close())
		}
		require.Equal(t, names[0], names[1])
		require.Equal(t, values[0], values[1])
	}

	appendSeries(0, 50, 1_000)
	check(0, 10_000)
	// New series after a lookup are added to what it cached.
	appendSeries(50, 80, 2_000)
	check(0, 10_000)
	// A range before the head sees none of it.
	check(-10_000, -5_000)
	// A series far ahead moves the head past the others, which compaction takes out of it.
	appendSeries(1000, 1001, 6*3_600_000)
	for _, head := range heads {
		require.NoError(t, head.Compact(ctx))
	}
	check(0, 7*3_600_000)
	check(6*3_600_000, 7*3_600_000)
	appendSeries(80, 90, 6*3_600_000+1000)
	check(0, 7*3_600_000)
}

// TestEngineRegexSelectsOverLargeLabelsMatchPrometheus covers the regexes looked up in the label's
// values, which only large enough shards do.
func TestEngineRegexSelectsOverLargeLabelsMatchPrometheus(t *testing.T) {
	ctx := context.Background()
	heads := [2]headUnderTest{openPrometheus(t, t.TempDir(), differentialOptions{}, nil), openEngine(t, t.TempDir(), differentialOptions{}, nil)}
	t.Cleanup(func() {
		for _, head := range heads {
			require.NoError(t, head.Close())
		}
	})
	appendSeries := func(first, last int) {
		for _, head := range heads {
			app := head.Appender(ctx)
			for n := first; n < last; n++ {
				_, err := app.Append(0, promlabels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%2), "pod", fmt.Sprintf("pod-%d", n), "zone", fmt.Sprintf("z%d", n%5), "region", "r1"), 0, 1_000, 1, nil, nil, storage.AppendV2Options{})
				require.NoError(t, err)
			}
			require.NoError(t, app.Commit())
		}
	}
	selected := func(matchers ...*promlabels.Matcher) [2][]string {
		var out [2][]string
		for index, head := range heads {
			q, err := head.ChunkQuerier(0, 10_000)
			require.NoError(t, err)
			set := q.Select(ctx, true, nil, matchers...)
			for set.Next() {
				out[index] = append(out[index], set.At().Labels().String())
			}
			require.NoError(t, set.Err())
			require.NoError(t, q.Close())
		}
		return out
	}
	check := func() {
		t.Helper()
		for _, matchers := range [][]*promlabels.Matcher{
			{re("pod", "pod-1.")},
			{re("pod", "pod-12.*")},
			{re("pod", ".*99")},
			{re("pod", "pod-1.*"), eq("zone", "z3")},
			{eq("__name__", "metric_1"), re("pod", "pod-2[0-9]+")},
			{re("pod", "pod-(1|2).*")},
			// Equality matchers narrow each other by their postings.
			{eq("zone", "z3"), eq("__name__", "metric_1")},
			// A label every series has has more postings than the name group: the group is scanned by the
			// hashes of the label's values.
			{eq("__name__", "metric_1"), eq("region", "r1")},
			{eq("__name__", "metric_1"), eq("region", "r1"), eq("zone", "z2")},
			{eq("zone", "z2"), re("pod", "pod-1.*"), eq("__name__", "metric_0")},
			{eq("zone", "z1"), eq("pod", "pod-1001")},
		} {
			got := selected(matchers...)
			require.NotEmpty(t, got[0], matchers)
			require.Equal(t, got[0], got[1], matchers)
		}
		got := selected(re("pod", "nothing.+"))
		require.Equal(t, got[0], got[1])
		// Postings that don't meet select nothing.
		got = selected(eq("zone", "z4"), eq("pod", "pod-1001"))
		require.Empty(t, got[0])
		require.Equal(t, got[0], got[1])
		got = selected(eq("__name__", "metric_1"), eq("region", "r2"))
		require.Empty(t, got[0])
		require.Equal(t, got[0], got[1])
	}
	appendSeries(0, 20000)
	check()
	// Series added after the lookups cached the label's values.
	appendSeries(20000, 24000)
	check()
}

// TestEngineLabelValuesWithMatchersMatchPrometheus checks label values under wide matchers, which look each value's
// series up by postings, and under narrow ones, which visit the selected series, against the TSDB.
func TestEngineLabelValuesWithMatchersMatchPrometheus(t *testing.T) {
	ctx := context.Background()
	heads := [2]headUnderTest{openPrometheus(t, t.TempDir(), differentialOptions{}, nil), openEngine(t, t.TempDir(), differentialOptions{}, nil)}
	t.Cleanup(func() {
		for _, head := range heads {
			require.NoError(t, head.Close())
		}
	})
	for _, head := range heads {
		app := head.Appender(ctx)
		for n := range 2000 {
			// job-6 is only in dev, so matchers on prod select none of its series.
			env := "prod"
			if n%7 == 6 {
				env = "dev"
			}
			lbls := promlabels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%20), "job", fmt.Sprintf("job-%d", n%7), "env", env, "pod", fmt.Sprintf("pod-%d", n))
			_, err := app.Append(0, lbls, 0, 1000, float64(n), nil, nil, storage.AppendV2Options{})
			require.NoError(t, err)
		}
		require.NoError(t, app.Commit())
	}
	for _, lookup := range []struct {
		label    string
		matchers []*promlabels.Matcher
	}{
		{"job", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", "metric_.*"), promlabels.MustNewMatcher(promlabels.MatchRegexp, "env", "prod")}},
		{"job", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "env", "prod")}},
		{"env", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchNotEqual, "job", "job-6")}},
		{"pod", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchRegexp, "job", "job-[0-2]")}},
		{"job", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "metric_3")}},
		{"job", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "env", "staging")}},
	} {
		var got [2][]string
		for i, head := range heads {
			q, err := head.Querier(0, 2000)
			require.NoError(t, err)
			got[i], _, err = q.LabelValues(ctx, lookup.label, nil, lookup.matchers...)
			require.NoError(t, err)
			require.NoError(t, q.Close())
		}
		if len(got[0]) == 0 && len(got[1]) == 0 {
			// An empty answer is nil from one head and empty from the other.
			continue
		}
		require.Equal(t, got[0], got[1], "%s under %v", lookup.label, lookup.matchers)
	}
}

// TestEngineLabelValuesWithoutMatchersOnceASeriesLeftTheHead checks the values of the postings' lookups, which answer
// without matchers once a series of a shard left the head, against the head's cached values from before.
func TestEngineLabelValuesWithoutMatchersOnceASeriesLeftTheHead(t *testing.T) {
	ctx := context.Background()
	engine := openEngine(t, t.TempDir(), differentialOptions{}, nil)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	app := engine.Appender(ctx)
	for n := range 2000 {
		lbls := promlabels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%20), "job", fmt.Sprintf("job-%d", n%7), "pod", fmt.Sprintf("pod-%d", n))
		_, err := app.Append(0, lbls, 0, 1000, float64(n), nil, nil, storage.AppendV2Options{})
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())
	lookup := func() []string {
		q, err := engine.Querier(0, 2000)
		require.NoError(t, err)
		defer func() { require.NoError(t, q.Close()) }()
		values, _, err := q.LabelValues(ctx, "job", nil)
		require.NoError(t, err)
		return values
	}
	cached := lookup()
	require.Len(t, cached, 7)
	// A series that left the head but is still in its shard's index, as idle eviction leaves them for a while.
	for _, shard := range engine.store.shards {
		shard.Lock()
		if tenant, ok := shard.tenants["tenant"]; ok {
			evicted := false
			tenant.series.forEach(func(entry *seriesEntry) {
				if !evicted {
					entry.series.headEvicted, evicted = true, true
				}
			})
		}
		shard.Unlock()
	}
	engine.store.evictEpoch.Add(1)
	require.Equal(t, cached, lookup())
}

// TestEngineLabelValuesOverIdleSeriesMatchPrometheus checks label values over the head once series left it for being
// idle, which the engine finds in the blocks they went to by each block's index of the label's values, against the
// TSDB, whose head keeps them.
func TestEngineLabelValuesOverIdleSeriesMatchPrometheus(t *testing.T) {
	ctx := context.Background()
	prometheus := openPrometheus(t, t.TempDir(), differentialOptions{}, nil)
	engine, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, IdleEvictionMs: 1_800_000, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	heads := [2]headUnderTest{prometheus, engine}
	t.Cleanup(func() {
		for _, head := range heads {
			require.NoError(t, head.Close())
		}
	})
	const now = 3 * 3_600_000
	for _, head := range heads {
		app := head.Appender(ctx)
		// Enough idle series that each shard's block has more pods than its index keeps.
		for n := range 8000 {
			// The idle series' job-9 and region are on none of the live series.
			lbls := promlabels.FromStrings("__name__", fmt.Sprintf("idle_%d", n%20), "job", fmt.Sprintf("job-%d", 3+n%7), "region", fmt.Sprintf("region-%d", n%3), "pod", fmt.Sprintf("idle-%d", n))
			_, err := app.Append(0, lbls, 0, 2*3_600_000, 1, nil, nil, storage.AppendV2Options{})
			require.NoError(t, err)
		}
		require.NoError(t, app.Commit())
		for sample := range 4 {
			app := head.Appender(ctx)
			for n := range 2000 {
				lbls := promlabels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%20), "job", fmt.Sprintf("job-%d", n%7), "pod", fmt.Sprintf("pod-%d", n))
				_, err := app.Append(0, lbls, 0, now+int64(sample)*15_000, float64(n), nil, nil, storage.AppendV2Options{})
				require.NoError(t, err)
			}
			require.NoError(t, app.Commit())
		}
	}
	require.NoError(t, engine.Compact(ctx))
	require.Equal(t, uint64(2000), engine.NumSeries(), "the idle series left the engine's head")
	for _, lookup := range []struct {
		label    string
		matchers []*promlabels.Matcher
	}{
		{"job", nil},
		{"region", nil},
		// More values than a block's index keeps.
		{"pod", nil},
		{"job", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", "idle_.*")}},
		{"region", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "job", "job-9")}},
		{"job", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "region", "region-1")}},
	} {
		var got [2][]string
		for i, head := range heads {
			q, err := head.Querier(now, now+60_000)
			require.NoError(t, err)
			got[i], _, err = q.LabelValues(ctx, lookup.label, nil, lookup.matchers...)
			require.NoError(t, err)
			require.NoError(t, q.Close())
		}
		require.NotEmpty(t, got[0], "%s under %v", lookup.label, lookup.matchers)
		require.Equal(t, got[0], got[1], "%s under %v", lookup.label, lookup.matchers)
		for i, head := range heads {
			q, err := head.Querier(now, now+60_000)
			require.NoError(t, err)
			got[i], _, err = q.LabelNames(ctx, nil, lookup.matchers...)
			require.NoError(t, err)
			require.NoError(t, q.Close())
		}
		require.Equal(t, got[0], got[1], "label names under %v", lookup.matchers)
	}
}

// TestLabelDictionaryKeepsItsValuesWhenSeriesLeave checks that a label's dictionary survives series being removed, which
// idle eviction does all the time, with the values of the series added since, and starts over when the postings are
// rebuilt.
func TestLabelDictionaryKeepsItsValuesWhenSeriesLeave(t *testing.T) {
	byName := newSeriesByName()
	insert := func(n int, job string) {
		stored := labels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%7), "job", job, "pod", fmt.Sprintf("pod_%d", n))
		byName.insert(stored.Hash(), stored, Series{lastIngestedMs: int64(n)})
	}
	for n := range 100 {
		insert(n, fmt.Sprintf("job_%d", n%5))
	}
	job := labels.Intern("job")
	dictionary, ok := byName.labelDictionary(job)
	require.True(t, ok)
	require.Equal(t, []string{"job_0", "job_1", "job_2", "job_3", "job_4"}, dictionary)
	built := byName.headLabels.dictionaries[job]

	// The removed series' values stay: they only select none.
	byName.retain(func(entry *seriesEntry) bool { return entry.series.lastIngestedMs%5 != 4 })
	insert(100, "job_5")
	dictionary, ok = byName.labelDictionary(job)
	require.True(t, ok)
	require.Equal(t, []string{"job_0", "job_1", "job_2", "job_3", "job_4", "job_5"}, dictionary)
	require.Same(t, built, byName.headLabels.dictionaries[job], "kept across the removal")
	// Started over, as after a restart, from postings that still have the removed series' ids: only the values of the
	// series there are.
	byName.headLabels.dictionaries = nil
	dictionary, ok = byName.labelDictionary(job)
	require.True(t, ok)
	require.Equal(t, []string{"job_0", "job_1", "job_2", "job_3", "job_5"}, dictionary)
	require.Positive(t, byName.stale, "the postings still have the removed series")

	// A rebuild of the postings, with a series added meanwhile, has only the series there are.
	snapshot := byName.snapshotForPostings()
	rebuilt := buildPostings(snapshot)
	insert(101, "job_6")
	require.True(t, byName.installPostings(rebuilt, snapshot))
	dictionary, ok = byName.labelDictionary(job)
	require.True(t, ok)
	require.Equal(t, []string{"job_0", "job_1", "job_2", "job_3", "job_5", "job_6"}, dictionary)
	insert(102, "job_7")
	dictionary, ok = byName.labelDictionary(job)
	require.True(t, ok)
	require.Contains(t, dictionary, "job_7", "the series added since the rebuild")
}
