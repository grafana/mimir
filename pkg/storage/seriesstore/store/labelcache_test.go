// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
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
				_, err := app.Append(0, lset, ts, 1)
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
				_, err := app.Append(0, promlabels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%2), "pod", fmt.Sprintf("pod-%d", n), "zone", fmt.Sprintf("z%d", n%5)), 1_000, 1)
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
	}
	appendSeries(0, 20000)
	check()
	// Series added after the lookups cached the label's values.
	appendSeries(20000, 24000)
	check()
}
