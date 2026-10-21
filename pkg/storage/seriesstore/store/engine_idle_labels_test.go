// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"math"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
)

// The head-only label lookups, which Mimir's LabelNamesAndValues makes, those of a range of the head and the in-memory
// label values cardinality see the series the engine evicted for being idle as Prometheus's head does: until a head
// compaction leaves them without samples, and across a restart.
func TestHeadLabelLookupsSeeIdleEvictedSeriesLikePrometheus(t *testing.T) {
	ctx := context.Background()
	const minute = int64(60_000)
	base := int64(10) * 60 * minute
	dir := t.TempDir()
	options := EngineOptions{Shards: 4, IdleEvictionMs: 30 * minute, SecondaryHashFunction: secondaryHash}
	engine, err := OpenEngine(dir, "tenant", options)
	require.NoError(t, err)
	reference := openPrometheus(t, t.TempDir(), differentialOptions{}, nil)
	t.Cleanup(func() { require.NoError(t, reference.db.Close()) })

	appendBoth := func(lset promlabels.Labels, at int64) {
		for _, app := range []EngineAppender{engine.Appender(ctx), reference.Appender(ctx)} {
			_, err := app.Append(0, lset, at, 1)
			require.NoError(t, err)
			require.NoError(t, app.Commit())
		}
	}
	// Pods of series that live ten minutes each, one after the other, each with a label of its own: the head ends an hour in.
	for pod := range 6 {
		for n := range 5 {
			for at := int64(0); at <= 10; at += 5 {
				appendBoth(promlabels.FromStrings("__name__", "metric", "pod", fmt.Sprintf("pod-%d-%d", pod, n), fmt.Sprintf("only_pod_%d", pod), "yes"), base+(int64(pod)*10+at)*minute)
			}
		}
	}
	require.NoError(t, engine.Compact(ctx))
	require.Less(t, engine.NumSeries(), reference.NumSeries(), "the engine evicted the pods that stopped")

	lookups := func(index tsdb.IndexReader) map[string][]string {
		out := map[string][]string{}
		for _, matchers := range [][]*promlabels.Matcher{nil, {promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "metric")}} {
			names, err := index.LabelNames(ctx, matchers...)
			require.NoError(t, err)
			out[fmt.Sprint("names ", matchers)] = names
			values, err := index.LabelValues(ctx, "pod", nil, matchers...)
			require.NoError(t, err)
			out[fmt.Sprint("pod values ", matchers)] = values
		}
		return out
	}
	// The lookups of a range of the head's newest samples, which Mimir's LabelNames and LabelValues make: Prometheus's
	// head index answers them with all of its series, whichever block range the range is in.
	rangeStart := base + 55*minute
	ranged := func(head headUnderTest) map[string][]string {
		querier, err := head.Querier(rangeStart, rangeStart+5*minute)
		require.NoError(t, err)
		defer func() { require.NoError(t, querier.Close()) }()
		out := map[string][]string{}
		for _, matchers := range [][]*promlabels.Matcher{nil, {promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "metric")}} {
			names, _, err := querier.LabelNames(ctx, nil, matchers...)
			require.NoError(t, err)
			out[fmt.Sprint("names ", matchers)] = names
			values, _, err := querier.LabelValues(ctx, "pod", nil, matchers...)
			require.NoError(t, err)
			out[fmt.Sprint("pod values ", matchers)] = values
		}
		return out
	}
	// The counts of the in-memory label values cardinality, which Prometheus's head index has the postings of.
	counts := func(index tsdb.IndexReader) map[string]uint64 {
		values, err := index.LabelValues(ctx, "pod", nil)
		require.NoError(t, err)
		out := map[string]uint64{}
		for _, value := range values {
			postings, err := index.Postings(ctx, "pod", value)
			require.NoError(t, err)
			for postings.Next() {
				out[value]++
			}
			require.NoError(t, postings.Err())
		}
		return out
	}
	compare := func(msg string) {
		engineIndex, err := engine.Index()
		require.NoError(t, err)
		referenceIndex, err := reference.Index()
		require.NoError(t, err)
		require.Equal(t, lookups(referenceIndex), lookups(engineIndex), msg)
		require.Equal(t, ranged(reference), ranged(engine), msg)
		engineCounts, err := engineIndex.(interface {
			LabelValuesCounts(context.Context, []string, []*promlabels.Matcher, int64) (map[string]map[string]uint64, error)
		}).LabelValuesCounts(ctx, []string{"pod"}, nil, math.MinInt64)
		require.NoError(t, err)
		require.Equal(t, counts(referenceIndex), engineCounts["pod"], msg)
		require.NoError(t, referenceIndex.Close())
	}
	compare("evicted, but still in Prometheus's head")

	require.NoError(t, engine.Close())
	engine, err = OpenEngine(dir, "tenant", options)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	compare("after a restart")

	// A sample in the next block range, which the head spans too.
	appendBoth(promlabels.FromStrings("__name__", "metric", "pod", "recent"), base+125*minute)
	require.NoError(t, engine.Compact(ctx))
	require.NoError(t, reference.Compact(ctx))
	rangeStart = base + 121*minute
	compare("evicted, but still in Prometheus's head, and a range of the next block")

	// Samples hours later: the head compactions leave the stopped pods without samples, and Prometheus collects them.
	for at := int64(3); at <= 7; at++ {
		appendBoth(promlabels.FromStrings("__name__", "metric", "pod", "late"), base+at*60*minute)
		require.NoError(t, engine.Compact(ctx))
		require.NoError(t, reference.Compact(ctx))
	}
	rangeStart = base + 55*minute
	compare("after the head compactions")
}

// BenchmarkEngineHeadLabelLookups measures the head-only label lookups of LabelNamesAndValues, with and without series
// the engine evicted for being idle that Prometheus's head would still hold.
func BenchmarkEngineHeadLabelLookups(b *testing.B) {
	ctx := context.Background()
	const minute = int64(60_000)
	base := int64(10) * 60 * minute
	for _, evicted := range []int{0, 20_000} {
		b.Run(fmt.Sprintf("evicted=%d", evicted), func(b *testing.B) {
			engine, err := OpenEngine(b.TempDir(), "tenant", EngineOptions{Shards: 4, IdleEvictionMs: 30 * minute, SecondaryHashFunction: secondaryHash})
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, engine.Close()) })
			app := engine.Appender(ctx)
			for n := range evicted {
				_, err := app.Append(0, promlabels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%50), "pod", fmt.Sprintf("gone-%d", n), "job", "old"), base, 1)
				require.NoError(b, err)
			}
			for n := range 20_000 {
				_, err := app.Append(0, promlabels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%50), "pod", fmt.Sprintf("pod-%d", n), "job", "live"), base+60*minute, 1)
				require.NoError(b, err)
			}
			require.NoError(b, app.Commit())
			require.NoError(b, engine.Compact(ctx))
			require.Equal(b, uint64(20_000), engine.NumSeries())
			index, err := engine.Index()
			require.NoError(b, err)
			matcher := promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "metric_7")
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, err := index.LabelNames(ctx, matcher)
				require.NoError(b, err)
				_, err = index.LabelValues(ctx, "pod", nil, matcher)
				require.NoError(b, err)
				_, err = index.LabelValues(ctx, "job", nil)
				require.NoError(b, err)
			}
		})
	}
}
