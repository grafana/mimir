// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

// A slow regex's kept verdicts are its own, from goroutines at once, and once dropped to make room.
func TestRegexMemoAgreesWithTheRegex(t *testing.T) {
	defer func(ns int64) { slowRegexNs = ns }(slowRegexNs)
	slowRegexNs = 0
	re, err := anchoredRegex(fmt.Sprintf("(?i).*(vault|tenet%d).*", t.Name()[0]))
	require.NoError(t, err)
	values := make([]string, 5000)
	for n := range values {
		values[n] = fmt.Sprintf("Service-%d-%s", n, []string{"VAULT", "vaultish", "tenet", "other", "Tenet84"}[n%5])
	}
	check := func() {
		var wg sync.WaitGroup
		for worker := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for step := range values {
					value := values[(step+worker*625)%len(values)]
					if re.memoMatch(value) != re.isMatch(value) {
						t.Errorf("%q", value)
						return
					}
				}
			}()
		}
		wg.Wait()
	}
	check()
	require.Equal(t, regexMemoSlow, re.verdicts.state.Load())
	require.Positive(t, re.verdicts.values.Load())
	re.verdicts.reset()
	require.Zero(t, re.verdicts.values.Load())
	check()
}

// Selectors without a metric name with a slow regex select the series a scan finds, in every query shard of queries
// sent at once, the first time and the next, when the regexes' kept verdicts answer.
func TestEngineNamelessSlowRegexSelectsTheSeriesThatMatch(t *testing.T) {
	defer func(ns int64) { slowRegexNs = ns }(slowRegexNs)
	slowRegexNs = 0
	ctx := t.Context()
	engine, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 8, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	words := []string{"Vault", "tenet", "TESTAMENT", "aurora"}
	var all []promlabels.Labels
	app := engine.Appender(ctx)
	for n := range 6000 {
		lset := promlabels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%9), "name", fmt.Sprintf("svc-%d-%s", n%1500, words[n%1500%len(words)]))
		all = append(all, lset)
		_, err := app.Append(0, lset, 0, 1000, 1, nil, nil, storage.AppendV2Options{})
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())
	for _, pattern := range []string{"(?i).*(vault).*", "(?i).*(tenet|testament).*", "(?i).*(nothing).*"} {
		matcher := promlabels.MustNewMatcher(promlabels.MatchRegexp, "name", pattern)
		var expected []string
		for _, lset := range all {
			if matcher.Matches(lset.Get("name")) {
				expected = append(expected, lset.String())
			}
		}
		slices.Sort(expected)
		// The series of the same labels are one.
		expected = slices.Compact(expected)
		for round := range 3 {
			const shards = 8
			got := make([][]string, shards)
			var wg sync.WaitGroup
			for shard := range shards {
				wg.Add(1)
				go func() {
					defer wg.Done()
					q, err := engine.ChunkQuerier(0, 2000)
					if err != nil {
						t.Error(err)
						return
					}
					defer func() { _ = q.Close() }()
					set := q.Select(ctx, true, &storage.SelectHints{Start: 0, End: 2000, ShardIndex: uint64(shard), ShardCount: shards}, matcher)
					for set.Next() {
						got[shard] = append(got[shard], set.At().Labels().String())
					}
					if err := set.Err(); err != nil {
						t.Error(err)
					}
				}()
			}
			wg.Wait()
			union := slices.Concat(got...)
			if round > 0 && pattern != "(?i).*(nothing).*" {
				re, err := anchoredRegex(pattern)
				require.NoError(t, err)
				require.Equal(t, regexMemoSlow, re.verdicts.state.Load(), pattern)
			}
			slices.Sort(union)
			require.Equal(t, expected, union, "%s, round %d", pattern, round)
		}
		// Not matching: what the regex rejects.
		not := promlabels.MustNewMatcher(promlabels.MatchNotRegexp, "name", pattern)
		q, err := engine.ChunkQuerier(0, 2000)
		require.NoError(t, err)
		set := q.Select(ctx, true, nil, promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "metric_3"), not)
		count := 0
		for set.Next() {
			require.False(t, matcher.Matches(set.At().Labels().Get("name")))
			count++
		}
		require.NoError(t, set.Err())
		want := map[string]struct{}{}
		for _, lset := range all {
			if lset.Get("__name__") == "metric_3" && !matcher.Matches(lset.Get("name")) {
				want[lset.String()] = struct{}{}
			}
		}
		require.Equal(t, len(want), count, pattern)
		require.NoError(t, q.Close())
	}
}
