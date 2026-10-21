// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"math"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
)

// A series an appender created has no samples until it commits: the head's garbage collection, retention and the
// compaction of selected series leave it meanwhile, as Prometheus's head leaves a series pending a commit, so that
// its first sample isn't dropped at the commit.
func TestEngineKeepsTheSeriesAnAppenderCreatedUntilItCommits(t *testing.T) {
	ctx := context.Background()
	const hour = int64(3_600_000)
	engine, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, RetentionMs: 1, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	old := promlabels.FromStrings("__name__", "old")
	app := engine.Appender(ctx)
	for at := int64(0); at <= 7*hour/2; at += hour / 2 {
		_, err := app.Append(0, old, at, 1)
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())

	matcher := promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "new")
	for _, c := range []struct {
		name    string
		between func(ref uint64)
	}{
		// The head spans more than one and a half block ranges: the compaction truncates it, and collects the series
		// without samples since.
		{"head compaction", func(uint64) { require.NoError(t, engine.Compact(ctx)) }},
		{"selected series", func(ref uint64) {
			require.NoError(t, engine.CompactSelectedSeries([]storage.SeriesRef{storage.SeriesRef(ref)}))
		}},
		{"retention", func(uint64) {
			require.NoError(t, engine.prune(&retained{cutoff: math.MaxInt64}, math.MinInt64))
		}},
	} {
		name, between := c.name, c.between
		t.Run(name, func(t *testing.T) {
			lset := promlabels.FromStrings("__name__", "new", "case", name)
			at := engine.MaxTime() + 60_000
			app := engine.Appender(ctx)
			ref, err := app.Append(0, lset, at, 1)
			require.NoError(t, err)
			between(uint64(ref))
			require.NoError(t, app.Commit())
			caseMatcher := promlabels.MustNewMatcher(promlabels.MatchEqual, "case", name)
			got := querySamples(t, engine, at, at, nil, matcher, caseMatcher)
			require.Len(t, got, 1)
			require.Equal(t, lset.String(), got[0].labels)
			require.Equal(t, []decodedSample{{t: at, f: math.Float64bits(1), typ: chunkenc.ValFloat}}, got[0].samples)
		})
	}
}
