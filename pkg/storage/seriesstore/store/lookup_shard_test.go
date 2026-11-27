// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"math"
	"regexp"
	"strings"
	"testing"

	"github.com/prometheus/common/model"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/sharding"
)

// A lookup's query shard keeps the series a select with the same shard keeps, so that each shard of a sharded query
// looks up the resources of the series it selects.
func TestNativeResourcesCapped_QueryShard(t *testing.T) {
	const shards = 3
	var series []promlabels.Labels
	var instances []string
	for n := range 300 {
		instance := fmt.Sprintf("10.0.%d.%d:9100", n/256, n%256)
		instances = append(instances, regexp.QuoteMeta(instance))
		series = append(series, promlabels.FromStrings(model.MetricNameLabel, "up", "instance", instance, "job", "node"))
	}
	e := resourcesEngine(t, 1_000, series...)
	resourceRoutes["appender"](t, e, 2_000, testResourceAttributes("node-exporter", "node-1"), series...)

	lookup := func(opts NativeResourcesOptions, matchers ...*promlabels.Matcher) []string {
		found, _, _, err := e.NativeResourcesCapped(t.Context(), [][]*promlabels.Matcher{matchers}, math.MinInt64, math.MaxInt64, 0, 0, opts)
		require.NoError(t, err)
		out := make([]string, 0, len(found))
		for _, s := range found {
			out = append(out, s.Labels.String())
		}
		return out
	}
	selected := func(shard uint64, matchers ...*promlabels.Matcher) []string {
		q, err := e.ChunkQuerier(math.MinInt64, math.MaxInt64)
		require.NoError(t, err)
		defer q.Close()
		set := q.Select(t.Context(), true, &storage.SelectHints{Start: math.MinInt64, End: math.MaxInt64, ShardIndex: shard, ShardCount: shards}, matchers...)
		var out []string
		for set.Next() {
			out = append(out, set.At().Labels().String())
		}
		require.NoError(t, set.Err())
		return out
	}

	for name, tc := range map[string]struct {
		matcher *promlabels.Matcher
		opts    NativeResourcesOptions
	}{
		"an equality": {matcher: promlabels.MustNewMatcher(promlabels.MatchEqual, "job", "node")},
		// The shard's matcher comes first, before an alternation the lookup matches as a set, and the shadow check
		// compares the set with its own matcher: paired with the shard's instead, it would count disagreements.
		"a set, with the shadow check": {
			matcher: promlabels.MustNewMatcher(promlabels.MatchRegexp, "instance", "("+strings.Join(instances, "|")+")"),
			opts:    NativeResourcesOptions{SetMatching: true},
		},
	} {
		t.Run(name, func(t *testing.T) {
			checkEveryLookup(t)
			require.Len(t, lookup(tc.opts, tc.matcher), len(series), "unsharded, the lookup finds every series")
			_, acceptsBefore, rejectsBefore := SetShadowStats()
			for shard := range uint64(shards) {
				shardMatcher := sharding.ShardSelector{ShardIndex: shard, ShardCount: shards}.Matcher()
				want := selected(shard, tc.matcher)
				require.NotEmpty(t, want, "every shard selects some series")
				require.Less(t, len(want), len(series), "no shard selects every series")
				require.Equal(t, want, lookup(tc.opts, shardMatcher, tc.matcher))
			}
			_, accepts, rejects := SetShadowStats()
			require.Equal(t, acceptsBefore, accepts)
			require.Equal(t, rejectsBefore, rejects)
		})
	}
}
