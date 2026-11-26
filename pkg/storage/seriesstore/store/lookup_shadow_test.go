// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/prometheus/common/model"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// shadowEngine is an engine of targets' series with resources, and as many other series, with instances the pattern
// it returns doesn't have, so that every one of its shards holds values of both.
func shadowEngine(t *testing.T) (*Engine, string) {
	var series []promlabels.Labels
	var instances []string
	for n := range 300 {
		instance := fmt.Sprintf("10.0.%d.%d:9100", n/256, n%256)
		instances = append(instances, regexp.QuoteMeta(instance))
		series = append(series, promlabels.FromStrings(model.MetricNameLabel, "up", "instance", instance))
		series = append(series, promlabels.FromStrings(model.MetricNameLabel, "up", "instance", fmt.Sprintf("outside-%d", n)))
	}
	e := resourcesEngine(t, 1_000, series...)
	resourceRoutes["appender"](t, e, 2_000, testResourceAttributes("frontend", "node-1"), series...)
	return e, "(" + strings.Join(instances, "|") + ")"
}

func checkEveryLookup(t *testing.T) {
	shadowCheckAll = true
	t.Cleanup(func() { shadowCheckAll, shadowHook = false, nil })
}

// The shadow check counts the values a set answers differently from its regex, by direction: none for a correct set,
// and both kinds for one that leaves the pattern's values out and takes others in.
func TestEngine_ShadowCheckSets(t *testing.T) {
	emptyRegexCache(t)
	checkEveryLookup(t)
	e, pattern := shadowEngine(t)
	matchers := [][]*promlabels.Matcher{{promlabels.MustNewMatcher(promlabels.MatchRegexp, "instance", pattern)}}
	lookup := func() {
		_, _, _, err := e.NativeResourcesCapped(context.Background(), matchers, math.MinInt64, math.MaxInt64, 0, 0, NativeResourcesOptions{SetMatching: true})
		require.NoError(t, err)
	}

	checkedBefore, acceptsBefore, rejectsBefore := SetShadowStats()
	lookup()
	checked, accepts, rejects := SetShadowStats()
	require.Greater(t, checked, checkedBefore)
	require.Equal(t, acceptsBefore, accepts, "a correct set")
	require.Equal(t, rejectsBefore, rejects, "a correct set")

	wrong, err := lookupRegex(pattern)
	require.NoError(t, err)
	require.NotNil(t, wrong.set)
	clear(wrong.set)
	for n := range 300 {
		wrong.set[fmt.Sprintf("outside-%d", n)] = struct{}{}
	}
	lookup()
	_, wrongAccepts, wrongRejects := SetShadowStats()
	require.Greater(t, wrongAccepts, accepts, "the values it took in")
	require.Greater(t, wrongRejects, rejects, "the pattern's values it left out")
}

// The check compares a shard's dictionary after releasing the shard's lock, so the slice it took must stay as it was
// while pushes add values and other readers get the dictionary anew: under -race, a later change of the dictionary in
// place fails here. Pushes alone wouldn't touch the dictionary, which only a reader starts again.
func TestEngine_ShadowCheckSetsWhileTheDictionaryChanges(t *testing.T) {
	emptyRegexCache(t)
	checkEveryLookup(t)
	e, pattern := shadowEngine(t)
	matchers := [][]*promlabels.Matcher{{promlabels.MustNewMatcher(promlabels.MatchRegexp, "instance", pattern)}}
	lookup := func() {
		_, _, _, err := e.NativeResourcesCapped(context.Background(), matchers, math.MinInt64, math.MaxInt64, 0, 0, NativeResourcesOptions{SetMatching: true})
		require.NoError(t, err)
	}
	var hooked atomic.Bool
	shadowHook = func() {
		if !hooked.CompareAndSwap(false, true) {
			return
		}
		var added []promlabels.Labels
		for n := range 100 {
			added = append(added, promlabels.FromStrings(model.MetricNameLabel, "up", "instance", fmt.Sprintf("added-%d", n)))
		}
		resourceRoutes["appender"](t, e, 3_000, testResourceAttributes("frontend", "node-1"), added...)
		// Another reader, which gets every shard's dictionary anew.
		id, known := labels.Lookup("instance")
		require.True(t, known)
		for index, shard := range e.store.shards {
			if tenant := e.tenant(index); tenant != nil {
				shard.RLock()
				tenant.series.labelDictionary(id)
				shard.RUnlock()
			}
		}
	}
	_, acceptsBefore, rejectsBefore := SetShadowStats()
	lookup()
	require.True(t, hooked.Load())
	_, accepts, rejects := SetShadowStats()
	require.Equal(t, acceptsBefore, accepts)
	require.Equal(t, rejectsBefore, rejects)
}
