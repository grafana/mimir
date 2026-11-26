// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/common/model"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

// Which patterns a native resources lookup matches against a set: large alternations of literals, such as info()'s,
// whatever Go's parser factors them into, and nothing the set could answer differently from the regex or that
// enumerates past what the pattern spells out. A pattern that doesn't make one is the regex mode's, marked so that the
// next lookups don't parse it again.
func TestLookupRegex_Sets(t *testing.T) {
	targets := "(" + targetsAlternation("", 300) + ")"
	for name, tc := range map[string]struct {
		pattern string
		// The set's values, or 0 for none, and what they all start with.
		values int
		prefix string
	}{
		"info()'s alternation of every target": {pattern: targets, values: 300, prefix: "10.0."},
		// The empty value has no prefix.
		"with the empty alternative of a mixed label":  {pattern: "(" + targetsAlternation("", 300) + "|)", values: 301},
		"of up to maxAcceptedValues values, as before": {pattern: "(" + targetsAlternation("", maxAcceptedValues) + ")"},
		// Each literal would enumerate into every case of its letters.
		"case-insensitive": {pattern: "(?i)" + targets},
		// Go's regexp reads an invalid byte as U+FFFD, which a set of the literals wouldn't.
		"with U+FFFD":            {pattern: "(" + targetsAlternation("", 300) + `|\x{fffd})`},
		"with a class over it":   {pattern: "(" + targetsAlternation("", 300) + `|[\x{fff0}-\x{ffff}])`},
		"more values than spelt": {pattern: `10\.0\.[0-9]{3}:9100`},
		"not finite":             {pattern: "(" + targetsAlternation("", 300) + "|.*)"},
	} {
		t.Run(name, func(t *testing.T) {
			emptyRegexCache(t)
			re, err := lookupRegex(tc.pattern)
			require.NoError(t, err)
			if tc.values == 0 {
				require.Nil(t, re.set)
				regex, err := anchoredRegex(tc.pattern)
				require.NoError(t, err)
				require.Same(t, regex, re, "the regex mode's own")
				require.True(t, re.notSet.Load())
				return
			}
			require.Len(t, re.set, tc.values)
			require.Nil(t, re.matcher)
			require.Equal(t, tc.prefix, re.prefix)
		})
	}
}

// A long literal times many alternatives multiplies into far more bytes than the pattern has: the enumeration refuses
// before it allocates them, since every native resources lookup with the switch on checks its patterns.
func TestSetValues_RefusesPastItsBytesBeforeAllocating(t *testing.T) {
	alternatives := make([]string, 2_000)
	for n := range alternatives {
		alternatives[n] = fmt.Sprint(n)
	}
	// About 30 KB, whose 2,000 values would be 40 MB.
	pattern := "(" + strings.Repeat("a", 20_000) + ")(" + strings.Join(alternatives, "|") + ")"
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, ok := setValues(pattern)
	runtime.ReadMemStats(&after)
	require.False(t, ok)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(8<<20))
}

// A native resources lookup's set is cached under a key of its own: a select or a label lookup with the same pattern
// gets the regex, also after a lookup cached the set, and a lookup with the switch off does too. Metric name matchers
// keep the regex in a lookup, since the head keeps their verdicts on metric names by pattern for every reader. A set
// that answers wrongly on purpose shows which one each reader used: a correct one answers as the regex does.
func TestLookupRegex_KeyedByMode(t *testing.T) {
	emptyRegexCache(t)
	var series []promlabels.Labels
	var instances []string
	for n := range 300 {
		instance := fmt.Sprintf("10.0.%d.%d:9100", n/256, n%256)
		instances = append(instances, regexp.QuoteMeta(instance))
		series = append(series, promlabels.FromStrings(model.MetricNameLabel, "up", "instance", instance, "job", "node"))
	}
	// At most 64 KiB, so that both are cached, and past maxAcceptedValues.
	pattern := "(" + strings.Join(instances, "|") + ")"
	require.LessOrEqual(t, len(pattern), maxSizedRegexPattern)
	e := resourcesEngine(t, 1_000, series...)
	resourceRoutes["appender"](t, e, 2_000, testResourceAttributes("frontend", "node-1"), series...)

	// The lookup first, on the emptied cache, so that a reader keyed by pattern alone would find the set.
	set, err := lookupRegex(pattern)
	require.NoError(t, err)
	require.NotNil(t, set.set)
	// The set leaves the first target out.
	delete(set.set, "10.0.0.0:9100")

	matchers := [][]*promlabels.Matcher{{promlabels.MustNewMatcher(promlabels.MatchRegexp, "instance", pattern)}}
	lookup := func(setMatching bool) int {
		found, _, _, err := e.NativeResourcesCapped(context.Background(), matchers, math.MinInt64, math.MaxInt64, 0, 0, NativeResourcesOptions{SetMatching: setMatching})
		require.NoError(t, err)
		return len(found)
	}
	require.Equal(t, 299, lookup(true), "the lookup matches against the set")
	require.Equal(t, 300, lookup(false), "with the switch off, against the regex")

	q, err := e.Querier(math.MinInt64, math.MaxInt64)
	require.NoError(t, err)
	defer q.Close()
	selected := q.Select(context.Background(), false, nil, matchers[0]...)
	count := 0
	for selected.Next() {
		count++
	}
	require.NoError(t, selected.Err())
	require.Equal(t, 300, count, "a select matches against the regex")
	values, _, err := q.LabelValues(context.Background(), "instance", nil, matchers[0]...)
	require.NoError(t, err)
	require.Len(t, values, 300, "a label lookup matches against the regex")

	names := make([]string, 0, 300)
	for n := range 300 {
		names = append(names, fmt.Sprintf("metric_%d", n))
	}
	byName, err := compilePromMatchersWith(nil, []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchRegexp, model.MetricNameLabel, strings.Join(names, "|"))}, true)
	require.NoError(t, err)
	require.Nil(t, byName[0].re.set, "a metric name matcher keeps the regex")
}

// The set accepts exactly what the regex does, for patterns of info()'s shape: the identifying label's values quoted,
// in any order, in a capture, with or without the empty alternative, over values with the regex's special characters
// and runes past ASCII. Values are checked as they are, changed by a rune, empty, and with an invalid byte, which Go's
// regexp reads as U+FFFD.
func FuzzLookupRegex_MatchesAsTheRegex(f *testing.F) {
	f.Add(int64(1), uint16(300), false)
	f.Add(int64(2), uint16(257), true)
	f.Add(int64(3), uint16(1_000), true)
	f.Fuzz(func(t *testing.T, seed int64, count uint16, withEmpty bool) {
		r := rand.New(rand.NewSource(seed))
		// A small alphabet, so that values share prefixes and the parser factors them.
		alphabet := []rune("ab01.:-_/*+?()[]{}|^$\\é")
		value := func() string {
			runes := make([]rune, 1+r.Intn(6))
			for i := range runes {
				runes[i] = alphabet[r.Intn(len(alphabet))]
			}
			return string(runes)
		}
		values := make([]string, 0, int(count)+1)
		for range max(int(count), maxAcceptedValues+1) {
			values = append(values, value())
		}
		quoted := make([]string, 0, len(values)+1)
		for _, v := range values {
			quoted = append(quoted, regexp.QuoteMeta(v))
		}
		r.Shuffle(len(quoted), func(i, j int) { quoted[i], quoted[j] = quoted[j], quoted[i] })
		if withEmpty {
			quoted = append(quoted, "")
		}
		pattern := "(" + strings.Join(quoted, "|") + ")"

		emptyRegexCache(t)
		re, err := lookupRegex(pattern)
		require.NoError(t, err)
		if re.set == nil {
			// Duplicates can leave too few values for a set, which the regex mode then answers.
			return
		}
		prometheus, err := promlabels.NewFastRegexMatcher(pattern)
		require.NoError(t, err)
		candidates := []string{"", "\xff", value(), value() + "\xff"}
		for _, v := range values {
			candidates = append(candidates, v, v+"a", v[:len(v)-1], "\xff"+v)
		}
		for _, candidate := range slices.Compact(candidates) {
			require.Equal(t, prometheus.MatchString(candidate), re.isMatch(candidate), "value %q of pattern %q", candidate, pattern)
		}
	})
}

// A lookup by target leaves the series unsorted, since it sorts the targets itself: the same series, in shard order.
// A positive limit keeps the first series in label order, so the series are sorted anyway.
func TestNativeResourcesCapped_Unsorted(t *testing.T) {
	var series []promlabels.Labels
	for n := range 20 {
		series = append(series, promlabels.FromStrings(model.MetricNameLabel, "up", "instance", fmt.Sprint(n)))
	}
	e := resourcesEngine(t, 1_000, series...)
	resourceRoutes["appender"](t, e, 2_000, testResourceAttributes("frontend", "node-1"), series...)
	all := [][]*promlabels.Matcher{{promlabels.MustNewMatcher(promlabels.MatchEqual, model.MetricNameLabel, "up")}}
	lookup := func(limit int, opts NativeResourcesOptions) []string {
		found, _, _, err := e.NativeResourcesCapped(context.Background(), all, math.MinInt64, math.MaxInt64, limit, 0, opts)
		require.NoError(t, err)
		var out []string
		for _, s := range found {
			out = append(out, s.Labels.String())
		}
		return out
	}
	sorted := lookup(0, NativeResourcesOptions{})
	require.True(t, slices.IsSorted(sorted))

	_, before, _ := NativeResourcesPaths()
	unsorted := lookup(0, NativeResourcesOptions{Unsorted: true})
	require.ElementsMatch(t, sorted, unsorted)
	_, after, _ := NativeResourcesPaths()
	require.Equal(t, before+1, after)

	require.Equal(t, sorted[:5], lookup(5, NativeResourcesOptions{Unsorted: true}))
	_, limited, _ := NativeResourcesPaths()
	require.Equal(t, after, limited, "a positive limit sorts")
}

// The memo answers as the per-series path does, over what a lookup meets: series sharing an identity, descriptions
// changing past the limit, which coalesces them and changes the latest version in place, identity changes, past their
// limit too, and a series without a resource. The memo is per lookup and keyed by the encoded
// values, not their ids, so an id that a trim reclaims and another value takes, between shards or between lookups,
// finds no entry of the value it had.
func TestNativeResourcesCapped_Memo(t *testing.T) {
	var series []promlabels.Labels
	for n := range 12 {
		series = append(series, promlabels.FromStrings(model.MetricNameLabel, "up", "instance", fmt.Sprint(n%4), "pod", fmt.Sprint(n)))
	}
	e := resourcesEngine(t, 1_000, series...)
	e.SetResourceLimits(ResourceLimits{WindowMs: 3_600_000, MaxIdentityChangesPerSeries: 1, MaxVersionsPerIdentity: 2})
	for step := range 6 {
		for n, l := range series[:11] {
			service := "frontend"
			if step >= 3 && n%3 == 0 {
				service = fmt.Sprintf("other-%d", step)
			}
			resourceRoutes["appender"](t, e, int64(2_000+step*1_000), testResourceAttributes(service, fmt.Sprintf("node-%d", step)), l)
		}
	}
	all := [][]*promlabels.Matcher{{promlabels.MustNewMatcher(promlabels.MatchEqual, model.MetricNameLabel, "up")}}
	lookup := func(mint, maxt int64, memo bool) []NativeResourcesSeries {
		found, _, _, err := e.NativeResourcesCapped(context.Background(), all, mint, maxt, 0, 0, NativeResourcesOptions{Memo: memo})
		require.NoError(t, err)
		return found
	}
	coalesced, identityChanged := false, false
	for _, s := range lookup(math.MinInt64, math.MaxInt64, false) {
		for i, v := range s.Versions {
			coalesced = coalesced || v.Coalesced
			identityChanged = identityChanged || (i > 0 && !reflect.DeepEqual(v.Identifying, s.Versions[i-1].Identifying))
		}
	}
	require.True(t, coalesced, "the data has coalesced versions")
	require.True(t, identityChanged, "the data has identity changes")
	for _, r := range [][2]int64{{math.MinInt64, math.MaxInt64}, {3_000, 4_500}, {6_000, 7_000}} {
		require.Equal(t, lookup(r[0], r[1], false), lookup(r[0], r[1], true), "[%d, %d]", r[0], r[1])
	}
}
