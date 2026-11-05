// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

func TestRegexesAcceptExactlyTheValuesTheyExpandTo(t *testing.T) {
	for _, c := range []struct {
		pattern  string
		expected []string
		finite   bool
	}{
		{"a|b", []string{"a", "b"}, true},
		{"job-(0|4)", []string{"job-0", "job-4"}, true},
		{"x[0-2]", []string{"x0", "x1", "x2"}, true},
		{"(?i)ab", []string{"AB", "Ab", "aB", "ab"}, true},
		{"|a", []string{"", "a"}, true},
		{"a?b{2}", []string{"abb", "bb"}, true},
		{"", []string{""}, true},
		{"é|ü", []string{"é", "ü"}, true},
		{".*", nil, false},
		{"a+", nil, false},
		{"^a", nil, false},
		{`a\b`, nil, false},
		{"[a-z]{3}", nil, false},
		{"a)|(b", nil, false},
		{`(?-u:\xff)`, nil, false},
	} {
		values, finite := acceptedValues(c.pattern)
		require.Equal(t, c.finite, finite, c.pattern)
		if c.finite {
			require.Equal(t, c.expected, values, c.pattern)
		}
	}
	// Whatever the value, looking it up agrees with the regex.
	for _, pattern := range []string{
		"a|b", "job-(0|4)|job-1[0-9]", "(?i)abc|d", "|a|aa", "a?b?c?", "(a|b)(c|d)(e|f)", "x{1,3}",
		"api_(requests|errors)_total", "[0-9]", `\.|\*`,
	} {
		re, err := anchoredRegex(pattern)
		require.NoError(t, err)
		values, finite := re.acceptedValues()
		require.True(t, finite, pattern)
		probes := []string{"", "a", "ab", "job-"}
		for _, value := range values {
			runes := []rune(value)
			reversed := slices.Clone(runes)
			slices.Reverse(reversed)
			skipped := ""
			if len(runes) > 0 {
				skipped = string(runes[1:])
			}
			probes = append(probes, value+"x", "x"+value, strings.ToUpper(value), skipped, string(reversed), value)
		}
		for _, probe := range probes {
			require.Equal(t, re.matcher.MatchString(probe), re.isMatch(probe), "%s on %q", pattern, probe)
		}
	}
}

func TestAlternationsLookTheirValuesUp(t *testing.T) {
	e, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 1})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })
	app := e.Appender(t.Context())
	for _, name := range []string{"api_requests", "api_errors", "db_queries"} {
		for n := range 50 {
			_, err := app.Append(0, promlabels.FromStrings("__name__", name, "job", fmt.Sprintf("job-%d", n%10), "n", strconv.Itoa(n)), 0, 1000, 1, nil, nil, storage.AppendV2Options{})
			require.NoError(t, err)
		}
	}
	require.NoError(t, app.Commit())
	count := func(name, pattern string) int {
		q, err := e.ChunkQuerier(0, 2000)
		require.NoError(t, err)
		defer func() { require.NoError(t, q.Close()) }()
		selected := q.Select(t.Context(), false, nil, promlabels.MustNewMatcher(promlabels.MatchRegexp, name, pattern))
		n := 0
		for selected.Next() {
			n++
		}
		require.NoError(t, selected.Err())
		return n
	}
	counters := func() [2]uint64 { return [2]uint64{nameMatchCount.Load(), labelValueReads.Load()} }
	before := counters()
	require.Equal(t, 100, count("__name__", "api_errors|db_queries|missing"))
	require.Equal(t, before, counters(), "no metric name or label checked")
	before = counters()
	require.Equal(t, 30, count("job", "job-1|job-2"))
	after := counters()
	require.Equal(t, before[0], after[0])
	require.Equal(t, uint64(30), after[1]-before[1], "only the matching candidates are checked")
}

// Sorting a query's series compares their encodings from the first pair that differs.
func TestCompareLabelsOrdersLikePrometheus(t *testing.T) {
	long := strings.Repeat("v", 130)
	var all []labels.Labels
	for _, metric := range []string{"a", "b"} {
		for _, job := range []string{"", "job", long, long + "w"} {
			for _, pod := range []string{"p1", "p10", "p2", long} {
				pairs := []string{"__name__", metric}
				if job != "" {
					pairs = append(pairs, "job", job)
				}
				all = append(all, labelsFromStrings(append(pairs, "pod", pod)...), labelsFromStrings(pairs...))
			}
		}
	}
	names := labels.TakeSnapshot()
	for _, a := range all {
		for _, b := range all {
			want := promlabels.Compare(promlabels.FromStrings(flattenPairs(a.Pairs())...), promlabels.FromStrings(flattenPairs(b.Pairs())...))
			got := compareLabels(names, a, b)
			require.Equal(t, want < 0, got < 0, "%v vs %v", a, b)
			require.Equal(t, want == 0, got == 0, "%v vs %v", a, b)
		}
	}
}

func flattenPairs(pairs [][2]string) []string {
	var out []string
	for _, pair := range pairs {
		out = append(out, pair[0], pair[1])
	}
	return out
}

func BenchmarkCompareLabelsOfOneMetric(b *testing.B) {
	series := make([]labels.Labels, 10_000)
	for n := range series {
		series[n] = labelsFromStrings("__name__", "http_requests_total", "cluster", "prod-us-east-0", "namespace", "mimir", "pod", fmt.Sprintf("pod-%d", n), "status", fmt.Sprint(200+n%5))
	}
	names := labels.TakeSnapshot()
	work := make([]labels.Labels, len(series))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(work, series)
		slices.SortFunc(work, func(x, y labels.Labels) int { return compareLabels(names, x, y) })
	}
}

// sortByLabels orders series as comparing their labels does, with labels that are prefixes of others', shared values
// and zero bytes.
func TestSortByLabelsOrdersAsCompareLabels(t *testing.T) {
	random := rand.New(rand.NewPCG(1, 2))
	values := []string{"", "a", "ab", "b", "a\x00", "a\x00b", "z", "\xff"}
	// Names whose ids are in the opposite order of the names.
	labels.FromSorted([][2]string{{"sort_zz", "x"}})
	labels.FromSorted([][2]string{{"sort_aa", "x"}})
	names := []string{"__name__", "a", "ab", "b", "job", "pod", "sort_aa", "sort_zz"}
	for _, zeros := range []bool{false, true} {
		var selected []rawSeries
		for range 500 {
			var pairs [][2]string
			for _, name := range names {
				if random.IntN(3) == 0 {
					continue
				}
				value := values[random.IntN(len(values))]
				if !zeros {
					value = strings.ReplaceAll(value, "\x00", "")
				}
				pairs = append(pairs, [2]string{name, value})
			}
			selected = append(selected, rawSeries{stored: labels.FromSorted(pairs)})
		}
		expected := slices.Clone(selected)
		snapshot := labels.TakeSnapshot()
		slices.SortStableFunc(expected, func(x, y rawSeries) int { return compareLabels(snapshot, x.stored, y.stored) })
		sortByLabels(selected)
		for index := range selected {
			require.Zero(t, compareLabels(snapshot, expected[index].stored, selected[index].stored), "zeros=%v at %d", zeros, index)
		}
	}
}

func TestSortKeysOrdersAsStrings(t *testing.T) {
	random := rand.New(rand.NewPCG(3, 4))
	for _, size := range []int{0, 1, 2, 15, 16, 17, 100, 5000} {
		var buffer []byte
		keys := make([]sortKey, size)
		for i := range keys {
			// Keys of a few letters with long shared prefixes, some prefixes of others, some equal.
			prefix := "__name__\x00metric\x00label\x00"
			key := prefix[:random.IntN(len(prefix)+1)]
			for range random.IntN(6) {
				key += string(rune('a' + random.IntN(3)))
			}
			start := len(buffer)
			buffer = append(buffer, key...)
			keys[i] = sortKey{start: uint32(start), end: uint32(len(buffer)), index: uint32(i)}
		}
		expected := slices.Clone(keys)
		slices.SortStableFunc(expected, func(x, y sortKey) int { return strings.Compare(x.key(string(buffer)), y.key(string(buffer))) })
		sortKeys(string(buffer), keys, 0)
		for i := range keys {
			require.Equal(t, expected[i].key(string(buffer)), keys[i].key(string(buffer)), "%d keys, at %d", size, i)
		}
	}
}

func BenchmarkSortByLabels(b *testing.B) {
	random := rand.New(rand.NewPCG(1, 2))
	for _, size := range []int{20, 200, 2000, 20000} {
		var selected []rawSeries
		// The labels apart from each other, as a store's series are: reading them in a query is mostly cache misses.
		var apart [][]byte
		for n := range size {
			apart = append(apart, make([]byte, 1024))
			pairs := [][2]string{{"__name__", "metric"}}
			for l := range 10 {
				pairs = append(pairs, [2]string{fmt.Sprintf("label_%02d", l), fmt.Sprintf("value-%d-%d", l, n%(10<<l))})
			}
			pairs = append(pairs, [2]string{"pod", fmt.Sprintf("pod-%05d", n)})
			selected = append(selected, rawSeries{stored: labels.FromSorted(pairs)})
		}
		random.Shuffle(len(selected), func(i, j int) { selected[i], selected[j] = selected[j], selected[i] })
		shuffled := slices.Clone(selected)
		runtime.KeepAlive(apart)
		for _, mode := range []string{"compare", "keys"} {
			b.Run(fmt.Sprintf("%d/%s", size, mode), func(b *testing.B) {
				snapshot := labels.TakeSnapshot()
				b.ReportAllocs()
				for range b.N {
					copy(selected, shuffled)
					if mode == "keys" {
						sortByLabels(selected)
					} else {
						slices.SortFunc(selected, func(x, y rawSeries) int { return compareLabels(snapshot, x.stored, y.stored) })
					}
				}
			})
		}
	}
}
