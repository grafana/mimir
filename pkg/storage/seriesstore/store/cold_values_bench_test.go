// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A physical file can hold high-cardinality values from several logical blocks.
func BenchmarkColdLabelValues(b *testing.B) {
	const total = 100_000
	series := make([]frozenSeries, total)
	for n := range series {
		series[n] = frozen("tenant", [][2]string{{"__name__", "metric"}, {"group", fmt.Sprintf("group-%d", n%100)}, {"series", fmt.Sprintf("value-%06d", n)}}, [2]int64{10, 20}, [2]int64{100, 110})
	}
	block, err := buildColdBlock(b.TempDir(), 1, series)
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, block.close()) })
	table := block.tenant("tenant")
	all := make([]uint64, 0, total)
	var sparse []uint64
	for n := range table.seriesCount {
		hash := block.labelsHash(table, n)
		all = append(all, hash)
		stored := block.seriesIn(table, n)
		if value, _ := stored.lookup("group"); value == "group-0" {
			sparse = append(sparse, hash)
		}
	}
	slices.Sort(all)
	slices.Sort(sparse)
	for _, membership := range []struct {
		name       string
		hashes     []uint64
		start, end int64
		position   int
	}{{"all", all, 0, 200, 0}, {"sparse", sparse, 0, 200, 1}, {"gap", all, 40, 50, 2}} {
		window := labelWindow{blocks: []emulatedBlock{{minTime: membership.start, maxTime: membership.end, series: membership.hashes}}}
		for _, lookup := range []struct {
			name     string
			matchers []LabelMatcher
			limit    valueLimit
			want     [3]int
		}{
			{"unfiltered", nil, 0, [3]int{total, total / 100, 0}},
			{"limited", nil, 100, [3]int{100, 100, 0}},
			{"equal", []LabelMatcher{{Type: MatchEqual, Name: "series", Value: "value-001000"}}, 0, [3]int{1, 1, 0}},
			{"equal_excluded", []LabelMatcher{{Type: MatchEqual, Name: "series", Value: "value-001001"}}, 0, [3]int{1, 0, 0}},
			{"finite_regex", []LabelMatcher{{Type: MatchRegex, Name: "series", Value: "value-001000|value-002000"}}, 0, [3]int{2, 2, 0}},
			{"prefix_regex", []LabelMatcher{{Type: MatchRegex, Name: "series", Value: "value-001.*"}}, 0, [3]int{1000, 10, 0}},
		} {
			b.Run(membership.name+"/"+lookup.name, func(b *testing.B) {
				compiled, err := compileMatchers(lookup.matchers)
				require.NoError(b, err)
				check := map[string]struct{}{}
				window.coldLabelValues(coldView{block}, "tenant", "series", compiled, check, lookup.limit)
				require.Len(b, check, lookup.want[membership.position])
				b.ReportAllocs()
				for b.Loop() {
					values := map[string]struct{}{}
					window.coldLabelValues(coldView{block}, "tenant", "series", compiled, values, lookup.limit)
					benchmarkColdValueCount = len(values)
				}
			})
		}
	}
}

func BenchmarkColdLabelValuesColdIndex(b *testing.B) {
	const total = 4096
	series := make([]frozenSeries, total)
	for n := range series {
		series[n] = frozen("tenant", [][2]string{{"__name__", "metric"}, {"series", fmt.Sprintf("value-%06d", n)}}, [2]int64{10, 20})
	}
	block, err := buildColdBlock(b.TempDir(), 1, series)
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, block.close()) })
	table := block.tenant("tenant")
	var hashes []uint64
	for index := range table.seriesCount {
		hashes = append(hashes, block.labelsHash(table, index))
	}
	slices.Sort(hashes)
	window := labelWindow{blocks: []emulatedBlock{{minTime: 0, maxTime: 30, series: hashes}}}
	for _, lookup := range []struct {
		name    string
		matcher LabelMatcher
		want    int
	}{
		{"equal", LabelMatcher{Type: MatchEqual, Name: "series", Value: "value-001000"}, 1},
		{"finite_regex", LabelMatcher{Type: MatchRegex, Name: "series", Value: "value-001000|value-002000"}, 2},
	} {
		b.Run(lookup.name, func(b *testing.B) {
			compiled, err := compileMatchers([]LabelMatcher{lookup.matcher})
			require.NoError(b, err)
			check := map[string]struct{}{}
			window.coldLabelValues(coldView{block}, "tenant", "series", compiled, check, 0)
			require.Len(b, check, lookup.want)
			b.ReportAllocs()
			for b.Loop() {
				// A newly published block has no value index. Include the same reset in both versions.
				table.valueIndexesLock.Lock()
				table.valueIndexes = nil
				table.valueIndexesLock.Unlock()
				values := map[string]struct{}{}
				window.coldLabelValues(coldView{block}, "tenant", "series", compiled, values, 0)
				benchmarkColdValueCount = len(values)
			}
		})
	}
}

var benchmarkColdValueCount int

func BenchmarkColdLabelValuesFiniteSets(b *testing.B) {
	for _, cardinality := range []int{8, 1024} {
		series := make([]frozenSeries, 4*cardinality)
		for n := range series {
			series[n] = frozen("tenant", [][2]string{{"__name__", "metric"}, {"id", fmt.Sprint(n)}, {"value", fmt.Sprintf("value-%04d", n%cardinality)}}, [2]int64{10, 20})
		}
		block, err := buildColdBlock(b.TempDir(), 1, series)
		require.NoError(b, err)
		b.Cleanup(func() { require.NoError(b, block.close()) })
		table := block.tenant("tenant")
		hashes := make([]uint64, table.seriesCount)
		for n := range hashes {
			hashes[n] = block.labelsHash(table, n)
		}
		slices.Sort(hashes)
		window := labelWindow{blocks: []emulatedBlock{{minTime: 0, maxTime: 30, series: hashes}}}
		for _, lookup := range []struct {
			name   string
			width  int
			narrow bool
		}{
			{"equal", 0, false},
			{"set2", 2, false},
			{"set256", 256, false},
			{"set256_narrow", 256, true},
		} {
			b.Run(fmt.Sprintf("values%d/%s", cardinality, lookup.name), func(b *testing.B) {
				accepted := make([]string, lookup.width)
				for n := range accepted {
					accepted[n] = fmt.Sprintf("value-%04d", n)
				}
				matchers := []LabelMatcher{{Type: MatchRegex, Name: "value", Value: strings.Join(accepted, "|")}}
				want := min(cardinality, lookup.width)
				if lookup.width == 0 {
					matchers = []LabelMatcher{{Type: MatchEqual, Name: "value", Value: "value-0000"}}
					want = 1
				} else if lookup.narrow {
					matchers = append(matchers, LabelMatcher{Type: MatchEqual, Name: "id", Value: "17"})
					want = 1
				}
				compiled, err := compileMatchers(matchers)
				require.NoError(b, err)
				check := map[string]struct{}{}
				window.coldLabelValues(coldView{block}, "tenant", "value", compiled, check, 0)
				require.Len(b, check, want)
				b.ReportAllocs()
				for b.Loop() {
					values := map[string]struct{}{}
					window.coldLabelValues(coldView{block}, "tenant", "value", compiled, values, 0)
					benchmarkColdValueCount = len(values)
				}
			})
		}
	}
}
