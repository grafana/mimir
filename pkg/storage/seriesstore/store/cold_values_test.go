// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColdLabelValuesRespectTenantTimeAndBlockMembership(t *testing.T) {
	block, err := buildColdBlock("", 1, []frozenSeries{
		frozen("a", [][2]string{{"__name__", "up"}, {"id", "alpha_old"}, {"pod", "alpha"}}, [2]int64{1, 2}),
		frozen("a", [][2]string{{"__name__", "up"}, {"id", "alpha_new"}, {"pod", "alpha"}}, [2]int64{50, 60}),
		frozen("a", [][2]string{{"__name__", "up"}, {"id", "beta"}, {"pod", "beta"}}, [2]int64{1, 2}),
		frozen("a", [][2]string{{"__name__", "up"}, {"id", "gamma"}, {"pod", "gamma"}}, [2]int64{1, 2}),
		frozen("a", [][2]string{{"__name__", "up"}, {"id", "empty"}, {"pod", ""}}, [2]int64{1, 2}),
		frozen("a", [][2]string{{"__name__", "up"}, {"id", "absent"}}, [2]int64{1, 2}),
		frozen("b", [][2]string{{"__name__", "up"}, {"pod", "other_tenant"}}, [2]int64{1, 2}),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, block.close()) })
	table := block.tenant("a")
	var all, selected, oldAlpha []uint64
	for index := range table.seriesCount {
		hash := block.labelsHash(table, index)
		all = append(all, hash)
		series := block.seriesIn(table, index)
		id, _ := series.lookup("id")
		if id == "alpha_old" || id == "beta" {
			selected = append(selected, hash)
		}
		if id == "alpha_old" {
			oldAlpha = append(oldAlpha, hash)
		}
	}
	slices.Sort(all)
	slices.Sort(selected)
	for _, window := range []struct {
		name       string
		start, end int64
		hashes     []uint64
		want       []string
		rangeOnly  bool
	}{
		{"all", 0, 100, all, []string{"", "alpha", "beta", "gamma"}, false},
		{"old", 0, 3, all, []string{"", "alpha", "beta", "gamma"}, false},
		{"selected", 0, 100, selected, []string{"alpha", "beta"}, false},
		{"selected_without_data", 50, 60, oldAlpha, nil, false},
		{"gap", 10, 20, all, nil, false},
		{"no_members", 0, 100, nil, nil, false},
		{"range_all", 0, 100, nil, []string{"", "alpha", "beta", "gamma"}, true},
		{"range_old", 0, 3, nil, []string{"", "alpha", "beta", "gamma"}, true},
		{"range_new", 50, 60, nil, []string{"alpha"}, true},
		{"range_gap", 10, 20, nil, nil, true},
	} {
		for _, matcher := range []LabelMatcher{
			{Type: MatchEqual, Name: "pod", Value: "alpha"},
			{Type: MatchEqual, Name: "pod", Value: "beta"},
			{Type: MatchEqual, Name: "pod", Value: "missing"},
			{Type: MatchEqual, Name: "pod", Value: ""},
			{Type: MatchRegex, Name: "pod", Value: "alpha|beta"},
			{Type: MatchRegex, Name: "pod", Value: "(|alpha)"},
			{Type: MatchRegex, Name: "pod", Value: "^$"},
			{Type: MatchRegex, Name: "pod", Value: "a.*"},
			{Type: MatchNotEqual, Name: "pod", Value: "beta"},
		} {
			for _, limit := range []valueLimit{0, 1, 2} {
				t.Run(fmt.Sprintf("%s/%d=%q/limit%d", window.name, matcher.Type, matcher.Value, limit), func(t *testing.T) {
					compiled, err := compileMatchers([]LabelMatcher{matcher})
					require.NoError(t, err)
					var want []string
					for _, value := range window.want {
						if compiled[0].matchesValue(value) {
							want = append(want, value)
						}
					}
					values := map[string]struct{}{}
					w := labelWindow{blocks: []emulatedBlock{{minTime: window.start, maxTime: window.end, series: window.hashes}}}
					if window.rangeOnly {
						w.blocks[0].series = all
					}
					w.coldLabelValues(coldView{block}, "a", "pod", compiled, values, limit)
					count := len(want)
					if limit > 0 {
						count = min(count, int(limit))
					}
					require.Len(t, values, count)
					for value := range values {
						require.Contains(t, want, value)
					}
				})
			}
		}
	}
}

func TestColdLabelValuesVerifyPostingHashCollisions(t *testing.T) {
	series := []frozenSeries{
		frozen("tenant", [][2]string{{"__name__", "up"}, {"pod", "other"}}, [2]int64{1, 2}),
		frozen("tenant", [][2]string{{"__name__", "up"}, {"pod", "wanted"}}, [2]int64{1, 2}),
	}
	// A selective finite set takes direct postings instead of the small-dictionary fallback.
	for n := range 16 {
		series = append(series, frozen("tenant", [][2]string{{"__name__", "up"}, {"pod", fmt.Sprintf("filler-%d", n)}}, [2]int64{1, 2}))
	}
	block, err := buildColdBlock("", 1, series)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, block.close()) })
	table := block.tenant("tenant")
	offset, count := block.postingRef(table, "__name__", "up")
	require.Equal(t, len(series), count)
	wantedOffset, count := block.postingRef(table, "pod", "wanted")
	require.Equal(t, 1, count)
	low, high := block.nameRange(table, block.localIDs["pod"])
	for index := low; index < high; index++ {
		_, _, entryOffset := block.entry(table, index)
		if entryOffset == wantedOffset {
			// A hash collision puts rejected values before the requested value in one bucket.
			binary.LittleEndian.PutUint32(block.data[table.postingsTable+index*16+12:], offset)
		}
	}
	var all []uint64
	for index := range table.seriesCount {
		all = append(all, block.labelsHash(table, index))
	}
	slices.Sort(all)
	w := labelWindow{blocks: []emulatedBlock{{minTime: 0, maxTime: 10, series: all}}}
	for _, matcher := range []LabelMatcher{
		{Type: MatchEqual, Name: "pod", Value: "wanted"},
		{Type: MatchRegex, Name: "pod", Value: "wanted|missing"},
	} {
		t.Run(matcher.Value, func(t *testing.T) {
			compiled, err := compileMatchers([]LabelMatcher{matcher})
			require.NoError(t, err)
			values := map[string]struct{}{}
			w.coldLabelValues(coldView{block}, "tenant", "pod", compiled, values, 0)
			require.Equal(t, []string{"wanted"}, sortedKeys(values))
		})
	}
}
