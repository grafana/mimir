// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"math"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLexDistance(t *testing.T) {
	t.Run("all-a maps to 0", func(t *testing.T) {
		assert.Equal(t, 0.0, lexDistance("aaaaaa"))
	})

	t.Run("all-z maps to near 1", func(t *testing.T) {
		// 25 * (1/26 + 1/26^2 + ... + 1/26^6) = 1 - 26^-6
		got := lexDistance("zzzzzz")
		assert.InDelta(t, 1-math.Pow(26, -6), got, 1e-12)
		assert.Less(t, got, 1.0)
	})

	t.Run("empty string maps to 0", func(t *testing.T) {
		assert.Equal(t, 0.0, lexDistance(""))
	})

	t.Run("case fold", func(t *testing.T) {
		assert.Equal(t, lexDistance("ABC"), lexDistance("abc"))
	})

	t.Run("ordering preserved alphabetically", func(t *testing.T) {
		ord := []string{"aaaa", "cccc", "cpu_limit", "cpu_usage", "memory", "yyy", "zzzz"}
		for i := 1; i < len(ord); i++ {
			assert.LessOrEqual(t, lexDistance(ord[i-1]), lexDistance(ord[i]),
				"expected %q <= %q in lex space", ord[i-1], ord[i])
		}
	})

	t.Run("shared prefix gives near-zero distance", func(t *testing.T) {
		// cpu_usage and cpu_limit share "cpu_" so they should map very close.
		a := lexDistance("cpu_usage")
		b := lexDistance("cpu_limit")
		assert.InDelta(t, a, b, 1e-3)
	})

	t.Run("range bounded in [0, 1]", func(t *testing.T) {
		for _, s := range []string{"", "a", "z", "AAAAAAAA", "zzzzzzzzzzzz", "node_cpu_seconds_total", "{}!@#$%^&*()"} {
			v := lexDistance(s)
			assert.GreaterOrEqual(t, v, 0.0, s)
			assert.LessOrEqual(t, v, 1.0, s)
		}
	})
}

func TestLexFraction(t *testing.T) {
	t.Run("empty -> 0", func(t *testing.T) {
		assert.Equal(t, 0.0, lexFraction(""))
	})

	t.Run("preserves byte-lex order", func(t *testing.T) {
		ord := []string{
			"",
			"a",
			"aa",
			"aaaa",
			"aaaab",
			"aab",
			"ab",
			"cpu_limit",
			"cpu_usage",
			"memory_limit",
			"zzzzzz",
		}
		for i := 1; i < len(ord); i++ {
			assert.Less(t, lexFraction(ord[i-1]), lexFraction(ord[i]),
				"expected %q < %q under lexFraction", ord[i-1], ord[i])
		}
	})

	t.Run("range bounded in [0, 1)", func(t *testing.T) {
		for _, s := range []string{"", "a", "z", "\xff\xff\xff\xff", "node_cpu_seconds_total"} {
			v := lexFraction(s)
			assert.GreaterOrEqual(t, v, 0.0, s)
			assert.Less(t, v, 1.0, s)
		}
	})

	t.Run("exact base-256 values", func(t *testing.T) {
		// f("a") = 97/256.
		assert.InDelta(t, 97.0/256.0, lexFraction("a"), 1e-15)
		// f("aa") = 97/256 + 97/256^2.
		assert.InDelta(t, 97.0/256.0+97.0/65536.0, lexFraction("aa"), 1e-15)
	})

	t.Run("cpu_usage vs cpu_limit differ at byte 4", func(t *testing.T) {
		diff := lexFractionDiff("cpu_limit", "cpu_usage")
		// Byte 4: 'u' - 'l' = 9, then 's' - 'i' = 10, then 'a' - 'm' = -12, etc.
		expected := 9.0/math.Pow(256, 5) +
			10.0/math.Pow(256, 6) +
			-12.0/math.Pow(256, 7) +
			-2.0/math.Pow(256, 8) +
			-15.0/math.Pow(256, 9)
		assert.InDelta(t, expected, diff, 1e-18)
		assert.Positive(t, diff)
	})
}

func TestLexFractionDiffPrecision(t *testing.T) {
	t.Run("preserves precision for long shared prefix", func(t *testing.T) {
		// memory_usage and memory_limit share 7 bytes; the absolute lexFraction
		// values would round to the same float64, but lexFractionDiff still
		// produces a non-zero answer.
		diff := lexFractionDiff("memory_limit", "memory_usage")
		assert.Positive(t, diff)
		assert.Less(t, diff, 1e-18)

		// Compare against the lossy direct subtraction to demonstrate the
		// difference.
		lossy := lexFraction("memory_usage") - lexFraction("memory_limit")
		assert.Zero(t, lossy, "direct subtraction is expected to lose precision here")
	})

	t.Run("negative when b < a", func(t *testing.T) {
		diff := lexFractionDiff("cpu_usage", "cpu_limit")
		assert.Negative(t, diff)
	})

	t.Run("zero for equal strings", func(t *testing.T) {
		assert.Zero(t, lexFractionDiff("foo_bar", "foo_bar"))
	})
}

func TestNameDistanceScore(t *testing.T) {
	t.Run("single name -> 0", func(t *testing.T) {
		score, ok := nameDistanceScore(map[string]struct{}{"cpu_usage": {}})
		require.True(t, ok)
		assert.Equal(t, 0.0, score)
	})

	t.Run("score is lexFractionDiff(min, max)", func(t *testing.T) {
		score, ok := nameDistanceScore(map[string]struct{}{
			"cpu_usage": {}, "cpu_limit": {},
		})
		require.True(t, ok)
		assert.Equal(t, lexFractionDiff("cpu_limit", "cpu_usage"), score)
		assert.Positive(t, score)
	})

	t.Run("aaaaaa / zzzzzz spans ~0.098", func(t *testing.T) {
		score, ok := nameDistanceScore(map[string]struct{}{
			"aaaaaa": {}, "zzzzzz": {},
		})
		require.True(t, ok)
		// 25 * (1/256 + 1/256^2 + ... + 1/256^6) ≈ 25/255 * (1 - 256^-6).
		assert.InDelta(t, 25.0/255.0*(1-math.Pow(256, -6)), score, 1e-15)
	})

	t.Run("only min and max matter", func(t *testing.T) {
		a, _ := nameDistanceScore(map[string]struct{}{"aaa": {}, "zzz": {}})
		b, _ := nameDistanceScore(map[string]struct{}{
			"aaa": {}, "mmm": {}, "nnn": {}, "ppp": {}, "zzz": {},
		})
		assert.Equal(t, a, b)
	})

	t.Run("prefix-shared pairs of different prefix lengths are distinct", func(t *testing.T) {
		// cpu_* share 4 bytes; memory_* share 7 bytes. Both produce positive
		// scores, but cpu_* should be larger by orders of magnitude because
		// the divergence happens earlier in the encoding.
		a, _ := nameDistanceScore(map[string]struct{}{"cpu_usage": {}, "cpu_limit": {}})
		b, _ := nameDistanceScore(map[string]struct{}{"memory_usage": {}, "memory_limit": {}})
		assert.Positive(t, a)
		assert.Positive(t, b)
		assert.Greater(t, a, b, "earlier divergence -> larger score")
		// a is on the order of 256^-5 (~9e-13); b on the order of 256^-8
		// (~9e-20).
		assert.Less(t, a, 1e-9)
		assert.Less(t, b, 1e-15)
	})

	t.Run("empty matched -> undefined", func(t *testing.T) {
		_, ok := nameDistanceScore(nil)
		assert.False(t, ok)
	})
}

func TestResolveNoUniverseHandlesRegex(t *testing.T) {
	pq := parsedQuery{
		EqualityNames: []string{"cpu_usage"},
		NameRegexes:   []*regexp.Regexp{regexp.MustCompile(`^cpu_.*$`)},
	}
	resolved, unresolved := resolve(pq, nil)
	assert.True(t, unresolved, "regex without universe must be unresolved")
	assert.Nil(t, resolved)
}

func TestResolveNoUniverseEqualityOnly(t *testing.T) {
	pq := parsedQuery{EqualityNames: []string{"cpu_usage", "memory_usage"}}
	resolved, unresolved := resolve(pq, nil)
	require.False(t, unresolved)
	assert.Equal(t, map[string]struct{}{"cpu_usage": {}, "memory_usage": {}}, resolved)
}

func TestResolveWithUniverseAndRegex(t *testing.T) {
	uni := newUniverse([]string{"cpu_usage", "cpu_limit", "memory_usage", "disk_used_bytes"})
	pq := parsedQuery{NameRegexes: []*regexp.Regexp{regexp.MustCompile(`^cpu_.*$`)}}
	resolved, unresolved := resolve(pq, uni)
	require.False(t, unresolved)
	assert.Equal(t, map[string]struct{}{"cpu_usage": {}, "cpu_limit": {}}, resolved)
}

func TestHashByNamePartitionsFor(t *testing.T) {
	s := hashByNameScheme{}

	t.Run("full shard short circuits", func(t *testing.T) {
		assert.Equal(t, 100, s.PartitionsFor(nil, true, 100))
	})

	t.Run("single metric touches single partition", func(t *testing.T) {
		got := s.PartitionsFor(map[string]struct{}{"cpu_usage": {}}, false, 100)
		assert.Equal(t, 1, got)
	})

	t.Run("multiple metrics bounded by min(M, N)", func(t *testing.T) {
		matched := map[string]struct{}{"a": {}, "b": {}, "c": {}, "d": {}}
		got := s.PartitionsFor(matched, false, 1000)
		assert.LessOrEqual(t, got, 4)
		assert.GreaterOrEqual(t, got, 1)
	})

	t.Run("hash is deterministic", func(t *testing.T) {
		matched := map[string]struct{}{"foo": {}, "bar": {}, "baz": {}}
		first := s.PartitionsFor(matched, false, 50)
		second := s.PartitionsFor(matched, false, 50)
		assert.Equal(t, first, second)
	})
}

func TestLexSortPartitionsFor_Static(t *testing.T) {
	s := lexSortScheme{}

	t.Run("single metric -> single partition", func(t *testing.T) {
		got := s.PartitionsFor(map[string]struct{}{"cpu_usage": {}}, false, 100)
		assert.Equal(t, 1, got)
	})

	t.Run("shared-prefix metrics -> single partition", func(t *testing.T) {
		matched := map[string]struct{}{"cpu_usage": {}, "cpu_limit": {}}
		got := s.PartitionsFor(matched, false, 100)
		assert.Equal(t, 1, got, "cpu_usage and cpu_limit should share a bucket at any reasonable N")
	})

	t.Run("extreme metrics -> two distinct partitions", func(t *testing.T) {
		matched := map[string]struct{}{"aaaaaa": {}, "zzzzzz": {}}
		got := s.PartitionsFor(matched, false, 8)
		assert.Equal(t, 2, got)
	})

	t.Run("full shard short circuits", func(t *testing.T) {
		assert.Equal(t, 7, s.PartitionsFor(nil, true, 7))
	})

	t.Run("empty matched -> full shard", func(t *testing.T) {
		assert.Equal(t, 7, s.PartitionsFor(map[string]struct{}{}, false, 7))
	})
}

func TestAllLabelsAlwaysFullShard(t *testing.T) {
	s := allLabelsScheme{}
	assert.Equal(t, 42, s.PartitionsFor(map[string]struct{}{"cpu_usage": {}}, false, 42))
	assert.Equal(t, 42, s.PartitionsFor(nil, true, 42))
}

func TestNewUniverseDedupAndSort(t *testing.T) {
	u := newUniverse([]string{"foo", "bar", "foo", "", "baz"})
	assert.Equal(t, []string{"bar", "baz", "foo"}, u.Names)
}
