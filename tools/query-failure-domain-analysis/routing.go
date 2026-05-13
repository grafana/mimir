// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"sort"

	"github.com/cespare/xxhash/v2"
)

// lexDistance maps a metric name to a value in [0, 1] using a base-26
// positional encoding over the lowercase alphabet:
//
//	lexDistance(s) = sum_i alphabetIndex(s[i]) / 26^(i+1)
//
// where alphabetIndex is in [0, 25]. Uppercase letters are case-folded; bytes
// outside [a-zA-Z] (digits, '_', ':', etc.) map to position 0, which makes
// them sort like 'a' in this encoding. The mapping is deterministic and
// universe-free, which matches what a real lex-sort routing layer would
// compute from the metric name alone.
//
// Concrete points:
//
//	lexDistance("aaaaaa")  == 0
//	lexDistance("zzzzzz")  ≈ 1 (more precisely 1 - 26^-6)
//	lexDistance("cpu_*")   ≈ 0.100  (shared prefix → same neighbourhood)
//	lexDistance("memory_*")≈ 0.494
//
// This function is used to bucket each metric name into one of N partitions
// (lexSortScheme.PartitionsFor). It deliberately scales the alphabet down to
// 26 distinct letter positions (with non-letters folded to 0) so that real
// metric names — which almost all start with a lowercase letter — spread
// across the full [0, 1) bucket range instead of clustering inside the
// narrow letter byte range [0x61, 0x7B) / 256.
//
// The per-query name-distance score (nameDistanceScore) uses a different
// function, lexFraction, which is byte-positional and preserves the full
// lex spectrum at the cost of producing tiny numbers for prefix-shared
// names.
func lexDistance(name string) float64 {
	var (
		r      float64
		weight = 1.0 / 26.0
	)
	for i := 0; i < len(name); i++ {
		b := name[i]
		if b >= 'A' && b <= 'Z' {
			b += 'a' - 'A'
		}
		var pos int
		if b >= 'a' && b <= 'z' {
			pos = int(b - 'a')
		}
		// Note: non-letters fall through with pos=0, treating them as
		// equivalent to 'a' for ordering purposes. This is slightly coarse
		// (e.g. "cpu0" and "cpua" map identically), but is good enough for
		// the failure-domain estimation we care about.
		r += float64(pos) * weight
		weight /= 26
	}
	return r
}

// lexFraction maps a string to its position in [0, 1) on the spectrum of all
// possible byte strings, by treating each byte as a digit in base 256:
//
//	lexFraction(s) = sum_{i=0..|s|-1} byte(s[i]) / 256^(i+1)
//
// The empty string maps to 0. The mapping preserves byte-level lex order:
// s1 < s2 (byte-wise) iff lexFraction(s1) < lexFraction(s2).
//
// IMPORTANT: do not compute distances as lexFraction(a) - lexFraction(b) for
// prefix-sharing strings — float64 catastrophic cancellation will lose the
// shared-prefix bits and return 0 for names that share more than ~7 bytes.
// Use lexFractionDiff (below) to compute differences directly without
// precision loss.
//
// This function is exposed primarily for tests and documentation; the
// per-query score uses lexFractionDiff.
func lexFraction(s string) float64 {
	var (
		r      float64
		weight = 1.0 / 256.0
	)
	for i := 0; i < len(s); i++ {
		r += float64(s[i]) * weight
		weight /= 256
	}
	return r
}

// lexFractionDiff returns lexFraction(b) - lexFraction(a), computed directly
// from the per-byte differences so the result retains full precision even
// for names that share a long common prefix:
//
//	lexFractionDiff(a, b) = sum_i (byte(b[i]) - byte(a[i])) / 256^(i+1)
//
// where missing bytes (when one string is shorter) are treated as 0. The
// returned value has the same sign as bytes.Compare(a, b)'s alphabetical
// ordering: positive when b > a, negative when b < a, zero when equal.
//
// Example: lexFractionDiff("memory_limit", "memory_usage") differs at byte 7
// by 9 (`u`-`l`), giving ≈ 9 / 256^8 ≈ 8.7e-20, which is preserved exactly
// even though both absolute fractions round to the same float64 value when
// computed independently.
func lexFractionDiff(a, b string) float64 {
	var diff float64
	weight := 1.0 / 256.0
	maxLen := len(a)
	if len(b) > maxLen {
		maxLen = len(b)
	}
	for i := 0; i < maxLen; i++ {
		var ai, bi int
		if i < len(a) {
			ai = int(a[i])
		}
		if i < len(b) {
			bi = int(b[i])
		}
		diff += float64(bi-ai) * weight
		weight /= 256
	}
	return diff
}

// nameDistanceScore returns the lex-spectrum span of a query: the position
// of the alphabetically largest matched name in [0, 1) minus the position
// of the alphabetically smallest matched name, where positions are taken
// under lexFraction (base-256 byte-positional encoding).
//
// Equivalently: how far apart on the spectrum of all possible byte strings
// do the query's most extreme metric names sit. 0 means a single matched
// name (or all matched names are equal). The second return value is false
// when the score is undefined — when matched is empty.
//
// The difference is computed directly via lexFractionDiff so that names
// sharing a long common prefix produce meaningful (though very small)
// scores rather than zero.
func nameDistanceScore(matched map[string]struct{}) (float64, bool) {
	if len(matched) == 0 {
		return 0, false
	}
	var minName, maxName string
	first := true
	for n := range matched {
		if first {
			minName, maxName = n, n
			first = false
			continue
		}
		if n < minName {
			minName = n
		}
		if n > maxName {
			maxName = n
		}
	}
	return lexFractionDiff(minName, maxName), true
}

// universe is the optional set of metric names known to exist in the cell.
// It is consulted only when a query has a regex matcher on __name__ — in
// which case it provides the candidates to enumerate. Equality matchers and
// scoring/bucketing operate without any universe knowledge.
type universe struct {
	Names []string // sorted, deduplicated
}

func newUniverse(names []string) *universe {
	seen := map[string]struct{}{}
	deduped := make([]string, 0, len(names))
	for _, n := range names {
		if n == "" {
			continue
		}
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		deduped = append(deduped, n)
	}
	sort.Strings(deduped)
	return &universe{Names: deduped}
}

// resolve returns the set of metric names that the query could match through
// its __name__ matchers. If the query contains regex matchers on __name__, a
// universe must be supplied (non-nil and non-empty) to enumerate candidates;
// otherwise resolved is empty and unresolved is true.
//
// resolved is the matched metric-name set. unresolved is true when the query
// has at least one __name__ regex but no usable universe — in that case the
// caller should treat the query as fullShard.
func resolve(pq parsedQuery, u *universe) (resolved map[string]struct{}, unresolved bool) {
	resolved = map[string]struct{}{}
	for _, name := range pq.EqualityNames {
		resolved[name] = struct{}{}
	}
	if len(pq.NameRegexes) == 0 {
		return resolved, false
	}
	if u == nil || len(u.Names) == 0 {
		return nil, true
	}
	for _, re := range pq.NameRegexes {
		for _, n := range u.Names {
			if re.MatchString(n) {
				resolved[n] = struct{}{}
			}
		}
	}
	return resolved, false
}

// scheme simulates one routing strategy. PartitionsFor returns the number of
// distinct partitions the query would touch under this scheme; for fullShard
// queries it should return numPartitions.
type scheme interface {
	Name() string
	PartitionsFor(matched map[string]struct{}, fullShard bool, numPartitions int) int
}

// allLabelsScheme models the status quo: every query fans out across the
// entire shuffle shard, regardless of which metrics it asks for.
type allLabelsScheme struct{}

func (allLabelsScheme) Name() string { return "all-labels (status quo)" }
func (allLabelsScheme) PartitionsFor(_ map[string]struct{}, _ bool, n int) int {
	return n
}

// hashByNameScheme models Cortex-style routing where the partition is chosen
// purely by hashing the metric name. Series for the same metric all land on
// the same partition; unrelated metrics scatter across partitions uniformly.
type hashByNameScheme struct{}

func (hashByNameScheme) Name() string { return "hash-by-name" }
func (hashByNameScheme) PartitionsFor(matched map[string]struct{}, fullShard bool, n int) int {
	if fullShard || n <= 0 {
		return n
	}
	if len(matched) == 0 {
		return n
	}
	seen := map[uint64]struct{}{}
	for name := range matched {
		seen[xxhash.Sum64String(name)%uint64(n)] = struct{}{}
	}
	return len(seen)
}

// lexSortScheme models a routing where each metric name is mapped via
// lexDistance to a fraction in [0, 1] and then placed in one of N equally-
// sized buckets. The bucketing is universe-free; adjacent metric names share
// a bucket because their lexDistance values are close, which is the locality
// property under evaluation.
type lexSortScheme struct{}

func (lexSortScheme) Name() string { return "lex-sort" }
func (lexSortScheme) PartitionsFor(matched map[string]struct{}, fullShard bool, n int) int {
	if fullShard || n <= 0 {
		return n
	}
	if len(matched) == 0 {
		return n
	}
	seen := map[int]struct{}{}
	for name := range matched {
		bucket := int(lexDistance(name) * float64(n))
		if bucket >= n {
			bucket = n - 1
		}
		if bucket < 0 {
			bucket = 0
		}
		seen[bucket] = struct{}{}
	}
	return len(seen)
}
