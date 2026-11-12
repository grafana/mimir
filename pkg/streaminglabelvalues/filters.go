// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/e8e25eb09e41bf295e0c9e847cd27cf9016a553a/web/api/v1/search_filters.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streaminglabelvalues

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/strutil"
)

// FilterContains accepts values that contain a fixed substring.
//
// When anyPosition is false, the score is 1.0 on prefix match; for non-prefix
// substrings the score decays linearly with the match position from 1.0
// (early match) to 0.1 (latest match). This mirrors Prometheus PR #18573's
// SubstringFilter score semantics. When anyPosition is true, every match
// scores 1.0.
//
// caseSensitive drives in-Accept folding only for direct callers of
// NewFilterContains; BuildFilter constructs leaves with caseSensitive=true
// and lets caseFoldingFilter at the OR root fold each value once. Treat
// this field as advisory when reading code through BuildFilter.
type FilterContains struct {
	term          string
	caseSensitive bool
	anyPosition   bool
}

// NewFilterContains returns a substring-containment filter. Term must be
// non-empty. When caseSensitive is false the term is lowercased once at
// construction time. See FilterContains for the meaning of anyPosition.
func NewFilterContains(term string, caseSensitive bool, anyPosition bool) (*FilterContains, error) {
	if term == "" {
		return nil, errors.New("FilterContains: empty term")
	}
	if !caseSensitive {
		term = strings.ToLower(term)
	}
	return &FilterContains{term: term, caseSensitive: caseSensitive, anyPosition: anyPosition}, nil
}

// Accept returns (true, 1.0) on prefix match, or on any match when
// anyPosition is true; (true, score) for a non-prefix substring where
// score = 1.0 - 0.9 * idx / maxIdx (range [0.1, 1.0)); (false, 0) on reject.
func (f *FilterContains) Accept(value string) (bool, float64) {
	if !f.caseSensitive {
		value = strings.ToLower(value)
	}
	if f.anyPosition {
		if strings.Contains(value, f.term) {
			return true, 1.0
		}
		return false, 0
	}
	idx := strings.Index(value, f.term)
	if idx < 0 {
		return false, 0
	}
	if idx == 0 {
		return true, 1.0
	}
	maxIdx := len(value) - len(f.term)
	return true, 1.0 - 0.9*float64(idx)/float64(maxIdx)
}

// FilterJaro accepts values whose Jaro-Winkler similarity to a fixed term is
// at least threshold. The underlying matcher caches term runes lazily on the
// first Unicode candidate and is therefore not safe for concurrent use.
//
// See the note on FilterContains: caseSensitive is advisory under BuildFilter,
// authoritative for direct NewFilterJaro callers.
type FilterJaro struct {
	matcher       *strutil.JaroWinklerMatcher
	threshold     float64
	caseSensitive bool
}

// NewFilterJaro returns a Jaro-Winkler filter. Term must be non-empty;
// threshold must lie in [0, 1] (the native matcher unit).
func NewFilterJaro(term string, threshold float64, caseSensitive bool) (*FilterJaro, error) {
	if term == "" {
		return nil, errors.New("FilterJaro: empty term")
	}
	if threshold < 0 || threshold > 1 {
		return nil, fmt.Errorf("FilterJaro: threshold %v out of [0,1]", threshold)
	}
	if !caseSensitive {
		term = strings.ToLower(term)
	}
	return &FilterJaro{
		matcher:       strutil.NewJaroWinklerMatcher(term),
		threshold:     threshold,
		caseSensitive: caseSensitive,
	}, nil
}

// Accept returns (score >= threshold, score).
func (f *FilterJaro) Accept(value string) (bool, float64) {
	if !f.caseSensitive {
		value = strings.ToLower(value)
	}
	score := f.matcher.Score(value)
	return score >= f.threshold, score
}

// FilterSubsequence accepts values where the configured pattern occurs as a
// subsequence and the resulting similarity score is at least threshold.
// Prefix matches always score 1.0 (overrides raw subseq score). Score 0 is
// always rejected (no subsequence match). Mirrors Prometheus PR #18573's
// SubsequenceFilter semantics. Underlying matcher is not concurrency-safe.
//
// See the note on FilterContains: caseSensitive is advisory under BuildFilter,
// authoritative for direct NewFilterSubsequence callers.
type FilterSubsequence struct {
	pattern       string // stored locally for prefix check; matcher does not expose its pattern.
	matcher       *strutil.SubsequenceMatcher
	threshold     float64
	caseSensitive bool
}

// NewFilterSubsequence returns a subsequence filter. Pattern must be
// non-empty and threshold must lie in [0, 1].
func NewFilterSubsequence(pattern string, threshold float64, caseSensitive bool) (*FilterSubsequence, error) {
	if pattern == "" {
		return nil, errors.New("FilterSubsequence: empty pattern")
	}
	if threshold < 0 || threshold > 1 {
		return nil, fmt.Errorf("FilterSubsequence: threshold %v out of [0,1]", threshold)
	}
	if !caseSensitive {
		pattern = strings.ToLower(pattern)
	}
	return &FilterSubsequence{
		pattern:       pattern,
		matcher:       strutil.NewSubsequenceMatcher(pattern),
		threshold:     threshold,
		caseSensitive: caseSensitive,
	}, nil
}

// Accept returns (true, 1.0) on prefix match; otherwise (score > 0 &&
// score >= threshold, score).
func (f *FilterSubsequence) Accept(value string) (bool, float64) {
	if !f.caseSensitive {
		value = strings.ToLower(value)
	}
	if strings.HasPrefix(value, f.pattern) {
		return true, 1.0
	}
	score := f.matcher.Score(value)
	return score > 0 && score >= f.threshold, score
}

// filterOr is the OR-max combinator across multiple child filters. Returns
// (any-accepted, max-score-across-accepts). Short-circuits on score 1.0.
// Equivalent of Prometheus PR #18573's private orSearchesFilter.
type filterOr struct {
	filters []storage.Filter
}

func newFilterOr(filters ...storage.Filter) *filterOr {
	return &filterOr{filters: filters}
}

func (o *filterOr) Accept(value string) (bool, float64) {
	var (
		any  bool
		best float64
	)
	for _, f := range o.filters {
		accepted, score := f.Accept(value)
		if !accepted {
			continue
		}
		any = true
		if score > best {
			best = score
		}
		if score >= 1.0 {
			return true, 1.0
		}
	}
	return any, best
}

// filterFallback is the substring-then-fuzzy combinator per term. Tries
// substring first (cheap); if substring rejects, falls through to fuzzy.
// Equivalent of Prometheus PR #18573's private orFilter.
type filterFallback struct {
	substring storage.Filter
	fuzzy     storage.Filter
}

func (f *filterFallback) Accept(value string) (bool, float64) {
	if accepted, score := f.substring.Accept(value); accepted {
		return true, score
	}
	return f.fuzzy.Accept(value)
}

// caseFoldingFilter wraps a child filter and lowercases each candidate value
// once before delegating, so a chain of leaf filters constructed with
// caseSensitive=true can be reused for a case-insensitive search without
// each leaf re-folding the same value on every Accept call. Mirrors
// Prometheus PR #18573's caseFoldingFilter.
type caseFoldingFilter struct {
	inner storage.Filter
}

func (c *caseFoldingFilter) Accept(value string) (bool, float64) {
	return c.inner.Accept(strings.ToLower(value))
}

// BuildSearchFilter returns BuildFilter(p), wrapped to also reject every value
// that a previous page returned under order. A cursor makes the filter non-nil,
// which turns off the no-filter fast paths of storage.ApplySearchHints and the
// TSDB limit pushdown. Callers that hold the values use BuildSortedSearch.
func BuildSearchFilter(p *Params, order storage.Ordering) (storage.Filter, error) {
	filter, err := BuildFilter(p)
	if err != nil {
		return nil, err
	}
	return applyCursor(filter, p, order), nil
}

// BuildSortedSearch is BuildSearchFilter for values sorted ascending, as
// storage.ApplySearchHints requires. It cuts the values of previous pages off
// with a binary search where order allows it. The filter then stays nil when p
// has no terms.
func BuildSortedSearch(values []string, p *Params, order storage.Ordering) ([]string, storage.Filter, error) {
	filter, err := BuildFilter(p)
	if err != nil {
		return nil, nil, err
	}
	if p == nil || p.ResumeAfter == "" {
		return values, filter, nil
	}
	switch {
	case order == storage.OrderByValueDesc:
		return values[:sort.SearchStrings(values, p.ResumeAfter)], filter, nil
	case order != storage.OrderByScoreDesc:
		return values[firstAfter(values, p.ResumeAfter):], filter, nil
	case filter != nil:
		return values, applyCursor(filter, p, order), nil
	case p.ScoreAfter < 1.0:
		// Without terms every value scores 1.0, so a page that ended on a lower
		// score returned every value.
		return nil, nil, nil
	default:
		return values[firstAfter(values, p.ResumeAfter):], nil, nil
	}
}

// firstAfter returns the index of the first value that sorts after v.
func firstAfter(values []string, v string) int {
	return sort.Search(len(values), func(i int) bool { return values[i] > v })
}

// applyCursor wraps inner so that it also rejects every value that sorts at or
// before p.ResumeAfter under order. It returns inner when p has no cursor. A nil
// inner accepts every value with score 1.0, as BuildFilter documents.
func applyCursor(inner storage.Filter, p *Params, order storage.Ordering) storage.Filter {
	if p == nil || p.ResumeAfter == "" {
		return inner
	}
	switch order {
	case storage.OrderByScoreDesc:
		return &scoreCursorFilter{inner: inner, afterScore: p.ScoreAfter, after: p.ResumeAfter}
	case storage.OrderByValueDesc:
		return &valueCursorFilter{inner: inner, after: p.ResumeAfter, descending: true}
	default:
		return &valueCursorFilter{inner: inner, after: p.ResumeAfter}
	}
}

// valueCursorFilter checks the value before inner, because the value alone
// decides if a previous page returned it.
type valueCursorFilter struct {
	inner      storage.Filter
	after      string
	descending bool
}

func (f *valueCursorFilter) Accept(value string) (bool, float64) {
	if f.descending {
		if value >= f.after {
			return false, 0
		}
	} else if value <= f.after {
		return false, 0
	}
	if f.inner == nil {
		return true, 1.0
	}
	return f.inner.Accept(value)
}

// scoreCursorFilter calls inner first, because the exclusion needs the score.
// A previous page returned every value with a higher score, and every value
// with the same score that sorts at or before after. The comparison is exact,
// so every backend must compute the same score bits for a value.
type scoreCursorFilter struct {
	inner      storage.Filter
	afterScore float64
	after      string
}

func (f *scoreCursorFilter) Accept(value string) (bool, float64) {
	accepted, score := true, 1.0
	if f.inner != nil {
		accepted, score = f.inner.Accept(value)
	}
	if !accepted || score > f.afterScore || (score == f.afterScore && value <= f.after) {
		return false, 0
	}
	return true, score
}

// BuildFilter constructs a storage.Filter from Params. Returns (nil, nil)
// when Params is nil or has zero Terms — a nil storage.Filter accepts every
// value with score 1.0 by Prometheus convention.
//
// BuildFilter trusts that p has already been validated (via NewParams or
// an equivalent caller-side check). Per-leaf constructors still reject
// obviously bad inputs (empty term, threshold out of [0,1]), so a malformed
// Params will surface an error rather than silently producing a wrong
// filter, but validation is not BuildFilter's responsibility.
//
// Per-term composition for FuzzAlgSubsequence and FuzzAlgJaroWinkler mirrors
// Prometheus PR #18573's buildSearchFilter. Prometheus has no substring
// algorithms.
//   - FuzzAlgSubsequence: just a FilterSubsequence (no substring fallback;
//     prefix matches still score 1.0 inside FilterSubsequence).
//   - FuzzAlgJaroWinkler: FilterContains OR FilterJaro via filterFallback;
//     the FilterJaro is omitted when threshold is 0 (substring only).
//   - FuzzAlgSubstringLeft: just a position-scored FilterContains.
//   - FuzzAlgSubstring: just a FilterContains that scores every match 1.0.
//
// The substring algorithms ignore FuzzThreshold.
//
// Across terms, combines with filterOr (OR-max). FuzzThreshold (int 0-100)
// is divided by 100 internally.
//
// Case folding for !CaseSensitive is applied once per value at the OR root
// via caseFoldingFilter; per-term filters are built case-sensitive against a
// pre-lowercased term so they do not re-fold the same value per Accept call.
func BuildFilter(p *Params) (storage.Filter, error) {
	if p == nil || len(p.Terms) == 0 {
		return nil, nil
	}
	threshold := float64(p.FuzzThreshold) / 100.0
	perTerm := make([]storage.Filter, 0, len(p.Terms))
	for _, term := range p.Terms {
		if !p.CaseSensitive {
			term = strings.ToLower(term)
		}
		f, err := buildPerTermFilter(term, true, p.FuzzAlg, p.FuzzThreshold, threshold)
		if err != nil {
			return nil, err
		}
		perTerm = append(perTerm, f)
	}
	var inner storage.Filter
	if len(perTerm) == 1 {
		inner = perTerm[0]
	} else {
		inner = newFilterOr(perTerm...)
	}
	if p.CaseSensitive {
		return inner, nil
	}
	return &caseFoldingFilter{inner: inner}, nil
}

// buildPerTermFilter composes the per-term filter. fuzzThresholdInt is the
// raw integer value from Params (0-100) — used to decide whether to attach a
// Jaro-Winkler fuzzy filter under the FuzzAlgJaroWinkler branch (omitted when
// threshold is 0, mirroring Prometheus's buildSearchFilter).
func buildPerTermFilter(term string, caseSensitive bool, alg FuzzAlg, fuzzThresholdInt int, threshold float64) (storage.Filter, error) {
	switch alg {
	case FuzzAlgJaroWinkler:
		substring, err := NewFilterContains(term, caseSensitive, false)
		if err != nil {
			return nil, err
		}
		if fuzzThresholdInt == 0 {
			return substring, nil
		}
		fuzzy, err := NewFilterJaro(term, threshold, caseSensitive)
		if err != nil {
			return nil, err
		}
		return &filterFallback{substring: substring, fuzzy: fuzzy}, nil
	case FuzzAlgSubstringLeft:
		return NewFilterContains(term, caseSensitive, false)
	case FuzzAlgSubstring:
		return NewFilterContains(term, caseSensitive, true)
	default: // FuzzAlgSubsequence — no substring fallback; prefix matches still score 1.0 inside FilterSubsequence.
		return NewFilterSubsequence(term, threshold, caseSensitive)
	}
}
