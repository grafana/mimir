// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/e8e25eb09e41bf295e0c9e847cd27cf9016a553a/web/api/v1/search_filters.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streaminglabelvalues

import (
	"errors"
	"fmt"
	"strings"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/strutil"

	"github.com/grafana/mimir/pkg/streaminglabelvalues/internal/searchexpr"
)

// FilterContains accepts values that contain a fixed substring.
//
// anyPosition selects the scoring rule: false gives position-weighted
// scoring (1.0 on prefix match, decaying linearly with match position down
// to 0.1 at the latest possible position — mirrors Prometheus PR #18573's
// SubstringFilter); true scores every match 1.0 regardless of position
// (used for fuzz_alg=substring and for negated terms, where fuzzy recall
// must not over-exclude candidates). Hardcoded as a branch inside Accept
// rather than a scorer func field: benchmarked faster (~5-10%, avoids an
// indirect call) and this is the only place either rule is needed.
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
// construction time. See FilterContains for anyPosition's meaning.
func NewFilterContains(term string, caseSensitive bool, anyPosition bool) (*FilterContains, error) {
	if term == "" {
		return nil, errors.New("FilterContains: empty term")
	}
	if !caseSensitive {
		term = strings.ToLower(term)
	}
	return &FilterContains{term: term, caseSensitive: caseSensitive, anyPosition: anyPosition}, nil
}

// Accept returns (true, 1.0) on prefix match or (with anyPosition) any
// match; (true, score) for a non-prefix substring under position-weighted
// scoring where score = 1.0 - 0.9 * idx / maxIdx (range [0.1, 1.0));
// (false, 0) on reject.
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

// ApplyResumeAfter wraps inner so it also rejects any value already
// returned by an earlier page of a cursor-paginated search. after is the
// last value emitted on the previous page; an empty after means no cursor
// is in effect, and inner is returned unchanged (no wrapper allocated).
// order determines comparison direction: OrderByValueDesc rejects values
// >= after; anything else (OrderByValueAsc, and defensively any other
// value) rejects values <= after, matching protoToOrdering/storepbToOrdering's
// own default-to-ascending convention. A nil inner is treated as
// BuildFilter's "accept everything, score 1.0" convention.
//
// This is deliberately a Filter-level concern, not a storage.SearchHints
// field: SearchHints, ApplySearchHints, and MergeSearchResultSets live in
// the vendored github.com/grafana/mimir-prometheus fork, which this
// function does not touch.
func ApplyResumeAfter(inner storage.Filter, after string, order storage.Ordering) storage.Filter {
	if after == "" {
		return inner
	}
	return &resumeAfterFilter{inner: inner, after: after, descending: order == storage.OrderByValueDesc}
}

type resumeAfterFilter struct {
	inner      storage.Filter
	after      string
	descending bool
}

func (f *resumeAfterFilter) Accept(value string) (bool, float64) {
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

// BuildFilter constructs a storage.Filter from Params. Returns (nil, nil)
// when Params is nil or has neither Terms nor a parsed expression — a nil
// storage.Filter accepts every value with score 1.0 by Prometheus convention.
//
// BuildFilter trusts that p has already been validated via NewParams or
// NewExpressionParams. Per-leaf constructors still reject
// obviously bad inputs (empty term, threshold out of [0,1]), so a malformed
// Params will surface an error rather than silently producing a wrong
// filter, but validation is not BuildFilter's responsibility.
//
// Per-term composition mirrors Prometheus PR #18573's buildSearchFilter:
//   - FuzzAlgSubsequence: just a FilterSubsequence (no substring fallback;
//     prefix matches still score 1.0 inside FilterSubsequence).
//   - FuzzAlgJaroWinkler: FilterContains OR FilterJaro via filterFallback;
//     the FilterJaro is omitted when threshold is 0 (substring only).
//
// Across legacy terms, combines with filterOr (OR-max). When an expression is
// present, compiles the AST parsed and validated by NewExpressionParams;
// positive term leaves reuse the same matchers, while negated leaves use
// literal substring matching so fuzzy recall cannot over-exclude candidates.
// NOT does not contribute a score.
// FuzzThreshold (int 0-100) is divided by 100 internally.
//
// Case folding for !CaseSensitive is applied once per value at the filter root
// via caseFoldingFilter; per-term filters are built case-sensitive against a
// pre-lowercased term so they do not re-fold the same value per Accept call.
// The returned filter is not safe for concurrent use when its positive leaves
// use a matcher that caches state; build one filter per request goroutine.
func BuildFilter(p *Params) (storage.Filter, error) {
	if p == nil || len(p.Terms) == 0 && p.expressionExpr == nil {
		return nil, nil
	}
	if len(p.Terms) > 0 && p.expressionExpr != nil {
		return nil, fmt.Errorf("invalid search parameters: %w", ErrTermsAndExpression)
	}

	threshold := float64(p.FuzzThreshold) / 100.0
	newTermFilter := func(term string, negated bool) (storage.Filter, error) {
		if !p.CaseSensitive {
			term = strings.ToLower(term)
		}
		if negated {
			return NewFilterContains(term, p.CaseSensitive, true)
		}
		return buildPerTermFilter(term, true, p.FuzzAlg, p.FuzzThreshold, threshold)
	}

	inner, err := buildRootFilter(p, newTermFilter)
	if err != nil {
		return nil, err
	}
	if p.CaseSensitive {
		return inner, nil
	}
	return &caseFoldingFilter{inner: inner}, nil
}

func buildRootFilter(p *Params, newTermFilter searchexpr.TermFilterFactory) (storage.Filter, error) {
	if p.expressionExpr != nil {
		return searchexpr.CompileValidated(p.expressionExpr, newTermFilter)
	}
	return buildLegacyTermsFilter(p.Terms, newTermFilter)
}

func buildLegacyTermsFilter(terms []string, newTermFilter searchexpr.TermFilterFactory) (storage.Filter, error) {
	perTerm := make([]storage.Filter, 0, len(terms))
	for _, term := range terms {
		filter, err := newTermFilter(term, false)
		if err != nil {
			return nil, err
		}
		perTerm = append(perTerm, filter)
	}
	if len(perTerm) == 1 {
		return perTerm[0], nil
	}
	return newFilterOr(perTerm...), nil
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
