package streaminglabelvalues

import (
	"errors"
	"fmt"
	"strings"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/strutil"
)

// FilterContains accepts values that contain a fixed substring. Score is
// 1.0 on prefix match, 0.9 on non-prefix substring, 0 on reject. Mirrors
// Prometheus PR #18573's SubstringFilter score semantics.
type FilterContains struct {
	term          string
	caseSensitive bool
}

// NewFilterContains returns a substring-containment filter. Term must be
// non-empty. When caseSensitive is false the term is lowercased once at
// construction time.
func NewFilterContains(term string, caseSensitive bool) (*FilterContains, error) {
	if term == "" {
		return nil, errors.New("FilterContains: empty term")
	}
	if !caseSensitive {
		term = strings.ToLower(term)
	}
	return &FilterContains{term: term, caseSensitive: caseSensitive}, nil
}

// Accept returns (true, 1.0) on prefix match, (true, 0.9) on non-prefix
// substring, (false, 0) on reject.
func (f *FilterContains) Accept(value string) (bool, float64) {
	if !f.caseSensitive {
		value = strings.ToLower(value)
	}
	if !strings.Contains(value, f.term) {
		return false, 0
	}
	if strings.HasPrefix(value, f.term) {
		return true, 1.0
	}
	return true, 0.9
}

// FilterJaro accepts values whose Jaro-Winkler similarity to a fixed term is
// at least threshold. The underlying matcher caches term runes lazily on the
// first Unicode candidate and is therefore not safe for concurrent use.
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

// BuildFilter constructs a storage.Filter from Params. Returns (nil, nil)
// when Params is nil or has zero Terms — a nil storage.Filter accepts every
// value with score 1.0 by Prometheus convention.
//
// Per term, builds a filterFallback{FilterContains, fuzzy} (substring tried
// first, fuzzy fallback). Across terms, combines with filterOr (OR-max).
// FuzzThreshold (int 0-100) is divided by 100 internally.
func BuildFilter(p *Params) (storage.Filter, error) {
	if p == nil || len(p.Terms) == 0 {
		return nil, nil
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	threshold := float64(p.FuzzThreshold) / 100.0
	perTerm := make([]storage.Filter, 0, len(p.Terms))
	for _, term := range p.Terms {
		substring, err := NewFilterContains(term, p.CaseSensitive)
		if err != nil {
			return nil, err
		}
		var fuzzy storage.Filter
		switch p.FuzzAlg {
		case FuzzAlgJaroWinkler:
			fuzzy, err = NewFilterJaro(term, threshold, p.CaseSensitive)
		default: // FuzzAlgSubsequence
			fuzzy, err = NewFilterSubsequence(term, threshold, p.CaseSensitive)
		}
		if err != nil {
			return nil, err
		}
		perTerm = append(perTerm, &filterFallback{substring: substring, fuzzy: fuzzy})
	}
	if len(perTerm) == 1 {
		return perTerm[0], nil
	}
	return newFilterOr(perTerm...), nil
}
