// SPDX-License-Identifier: AGPL-3.0-only

package streaminglabelvalues

import (
	"math"
	"sort"
	"strings"

	"github.com/grafana/mimir/pkg/storage"
)

const (
	noscore  = 0.0
	unscored = -1.0
)

// FilterChain evaluates a slice of filters with a single logical operator (AND/OR).
//
// For OR: accepts on the first matching filter (short-circuits).
// For AND: rejects on the first non-matching filter (short-circuits).
//
// The score returned is the maximum score seen across all evaluated filters.
type FilterChain struct {
	filters []storage.Filter
	op      Operator
}

// NewFilterChain creates a FilterChain with the given logical operator.
func NewFilterChain(op Operator, size int) *FilterChain {
	return &FilterChain{op: op, filters: make([]storage.Filter, 0, size)}
}

// AddFilter appends a filter to the chain.
func (c *FilterChain) AddFilter(f storage.Filter) {
	c.filters = append(c.filters, f)
}

// Accept implements storage.Filter.
func (c *FilterChain) Accept(value string) (accepted bool, score float64) {
	if len(c.filters) == 0 {
		return true, unscored
	}
	score = unscored
	switch c.op {
	case And:
		for _, f := range c.filters {
			ok, s := f.Accept(value)
			if ok {
				score = math.Max(score, s)
			} else {
				return false, noscore
			}
		}
		return true, score
	default: // Or
		accepted := false
		for _, f := range c.filters {
			ok, s := f.Accept(value)
			accepted = accepted || ok
			if ok {
				score = math.Max(score, s)
			}
		}
		return accepted, score
	}
}

// FilterChains is an outer container for multiple FilterChain instances.
// A value is accepted if ANY chain accepts it (OR logic between chains).
//
// If caseSensitive is false, the value is lowercased before being passed to
// each chain; filter terms should therefore also be stored lowercased.
type FilterChains struct {
	chains        []*FilterChain
	caseSensitive bool
}

// NewFilterChains creates a FilterChains container with the given case sensitivity.
func NewFilterChains(caseSensitive bool) *FilterChains {
	return &FilterChains{caseSensitive: caseSensitive}
}

// AddFilterChain appends a chain to the container.
func (c *FilterChains) AddFilterChain(chain *FilterChain) {
	c.chains = append(c.chains, chain)
}

// Accept implements storage.Filter.
// Returns true if any chain accepts the value. If there are no chains all values are accepted.
// Score is the maximum score seen across all chains regardless of whether any accepted.
func (c *FilterChains) Accept(value string) (accepted bool, score float64) {
	if len(c.chains) == 0 {
		return true, unscored
	}
	v := value
	if !c.caseSensitive {
		v = strings.ToLower(value)
	}
	score = -1
	for _, chain := range c.chains {
		ok, s := chain.Accept(v)
		if ok {
			return true, math.Max(score, s)
		}
	}
	return false, noscore
}

// FilterContains accepts values that contain the given term as a substring.
// Pass a lowercased term when using FilterChains with caseSensitive=false.
type FilterContains struct {
	term string
}

// NewFilterContains creates a FilterContains for the given term.
func NewFilterContains(term string) FilterContains {
	return FilterContains{term: term}
}

// Accept implements storage.Filter. Score is -1 on a match (exact containment, not fuzzy).
func (c FilterContains) Accept(value string) (accepted bool, score float64) {
	if strings.Contains(value, c.term) {
		return true, unscored
	}
	return false, noscore
}

// FilterJaro accepts values whose Jaro similarity to the reference term meets the threshold.
// Pass a lowercased term when using FilterChains with caseSensitive=false.
type FilterJaro struct {
	jaro      JaroMatcher
	threshold float64
}

// NewFilterJaro creates a FilterJaro for the given reference term and acceptance threshold [0,1].
func NewFilterJaro(term string, threshold float64) FilterJaro {
	return FilterJaro{
		jaro:      NewJaroMatcher(term),
		threshold: threshold,
	}
}

// Accept implements storage.Filter. Score is the Jaro similarity value.
func (c FilterJaro) Accept(value string) (accepted bool, score float64) {
	score = c.jaro.Similarity(value)
	if score >= c.threshold {
		return true, score
	}
	return false, noscore
}

// BuildFilterChains constructs a FilterChains from the given search parameters.
// Returns nil if searchTerms is empty, indicating no filtering should be applied.
// The returned FilterChains is safe to call concurrently from multiple goroutines.
func BuildFilterChains(searchTerms []string, caseInsensitive bool, op Operator, fuzzThreshold float64) *FilterChains {
	if len(searchTerms) == 0 {
		return nil
	}

	chain := NewFilterChains(!caseInsensitive)

	sc := NewFilterChain(op, len(searchTerms))
	for _, term := range searchTerms {
		if caseInsensitive {
			term = strings.ToLower(term)
		}
		sc.AddFilter(NewFilterContains(term))
	}
	chain.AddFilterChain(sc)

	if fuzzThreshold > 0 {
		fc := NewFilterChain(op, len(searchTerms))
		for _, term := range searchTerms {
			if caseInsensitive {
				term = strings.ToLower(term)
			}
			fc.AddFilter(NewFilterJaro(term, fuzzThreshold))
		}
		chain.AddFilterChain(fc)
	}

	return chain
}

// ScoreAndSort scores values using filter and sorts them by score in-place.
// If ascending is false, highest score first; if true, lowest score first.
// If filter is nil, values are returned unchanged.
func ScoreAndSort(values []string, filter *FilterChains, ascending bool) []string {
	if filter == nil || len(values) == 0 {
		return values
	}
	scores := make([]float64, len(values))
	for i, v := range values {
		_, scores[i] = filter.Accept(v)
	}
	sort.Sort(scoreSort{values: values, scores: scores, ascending: ascending})
	return values
}

// scoreSort implements sort.Interface to sort values and scores in parallel.
type scoreSort struct {
	values    []string
	scores    []float64
	ascending bool
}

func (s scoreSort) Len() int { return len(s.values) }
func (s scoreSort) Less(i, j int) bool {
	if s.ascending {
		return s.scores[i] < s.scores[j]
	}
	return s.scores[i] > s.scores[j]
}
func (s scoreSort) Swap(i, j int) {
	s.values[i], s.values[j] = s.values[j], s.values[i]
	s.scores[i], s.scores[j] = s.scores[j], s.scores[i]
}

// MergeSlicesAndSortByScore deduplicates values from multiple slices, scores each
// unique value using filter, and returns them sorted by score. If filter is nil,
// values are deduplicated in insertion order without scoring.
func MergeSlicesAndSortByScore(ss [][]string, filter *FilterChains, ascending bool) []string {
	seen := make(map[string]struct{})
	var unique []string
	for _, slice := range ss {
		for _, v := range slice {
			if _, ok := seen[v]; !ok {
				seen[v] = struct{}{}
				unique = append(unique, v)
			}
		}
	}
	return ScoreAndSort(unique, filter, ascending)
}

// ApplyFilterChains filters values in-place using a pre-built FilterChains.
// The returned slice shares the same backing array as the input.
// If chain is nil all values are retained unchanged.
func ApplyFilterChains(values []string, chain *FilterChains) []string {
	if chain == nil {
		return values
	}
	n := 0
	for _, v := range values {
		if accepted, _ := chain.Accept(v); accepted {
			values[n] = v
			n++
		}
	}
	return values[:n]
}
