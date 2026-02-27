// SPDX-License-Identifier: AGPL-3.0-only

package streaminglabelvalues

import (
	"math"
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
