// SPDX-License-Identifier: AGPL-3.0-only

package streaminglabelvalues

import (
	"fmt"
	"math"
	"strings"

	"github.com/grafana/mimir/pkg/storage"
)

const (
	noscore  = 0.0
	unscored = -1.0
)

// FilterChain evaluates a slice of filters with a single logical operator.
// Filters are evaluated as OR conditions
// The score returned is the maximum score seen across all evaluated filters.
type FilterChain struct {
	filters []storage.Filter
}

// NewFilterChain creates a FilterChain with the given logical operator.
func NewFilterChain(size int) *FilterChain {
	return &FilterChain{filters: make([]storage.Filter, 0, size)}
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
	accepted = false
	for _, f := range c.filters {
		ok, s := f.Accept(value)
		if ok {
			accepted = true
			score = math.Max(score, s)
			// Short-circuit on an exact/substring match (score == unscored = -1):
			// the contains filter already matched, no need to compute a fuzz score.
			if s == unscored {
				break
			}
		}
	}
	if !accepted {
		return false, noscore
	}
	return true, score
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
func NewFilterChains(caseSensitive bool, numChains int) *FilterChains {
	return &FilterChains{caseSensitive: caseSensitive, chains: make([]*FilterChain, 0, numChains)}
}

// AddFilterChain appends a chain to the container.
func (c *FilterChains) AddFilterChain(chain *FilterChain) {
	c.chains = append(c.chains, chain)
}

// Accept implements storage.Filter.
// Returns true if any chain accepts the value. If there are no chains all values are accepted.
// Score is the maximum score seen across accepting chains; noscore (0) is returned for rejected values.
func (c *FilterChains) Accept(value string) (accepted bool, score float64) {
	if len(c.chains) == 0 {
		return true, unscored
	}
	v := value
	if !c.caseSensitive {
		v = strings.ToLower(value)
	}
	score = unscored
	for _, chain := range c.chains {
		ok, s := chain.Accept(v)
		if ok {
			accepted = true
			score = math.Max(score, s)
			if s == unscored {
				// Exact/substring match in this chain: no fuzz score needed.
				break
			}
		}
	}
	if !accepted {
		return false, noscore
	}
	return true, score
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
	term      string
	threshold float64
}

// NewFilterJaro creates a FilterJaro for the given reference term and acceptance threshold [0,1].
func NewFilterJaro(term string, threshold float64) FilterJaro {
	return FilterJaro{
		term:      term,
		threshold: threshold,
	}
}

// Accept implements storage.Filter. Score is the Jaro similarity value.
func (c FilterJaro) Accept(value string) (accepted bool, score float64) {
	score = JaroWinkler(c.term, value)
	if score >= c.threshold {
		return true, score
	}
	return false, noscore
}

type FilterSubsequence struct {
	term      string
	threshold float64
}

func NewFilterSubsequence(term string, threshold float64) FilterSubsequence {
	return FilterSubsequence{
		term:      term,
		threshold: threshold,
	}
}

func (c FilterSubsequence) Accept(value string) (accepted bool, score float64) {
	score = SubsequenceScore(c.term, value)
	if score >= c.threshold {
		return true, score
	}
	return false, noscore
}

// BuildFilterChains constructs a FilterChains from the given search parameters.
// Returns nil if searchTerms is empty, indicating no filtering should be applied.
// The returned FilterChains is safe to call concurrently from multiple goroutines.
func BuildFilterChains(searchTerms []string, caseInsensitive bool, fuzzAlg string, fuzzThreshold float64) (*FilterChains, error) {
	if len(searchTerms) == 0 {
		return nil, nil
	}

	// This default should already be set, but added for clarity
	if fuzzAlg == "" && fuzzThreshold > 0 {
		fuzzAlg = SearchAlgJaroWinkler
	}

	// We should not be here if this is incorrect, but added for safety
	if fuzzAlg != "" && fuzzAlg != SearchAlgJaroWinkler && fuzzAlg != SearchAlgSubsequence {
		return nil, fmt.Errorf("fuzzAlg must be one of: %s, %s", SearchAlgJaroWinkler, SearchAlgSubsequence)
	}

	// For each search term we have a chain. Each chain then has 1 or more scoring filters
	chain := NewFilterChains(!caseInsensitive, len(searchTerms))

	filtersPerChain := 1
	if fuzzThreshold > 0 && len(fuzzAlg) > 0 {
		filtersPerChain++
	}

	for _, term := range searchTerms {
		sc := NewFilterChain(filtersPerChain)

		if caseInsensitive {
			term = strings.ToLower(term)
		}

		sc.AddFilter(NewFilterContains(term))

		if fuzzThreshold > 0 && len(fuzzAlg) > 0 {
			switch fuzzAlg {
			case SearchAlgJaroWinkler:
				sc.AddFilter(NewFilterJaro(term, fuzzThreshold))
			case SearchAlgSubsequence:
				sc.AddFilter(NewFilterSubsequence(term, fuzzThreshold))
			}
		}
		chain.AddFilterChain(sc)
	}

	return chain, nil
}
