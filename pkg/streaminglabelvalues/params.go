// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/e8e25eb09e41bf295e0c9e847cd27cf9016a553a/web/api/v1/search.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streaminglabelvalues

import (
	"fmt"
	"math"
)

// FuzzAlg identifies which fuzzy-matching algorithm a filter uses.
// The zero value is FuzzAlgSubsequence, matching Prometheus PR #18573's
// FuzzAlgorithms[0] default.
type FuzzAlg uint8

const (
	// FuzzAlgSubsequence uses the greedy subsequence-match score.
	// Default value (zero).
	FuzzAlgSubsequence FuzzAlg = iota
	// FuzzAlgJaroWinkler uses the Jaro-Winkler similarity score.
	FuzzAlgJaroWinkler
	// FuzzAlgSubstringLeft applies no fuzzy matching. It accepts a value that
	// contains the term, and gives a higher score to a match nearer the start.
	FuzzAlgSubstringLeft
	// FuzzAlgSubstring applies no fuzzy matching. It accepts a value that
	// contains the term, and scores every match 1.0.
	FuzzAlgSubstring
)

// Params is the wire-decoupled input to the search call. Each gRPC
// server translates its proto request into this struct before it builds
// a filter, so this package does not depend on any proto.
//
// Use NewParams to construct a validated Params object, and WithCursor to add
// the cursor of a previous page.
type Params struct {
	// Terms are the search terms. An empty slice (or nil) yields a nil filter.
	// Multiple terms are combined with OR semantics by filterOr.
	Terms []string
	// CaseSensitive matches Prometheus URL param polarity. Prometheus's HTTP
	// default is true; Mimir's gRPC wire default is the proto zero (false),
	// and the gRPC clients (HTTP handler in PR #4) set this explicitly.
	CaseSensitive bool
	// FuzzAlg selects the fuzzy algorithm. Zero value is FuzzAlgSubsequence.
	FuzzAlg FuzzAlg
	// FuzzThreshold is the minimum fuzzy score expressed as int 0-100,
	// matching Prometheus PR #18573's fuzz_threshold URL param. Internally
	// BuildFilter divides by 100 before passing to filter constructors.
	// Zero accepts any subseq match (Prometheus's default).
	FuzzThreshold int
	// ResumeAfter is the last value of the previous page of a cursor search.
	// Empty means no cursor. BuildSearchFilter and BuildSortedSearch apply it;
	// BuildFilter ignores it.
	ResumeAfter string
	// ScoreAfter is the score of ResumeAfter, for an OrderByScoreDesc cursor.
	// It must be 0 when ResumeAfter is empty.
	ScoreAfter float64
}

// NewParams constructs and validates a Params. Returns an error if any
// field is outside its permitted range. This is the canonical way to build
// a Params from wire input — callers (proto translators, the HTTP handler
// in PR #4) route untrusted values through here so BuildFilter can trust
// what it receives.
func NewParams(terms []string, caseSensitive bool, alg FuzzAlg, threshold int) (*Params, error) {
	p := &Params{
		Terms:         terms,
		CaseSensitive: caseSensitive,
		FuzzAlg:       alg,
		FuzzThreshold: threshold,
	}
	if err := p.validate(); err != nil {
		return nil, err
	}
	return p, nil
}

// WithCursor returns a validated copy of p with the cursor of a previous page.
// A nil p gives a Params without terms.
func (p *Params) WithCursor(resumeAfter string, scoreAfter float64) (*Params, error) {
	var c Params
	if p != nil {
		c = *p
	}
	c.ResumeAfter = resumeAfter
	c.ScoreAfter = scoreAfter
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// validate returns a non-nil error if Params has fields outside their
// permitted ranges. Empty Terms is permitted (yields a nil filter).
// Internal to the package — external callers should construct via NewParams
// and WithCursor.
func (p *Params) validate() error {
	if p == nil {
		return nil
	}
	switch p.FuzzAlg {
	case FuzzAlgSubsequence, FuzzAlgJaroWinkler, FuzzAlgSubstringLeft, FuzzAlgSubstring:
	default:
		return fmt.Errorf("unknown fuzz algorithm %d", p.FuzzAlg)
	}
	if p.FuzzThreshold < 0 || p.FuzzThreshold > 100 {
		return fmt.Errorf("fuzz threshold %d out of [0,100]", p.FuzzThreshold)
	}
	for i, t := range p.Terms {
		if t == "" {
			return fmt.Errorf("search term %d is empty", i)
		}
	}
	// storage.Filter scores are in [0, 1], so a score outside that range
	// cannot come from a previous page.
	if math.IsNaN(p.ScoreAfter) || p.ScoreAfter < 0 || p.ScoreAfter > 1 {
		return fmt.Errorf("score after %v out of [0,1]", p.ScoreAfter)
	}
	if p.ScoreAfter != 0 && p.ResumeAfter == "" {
		return fmt.Errorf("score after %v requires resume after", p.ScoreAfter)
	}
	return nil
}
