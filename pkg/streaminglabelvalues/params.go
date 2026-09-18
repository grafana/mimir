// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/e8e25eb09e41bf295e0c9e847cd27cf9016a553a/web/api/v1/search.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streaminglabelvalues

import (
	"errors"
	"fmt"
	"strings"

	"github.com/grafana/mimir/pkg/streaminglabelvalues/internal/searchexpr"
)

// ErrTermsAndExpression is returned when legacy search terms and a boolean
// search expression are supplied together.
var ErrTermsAndExpression = errors.New("search terms and search expression are mutually exclusive")

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
)

// Params is the wire-decoupled input to the search call. Each gRPC
// server translates its proto request into this struct before invoking
// BuildFilter, so this package does not depend on any proto.
//
// Use NewParams or NewExpressionParams to construct a validated Params object,
// then treat the returned object as immutable.
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

	// expression is retained privately so validated Params can be forwarded over
	// the wire without exposing mutable expression state. expressionExpr is the
	// parsed form used to build request-local filters. Both are populated
	// together by NewExpressionParams and are immutable afterwards.
	expression     string
	expressionExpr searchexpr.Expr
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

// NewExpressionParams constructs and validates Params for a boolean search
// expression. It is the expression counterpart to NewParams; API and wire
// integration can choose between the two without changing BuildFilter.
func NewExpressionParams(expression string, caseSensitive bool, alg FuzzAlg, threshold int) (*Params, error) {
	if strings.TrimSpace(expression) == "" {
		return nil, fmt.Errorf("search expression is empty")
	}
	expr, err := searchexpr.Parse(expression)
	if err != nil {
		return nil, err
	}
	p := &Params{
		CaseSensitive:  caseSensitive,
		FuzzAlg:        alg,
		FuzzThreshold:  threshold,
		expression:     strings.Clone(expression),
		expressionExpr: expr,
	}
	if err := p.validate(); err != nil {
		return nil, err
	}
	return p, nil
}

// validate returns a non-nil error if Params has fields outside their
// permitted ranges. Empty Terms and expressionExpr are permitted together
// (and yield a nil filter), but expressionExpr is mutually exclusive with
// Terms and must pass search-safety validation. Validation is read-only, so a
// Params can safely be validated after it has been shared. Internal to the
// package — external callers should construct via NewParams or
// NewExpressionParams.
func (p *Params) validate() error {
	if p == nil {
		return nil
	}
	switch p.FuzzAlg {
	case FuzzAlgSubsequence, FuzzAlgJaroWinkler:
	default:
		return fmt.Errorf("unknown fuzz algorithm %d", p.FuzzAlg)
	}
	if p.FuzzThreshold < 0 || p.FuzzThreshold > 100 {
		return fmt.Errorf("fuzz threshold %d out of [0,100]", p.FuzzThreshold)
	}
	if len(p.Terms) > 0 && p.expression != "" {
		return fmt.Errorf("invalid search parameters: %w", ErrTermsAndExpression)
	}
	for i, t := range p.Terms {
		if t == "" {
			return fmt.Errorf("search term %d is empty", i)
		}
	}
	if p.expressionExpr != nil {
		if err := searchexpr.Validate(p.expressionExpr); err != nil {
			return err
		}
	}
	return nil
}

// Expression returns the validated expression source for forwarding over the
// wire. It returns an empty string for legacy term searches and nil Params.
func (p *Params) Expression() string {
	if p == nil {
		return ""
	}
	return p.expression
}

// HasSearchTerms returns true if either Terms or an expression has been set.
func (p *Params) HasSearchTerms() bool {
	return p != nil && (len(p.Terms) > 0 || p.expression != "")
}
