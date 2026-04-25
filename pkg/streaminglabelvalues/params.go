// SPDX-License-Identifier: AGPL-3.0-only

package streaminglabelvalues

import (
	"fmt"
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
)

// Params is the wire-decoupled input to BuildFilter. Each gRPC server
// translates its proto SearchFilter into this struct before calling
// BuildFilter, so this package does not depend on any proto.
//
// Field names and defaults match Prometheus PR #18573's HTTP URL params so
// the eventual HTTP layer is a verbatim translation.
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
}

// Validate returns a non-nil error if Params has fields outside their
// permitted ranges. Empty Terms is permitted (yields a nil filter).
func (p *Params) Validate() error {
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
	for i, t := range p.Terms {
		if t == "" {
			return fmt.Errorf("search term %d is empty", i)
		}
	}
	return nil
}
