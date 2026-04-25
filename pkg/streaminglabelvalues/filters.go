package streaminglabelvalues

import (
	"errors"
	"strings"
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
