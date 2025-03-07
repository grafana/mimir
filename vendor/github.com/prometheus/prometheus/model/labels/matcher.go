// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package labels

import (
	"bytes"
	"strconv"
)

// MatchType is an enum for label matching types.
type MatchType int

// Possible MatchTypes.
const (
	MatchEqual MatchType = iota
	MatchNotEqual
	MatchRegexp
	MatchNotRegexp
)

var matchTypeToStr = [...]string{
	MatchEqual:     "=",
	MatchNotEqual:  "!=",
	MatchRegexp:    "=~",
	MatchNotRegexp: "!~",
}

func (m MatchType) String() string {
	if m < MatchEqual || m > MatchNotRegexp {
		panic("unknown match type")
	}
	return matchTypeToStr[m]
}

// Matcher models the matching of a label.
type Matcher struct {
	Type  MatchType
	Name  string
	Value string

	re *FastRegexMatcher
}

// NewMatcher returns a matcher object.
func NewMatcher(t MatchType, n, v string) (*Matcher, error) {
	m := &Matcher{
		Type:  t,
		Name:  n,
		Value: v,
	}
	if t == MatchRegexp || t == MatchNotRegexp {
		re, err := NewFastRegexMatcher(v)
		if err != nil {
			return nil, err
		}
		m.re = re
	}
	return m, nil
}

// MustNewMatcher panics on error - only for use in tests!
func MustNewMatcher(mt MatchType, name, val string) *Matcher {
	m, err := NewMatcher(mt, name, val)
	if err != nil {
		panic(err)
	}
	return m
}

func (m *Matcher) String() string {
	// Start a buffer with a pre-allocated size on stack to cover most needs.
	var bytea [1024]byte
	b := bytes.NewBuffer(bytea[:0])

	if m.shouldQuoteName() {
		b.Write(strconv.AppendQuote(b.AvailableBuffer(), m.Name))
	} else {
		b.WriteString(m.Name)
	}
	b.WriteString(m.Type.String())
	b.Write(strconv.AppendQuote(b.AvailableBuffer(), m.Value))

	return b.String()
}

func (m *Matcher) shouldQuoteName() bool {
	for i, c := range m.Name {
		if c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (i > 0 && c >= '0' && c <= '9') {
			continue
		}
		return true
	}
	return len(m.Name) == 0
}

// Matches returns whether the matcher matches the given string value.
func (m *Matcher) Matches(s string) bool {
	switch m.Type {
	case MatchEqual:
		return s == m.Value
	case MatchNotEqual:
		return s != m.Value
	case MatchRegexp:
		return m.re.MatchString(s)
	case MatchNotRegexp:
		return !m.re.MatchString(s)
	}
	panic("labels.Matcher.Matches: invalid match type")
}

// Inverse returns a matcher that matches the opposite.
func (m *Matcher) Inverse() (*Matcher, error) {
	switch m.Type {
	case MatchEqual:
		return NewMatcher(MatchNotEqual, m.Name, m.Value)
	case MatchNotEqual:
		return NewMatcher(MatchEqual, m.Name, m.Value)
	case MatchRegexp:
		return NewMatcher(MatchNotRegexp, m.Name, m.Value)
	case MatchNotRegexp:
		return NewMatcher(MatchRegexp, m.Name, m.Value)
	}
	panic("labels.Matcher.Matches: invalid match type")
}

// GetRegexString returns the regex string.
func (m *Matcher) GetRegexString() string {
	if m.re == nil {
		return ""
	}
	return m.re.GetRegexString()
}

// SetMatches returns a set of equality matchers for the current regex matchers if possible.
// For examples the regexp `a(b|f)` will returns "ab" and "af".
// Returns nil if we can't replace the regexp by only equality matchers.
func (m *Matcher) SetMatches() []string {
	if m.re == nil {
		return nil
	}
	return m.re.SetMatches()
}

// Prefix returns the required prefix of the value to match, if possible.
// It will be empty if it's an equality matcher or if the prefix can't be determined.
func (m *Matcher) Prefix() string {
	if m.re == nil {
		return ""
	}
	return m.re.prefix
}

// IsRegexOptimized returns whether regex is optimized.
func (m *Matcher) IsRegexOptimized() bool {
	if m.re == nil {
		return false
	}
	return m.re.IsOptimized()
}

const (
	estimatedStringEqualityCost          = 1.0
	estimatedStingHasPrefixCost          = 0.5
	estimatedSliceContainsCostPerElement = 1.0
	estimatedMapContainsCostPerElement   = 0.01
	estimatedRegexMatchCost              = 10.0
)

// FixedCost returns the fixed cost of running this matcher against an arbitrary label value.
// TODO dimitarvdimitrov benchmark relative cost of different matchers
// TODO dimitarvdimitrov use the complexity of the regex string as a cost
func (m *Matcher) FixedCost() float64 {
	switch m.Type {
	case MatchEqual, MatchNotEqual:
		// String equality/inequality comparison is simple
		return estimatedStringEqualityCost
	case MatchRegexp, MatchNotRegexp:
		// If we have optimized set matches, use those
		if len(m.re.setMatches) > 0 {
			return estimatedSliceContainsCostPerElement * float64(len(m.re.setMatches))
		}

		// If we have a string matcher with a map, use that
		if mm, ok := m.re.stringMatcher.(*equalMultiStringMapMatcher); ok {
			return estimatedMapContainsCostPerElement*float64(len(mm.values)) + estimatedStringEqualityCost
		}

		// If we have a prefix optimization, use that
		if m.re.prefix != "" {
			return estimatedStingHasPrefixCost
		}

		// Fallback to default cost for unoptimized regex
		return estimatedRegexMatchCost
	}

	panic("labels.Matcher.FixedCost: invalid match type " + m.Type.String())
}

// EstimateSelectivity is the estimated fraction of all strings that it would match.
// For example
// * namespace!="" will match all values, so its selectivity is 1
// * namespace=~"foo" will match only a single value, so its selectivity across 100 values is 0.01
// * namespace=~"foo|bar" will match two values, so its selectivity across 100 values is 0.02
func (m *Matcher) EstimateSelectivity(totalLabelValues int64) float64 {
	var selectivity float64
	switch m.Type {
	case MatchEqual, MatchNotEqual:
		// For exact match, we expect to match exactly one value
		selectivity = 1.0 / float64(totalLabelValues)

	case MatchRegexp, MatchNotRegexp:
		// If we have optimized set matches, we know exactly how many values we'll match.
		// We assume that all of them will be present in the corpus we're testing against.
		if setMatchesSize := len(m.re.setMatches); setMatchesSize > 0 {
			selectivity = float64(setMatchesSize) / float64(totalLabelValues)
			break
		}

		// For prefix matches, estimate we'll match ~10% of values.
		if m.re.prefix != "" {
			selectivity = 0.1
			break
		}

		// For unoptimized regex, assume we'll match ~10% of values
		selectivity = 0.1
		break
	}

	switch m.Type {
	case MatchNotEqual, MatchNotRegexp:
		selectivity = 1.0 - selectivity
	}
	return selectivity
}
