// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/matchers.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package util

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"
)

// SplitFiltersAndMatchers splits empty matchers off, which are treated as filters, see #220
func SplitFiltersAndMatchers(allMatchers []*labels.Matcher) (filters, matchers []*labels.Matcher) {
	for _, matcher := range allMatchers {
		// If a matcher matches "", we need to fetch possible chunks where
		// there is no value and will therefore not be in our label index.
		// e.g. {foo=""} and {foo!="bar"} both match "", so we need to return
		// chunks which do not have a foo label set. When looking entries in
		// the index, we should ignore this matcher to fetch all possible chunks
		// and then filter on the matcher after the chunks have been fetched.
		if matcher.Matches("") {
			filters = append(filters, matcher)
		} else {
			matchers = append(matchers, matcher)
		}
	}
	return
}

// MultiMatchersStringer implements Stringer for a slice of slices of Prometheus matchers. Useful for logging.
type MultiMatchersStringer [][]*labels.Matcher

func (s MultiMatchersStringer) String() string {
	var b strings.Builder
	for _, multi := range s {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('{')
		b.WriteString(MatchersStringer(multi).String())
		b.WriteByte('}')
	}

	return b.String()
}

// MatchersStringer implements Stringer for a slice of Prometheus matchers. Useful for logging.
type MatchersStringer []*labels.Matcher

func (s MatchersStringer) String() string {
	var b strings.Builder
	for _, m := range s {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(m.String())
	}

	return b.String()
}

// MergeMatchers merges matchers to optimise querying.
func MergeMatchers(input []*labels.Matcher, logger log.Logger) []*labels.Matcher {
	if len(input) == 0 {
		return nil
	}
	ms := make([]*labels.Matcher, len(input))
	copy(ms, input)
	sort.Sort(sortedLabelMatchers(ms))

	for i := 0; i < len(ms); i++ {
		j := i
		for j+1 < len(ms) && ms[i].Name == ms[j+1].Name {
			j++
		}
		if j == i {
			continue
		}
		span := j + 1 - i
		merged, err := mergeSortedMatchersForSameLabelSpan(ms[i : i+span])
		if err != nil {
			level.Warn(logger).Log("msg", "can't merge matchers", "input", MatchersStringer(input), "err", err)
			return input
		}
		if merged < span {
			copy(ms[i+merged:], ms[i+span:])
			shrinked := span - merged
			ms = ms[:len(ms)-shrinked]
		}
		i += merged - 1
	}

	return ms
}

// mergeSortedMatchersForSameLabelSpan merges matchers in a span of matchers for same label.
// This function expects matchers to be sorted as Equal, Regexp, NotEqual, NotRegexp.
// The result integer is the length of the merged span, which has been replaced on the same slice.
func mergeSortedMatchersForSameLabelSpan(span []*labels.Matcher) (int, error) {
	out := 0
	// Rewind trhough MatchEqual labels.
	for out < len(span) && (span[out].Type == labels.MatchEqual || span[out].Type == labels.MatchRegexp) {
		out++
	}
	// If none or only one left, return.
	if len(span)-out <= 1 {
		return len(span), nil
	}

	// Merge the rest of NotEqual and NotRegexp matchers into one.
	sb := strings.Builder{}
	seen := map[string]bool{}
	for i := out; i < len(span); i++ {
		m := span[i]
		var val string
		if m.Type == labels.MatchNotEqual {
			if m.Value == "" {
				val = ".+"
			} else {
				val = regexp.QuoteMeta(m.Value)
			}
		} else if m.Type == labels.MatchNotRegexp {
			val = m.Value
		} else {
			return 0, fmt.Errorf("unsupported matcher type %d %q", m.Type, m.Type)
		}

		if !seen[val] {
			if sb.Len() > 0 {
				sb.WriteByte('|')
			}
			sb.WriteString(val)
			seen[val] = true
		}
	}
	m, err := labels.NewMatcher(labels.MatchNotRegexp, span[out].Name, sb.String())
	if err != nil {
		return 0, err
	}
	span[out] = m
	return out + 1, nil
}

type sortedLabelMatchers []*labels.Matcher

var matchersTypeOrder = [...]int{
	labels.MatchEqual:     0,
	labels.MatchRegexp:    1,
	labels.MatchNotEqual:  2,
	labels.MatchNotRegexp: 3,
}

func (c sortedLabelMatchers) Less(i, j int) bool {
	if c[i].Name != c[j].Name {
		return c[i].Name < c[j].Name
	}
	if c[i].Type != c[j].Type {
		return matchersTypeOrder[c[i].Type] < matchersTypeOrder[c[j].Type]
	}
	return c[i].Value < c[j].Value
}

func (c sortedLabelMatchers) Len() int      { return len(c) }
func (c sortedLabelMatchers) Swap(i, j int) { c[i], c[j] = c[j], c[i] }
