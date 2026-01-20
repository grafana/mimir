// Copyright 2025 Grafana Labs
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

import "github.com/grafana/regexp/syntax"

const (
	estimatedStringEqualityCost          = 1.0
	estimatedStringHasPrefixCost         = 1.0
	estimatedSliceContainsCostPerElement = 1.1
	estimatedMapContainsCost             = 4.0
)

// SingleMatchCost returns the fixed cost of running this matcher against an arbitrary label value..
func (m *Matcher) SingleMatchCost() float64 {
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
		if _, ok := m.re.stringMatcher.(*equalMultiStringMapMatcher); ok {
			return estimatedMapContainsCost
		}

		// If we have a short-circuit optimization to just check for literals in a particular order,
		// or the pattern matches everything, use that
		if _, ok := m.re.stringMatcher.(trueMatcher); ok {
			chars := 0

			for _, s := range m.re.contains {
				chars += len(s)
			}

			return max(float64(chars), 1) // 1 to ensure the cost is not 0 for matchers that match everything
		}

		// If we have a prefix optimization, use that
		if m.re.prefix != "" {
			return estimatedStringHasPrefixCost
		}

		return m.re.SingleMatchCost()
	}

	panic("labels.Matcher.SingleMatchCost: invalid match type " + m.Type.String() + m.String())
}

// EstimateSelectivity is the estimated fraction of all strings that it would match.
// If totalLabelValues is 0, then the selectivity is assumed to be 1.0.
// For example:
// * namespace!="" will match all values, so its selectivity is 1;
// * namespace=~"foo" will match only a single value, so its selectivity across 100 values is 0.01;
// * namespace=~"foo|bar" will match two values, so its selectivity across 100 values is 0.02.
func (m *Matcher) EstimateSelectivity(totalLabelValues uint64) float64 {
	if totalLabelValues == 0 {
		return 1.0
	}
	var selectivity float64
	// First, estimate the selectivity of the operation without taking into account whether it's an inclusive or exclusive matcher.
	switch m.Type {
	case MatchEqual, MatchNotEqual:
		if m.Value == "" {
			selectivity = 0
		} else {
			// For exact match, we expect to match exactly one value
			selectivity = 1.0 / float64(totalLabelValues)
		}

	case MatchRegexp, MatchNotRegexp:
		// If we have optimized set matches, we know exactly how many values we'll match.
		// We assume that all of them will be present in the corpus we're testing against.
		switch setMatchesSize := len(m.re.setMatches); {
		case setMatchesSize > 0:
			selectivity = float64(setMatchesSize) / float64(totalLabelValues)
		case m.Value == "":
			selectivity = 0
		case m.re.prefix != "":
			// For prefix matches, estimate we'll match ~10% of values.
			selectivity = 0.1
		case m.Value == ".+" || m.Value == ".*":
			selectivity = 1.0
		default:
			// For unoptimized regex, assume we'll match ~10% of values
			selectivity = 0.1
		}
	}
	selectivity = max(0.0, min(selectivity, 1.0))

	// Finally, we adjust for exclusive matchers.
	switch m.Type {
	case MatchNotEqual, MatchNotRegexp:
		selectivity = 1.0 - selectivity
	}
	return selectivity
}

func (m *FastRegexMatcher) SingleMatchCost() float64 {
	parsed := m.parsedRe
	if parsed == nil {
		var err error
		parsed, err = syntax.Parse(m.reString, syntax.Perl|syntax.DotNL)
		if err != nil {
			return estimatedStringEqualityCost
		}
	}
	return max(estimatedStringEqualityCost, costEstimate(parsed))
}

// TODO this doesn't account for backtracking, which can come with a large cost.
func costEstimate(re *syntax.Regexp) float64 {
	switch re.Op {
	case syntax.OpLiteral:
		return float64(len(re.Rune))
	case syntax.OpStar:
		return 10
	case syntax.OpAlternate:
		var total float64 = 1
		for _, sub := range re.Sub {
			total += costEstimate(sub)
		}
		return total
	case syntax.OpCapture:
		return costEstimate(re.Sub[0])
	case syntax.OpConcat:
		var total float64
		for _, sub := range re.Sub {
			total += costEstimate(sub)
		}
		return total
	case syntax.OpCharClass:
		return float64(len(re.Rune))
	default:
		return 1
	}
}
