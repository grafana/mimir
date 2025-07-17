package labels

import "github.com/grafana/regexp/syntax"

const (
	// TODO verify relative magnitude of these costs.
	estimatedStringEqualityCost          = 1.0
	estimatedStringHasPrefixCost         = 0.5
	estimatedSliceContainsCostPerElement = 1.0
	estimatedMapContainsCostPerElement   = 0.01
	estimatedRegexMatchCost              = 10.0
)

// SingleMatchCost returns the fixed cost of running this matcher against an arbitrary label value..
// TODO benchmark relative cost of different matchers.
// TODO use the complexity of the regex string as a cost.
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
		if mm, ok := m.re.stringMatcher.(*equalMultiStringMapMatcher); ok {
			return estimatedMapContainsCostPerElement*float64(len(mm.values)) + estimatedStringEqualityCost
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
// For example:
// * namespace!="" will match all values, so its selectivity is 1;
// * namespace=~"foo" will match only a single value, so its selectivity across 100 values is 0.01;
// * namespace=~"foo|bar" will match two values, so its selectivity across 100 values is 0.02.
func (m *Matcher) EstimateSelectivity(totalLabelValues uint64) float64 {
	var selectivity float64
	// First estimate the selectivity of the operation without taking into account whether it's an inclusive or exclusive matcher.
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
	}

	switch m.Type {
	case MatchNotEqual, MatchNotRegexp:
		selectivity = 1.0 - selectivity
	}
	return selectivity
}

func (m *FastRegexMatcher) SingleMatchCost() float64 {
	parsed, err := syntax.Parse(m.reString, syntax.Perl|syntax.DotNL)
	if err != nil {
		return 0
	}
	return costEstimate(parsed)
}

// TODO this doesn't account for backtracking, which can come with a large cost.
func costEstimate(re *syntax.Regexp) float64 {
	switch re.Op {
	case syntax.OpBeginText:
		return 1
	case syntax.OpEndText:
		return 1
	case syntax.OpLiteral:
		return float64(len(re.Rune))
	case syntax.OpEmptyMatch:
		return 1
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
