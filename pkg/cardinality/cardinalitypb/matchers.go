// SPDX-License-Identifier: AGPL-3.0-only

package cardinalitypb

import (
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
)

// ToLabelMatchers returns matchers as a CountQuery carries them.
func ToLabelMatchers(matchers []*labels.Matcher) []LabelMatcher {
	if len(matchers) == 0 {
		return nil
	}
	out := make([]LabelMatcher, 0, len(matchers))
	for _, m := range matchers {
		out = append(out, LabelMatcher{Type: m.Type, Name: m.Name, Value: m.Value})
	}
	return out
}

// FromLabelMatchers returns the matchers a CountQuery carries.
func FromLabelMatchers(matchers []LabelMatcher) ([]*labels.Matcher, error) {
	out := make([]*labels.Matcher, 0, len(matchers))
	for _, m := range matchers {
		if m.Type < labels.MatchEqual || m.Type > labels.MatchNotRegexp {
			return nil, fmt.Errorf("invalid matcher type %d", m.Type)
		}
		matcher, err := labels.NewMatcher(m.Type, m.Name, m.Value)
		if err != nil {
			return nil, err
		}
		out = append(out, matcher)
	}
	return out, nil
}
