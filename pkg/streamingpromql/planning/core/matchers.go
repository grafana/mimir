// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"strings"

	"github.com/prometheus/prometheus/model/labels"
)

func (m *LabelMatcher) Equal(other *LabelMatcher) bool {
	return m.Type == other.Type && m.Name == other.Name && m.Value == other.Value
}

func CompareMatchers(firstName, secondName string, firstType, secondType labels.MatchType, firstValue, secondValue string) int {
	if firstName != secondName {
		return strings.Compare(firstName, secondName)
	}

	if firstType != secondType {
		return int(firstType - secondType)
	}

	return strings.Compare(firstValue, secondValue)
}

func FormatMatchers(builder *strings.Builder, matchers []*LabelMatcher) {
	builder.WriteRune('{')

	for i, m := range matchers {
		if i > 0 {
			builder.WriteString(", ")
		}

		// Convert to the Prometheus type so we can use its String().
		promMatcher := labels.Matcher{Type: m.Type, Name: m.Name, Value: m.Value}
		builder.WriteString(promMatcher.String())
	}

	builder.WriteRune('}')
}
