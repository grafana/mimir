// SPDX-License-Identifier: AGPL-3.0-only

package model

import "github.com/prometheus/prometheus/model/labels"

// Truth returns the exact number of distinct series matching every matcher
// with at least one sample in [start, end). It is the answer every other
// counting method in this POC is checked against.
func (m *Model) Truth(matchers []*labels.Matcher, start, end int64) int {
	count := 0
	for _, s := range m.Series {
		if s.Live(start, end) && matches(s.Labels, matchers) {
			count++
		}
	}
	return count
}

func matches(lbls labels.Labels, matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(lbls.Get(m.Name)) {
			return false
		}
	}
	return true
}
