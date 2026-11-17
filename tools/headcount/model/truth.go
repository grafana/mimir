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

// TruthBy is Truth broken down by one label: for every value of label
// among the series matching matchers and live in [start, end), the number
// of such series. Series without the label are counted under "". It scans
// the population once, so a breakdown over many values costs the same as
// one Truth call.
func (m *Model) TruthBy(matchers []*labels.Matcher, label string, start, end int64) map[string]int {
	out := map[string]int{}
	for _, s := range m.Series {
		if s.Live(start, end) && matches(s.Labels, matchers) {
			out[s.Labels.Get(label)]++
		}
	}
	return out
}
