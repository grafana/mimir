// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/rulespb/custom.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package rulespb

import "github.com/prometheus/prometheus/model/rulefmt"

// RuleGroupList contains a set of rule groups
type RuleGroupList []*RuleGroupDesc

func (l RuleGroupList) Equal(that RuleGroupList) bool {
	if that == nil {
		return l == nil
	}

	if len(l) != len(that) {
		return false
	}

	if len(l) == 0 || &l[0] == &that[0] {
		return true
	}

	for i := range l {
		if !l[i].Equal(that[i]) {
			return false
		}
	}
	return true
}

// Formatted returns the rule group list as a set of formatted rule groups mapped
// by namespace
func (l RuleGroupList) Formatted() map[string][]rulefmt.RuleGroup {
	ruleMap := map[string][]rulefmt.RuleGroup{}
	for _, g := range l {
		if _, exists := ruleMap[g.Namespace]; !exists {
			ruleMap[g.Namespace] = []rulefmt.RuleGroup{FromProto(g)}
			continue
		}
		ruleMap[g.Namespace] = append(ruleMap[g.Namespace], FromProto(g))

	}
	return ruleMap
}

func (l RuleGroupList) FormattedProto() map[string]RuleGroupList {
	groupMap := map[string]RuleGroupList{}
	for _, g := range l {
		if _, exists := groupMap[g.Namespace]; !exists {
			groupMap[g.Namespace] = []*RuleGroupDesc{g}
		}
		groupMap[g.Namespace] = append(groupMap[g.Namespace], g)
	}
	return groupMap
}
