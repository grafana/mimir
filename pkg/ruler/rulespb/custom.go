// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/rulespb/custom.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package rulespb

import (
	"slices"

	"github.com/prometheus/prometheus/model/rulefmt"
)

// RuleGroupList contains a set of rule groups
type RuleGroupList []*RuleGroupDesc

// Equal returns true if both lists have equal groups in the same order.
func (l RuleGroupList) Equal(that RuleGroupList) bool {
	return slices.EqualFunc(l, that, func(a, b *RuleGroupDesc) bool { return a.Equal(b) })
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

// FormattedProto returns the rule group list mapped by namespace.
func (l RuleGroupList) FormattedProto() map[string]RuleGroupList {
	groupMap := map[string]RuleGroupList{}
	for _, g := range l {
		if _, exists := groupMap[g.Namespace]; !exists {
			groupMap[g.Namespace] = []*RuleGroupDesc{g}
			continue
		}
		groupMap[g.Namespace] = append(groupMap[g.Namespace], g)
	}
	return groupMap
}
