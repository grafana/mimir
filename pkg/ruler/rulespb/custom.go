package rulespb

import "github.com/prometheus/prometheus/pkg/rulefmt"

// RuleGroupList contains a set of rule groups
type RuleGroupList []*RuleGroupDesc

// FormattedPrometheus returns the rule group list as a set of formatted rule groups mapped
// by namespace in the Prometheus rulefmt format without any additional fields.
func (l RuleGroupList) FormattedPrometheus() map[string][]rulefmt.RuleGroup {
	ruleMap := map[string][]rulefmt.RuleGroup{}
	for _, g := range l {
		rg := FromProto(g)
		if _, exists := ruleMap[g.Namespace]; !exists {
			ruleMap[g.Namespace] = []rulefmt.RuleGroup{rg.RuleGroup}
			continue
		}
		ruleMap[g.Namespace] = append(ruleMap[g.Namespace], rg.RuleGroup)

	}
	return ruleMap
}

// Formatted returns the rule group list as a set of formatted rule groups mapped
// by namespace
func (l RuleGroupList) Formatted() map[string][]RuleGroup {
	ruleMap := map[string][]RuleGroup{}
	for _, g := range l {
		if _, exists := ruleMap[g.Namespace]; !exists {
			ruleMap[g.Namespace] = []RuleGroup{FromProto(g)}
			continue
		}
		ruleMap[g.Namespace] = append(ruleMap[g.Namespace], FromProto(g))

	}
	return ruleMap
}
