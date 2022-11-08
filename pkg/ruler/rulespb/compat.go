// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/rulespb/compat.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package rulespb

import (
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/mimirpb" //lint:ignore faillint allowed to import other protobuf
)

// ToProto transforms a formatted prometheus rulegroup to a rule group protobuf
func ToProto(user string, namespace string, rl rulefmt.RuleGroup) *RuleGroupDesc {
	rg := RuleGroupDesc{
		Name:          rl.Name,
		Namespace:     namespace,
		Interval:      time.Duration(rl.Interval),
		Rules:         formattedRuleToProto(rl.Rules),
		User:          user,
		SourceTenants: rl.SourceTenants,
	}
	if rl.EvaluationDelay != nil && *rl.EvaluationDelay > 0 {
		rg.EvaluationDelay = time.Duration(*rl.EvaluationDelay)
	}
	return &rg
}

func formattedRuleToProto(rls []rulefmt.RuleNode) []*RuleDesc {
	rules := make([]*RuleDesc, len(rls))
	for i := range rls {
		rules[i] = &RuleDesc{
			Expr:        rls[i].Expr.Value,
			Record:      rls[i].Record.Value,
			Alert:       rls[i].Alert.Value,
			For:         time.Duration(rls[i].For),
			Labels:      mimirpb.FromLabelsToLabelAdapters(labels.FromMap(rls[i].Labels)),
			Annotations: mimirpb.FromLabelsToLabelAdapters(labels.FromMap(rls[i].Annotations)),
		}
	}

	return rules
}

// FromProto generates a rulefmt RuleGroup
func FromProto(rg *RuleGroupDesc) rulefmt.RuleGroup {
	formattedRuleGroup := rulefmt.RuleGroup{
		Name:          rg.GetName(),
		Interval:      model.Duration(rg.Interval),
		Rules:         make([]rulefmt.RuleNode, len(rg.GetRules())),
		SourceTenants: rg.GetSourceTenants(),
	}
	if rg.EvaluationDelay > 0 {
		formattedRuleGroup.EvaluationDelay = new(model.Duration)
		*formattedRuleGroup.EvaluationDelay = model.Duration(rg.EvaluationDelay)
	}

	for i, rl := range rg.GetRules() {
		exprNode := yaml.Node{}
		exprNode.SetString(rl.GetExpr())

		newRule := rulefmt.RuleNode{
			Expr:        exprNode,
			Labels:      mimirpb.FromLabelAdaptersToLabels(rl.Labels).Map(),
			Annotations: mimirpb.FromLabelAdaptersToLabels(rl.Annotations).Map(),
			For:         model.Duration(rl.GetFor()),
		}

		if rl.GetRecord() != "" {
			recordNode := yaml.Node{}
			recordNode.SetString(rl.GetRecord())
			newRule.Record = recordNode
		} else {
			alertNode := yaml.Node{}
			alertNode.SetString(rl.GetAlert())
			newRule.Alert = alertNode
		}

		formattedRuleGroup.Rules[i] = newRule
	}

	return formattedRuleGroup
}
