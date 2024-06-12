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

// ToProto transforms a formatted prometheus rulegroup to a rule group protobuf.
// This function does a 1:1 mapping between the two data models.
func ToProto(user string, namespace string, rl rulefmt.RuleGroup) *RuleGroupDesc {
	rg := RuleGroupDesc{
		Name:                          rl.Name,
		Namespace:                     namespace,
		Interval:                      time.Duration(rl.Interval),
		Rules:                         formattedRuleToProto(rl.Rules),
		User:                          user,
		SourceTenants:                 rl.SourceTenants,
		AlignEvaluationTimeOnInterval: rl.AlignEvaluationTimeOnInterval,
	}

	// This function is designed to do a 1:1 mapping between the data models, so we
	// preserve QueryOffset and EvaluationDelay as they've been set in input. This
	// guarantees that when we'll convert RuleGroupDesc back into rulefmt.RuleGroup
	// we'll get the same model we originally had in input.
	if rl.QueryOffset != nil && *rl.QueryOffset > 0 {
		rg.QueryOffset = time.Duration(*rl.QueryOffset)
	}
	//nolint:staticcheck // We want to intentionally access a deprecated field
	if rl.EvaluationDelay != nil && *rl.EvaluationDelay > 0 {
		rg.EvaluationDelay = time.Duration(*rl.EvaluationDelay)
	}

	return &rg
}

func formattedRuleToProto(rls []rulefmt.RuleNode) []*RuleDesc {
	rules := make([]*RuleDesc, len(rls))
	for i := range rls {
		rules[i] = &RuleDesc{
			Expr:          rls[i].Expr.Value,
			Record:        rls[i].Record.Value,
			Alert:         rls[i].Alert.Value,
			For:           time.Duration(rls[i].For),
			KeepFiringFor: time.Duration(rls[i].KeepFiringFor),
			Labels:        mimirpb.FromLabelsToLabelAdapters(labels.FromMap(rls[i].Labels)),
			Annotations:   mimirpb.FromLabelsToLabelAdapters(labels.FromMap(rls[i].Annotations)),
		}
	}

	return rules
}

// FromProto generates a rulefmt RuleGroup. This function does a 1:1 mapping between the two data models.
func FromProto(rg *RuleGroupDesc) rulefmt.RuleGroup {
	formattedRuleGroup := rulefmt.RuleGroup{
		Name:                          rg.GetName(),
		Interval:                      model.Duration(rg.Interval),
		Rules:                         make([]rulefmt.RuleNode, len(rg.GetRules())),
		SourceTenants:                 rg.GetSourceTenants(),
		AlignEvaluationTimeOnInterval: rg.GetAlignEvaluationTimeOnInterval(),
	}

	// This function is designed to do a 1:1 mapping between the data models, so we
	// preserve QueryOffset and EvaluationDelay as they've been set in input.
	if rg.QueryOffset > 0 {
		formattedRuleGroup.QueryOffset = pointerOf[model.Duration](model.Duration(rg.QueryOffset))
	}
	//nolint:staticcheck // We want to intentionally access a deprecated field
	if rg.EvaluationDelay > 0 {
		formattedRuleGroup.EvaluationDelay = pointerOf[model.Duration](model.Duration(rg.EvaluationDelay))
	}

	for i, rl := range rg.GetRules() {
		exprNode := yaml.Node{}
		exprNode.SetString(rl.GetExpr())

		newRule := rulefmt.RuleNode{
			Expr:          exprNode,
			Labels:        mimirpb.FromLabelAdaptersToLabels(rl.Labels).Map(),
			Annotations:   mimirpb.FromLabelAdaptersToLabels(rl.Annotations).Map(),
			For:           model.Duration(rl.GetFor()),
			KeepFiringFor: model.Duration(rl.GetKeepFiringFor()),
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

func pointerOf[T any](value T) *T {
	return &value
}
