package rulespb

import (
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/cortexpb" //lint:ignore faillint allowed to import other protobuf
)

type Serde interface {
	YAMLToProto(user string, namespace string, data []byte) (*RuleGroupDesc, error)
	RuleGroupDescToYAML(*RuleGroupDesc) interface{}
	RuleGroupListToYAML(RuleGroupList) interface{}
}

type DefaultSerde struct{}

func (*DefaultSerde) validateRuleGroup(g rulefmt.RuleGroup) []error {
	var errs []error

	if g.Name == "" {
		errs = append(errs, errors.New("invalid rules config: rule group name must not be empty"))
		return errs
	}

	if len(g.Rules) == 0 {
		errs = append(errs, fmt.Errorf("invalid rules config: rule group '%s' has no rules", g.Name))
		return errs
	}

	for i, r := range g.Rules {
		for _, err := range r.Validate() {
			var ruleName string
			if r.Alert.Value != "" {
				ruleName = r.Alert.Value
			} else {
				ruleName = r.Record.Value
			}
			errs = append(errs, &rulefmt.Error{
				Group:    g.Name,
				Rule:     i,
				RuleName: ruleName,
				Err:      err,
			})
		}
	}

	return errs
}

func (d *DefaultSerde) YAMLToProto(user string, namespace string, data []byte) (*RuleGroupDesc, error) {
	rg := rulefmt.RuleGroup{}
	err := yaml.Unmarshal(data, &rg)
	if err != nil {
		return nil, err
	}

	errs := d.validateRuleGroup(rg)
	if len(errs) > 0 {
		e := []string{}
		var err error
		for _, err = range errs {
			e = append(e, err.Error())
		}
		return nil, err
	}

	return ToProto(user, namespace, rg), nil
}

func (d *DefaultSerde) RuleGroupDescToYAML(desc *RuleGroupDesc) interface{} {
	return FromProto(desc)
}

func (d *DefaultSerde) RuleGroupListToYAML(rls RuleGroupList) interface{} {
	return rls.Formatted()
}

// ToProto transforms a formatted prometheus rulegroup to a rule group protobuf
func ToProto(user string, namespace string, rl rulefmt.RuleGroup) *RuleGroupDesc {
	rg := RuleGroupDesc{
		Name:      rl.Name,
		Namespace: namespace,
		Interval:  time.Duration(rl.Interval),
		Rules:     formattedRuleToProto(rl.Rules),
		User:      user,
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
			Labels:      cortexpb.FromLabelsToLabelAdapters(labels.FromMap(rls[i].Labels)),
			Annotations: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(rls[i].Annotations)),
		}
	}

	return rules
}

// FromProto generates a rulefmt RuleGroup
func FromProto(rg *RuleGroupDesc) rulefmt.RuleGroup {
	formattedRuleGroup := rulefmt.RuleGroup{
		Name:     rg.GetName(),
		Interval: model.Duration(rg.Interval),
		Rules:    make([]rulefmt.RuleNode, len(rg.GetRules())),
	}

	for i, rl := range rg.GetRules() {
		exprNode := yaml.Node{}
		exprNode.SetString(rl.GetExpr())

		newRule := rulefmt.RuleNode{
			Expr:        exprNode,
			Labels:      cortexpb.FromLabelAdaptersToLabels(rl.Labels).Map(),
			Annotations: cortexpb.FromLabelAdaptersToLabels(rl.Annotations).Map(),
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
