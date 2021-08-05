package rulespb

import (
	"time"

	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/types"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/cortexpb" //lint:ignore faillint allowed to import other protobuf
	util_log "github.com/grafana/mimir/pkg/util/log"
)

// Wrapper around Prometheus rulefmt.

// RuleGroups is a set of rule groups that are typically exposed in a file.
//
// This is a copy/paste of the type in Prometheus so that we unmarshall a rules
// YAML file to a list of our custom extended RuleGroup objects.
type RuleGroups struct {
	Groups []RuleGroup `yaml:"groups"`
}

// RuleGroup is a list of sequentially evaluated recording and alerting rules. This also
// contains a simple remote-write config that is supported in GEM when a valid license is present.
type RuleGroup struct {
	rulefmt.RuleGroup `yaml:",inline"`
	// RWConfigs is used by the remote write forwarding ruler
	RWConfigs []RemoteWriteConfig `yaml:"remote_write,omitempty"`
}

// ToProto transforms a formatted prometheus rulegroup to a rule group protobuf
func ToProto(user string, namespace string, rl RuleGroup) *RuleGroupDesc {
	var rwConfigs []*types.Any
	for _, cfg := range rl.RWConfigs {
		any, err := types.MarshalAny(&cfg)
		if err != nil {
			level.Error(util_log.Logger).Log("msg",
				"error marshalling remote write config",
				"user",
				user,
				"namespace",
				namespace,
				"group",
				rl.Name)
			return nil

		}
		rwConfigs = append(rwConfigs, any)
	}

	rg := RuleGroupDesc{
		Name:      rl.Name,
		Namespace: namespace,
		Interval:  time.Duration(rl.Interval),
		Rules:     formattedRuleToProto(rl.Rules),
		User:      user,
		Options:   rwConfigs,
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
func FromProto(rg *RuleGroupDesc) (RuleGroup) {
	var rwConfigs []RemoteWriteConfig
	for _, option := range rg.Options {
		rwConfig := &RemoteWriteConfig{}
		err := types.UnmarshalAny(option, rwConfig)
		if err != nil {
			level.Error(util_log.Logger).Log("msg", "error unmarshalling remote write config", "group", rg.Name)
			continue
		}
		rwConfigs = append(rwConfigs, *rwConfig)
	}

	formattedRuleGroup := RuleGroup{
		RuleGroup: rulefmt.RuleGroup{
			Name:     rg.GetName(),
			Interval: model.Duration(rg.Interval),
			Rules:    make([]rulefmt.RuleNode, len(rg.GetRules())),
		},
		RWConfigs: rwConfigs,
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
