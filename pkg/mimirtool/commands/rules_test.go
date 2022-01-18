// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/rules_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"testing"

	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/mimirtool/rules/rwrulefmt"
)

func TestCheckDuplicates(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []rwrulefmt.RuleGroup
		want []compareRuleType
	}{
		{
			name: "no duplicates",
			in: []rwrulefmt.RuleGroup{{
				RuleGroup: rulefmt.RuleGroup{
					Name: "rulegroup",
					Rules: []rulefmt.RuleNode{
						{
							Record: yaml.Node{Value: "up"},
							Expr:   yaml.Node{Value: "up==1"},
						},
						{
							Record: yaml.Node{Value: "down"},
							Expr:   yaml.Node{Value: "up==0"},
						},
					},
				},
				RWConfigs: []rwrulefmt.RemoteWriteConfig{},
			}},
			want: nil,
		},
		{
			name: "with duplicates",
			in: []rwrulefmt.RuleGroup{{
				RuleGroup: rulefmt.RuleGroup{
					Name: "rulegroup",
					Rules: []rulefmt.RuleNode{
						{
							Record: yaml.Node{Value: "up"},
							Expr:   yaml.Node{Value: "up==1"},
						},
						{
							Record: yaml.Node{Value: "up"},
							Expr:   yaml.Node{Value: "up==0"},
						},
					},
				},
				RWConfigs: []rwrulefmt.RemoteWriteConfig{},
			}},
			want: []compareRuleType{{metric: "up", label: map[string]string(nil)}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, checkDuplicates(tc.in))
		})
	}
}
