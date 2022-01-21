// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/rules/compare_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package rules

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/prometheus/prometheus/model/rulefmt"
	yaml "gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/mimirtool/rules/rwrulefmt"
)

func Test_rulesEqual(t *testing.T) {
	tests := []struct {
		name string
		a    *rulefmt.RuleNode
		b    *rulefmt.RuleNode
		want bool
	}{
		{
			name: "rule_node_identical",
			a: &rulefmt.RuleNode{
				Record:      yaml.Node{Value: "one"},
				Expr:        yaml.Node{Value: "up"},
				Annotations: map[string]string{"a": "b", "c": "d"},
				Labels:      nil,
			},
			b: &rulefmt.RuleNode{
				Record:      yaml.Node{Value: "one"},
				Expr:        yaml.Node{Value: "up"},
				Annotations: map[string]string{"c": "d", "a": "b"},
				Labels:      nil,
			},
			want: true,
		},
		{
			name: "rule_node_diff",
			a: &rulefmt.RuleNode{
				Record: yaml.Node{Value: "one"},
				Expr:   yaml.Node{Value: "up"},
			},
			b: &rulefmt.RuleNode{
				Record: yaml.Node{Value: "two"},
				Expr:   yaml.Node{Value: "up"},
			},
			want: false,
		},
		{
			name: "rule_node_annotations_diff",
			a: &rulefmt.RuleNode{
				Record:      yaml.Node{Value: "one"},
				Expr:        yaml.Node{Value: "up"},
				Annotations: map[string]string{"a": "b"},
			},
			b: &rulefmt.RuleNode{
				Record:      yaml.Node{Value: "one", Column: 10},
				Expr:        yaml.Node{Value: "up"},
				Annotations: map[string]string{"c": "d"},
			},
			want: false,
		},
		{
			name: "rule_node_annotations_nil_diff",
			a: &rulefmt.RuleNode{
				Record:      yaml.Node{Value: "one"},
				Expr:        yaml.Node{Value: "up"},
				Annotations: map[string]string{"a": "b"},
			},
			b: &rulefmt.RuleNode{
				Record:      yaml.Node{Value: "one", Column: 10},
				Expr:        yaml.Node{Value: "up"},
				Annotations: nil,
			},
			want: false,
		},
		{
			name: "rule_node_yaml_diff",
			a: &rulefmt.RuleNode{
				Record: yaml.Node{Value: "one"},
				Expr:   yaml.Node{Value: "up"},
			},
			b: &rulefmt.RuleNode{
				Record: yaml.Node{Value: "one", Column: 10},
				Expr:   yaml.Node{Value: "up"},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rulesEqual(tt.a, tt.b); got != tt.want {
				t.Errorf("rulesEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompareGroups(t *testing.T) {
	tests := []struct {
		name        string
		groupOne    rwrulefmt.RuleGroup
		groupTwo    rwrulefmt.RuleGroup
		expectedErr error
	}{
		{
			name: "identical configs",
			groupOne: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name: "example_group",
					Rules: []rulefmt.RuleNode{
						{
							Record:      yaml.Node{Value: "one"},
							Expr:        yaml.Node{Value: "up"},
							Annotations: map[string]string{"a": "b", "c": "d"},
							Labels:      nil,
						},
					},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name: "example_group",
					Rules: []rulefmt.RuleNode{
						{
							Record:      yaml.Node{Value: "one"},
							Expr:        yaml.Node{Value: "up"},
							Annotations: map[string]string{"a": "b", "c": "d"},
							Labels:      nil,
						},
					},
				},
			},
			expectedErr: nil,
		},
		{
			name: "differently ordered source tenants (should still be equivalent)",
			groupOne: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:          "example_group",
					SourceTenants: []string{"tenant-2", "tenant-1"},
					Rules: []rulefmt.RuleNode{
						{
							Record:      yaml.Node{Value: "one"},
							Expr:        yaml.Node{Value: "up"},
							Annotations: map[string]string{"a": "b", "c": "d"},
							Labels:      nil,
						},
					},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:          "example_group",
					SourceTenants: []string{"tenant-1", "tenant-2"},
					Rules: []rulefmt.RuleNode{
						{
							Record:      yaml.Node{Value: "one"},
							Expr:        yaml.Node{Value: "up"},
							Annotations: map[string]string{"a": "b", "c": "d"},
							Labels:      nil,
						},
					},
				},
			},
			expectedErr: nil,
		},
		{
			name: "different rule length",
			groupOne: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name: "example_group",
					Rules: []rulefmt.RuleNode{
						{
							Record:      yaml.Node{Value: "one"},
							Expr:        yaml.Node{Value: "up"},
							Annotations: map[string]string{"a": "b", "c": "d"},
							Labels:      nil,
						},
					},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name: "example_group",
					Rules: []rulefmt.RuleNode{
						{
							Record:      yaml.Node{Value: "one"},
							Expr:        yaml.Node{Value: "up"},
							Annotations: map[string]string{"a": "b", "c": "d"},
							Labels:      nil,
						},
						{
							Record:      yaml.Node{Value: "one"},
							Expr:        yaml.Node{Value: "up"},
							Annotations: map[string]string{"a": "b", "c": "d"},
							Labels:      nil,
						},
					},
				},
			},
			expectedErr: errDiffRuleLen,
		},
		{
			name: "identical rw configs",
			groupOne: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name: "example_group",
					Rules: []rulefmt.RuleNode{
						{
							Record:      yaml.Node{Value: "one"},
							Expr:        yaml.Node{Value: "up"},
							Annotations: map[string]string{"a": "b", "c": "d"},
							Labels:      nil,
						},
					},
				},
				RWConfigs: []rwrulefmt.RemoteWriteConfig{
					{URL: "localhost"},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name: "example_group",
					Rules: []rulefmt.RuleNode{
						{
							Record:      yaml.Node{Value: "one"},
							Expr:        yaml.Node{Value: "up"},
							Annotations: map[string]string{"a": "b", "c": "d"},
							Labels:      nil,
						},
					},
				},
				RWConfigs: []rwrulefmt.RemoteWriteConfig{
					{URL: "localhost"},
				},
			},
			expectedErr: nil,
		},
		{
			name: "different rw config lengths",
			groupOne: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name: "example_group",
					Rules: []rulefmt.RuleNode{
						{
							Record:      yaml.Node{Value: "one"},
							Expr:        yaml.Node{Value: "up"},
							Annotations: map[string]string{"a": "b", "c": "d"},
							Labels:      nil,
						},
					},
				},
				RWConfigs: []rwrulefmt.RemoteWriteConfig{
					{URL: "localhost"},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name: "example_group",
					Rules: []rulefmt.RuleNode{
						{
							Record:      yaml.Node{Value: "one"},
							Expr:        yaml.Node{Value: "up"},
							Annotations: map[string]string{"a": "b", "c": "d"},
							Labels:      nil,
						},
					},
				},
				RWConfigs: []rwrulefmt.RemoteWriteConfig{
					{URL: "localhost"},
					{URL: "localhost"},
				},
			},
			expectedErr: errDiffRWConfigs,
		},
		{
			name: "different rw configs",
			groupOne: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name: "example_group",
					Rules: []rulefmt.RuleNode{
						{
							Record:      yaml.Node{Value: "one"},
							Expr:        yaml.Node{Value: "up"},
							Annotations: map[string]string{"a": "b", "c": "d"},
							Labels:      nil,
						},
					},
				},
				RWConfigs: []rwrulefmt.RemoteWriteConfig{
					{URL: "localhost"},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name: "example_group",
					Rules: []rulefmt.RuleNode{
						{
							Record:      yaml.Node{Value: "one"},
							Expr:        yaml.Node{Value: "up"},
							Annotations: map[string]string{"a": "b", "c": "d"},
							Labels:      nil,
						},
					},
				},
				RWConfigs: []rwrulefmt.RemoteWriteConfig{
					{URL: "localhost2"},
				},
			},
			expectedErr: errDiffRWConfigs,
		},
		{
			name: "different source tenants",
			groupOne: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:          "example_group",
					SourceTenants: []string{"tenant-1", "tenant-3"},
					Rules: []rulefmt.RuleNode{
						{
							Record:      yaml.Node{Value: "one"},
							Expr:        yaml.Node{Value: "up"},
							Annotations: map[string]string{"a": "b", "c": "d"},
							Labels:      nil,
						},
					},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:          "example_group",
					SourceTenants: []string{"tenant-1", "tenant-2"},
					Rules: []rulefmt.RuleNode{
						{
							Record:      yaml.Node{Value: "one"},
							Expr:        yaml.Node{Value: "up"},
							Annotations: map[string]string{"a": "b", "c": "d"},
							Labels:      nil,
						},
					},
				},
			},
			expectedErr: errDiffSourceTenants,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualErr := CompareGroups(tt.groupOne, tt.groupTwo)
			assert.ErrorIs(t, actualErr, tt.expectedErr)
		})
	}
}
