// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/rules/compare_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package rules

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/mimirtool/rules/rwrulefmt"
)

func TestNamespaceChange_ToOperations(t *testing.T) {
	// Create some fixtures.
	group1A := rwrulefmt.RuleGroup{RuleGroup: rulefmt.RuleGroup{Name: "group-1", Interval: 10}}
	group1B := rwrulefmt.RuleGroup{RuleGroup: rulefmt.RuleGroup{Name: "group-1", Interval: 20}}
	group2A := rwrulefmt.RuleGroup{RuleGroup: rulefmt.RuleGroup{Name: "group-2", Interval: 10}}
	group2B := rwrulefmt.RuleGroup{RuleGroup: rulefmt.RuleGroup{Name: "group-2", Interval: 20}}
	group3A := rwrulefmt.RuleGroup{RuleGroup: rulefmt.RuleGroup{Name: "group-3", Interval: 30}}
	group4A := rwrulefmt.RuleGroup{RuleGroup: rulefmt.RuleGroup{Name: "group-4", Interval: 40}}
	group5A := rwrulefmt.RuleGroup{RuleGroup: rulefmt.RuleGroup{Name: "group-5", Interval: 50}}

	change := NamespaceChange{
		Namespace: "test",
		GroupsUpdated: []UpdatedRuleGroup{
			{
				Original: group1A,
				New:      group1B,
			}, {
				Original: group2A,
				New:      group2B,
			},
		},
		GroupsCreated: []rwrulefmt.RuleGroup{group3A},
		GroupsDeleted: []rwrulefmt.RuleGroup{group4A, group5A},
	}

	assert.Equal(t, []NamespaceChangeOperation{
		{
			Namespace: "test",
			State:     Created,
			RuleGroup: group3A,
		}, {
			Namespace: "test",
			State:     Updated,
			RuleGroup: group1B,
		}, {
			Namespace: "test",
			State:     Updated,
			RuleGroup: group2B,
		}, {
			Namespace: "test",
			State:     Deleted,
			RuleGroup: group4A,
		}, {
			Namespace: "test",
			State:     Deleted,
			RuleGroup: group5A,
		},
	}, change.ToOperations())
}

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
	ruleOne := rulefmt.RuleNode{
		Record:      yaml.Node{Value: "one"},
		Expr:        yaml.Node{Value: "up"},
		Annotations: map[string]string{"a": "b", "c": "d"},
		Labels:      nil,
	}

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
					Name:  "example_group",
					Rules: []rulefmt.RuleNode{ruleOne},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:  "example_group",
					Rules: []rulefmt.RuleNode{ruleOne},
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
					Rules:         []rulefmt.RuleNode{ruleOne},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:          "example_group",
					SourceTenants: []string{"tenant-1", "tenant-2"},
					Rules:         []rulefmt.RuleNode{ruleOne},
				},
			},
			expectedErr: nil,
		},
		{
			name: "different rule length",
			groupOne: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:  "example_group",
					Rules: []rulefmt.RuleNode{ruleOne},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:  "example_group",
					Rules: []rulefmt.RuleNode{ruleOne, ruleOne},
				},
			},
			expectedErr: errDiffRuleLen,
		},
		{
			name: "identical rw configs",
			groupOne: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:  "example_group",
					Rules: []rulefmt.RuleNode{ruleOne},
				},
				RWConfigs: []rwrulefmt.RemoteWriteConfig{
					{URL: "localhost"},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:  "example_group",
					Rules: []rulefmt.RuleNode{ruleOne},
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
					Name:  "example_group",
					Rules: []rulefmt.RuleNode{ruleOne},
				},
				RWConfigs: []rwrulefmt.RemoteWriteConfig{
					{URL: "localhost"},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:  "example_group",
					Rules: []rulefmt.RuleNode{ruleOne},
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
					Name:  "example_group",
					Rules: []rulefmt.RuleNode{ruleOne},
				},
				RWConfigs: []rwrulefmt.RemoteWriteConfig{
					{URL: "localhost"},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:  "example_group",
					Rules: []rulefmt.RuleNode{ruleOne},
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
					Rules:         []rulefmt.RuleNode{ruleOne},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:          "example_group",
					SourceTenants: []string{"tenant-1", "tenant-2"},
					Rules:         []rulefmt.RuleNode{ruleOne},
				},
			},
			expectedErr: errDiffSourceTenants,
		},
		{
			name: "repeated subset of source tenants",
			groupOne: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:          "example_group",
					SourceTenants: []string{"tenant-1", "tenant-2"},
					Rules:         []rulefmt.RuleNode{ruleOne},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:          "example_group",
					SourceTenants: []string{"tenant-1", "tenant-1"},
					Rules:         []rulefmt.RuleNode{ruleOne},
				},
			},
			expectedErr: errDiffSourceTenants,
		},
		{
			name: "repeated single tenant (tenants should be deduplicated)",
			groupOne: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:          "example_group",
					SourceTenants: []string{"tenant-1"},
					Rules:         []rulefmt.RuleNode{ruleOne},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:          "example_group",
					SourceTenants: []string{"tenant-1", "tenant-1"},
					Rules:         []rulefmt.RuleNode{ruleOne},
				},
			},
			expectedErr: nil,
		},
		{
			name: "evaluation delay is set only in one of the two rule groups",
			groupOne: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:  "example_group",
					Rules: []rulefmt.RuleNode{ruleOne},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:            "example_group",
					EvaluationDelay: pointerOf[model.Duration](model.Duration(2 * time.Minute)),
					Rules:           []rulefmt.RuleNode{ruleOne},
				},
			},
			expectedErr: errDiffEvaluationDelay,
		},
		{
			name: "evaluation delay is set in both rule groups but with different value",
			groupOne: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:            "example_group",
					EvaluationDelay: pointerOf[model.Duration](model.Duration(5 * time.Minute)),
					Rules:           []rulefmt.RuleNode{ruleOne},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:            "example_group",
					EvaluationDelay: pointerOf[model.Duration](model.Duration(2 * time.Minute)),
					Rules:           []rulefmt.RuleNode{ruleOne},
				},
			},
			expectedErr: errDiffEvaluationDelay,
		},
		{
			name: "evaluation delay is set in both rule groups with same value",
			groupOne: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:            "example_group",
					EvaluationDelay: pointerOf[model.Duration](model.Duration(5 * time.Minute)),
					Rules:           []rulefmt.RuleNode{ruleOne},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:            "example_group",
					EvaluationDelay: pointerOf[model.Duration](model.Duration(5 * time.Minute)),
					Rules:           []rulefmt.RuleNode{ruleOne},
				},
			},
		},
		{
			name: "evaluation delay is set only in one rule group but with zero value (in Mimir ruler we treat it as not being set)",
			groupOne: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:  "example_group",
					Rules: []rulefmt.RuleNode{ruleOne},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:            "example_group",
					EvaluationDelay: pointerOf[model.Duration](model.Duration(0)),
					Rules:           []rulefmt.RuleNode{ruleOne},
				},
			},
		},
		{
			name: "query offset is set only in one of the two rule groups",
			groupOne: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:  "example_group",
					Rules: []rulefmt.RuleNode{ruleOne},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:        "example_group",
					QueryOffset: pointerOf[model.Duration](model.Duration(2 * time.Minute)),
					Rules:       []rulefmt.RuleNode{ruleOne},
				},
			},
			expectedErr: errDiffQueryOffset,
		},
		{
			name: "query offset is set in both rule groups but with different value",
			groupOne: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:        "example_group",
					QueryOffset: pointerOf[model.Duration](model.Duration(5 * time.Minute)),
					Rules:       []rulefmt.RuleNode{ruleOne},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:        "example_group",
					QueryOffset: pointerOf[model.Duration](model.Duration(2 * time.Minute)),
					Rules:       []rulefmt.RuleNode{ruleOne},
				},
			},
			expectedErr: errDiffQueryOffset,
		},
		{
			name: "query offset is set in both rule groups with same value",
			groupOne: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:        "example_group",
					QueryOffset: pointerOf[model.Duration](model.Duration(5 * time.Minute)),
					Rules:       []rulefmt.RuleNode{ruleOne},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:        "example_group",
					QueryOffset: pointerOf[model.Duration](model.Duration(5 * time.Minute)),
					Rules:       []rulefmt.RuleNode{ruleOne},
				},
			},
		},
		{
			name: "query offset is set only in one rule group but with zero value (in Mimir ruler we treat it as not being set)",
			groupOne: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:  "example_group",
					Rules: []rulefmt.RuleNode{ruleOne},
				},
			},
			groupTwo: rwrulefmt.RuleGroup{
				RuleGroup: rulefmt.RuleGroup{
					Name:        "example_group",
					QueryOffset: pointerOf[model.Duration](model.Duration(0)),
					Rules:       []rulefmt.RuleNode{ruleOne},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualErr := CompareGroups(tt.groupOne, tt.groupTwo)
			assert.ErrorIs(t, actualErr, tt.expectedErr)
		})
	}
}

func pointerOf[T any](value T) *T {
	return &value
}
