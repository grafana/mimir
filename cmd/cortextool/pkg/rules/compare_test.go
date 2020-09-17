package rules

import (
	"testing"

	"github.com/prometheus/prometheus/pkg/rulefmt"
	yaml "gopkg.in/yaml.v3"
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
