package rules

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/pkg/rulefmt"
)

func TestParseFiles(t *testing.T) {
	tests := []struct {
		name    string
		files   []string
		want    map[string]RuleNamespace
		wantErr bool
	}{
		{
			name: "basic_file",
			files: []string{
				"testdata/basic_namespace.yaml",
			},
			want: map[string]RuleNamespace{
				"example_namespace": RuleNamespace{
					Namespace: "example_namespace",
					Groups: []rulefmt.RuleGroup{
						rulefmt.RuleGroup{
							Name: "example_rule_group",
							Rules: []rulefmt.Rule{
								rulefmt.Rule{
									Record: "summed_up",
									Expr:   "sum(up)",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "file_namespace_overlap",
			files: []string{
				"testdata/basic_namespace.yaml",
				"testdata/basic_namespace_repeated.yaml",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseFiles(tt.files)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseFiles() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for k, g := range got {
				w, exists := tt.want[k]
				if !exists {
					t.Errorf("ParseFiles() namespace %v found and not expected", k)
					return
				}
				err = compareNamespace(g, w)
				if err != nil {
					t.Errorf("ParseFiles() namespaces do not match, err=%v", err)
					return
				}
			}
		})
	}
}

func compareNamespace(g, w RuleNamespace) error {
	if g.Namespace != w.Namespace {
		return fmt.Errorf("namespaces do not match, actual=%v expected=%v", g.Namespace, w.Namespace)
	}

	if len(g.Groups) != len(w.Groups) {
		return fmt.Errorf("returned namespace does not have the expected number of groups, actual=%d expected=%d", len(g.Groups), len(w.Groups))
	}

	for i := range g.Groups {
		if g.Groups[i].Name != w.Groups[i].Name {
			return fmt.Errorf("actual group with name %v does not match expected group name %v", g.Groups[i].Name, w.Groups[i].Name)
		}
		if g.Groups[i].Interval != w.Groups[i].Interval {
			return fmt.Errorf("actual group with Interval %v does not match expected group Interval %v", g.Groups[i].Interval, w.Groups[i].Interval)
		}
		if len(g.Groups[i].Rules) != len(w.Groups[i].Rules) {
			return fmt.Errorf("length of rules do not match, actual=%v expected=%v", len(g.Groups[i].Rules), len(w.Groups[i].Rules))
		}
	}

	return nil
}
