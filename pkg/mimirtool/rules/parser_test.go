// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/rules/parser_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package rules

import (
	"fmt"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/rulefmt"

	"github.com/grafana/mimir/pkg/mimirtool/rules/rwrulefmt"
)

func TestParseFiles(t *testing.T) {
	tests := []struct {
		name    string
		backend string
		files   []string
		want    map[string]RuleNamespace
		wantErr bool
	}{
		{
			name:    "basic_file",
			backend: MimirBackend,
			files: []string{
				"testdata/basic_namespace.yaml",
			},
			want: map[string]RuleNamespace{
				"example_namespace": {
					Namespace: "example_namespace",
					Groups: []rwrulefmt.RuleGroup{
						{
							RuleGroup: rulefmt.RuleGroup{
								Name: "example_rule_group",
								Rules: []rulefmt.Rule{
									{
										// currently the tests only check length
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    "file_namespace_overlap",
			backend: MimirBackend,
			files: []string{
				"testdata/basic_namespace.yaml",
				"testdata/basic_namespace_repeated.yaml",
			},
			wantErr: true,
		},
		{
			name:    "multiple_namespace_file",
			backend: MimirBackend,
			files: []string{
				"testdata/multiple_namespace.yaml",
			},
			want: map[string]RuleNamespace{
				"example_namespace": {
					Namespace: "example_namespace",
					Groups: []rwrulefmt.RuleGroup{
						{
							RuleGroup: rulefmt.RuleGroup{
								Name: "example_rule_group",
								Rules: []rulefmt.Rule{
									{
										// currently the tests only check length
									},
								},
							},
						},
					},
				},
				"other_example_namespace": {
					Namespace: "other_example_namespace",
					Groups: []rwrulefmt.RuleGroup{
						{
							RuleGroup: rulefmt.RuleGroup{
								Name: "other_example_rule_group",
								Rules: []rulefmt.Rule{
									{
										// currently the tests only check length
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    "federated_rule_groups",
			backend: MimirBackend,
			files: []string{
				"testdata/mimir_federated_rules.yaml",
			},
			want: map[string]RuleNamespace{
				"example_namespace": {
					Namespace: "example_namespace",
					Groups: []rwrulefmt.RuleGroup{
						{
							RuleGroup: rulefmt.RuleGroup{
								SourceTenants: []string{"tenant-1", "tenant-2"},
								Name:          "example_federated_rule_group",
								Rules: []rulefmt.Rule{
									{
										// currently, the tests only check length
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseFiles(tt.backend, tt.files, model.UTF8Validation, log.NewNopLogger())
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
			for k := range tt.want {
				if _, exists := got[k]; !exists {
					t.Errorf("ParseFiles() namespace %v wanted but not found", k)
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
		if len(g.Groups[i].SourceTenants) != len(w.Groups[i].SourceTenants) {
			return fmt.Errorf("length of source tenants does not match, actual=%d, expected=%d", len(g.Groups[i].SourceTenants), len(w.Groups[i].SourceTenants))
		}
		for j, got := range g.Groups[i].SourceTenants {
			if want := w.Groups[i].SourceTenants[j]; got != want {
				return fmt.Errorf("tenant %d (0-indexed) is different, actual=%s, expected=%s", j, got, want)
			}
		}
	}

	return nil
}
