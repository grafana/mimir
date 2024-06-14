// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/rules_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/mimirtool/rules"
	"github.com/grafana/mimir/pkg/mimirtool/rules/rwrulefmt"
)

func TestRuleCommand_executeChanges(t *testing.T) {
	group1 := rwrulefmt.RuleGroup{RuleGroup: rulefmt.RuleGroup{Name: "group-1"}}
	group2 := rwrulefmt.RuleGroup{RuleGroup: rulefmt.RuleGroup{Name: "group-2"}}
	group3 := rwrulefmt.RuleGroup{RuleGroup: rulefmt.RuleGroup{Name: "group-3"}}
	group4 := rwrulefmt.RuleGroup{RuleGroup: rulefmt.RuleGroup{Name: "group-4"}}
	group5 := rwrulefmt.RuleGroup{RuleGroup: rulefmt.RuleGroup{Name: "group-5"}}
	group6 := rwrulefmt.RuleGroup{RuleGroup: rulefmt.RuleGroup{Name: "group-6"}}

	changes := []rules.NamespaceChange{
		{Namespace: "namespace-1", State: rules.Created, GroupsCreated: []rwrulefmt.RuleGroup{group1}},
		{Namespace: "namespace-2", State: rules.Created, GroupsCreated: []rwrulefmt.RuleGroup{group2}},
		{Namespace: "namespace-3", State: rules.Updated, GroupsUpdated: []rules.UpdatedRuleGroup{{New: group3}}},
		{Namespace: "namespace-4", State: rules.Updated, GroupsUpdated: []rules.UpdatedRuleGroup{{New: group4}}},
		{Namespace: "namespace-5", State: rules.Deleted, GroupsDeleted: []rwrulefmt.RuleGroup{group5}},
		{Namespace: "namespace-6", State: rules.Deleted, GroupsDeleted: []rwrulefmt.RuleGroup{group6}},
	}

	for _, concurrencyLimit := range []int{1, len(changes)} {
		t.Run(fmt.Sprintf("concurrency limit = %d", concurrencyLimit), func(t *testing.T) {
			client := newRuleCommandClientMock()
			client.On("CreateRuleGroup", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			client.On("DeleteRuleGroup", mock.Anything, mock.Anything, mock.Anything).Return(nil)

			cmd := &RuleCommand{cli: client}
			err := cmd.executeChanges(context.Background(), changes, concurrencyLimit)
			require.NoError(t, err)

			// Ensure all correct APIs have been called.
			client.AssertNumberOfCalls(t, "CreateRuleGroup", 4)
			client.AssertNumberOfCalls(t, "DeleteRuleGroup", 2)

			client.AssertCalled(t, "CreateRuleGroup", mock.Anything, "namespace-1", group1)
			client.AssertCalled(t, "CreateRuleGroup", mock.Anything, "namespace-2", group2)
			client.AssertCalled(t, "CreateRuleGroup", mock.Anything, "namespace-3", group3)
			client.AssertCalled(t, "CreateRuleGroup", mock.Anything, "namespace-4", group4)

			client.AssertCalled(t, "DeleteRuleGroup", mock.Anything, "namespace-5", "group-5")
			client.AssertCalled(t, "DeleteRuleGroup", mock.Anything, "namespace-6", "group-6")
		})
	}
}

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

func TestRuleCommand_checkRules(t *testing.T) {
	completelyBadRuleName := rulefmt.RuleNode{
		Record: yaml.Node{Value: "up", Kind: yaml.ScalarNode},
		Expr:   yaml.Node{Value: "up==1", Kind: yaml.ScalarNode},
	}
	strictlyBadRuleName := rulefmt.RuleNode{
		Record: yaml.Node{Value: "up:onecolonmissing", Kind: yaml.ScalarNode},
		Expr:   yaml.Node{Value: "up==1", Kind: yaml.ScalarNode},
	}
	validRule1 := rulefmt.RuleNode{
		Record: yaml.Node{Value: "rule:up:nothing", Kind: yaml.ScalarNode},
		Expr:   yaml.Node{Value: "up==1", Kind: yaml.ScalarNode},
	}
	validRule2 := rulefmt.RuleNode{
		Record: yaml.Node{Value: "rule:down:nothing", Kind: yaml.ScalarNode},
		Expr:   yaml.Node{Value: "up==0", Kind: yaml.ScalarNode},
	}
	for _, tc := range []struct {
		name       string
		rules      []rulefmt.RuleNode
		strict     bool
		shouldFail bool
	}{
		{
			name:       "completely bad rule name, not strict fails too",
			rules:      []rulefmt.RuleNode{validRule1, completelyBadRuleName, validRule2},
			strict:     false,
			shouldFail: true,
		},
		{
			name:       "strictly bad rule name, strict",
			rules:      []rulefmt.RuleNode{validRule1, strictlyBadRuleName, validRule2},
			strict:     true,
			shouldFail: true,
		},
		{
			name:       "strictly bad rule name, not strict",
			rules:      []rulefmt.RuleNode{validRule1, strictlyBadRuleName, validRule2},
			strict:     false,
			shouldFail: false,
		},
		{
			name:       "no duplicates, strict",
			rules:      []rulefmt.RuleNode{validRule1, validRule2},
			strict:     true,
			shouldFail: false,
		},
		{
			name:       "with duplicates, not strict",
			rules:      []rulefmt.RuleNode{validRule1, validRule1},
			strict:     false,
			shouldFail: false,
		},
		{
			name:       "with duplicates, strict",
			rules:      []rulefmt.RuleNode{validRule1, validRule1},
			strict:     true,
			shouldFail: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "rules.yaml")
			{
				// Setup.
				f, err := os.Create(file)
				require.NoError(t, err)
				t.Cleanup(func() { _ = f.Close() })

				contents := rules.RuleNamespace{
					Namespace: "test",
					Groups: []rwrulefmt.RuleGroup{{
						RuleGroup: rulefmt.RuleGroup{
							Name:  "rulegroup",
							Rules: tc.rules,
						},
					}},
				}
				require.NoError(t, yaml.NewEncoder(f).Encode(contents))
				require.NoError(t, f.Close())
			}

			{
				// Test.
				cmd := &RuleCommand{Strict: tc.strict, RuleFilesList: []string{file}, Backend: rules.MimirBackend}
				err := cmd.checkRules(nil)
				if tc.shouldFail {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestRuleSaveToFile_NamespaceRuleGroup(t *testing.T) {
	t.Run("Fail save because marshal need node Kind defined", func(t *testing.T) {
		namespace := "ns1"
		rule1 := []rwrulefmt.RuleGroup{{
			RuleGroup: rulefmt.RuleGroup{
				Name: "group-1",
				Rules: []rulefmt.RuleNode{
					{
						Record: yaml.Node{Value: "up"},
						Expr:   yaml.Node{Value: "up==1"},
					},
				},
			},
		}}
		tempDir := t.TempDir()
		err := saveNamespaceRuleGroup(namespace, rule1, tempDir)
		assert.ErrorContains(t, err, "yaml: cannot encode node with unknown kind")
	})
	t.Run("Successful save", func(t *testing.T) {
		namespace := "ns1"
		rule1 := []rwrulefmt.RuleGroup{{
			RuleGroup: rulefmt.RuleGroup{
				Name: "group-1",
				Rules: []rulefmt.RuleNode{
					{
						Record: yaml.Node{Value: "up", Kind: yaml.ScalarNode},
						Expr:   yaml.Node{Value: "up==1", Kind: yaml.ScalarNode},
					},
				},
			},
		}}
		tempDir := t.TempDir()
		err := saveNamespaceRuleGroup(namespace, rule1, tempDir)
		assert.NoError(t, err)
	})
	t.Run("Successful save with a modified namespace", func(t *testing.T) {
		namespace := "a/b/c"
		rule1 := []rwrulefmt.RuleGroup{{
			RuleGroup: rulefmt.RuleGroup{
				Name: "group-1",
				Rules: []rulefmt.RuleNode{
					{
						Record: yaml.Node{Value: "up", Kind: yaml.ScalarNode},
						Expr:   yaml.Node{Value: "up==1", Kind: yaml.ScalarNode},
					},
				},
			},
		}}

		tempDir := t.TempDir()
		err := saveNamespaceRuleGroup(namespace, rule1, tempDir)
		assert.NoError(t, err)

		assert.NoFileExists(t, filepath.Join(tempDir, "a/b/c.yaml"))
		assert.FileExists(t, filepath.Join(tempDir, "a_b_c.yaml"))
	})
	t.Run("Successful save and load", func(t *testing.T) {
		expected := `namespace: ns1
groups:
    - name: group-1
      rules:
        - alert: up
          expr: up==1
`
		namespace := "ns1"
		rule1 := []rwrulefmt.RuleGroup{{
			RuleGroup: rulefmt.RuleGroup{
				Name: "group-1",
				Rules: []rulefmt.RuleNode{
					{
						Alert: yaml.Node{Value: "up", Kind: yaml.ScalarNode},
						Expr:  yaml.Node{Value: "up==1", Kind: yaml.ScalarNode},
					},
				},
			},
		}}
		tempDir := t.TempDir()
		err := saveNamespaceRuleGroup(namespace, rule1, tempDir)
		assert.NoError(t, err)
		savedFile := filepath.Join(tempDir, fmt.Sprintf("%s.yaml", namespace))
		content, err := os.ReadFile(savedFile)
		require.NoError(t, err)
		assert.Equal(t, expected, string(content))
	})
}

type ruleCommandClientMock struct {
	mock.Mock
}

func newRuleCommandClientMock() *ruleCommandClientMock {
	return &ruleCommandClientMock{}
}

func (m *ruleCommandClientMock) CreateRuleGroup(ctx context.Context, namespace string, rg rwrulefmt.RuleGroup) error {
	args := m.Called(ctx, namespace, rg)
	return args.Error(0)
}

func (m *ruleCommandClientMock) DeleteRuleGroup(ctx context.Context, namespace, groupName string) error {
	args := m.Called(ctx, namespace, groupName)
	return args.Error(0)
}

func (m *ruleCommandClientMock) GetRuleGroup(ctx context.Context, namespace, groupName string) (*rwrulefmt.RuleGroup, error) {
	args := m.Called(ctx, namespace, groupName)
	return args.Get(0).(*rwrulefmt.RuleGroup), args.Error(1)
}

func (m *ruleCommandClientMock) ListRules(ctx context.Context, namespace string) (map[string][]rwrulefmt.RuleGroup, error) {
	args := m.Called(ctx, namespace)
	return args.Get(0).(map[string][]rwrulefmt.RuleGroup), args.Error(1)
}

func (m *ruleCommandClientMock) DeleteNamespace(ctx context.Context, namespace string) error {
	args := m.Called(ctx, namespace)
	return args.Error(0)
}
