// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/rules_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"context"
	"fmt"
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
