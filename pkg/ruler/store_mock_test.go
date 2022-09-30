// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/store_mock_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/ruler/rulestore"
)

type mockRuleStore struct {
	rules map[string]rulespb.RuleGroupList
	mtx   sync.Mutex
}

var (
	delim       = "/"
	interval, _ = time.ParseDuration("1m")
	mockRules   = map[string]rulespb.RuleGroupList{
		"user1": {
			&rulespb.RuleGroupDesc{
				Name:      "group1",
				Namespace: "namespace1",
				User:      "user1",
				Rules:     []*rulespb.RuleDesc{mockRecordingRuleDesc("UP_RULE", "up"), mockAlertingRuleDesc("UP_ALERT", "up < 1")},
				Interval:  interval,
			},
		},
		"user2": {
			&rulespb.RuleGroupDesc{
				Name:      "group1",
				Namespace: "namespace1",
				User:      "user2",
				Rules:     []*rulespb.RuleDesc{mockRecordingRuleDesc("UP_RULE", "up")},
				Interval:  interval,
			},
		},
	}
)

func newMockRuleStore(rules map[string]rulespb.RuleGroupList) *mockRuleStore {
	return &mockRuleStore{
		rules: rules,
	}
}

func (m *mockRuleStore) ListAllUsers(_ context.Context) ([]string, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	var result []string
	for u := range m.rules {
		result = append(result, u)
	}
	return result, nil
}

func (m *mockRuleStore) ListRuleGroupsForUserAndNamespace(_ context.Context, userID, namespace string) (rulespb.RuleGroupList, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	var result rulespb.RuleGroupList
	for _, r := range m.rules[userID] {
		if namespace != "" && namespace != r.Namespace {
			continue
		}

		result = append(result, &rulespb.RuleGroupDesc{
			Namespace:     r.Namespace,
			Name:          r.Name,
			User:          userID,
			Interval:      r.Interval,
			SourceTenants: r.SourceTenants,
		})
	}
	return result, nil
}

func (m *mockRuleStore) LoadRuleGroups(ctx context.Context, groupsToLoad map[string]rulespb.RuleGroupList) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	gm := make(map[string]*rulespb.RuleGroupDesc)
	for _, gs := range m.rules {
		for _, gr := range gs {
			user, namespace, name := gr.GetUser(), gr.GetNamespace(), gr.GetName()
			key := user + delim + base64.URLEncoding.EncodeToString([]byte(namespace)) + delim + base64.URLEncoding.EncodeToString([]byte(name))
			gm[key] = gr
		}
	}

	for _, gs := range groupsToLoad {
		for _, gr := range gs {
			user, namespace, name := gr.GetUser(), gr.GetNamespace(), gr.GetName()
			key := user + delim + base64.URLEncoding.EncodeToString([]byte(namespace)) + delim + base64.URLEncoding.EncodeToString([]byte(name))
			mgr, ok := gm[key]
			if !ok {
				return fmt.Errorf("failed to get rule group user %s", gr.GetUser())
			}
			*gr = *mgr
		}
	}
	return nil
}

func (m *mockRuleStore) GetRuleGroup(_ context.Context, userID string, namespace string, group string) (*rulespb.RuleGroupDesc, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	userRules, exists := m.rules[userID]
	if !exists {
		return nil, rulestore.ErrUserNotFound
	}

	if namespace == "" {
		return nil, rulestore.ErrGroupNamespaceNotFound
	}

	for _, rg := range userRules {
		if rg.Namespace == namespace && rg.Name == group {
			return rg, nil
		}
	}

	return nil, rulestore.ErrGroupNotFound
}

func (m *mockRuleStore) SetRuleGroup(ctx context.Context, userID string, namespace string, group *rulespb.RuleGroupDesc) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	userRules, exists := m.rules[userID]
	if !exists {
		userRules = rulespb.RuleGroupList{}
		m.rules[userID] = userRules
	}

	if namespace == "" {
		return rulestore.ErrGroupNamespaceNotFound
	}

	for i, rg := range userRules {
		if rg.Namespace == namespace && rg.Name == group.Name {
			userRules[i] = group
			return nil
		}
	}

	m.rules[userID] = append(userRules, group)
	return nil
}

func (m *mockRuleStore) DeleteRuleGroup(ctx context.Context, userID string, namespace string, group string) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	userRules, exists := m.rules[userID]
	if !exists {
		userRules = rulespb.RuleGroupList{}
		m.rules[userID] = userRules
	}

	if namespace == "" {
		return rulestore.ErrGroupNamespaceNotFound
	}

	for i, rg := range userRules {
		if rg.Namespace == namespace && rg.Name == group {
			m.rules[userID] = append(userRules[:i], userRules[:i+1]...)
			return nil
		}
	}

	return nil
}

func (m *mockRuleStore) DeleteNamespace(ctx context.Context, userID, namespace string) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	userRules, exists := m.rules[userID]
	if !exists {
		userRules = rulespb.RuleGroupList{}
		m.rules[userID] = userRules
	}

	if namespace == "" {
		return rulestore.ErrGroupNamespaceNotFound
	}

	for i, rg := range userRules {
		if rg.Namespace == namespace {

			// Only here to assert on partial failures.
			if rg.Name == "fail" {
				return fmt.Errorf("unable to delete rg")
			}

			m.rules[userID] = append(userRules[:i], userRules[i+1:]...)
		}
	}

	return nil
}
