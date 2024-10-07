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
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/ruler/rulestore"
	"github.com/grafana/mimir/pkg/ruler/rulestore/bucketclient"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketcache"
)

var (
	delim       = "/"
	interval, _ = time.ParseDuration("1m")
	mockRules   = map[string]rulespb.RuleGroupList{
		"user1": {
			&rulespb.RuleGroupDesc{
				Name:      "group1",
				Namespace: "namespace1",
				User:      "user1",
				Rules:     []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up"), createAlertingRule("UP_ALERT", "up < 1")},
				Interval:  interval,
			},
		},
		"user2": {
			&rulespb.RuleGroupDesc{
				Name:      "group1",
				Namespace: "namespace1",
				User:      "user2",
				Rules:     []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up")},
				Interval:  interval,
			},
		},
	}
)

func newInMemoryRuleStore(t *testing.T) (*cache.InstrumentedMockCache, *bucketclient.BucketRuleStore) {
	bkt := objstore.NewInMemBucket()
	mockCache := cache.NewInstrumentedMockCache()
	cfg := bucketcache.NewCachingBucketConfig()
	cfg.CacheIter("iter", mockCache, isNotTenantsDir, time.Minute, &bucketcache.JSONIterCodec{})
	cfg.CacheGet("rules", mockCache, isRuleGroup, 1024^2, time.Minute, time.Minute, time.Minute)

	cachingBkt, err := bucketcache.NewCachingBucket("rules", bkt, cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry())
	require.NoError(t, err)

	return mockCache, bucketclient.NewBucketRuleStore(cachingBkt, nil, log.NewNopLogger())
}

type mockRuleStore struct {
	rules        map[string]rulespb.RuleGroupList
	missingRules rulespb.RuleGroupList
	mtx          sync.Mutex
}

func newMockRuleStore(rules map[string]rulespb.RuleGroupList) *mockRuleStore {
	return &mockRuleStore{
		rules: rules,
	}
}

// setMissingRuleGroups configures the list of rule groups what will be returned as missing
// from LoadRuleGroups().
func (m *mockRuleStore) setMissingRuleGroups(missing rulespb.RuleGroupList) {
	m.mtx.Lock()
	m.missingRules = missing
	m.mtx.Unlock()
}

func (m *mockRuleStore) ListAllUsers(_ context.Context, _ ...rulestore.Option) ([]string, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	var result []string
	for u := range m.rules {
		result = append(result, u)
	}
	return result, nil
}

func (m *mockRuleStore) ListRuleGroupsForUserAndNamespace(_ context.Context, userID, namespace string, _ ...rulestore.Option) (rulespb.RuleGroupList, error) {
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

func (m *mockRuleStore) LoadRuleGroups(_ context.Context, groupsToLoad map[string]rulespb.RuleGroupList) (rulespb.RuleGroupList, error) {
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
				return nil, fmt.Errorf("failed to get rule group user %s", gr.GetUser())
			}
			*gr = *mgr
		}
	}
	return m.missingRules, nil
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

func (m *mockRuleStore) SetRuleGroup(_ context.Context, userID string, namespace string, group *rulespb.RuleGroupDesc) error {
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

func (m *mockRuleStore) DeleteRuleGroup(_ context.Context, userID string, namespace string, group string) error {
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

func (m *mockRuleStore) DeleteNamespace(_ context.Context, userID, namespace string) error {
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
