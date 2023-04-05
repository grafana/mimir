// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/manager_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/rules"
	promRules "github.com/prometheus/prometheus/rules"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
)

func TestSyncRuleGroups(t *testing.T) {
	dir := t.TempDir()

	m, err := NewDefaultMultiTenantManager(Config{RulePath: dir}, factory, nil, log.NewNopLogger(), nil)
	require.NoError(t, err)

	const (
		user1      = "testUser1"
		user2      = "testUser2"
		namespace1 = "ns1"
		namespace2 = "ns2"
	)

	userRules := map[string]rulespb.RuleGroupList{
		user1: {
			&rulespb.RuleGroupDesc{
				Name:      "group1",
				Namespace: namespace1,
				Interval:  30 * time.Second,
				User:      user1,
			},
		},
		user2: {
			&rulespb.RuleGroupDesc{
				Name:      "group2",
				Namespace: namespace2,
				Interval:  1 * time.Minute,
				User:      user2,
			},
		},
	}
	m.SyncRuleGroups(context.Background(), userRules)
	m.Start()
	mgr1 := getManager(m, user1)
	require.NotNil(t, mgr1)
	test.Poll(t, 1*time.Second, true, func() interface{} {
		return mgr1.(*mockRulesManager).running.Load()
	})

	mgr2 := getManager(m, user2)
	require.NotNil(t, mgr2)
	test.Poll(t, 1*time.Second, true, func() interface{} {
		return mgr2.(*mockRulesManager).running.Load()
	})

	// Verify that user rule groups are now cached locally.
	{
		users, err := m.mapper.users()
		require.NoError(t, err)
		require.Contains(t, users, user2)
		require.Contains(t, users, user1)
	}

	// Verify that rule groups are now written to disk.
	{
		rulegroup1 := filepath.Join(m.mapper.Path, user1, namespace1)
		f, error := m.mapper.FS.Open(rulegroup1)
		require.NoError(t, error)
		defer f.Close()

		bt := make([]byte, 100)
		n, error := f.Read(bt)
		require.NoError(t, error)
		require.Equal(t, string("groups:\n    - name: group1\n      interval: 30s\n      rules: []\n"), string(bt[:n]))
	}

	{
		rulegroup2 := filepath.Join(m.mapper.Path, user2, namespace2)
		f, error := m.mapper.FS.Open(rulegroup2)
		require.NoError(t, error)
		defer f.Close()

		bt := make([]byte, 100)
		n, error := f.Read(bt)
		require.NoError(t, error)
		require.Equal(t, string("groups:\n    - name: group2\n      interval: 1m\n      rules: []\n"), string(bt[:n]))
	}

	// Passing empty map / nil stops all managers.
	m.SyncRuleGroups(context.Background(), nil)
	require.Nil(t, getManager(m, user1))

	// Make sure old manager was stopped.
	test.Poll(t, 1*time.Second, false, func() interface{} {
		return mgr1.(*mockRulesManager).running.Load()
	})

	test.Poll(t, 1*time.Second, false, func() interface{} {
		return mgr2.(*mockRulesManager).running.Load()
	})

	// Verify that local rule groups were removed.
	{
		users, err := m.mapper.users()
		require.NoError(t, err)
		require.Equal(t, []string(nil), users)
	}

	// Resync same rules as before. Previously this didn't restart the manager.
	m.SyncRuleGroups(context.Background(), userRules)

	newMgr := getManager(m, user1)
	require.NotNil(t, newMgr)
	require.True(t, mgr1 != newMgr)

	test.Poll(t, 1*time.Second, true, func() interface{} {
		return newMgr.(*mockRulesManager).running.Load()
	})

	// Verify that user rule groups are cached locally again.
	{
		users, err := m.mapper.users()
		require.NoError(t, err)
		require.Contains(t, users, user1)
	}

	m.Stop()

	test.Poll(t, 1*time.Second, false, func() interface{} {
		return newMgr.(*mockRulesManager).running.Load()
	})
}

func getManager(m *DefaultMultiTenantManager, user string) RulesManager {
	m.userManagerMtx.RLock()
	defer m.userManagerMtx.RUnlock()

	return m.userManagers[user]
}

func factory(_ context.Context, _ string, _ *notifier.Manager, _ log.Logger, _ prometheus.Registerer) RulesManager {
	return &mockRulesManager{done: make(chan struct{})}
}

type mockRulesManager struct {
	running atomic.Bool
	done    chan struct{}
}

func (m *mockRulesManager) Run() {
	m.running.Store(true)
	<-m.done
}

func (m *mockRulesManager) Stop() {
	m.running.Store(false)
	close(m.done)
}

func (m *mockRulesManager) Update(interval time.Duration, files []string, externalLabels labels.Labels, externalURL string, ruleGroupPostProcessFunc rules.RuleGroupPostProcessFunc) error {
	return nil
}

func (m *mockRulesManager) RuleGroups() []*promRules.Group {
	return nil
}
