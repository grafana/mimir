// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/manager_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"context"
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

	limits := ruleLimits{maxRuleGroups: 1, maxRulesPerRuleGroup: 1}
	m, err := NewDefaultMultiTenantManager(Config{RulePath: dir}, factory, nil, log.NewNopLogger(), nil, limits)
	require.NoError(t, err)

	const user = "testUser"

	userRules := map[string]rulespb.RuleGroupList{
		user: {
			&rulespb.RuleGroupDesc{
				Name:      "group1",
				Namespace: "ns",
				Interval:  1 * time.Minute,
				User:      user,
			},
		},
	}
	m.SyncRuleGroups(context.Background(), userRules)

	mgr := getManager(m, user)
	require.NotNil(t, mgr)

	test.Poll(t, 1*time.Second, true, func() interface{} {
		return mgr.(*mockRulesManager).running.Load()
	})

	// Verify that user rule groups are now cached locally.
	{
		users, err := m.mapper.users()
		require.NoError(t, err)
		require.Equal(t, []string{user}, users)
	}

	// Passing empty map / nil stops all managers.
	m.SyncRuleGroups(context.Background(), nil)
	require.Nil(t, getManager(m, user))

	// Make sure old manager was stopped.
	test.Poll(t, 1*time.Second, false, func() interface{} {
		return mgr.(*mockRulesManager).running.Load()
	})

	// Verify that local rule groups were removed.
	{
		users, err := m.mapper.users()
		require.NoError(t, err)
		require.Equal(t, []string(nil), users)
	}

	// Resync same rules as before. Previously this didn't restart the manager.
	m.SyncRuleGroups(context.Background(), userRules)

	newMgr := getManager(m, user)
	require.NotNil(t, newMgr)
	require.True(t, mgr != newMgr)

	test.Poll(t, 1*time.Second, true, func() interface{} {
		return newMgr.(*mockRulesManager).running.Load()
	})

	// Verify that user rule groups are cached locally again.
	{
		users, err := m.mapper.users()
		require.NoError(t, err)
		require.Equal(t, []string{user}, users)
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
