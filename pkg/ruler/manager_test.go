// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/manager_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/promql"
	promRules "github.com/prometheus/prometheus/rules"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/mimirpb"
	rulernotifier "github.com/grafana/mimir/pkg/ruler/notifier"
	"github.com/grafana/mimir/pkg/ruler/rulespb"
	util_log "github.com/grafana/mimir/pkg/util/log"
	testutil "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestDefaultMultiTenantManager_SyncFullRuleGroups(t *testing.T) {
	const (
		user1 = "user-1"
		user2 = "user-2"
	)

	var (
		ctx         = context.Background()
		logger      = testutil.NewTestingLogger(t)
		user1Group1 = createRuleGroup("group-1", user1, createRecordingRule("count:metric_1", "count(metric_1)"))
		user2Group1 = createRuleGroup("group-1", user2, createRecordingRule("sum:metric_1", "sum(metric_1)"))
	)

	m, err := NewDefaultMultiTenantManager(Config{RulePath: t.TempDir()}, managerMockFactory, nil, logger, nil, validation.MockOverrides(nil), afero.NewMemMapFs())
	require.NoError(t, err)

	// Initialise the manager with some rules and start it.
	m.SyncFullRuleGroups(ctx, map[string]rulespb.RuleGroupList{
		user1: {user1Group1},
		user2: {user2Group1},
	}, nil)
	m.Start()

	initialUser1Manager := assertManagerMockRunningForUser(t, m, user1)
	initialUser2Manager := assertManagerMockRunningForUser(t, m, user2)

	assertRuleGroupsMappedOnDisk(t, m, user1, rulespb.RuleGroupList{user1Group1})
	assertRuleGroupsMappedOnDisk(t, m, user2, rulespb.RuleGroupList{user2Group1})

	t.Run("calling SyncFullRuleGroups() with an empty map stops all managers", func(t *testing.T) {
		m.SyncFullRuleGroups(ctx, nil, nil)

		// Ensure the ruler manager has been stopped for all users.
		assertManagerMockStopped(t, initialUser1Manager)
		assertManagerMockStopped(t, initialUser2Manager)
		assertManagerMockNotRunningForUser(t, m, user1)
		assertManagerMockNotRunningForUser(t, m, user2)

		// Ensure the files have been removed from disk.
		assertRuleGroupsMappedOnDisk(t, m, user1, nil)
		assertRuleGroupsMappedOnDisk(t, m, user2, nil)

		// Check metrics.
		assert.Equal(t, 0.0, promtest.ToFloat64(m.managersTotal))
	})

	t.Run("calling SyncFullRuleGroups() with the previous config restores the managers", func(t *testing.T) {
		m.SyncFullRuleGroups(ctx, map[string]rulespb.RuleGroupList{
			user1: {user1Group1},
			user2: {user2Group1},
		}, nil)

		// Ensure the ruler manager has been started.
		currUser1Manager := assertManagerMockRunningForUser(t, m, user1)
		currUser2Manager := assertManagerMockRunningForUser(t, m, user2)
		assert.NotEqual(t, currUser1Manager, initialUser1Manager)
		assert.NotEqual(t, currUser2Manager, initialUser2Manager)

		// Ensure the files have been mapped to disk.
		assertRuleGroupsMappedOnDisk(t, m, user1, rulespb.RuleGroupList{user1Group1})
		assertRuleGroupsMappedOnDisk(t, m, user2, rulespb.RuleGroupList{user2Group1})

		// Check metrics.
		assert.Equal(t, 2.0, promtest.ToFloat64(m.managersTotal))
	})

	t.Run("calling Stop() should stop all managers", func(t *testing.T) {
		// Pre-condition check.
		currUser1Manager := assertManagerMockRunningForUser(t, m, user1)
		currUser2Manager := assertManagerMockRunningForUser(t, m, user2)

		m.Stop()

		assertManagerMockStopped(t, currUser1Manager)
		assertManagerMockStopped(t, currUser2Manager)

		assertManagerMockNotRunningForUser(t, m, user1)
		assertManagerMockNotRunningForUser(t, m, user2)

		// Ensure the files have been removed from disk.
		assertRuleGroupsMappedOnDisk(t, m, user1, nil)
		assertRuleGroupsMappedOnDisk(t, m, user2, nil)
	})
}

func TestDefaultMultiTenantManager_SyncPartialRuleGroups(t *testing.T) {
	const (
		user1 = "user-1"
		user2 = "user-2"
	)

	var (
		ctx         = context.Background()
		logger      = testutil.NewTestingLogger(t)
		user1Group1 = createRuleGroup("group-1", user1, createRecordingRule("count:metric_1", "count(metric_1)"))
		user1Group2 = createRuleGroup("group-2", user1, createRecordingRule("count:metric_2", "count(metric_2)"))
		user2Group1 = createRuleGroup("group-1", user2, createRecordingRule("sum:metric_1", "sum(metric_1)"))
	)

	m, err := NewDefaultMultiTenantManager(Config{RulePath: t.TempDir()}, managerMockFactory, nil, logger, nil, validation.MockOverrides(nil), afero.NewMemMapFs())
	require.NoError(t, err)
	t.Cleanup(m.Stop)

	// Initialise the manager with some rules and start it.
	m.SyncFullRuleGroups(ctx, map[string]rulespb.RuleGroupList{
		user1: {user1Group1},
		user2: {user2Group1},
	}, nil)
	m.Start()

	initialUser1Manager := assertManagerMockRunningForUser(t, m, user1)
	initialUser2Manager := assertManagerMockRunningForUser(t, m, user2)

	// Ensure the right rule groups have been mapped on disk.
	assertRuleGroupsMappedOnDisk(t, m, user1, rulespb.RuleGroupList{user1Group1})
	assertRuleGroupsMappedOnDisk(t, m, user2, rulespb.RuleGroupList{user2Group1})

	t.Run("calling SyncPartialRuleGroups() with an empty map should be a no-op", func(t *testing.T) {
		m.SyncPartialRuleGroups(ctx, nil, nil)

		// Ensure the per-tenant manager has not changed.
		currUser1Manager := assertManagerMockRunningForUser(t, m, user1)
		currUser2Manager := assertManagerMockRunningForUser(t, m, user2)
		assert.Equal(t, initialUser1Manager, currUser1Manager)
		assert.Equal(t, initialUser2Manager, currUser2Manager)

		// Ensure the right rule groups have been mapped on disk.
		assertRuleGroupsMappedOnDisk(t, m, user1, rulespb.RuleGroupList{user1Group1})
		assertRuleGroupsMappedOnDisk(t, m, user2, rulespb.RuleGroupList{user2Group1})

		// Check metrics.
		assert.Equal(t, 2.0, promtest.ToFloat64(m.managersTotal))
	})

	t.Run("calling SyncPartialRuleGroups() with a subset of users should re-sync them", func(t *testing.T) {
		m.SyncPartialRuleGroups(ctx, map[string]rulespb.RuleGroupList{
			user1: {user1Group1, user1Group2},
		}, nil)

		// Ensure the per-tenant manager has not changed.
		currUser1Manager := assertManagerMockRunningForUser(t, m, user1)
		currUser2Manager := assertManagerMockRunningForUser(t, m, user2)
		assert.Equal(t, initialUser1Manager, currUser1Manager)
		assert.Equal(t, initialUser2Manager, currUser2Manager)

		// Ensure the right rule groups have been mapped on disk.
		assertRuleGroupsMappedOnDisk(t, m, user1, rulespb.RuleGroupList{user1Group1, user1Group2})
		assertRuleGroupsMappedOnDisk(t, m, user2, rulespb.RuleGroupList{user2Group1})

		// Check metrics.
		assert.Equal(t, 2.0, promtest.ToFloat64(m.managersTotal))
	})

	t.Run("calling SyncPartialRuleGroups() with a user with no rule groups should stop its manager", func(t *testing.T) {
		m.SyncPartialRuleGroups(ctx, map[string]rulespb.RuleGroupList{
			user1: nil,
		}, nil)

		// Ensure the ruler manager has been stopped for the user with no rule groups.
		assertManagerMockStopped(t, initialUser1Manager)
		assertManagerMockNotRunningForUser(t, m, user1)

		// Ensure the ruler manager is still running for other users.
		currUser2Manager := assertManagerMockRunningForUser(t, m, user2)
		assert.Equal(t, initialUser2Manager, currUser2Manager)

		// Ensure the right rule groups have been mapped on disk.
		assertRuleGroupsMappedOnDisk(t, m, user1, nil)
		assertRuleGroupsMappedOnDisk(t, m, user2, rulespb.RuleGroupList{user2Group1})

		// Check metrics.
		assert.Equal(t, 1.0, promtest.ToFloat64(m.managersTotal))
	})
}

func TestFilterRuleGroupsByNotEmptyUsers(t *testing.T) {
	tests := map[string]struct {
		configs         map[string]rulespb.RuleGroupList
		expectedConfigs map[string]rulespb.RuleGroupList
		expectedRemoved map[string]struct{}
	}{
		"should return an empty map on empty input": {
			configs:         nil,
			expectedConfigs: nil,
		},
		"should return the input map on no empty users": {
			configs: map[string]rulespb.RuleGroupList{
				"user-1": {
					createRuleGroup("group-1", "user-1", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
				},
				"user-2": {
					createRuleGroup("group-1", "user-2", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-2", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
				},
			},
			expectedConfigs: map[string]rulespb.RuleGroupList{
				"user-1": {
					createRuleGroup("group-1", "user-1", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
				},
				"user-2": {
					createRuleGroup("group-1", "user-2", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-2", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
				},
			},
		},
		"should remove users with no rule groups from the input map": {
			configs: map[string]rulespb.RuleGroupList{
				"user-1": {},
				"user-2": {
					createRuleGroup("group-1", "user-2", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-2", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
				},
				"user-3": {},
			},
			expectedConfigs: map[string]rulespb.RuleGroupList{
				"user-2": {
					createRuleGroup("group-1", "user-2", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-2", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
				},
			},
			expectedRemoved: map[string]struct{}{
				"user-1": {},
				"user-3": {},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actualConfigs, actualRemoved := filterRuleGroupsByNotEmptyUsers(testData.configs)
			assert.Equal(t, testData.expectedConfigs, actualConfigs)
			assert.Equal(t, testData.expectedRemoved, actualRemoved)
		})
	}
}

func TestDefaultMultiTenantManager_NotifierConfiguration(t *testing.T) {
	// We have two alertmanagers.
	alertmanager1ReceivedRequest := make(chan struct{}, 2)
	alertmanager1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		alertmanager1ReceivedRequest <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer func() {
		alertmanager1.Close()
	}()

	alertmanager2ReceivedRequest := make(chan struct{}, 2)
	alertmanager2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		alertmanager2ReceivedRequest <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer func() {
		alertmanager2.Close()
	}()

	ctx := context.Background()
	logger := testutil.NewTestingLogger(t)

	// We have two users with rules.
	const user1 = "user-1"
	user1Group := createRuleGroup("group-1", user1, createRecordingRule("count:metric_1", "count(metric_1)"))

	const user2 = "user-2"
	user2Group := createRuleGroup("group-1", user2, createRecordingRule("count:metric_1", "count(metric_1)"))

	// The ruler config points at alertmanager 1.
	cfg := Config{
		RulePath:                  t.TempDir(),
		NotificationQueueCapacity: 1000,
		NotificationTimeout:       10 * time.Second,
	}

	// By default, tenants use alertmanager 1.
	// user-2's tenant configuration is overridden to point at alertmanager 2.
	overrides := validation.MockOverrides(func(defaults *validation.Limits, tenantLimits map[string]*validation.Limits) {
		*defaults = *validation.MockDefaultLimits()
		defaults.RulerAlertmanagerClientConfig = rulernotifier.AlertmanagerClientConfig{
			AlertmanagerURL: alertmanager1.URL,
		}

		// tenantLimits[user1] = validation.MockDefaultLimits()
		tenantLimits[user2] = validation.MockDefaultLimits()
		tenantLimits[user2].RulerAlertmanagerClientConfig = rulernotifier.AlertmanagerClientConfig{
			AlertmanagerURL: alertmanager2.URL,
		}
	})

	// Start a manager.
	m, err := NewDefaultMultiTenantManager(cfg, managerMockFactory, nil, logger, nil, overrides, afero.NewMemMapFs())
	require.NoError(t, err)
	defer m.Stop()
	m.SyncFullRuleGroups(ctx, map[string]rulespb.RuleGroupList{
		user1: {user1Group},
		user2: {user2Group},
	}, nil)
	m.Start()

	t.Run("creating notifier with global alertmanager settings sends to correct alertmanager", func(t *testing.T) {
		_ = assertManagerMockRunningForUser(t, m, user1)
		userNotifier := assertNotifierRunningForUser(t, m, user1)
		waitForAlertmanagerToBeDiscovered(t, userNotifier.notifier)

		require.Equal(t, 1, len(userNotifier.notifier.Alertmanagers()))
		require.Contains(t, userNotifier.notifier.Alertmanagers()[0].String(), alertmanager1.URL)

		// Send an alert.
		userNotifier.notifier.Send(&notifier.Alert{Labels: labels.FromStrings(labels.AlertName, "alert-1")})

		// Wait for the Alertmanager to receive the request.
		select {
		case <-alertmanager1ReceivedRequest:
			// We can continue.
		case <-alertmanager2ReceivedRequest:
			require.FailNow(t, "wrong alertmanager received the alert")
		case <-time.After(time.Second):
			require.FailNow(t, "gave up waiting for first notification request to be sent")
		}
	})

	// Clear out anything potentially leftover
	clearStructChannel(alertmanager1ReceivedRequest)
	clearStructChannel(alertmanager2ReceivedRequest)

	t.Run("creating notifier with tenant-overridden alertmanager settings sends to correct alertmanager", func(t *testing.T) {
		_ = assertManagerMockRunningForUser(t, m, user2)
		userNotifier := assertNotifierRunningForUser(t, m, user2)
		waitForAlertmanagerToBeDiscovered(t, userNotifier.notifier)

		require.Equal(t, 1, len(userNotifier.notifier.Alertmanagers()))
		require.Contains(t, userNotifier.notifier.Alertmanagers()[0].String(), alertmanager2.URL)

		// Send an alert.
		userNotifier.notifier.Send(&notifier.Alert{Labels: labels.FromStrings(labels.AlertName, "alert-2")})
		// Wait for the Alertmanager to receive the request.
		select {
		case <-alertmanager1ReceivedRequest:
			require.FailNow(t, "wrong alertmanager received the alert")
		case <-alertmanager2ReceivedRequest:
			// We can continue.
		case <-time.After(time.Second):
			require.FailNow(t, "gave up waiting for first notification request to be sent")
		}
	})
}

func TestDefaultMultiTenantManager_WaitsToDrainPendingNotificationsOnShutdown(t *testing.T) {
	releaseReceiver := make(chan struct{})
	receiverReceivedRequest := make(chan struct{}, 2)
	alertsReceived := atomic.NewInt64(0)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Let the test know we've received a request.
		receiverReceivedRequest <- struct{}{}

		var alerts []*Alert

		b, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		defer func() { _ = r.Body.Close() }()

		err = json.Unmarshal(b, &alerts)
		require.NoError(t, err)

		alertsReceived.Add(int64(len(alerts)))

		// Wait for the test to release us.
		<-releaseReceiver

		w.WriteHeader(http.StatusOK)
	}))
	defer func() {
		server.Close()
	}()

	const user = "user-1"
	ctx := context.Background()
	logger := testutil.NewTestingLogger(t)
	user1Group1 := createRuleGroup("group-1", user, createRecordingRule("count:metric_1", "count(metric_1)"))

	cfg := Config{
		RulePath:                  t.TempDir(),
		NotificationQueueCapacity: 1000,
		NotificationTimeout:       10 * time.Second,
	}
	limits := validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
		*defaults = *validation.MockDefaultLimits()
		defaults.RulerAlertmanagerClientConfig.AlertmanagerURL = server.URL
	})

	m, err := NewDefaultMultiTenantManager(cfg, managerMockFactory, nil, logger, nil, limits, afero.NewMemMapFs())
	require.NoError(t, err)

	m.SyncFullRuleGroups(ctx, map[string]rulespb.RuleGroupList{
		user: {user1Group1},
	}, nil)
	m.Start()

	// Wait for the manager to be running and have discovered the Alertmanager, then queue a notification.
	userManager := assertManagerMockRunningForUser(t, m, user)
	waitForAlertmanagerToBeDiscovered(t, userManager.notifier)
	userManager.notifier.Send(&notifier.Alert{Labels: labels.FromStrings(labels.AlertName, "alert-1")})

	// Wait for the Alertmanager to receive the request.
	select {
	case <-receiverReceivedRequest:
		// We can continue.
	case <-time.After(time.Second):
		require.FailNow(t, "gave up waiting for first notification request to be sent")
	}

	// Queue a second notification immediately before the manager is stopped.
	// This second notification will remain in the queue until we release the first notification's request by closing releaseReceiver below.
	userManager.onStop = func() {
		userManager.notifier.Send(&notifier.Alert{Labels: labels.FromStrings(labels.AlertName, "alert-2")})
	}

	// Stop() blocks until the user managers and notifiers have stopped, so run it in the background.
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		m.Stop()
	}()

	assertManagerMockStopped(t, userManager)

	// Wait a bit for the notifier to be told to shut down.
	// This is a hack, but we have no more robust way to ensure that the notifier has acknowledged the shutdown request.
	time.Sleep(100 * time.Millisecond)

	// The notifier should now be in the draining state.
	// Release the first request so that the second notification is drained from the queue, then check that both notifications are received and the manager has stopped.
	close(releaseReceiver)
	require.Eventually(t, func() bool {
		return alertsReceived.Load() == 2
	}, time.Second, 10*time.Millisecond, "gave up waiting for second notification to be sent")

	select {
	case <-stopped:
		// Manager has stopped, nothing more to do.
	case <-time.After(time.Second):
		require.FailNow(t, "gave up waiting for multi-tenant manager to stop")
	}
}

func waitForAlertmanagerToBeDiscovered(t *testing.T, notifier *notifier.Manager) {
	// There is a hardcoded 5 second refresh interval in discovery.Manager, so we need to wait for that to happen at least once.
	require.Eventually(t, func() bool {
		return len(notifier.Alertmanagers()) > 0
	}, 10*time.Second, 100*time.Millisecond, "gave up waiting for static Alertmanager URL to be discovered")
}

func TestDefaultMultiTenantManager_SyncRuleGroups_ShouldNotWriteStaleMarkersForRuleGroupsOwnedByAnotherRuler(t *testing.T) {
	const (
		user1 = "user-1"
		// user-2 has rule groups with the same namespaces and names.
		// Its rule groups stay on this ruler.
		user2 = "user-2"
	)

	for _, partialSync := range []bool{false, true} {
		// The second namespace needs url path escaping.
		// It has only group-b.
		// So its rule file goes away when group-b leaves.
		for _, namespaceB := range []string{"namespace", "team/b%c d"} {
			// moved names the rule groups of user-1 that another ruler owns after the second sync.
			for _, moved := range []string{"none", "group-b", "all"} {
				name := fmt.Sprintf("partial sync: %t, group-b namespace: %q, moved: %s", partialSync, namespaceB, moved)
				t.Run(name, func(t *testing.T) {
					t.Parallel()

					synctest.Test(t, func(t *testing.T) {
						const interval = time.Second
						var (
							ctx       = t.Context()
							cfg       = defaultRulerConfig(t)
							options   = applyPrepareOptions(t, cfg.Ring.Common.InstanceID)
							reg       = prometheus.NewRegistry()
							fs        = afero.NewMemMapFs()
							queryable = newMockQueryable()
							pusher    = newStaleMarkerPusher()
							eng       = promql.NewEngine(promql.EngineOpts{
								MaxSamples:         1e6,
								ActiveQueryTracker: newTestActiveQueryTracker(t, util_log.SlogFromGoKit(log.NewNopLogger())),
								Timeout:            time.Minute,
							})
						)
						ruleGroup := func(userID, namespace, name, record string) *rulespb.RuleGroupDesc {
							return &rulespb.RuleGroupDesc{User: userID, Namespace: namespace, Name: name, Interval: interval, Rules: []*rulespb.RuleDesc{{Record: record, Expr: "1"}}}
						}
						var (
							groupA1 = ruleGroup(user1, "namespace", "group-a", "a")
							groupB1 = ruleGroup(user1, namespaceB, "group-b", "b")
							// This ruler never loads group-c.
							groupC1 = ruleGroup(user1, "namespace", "group-c", "c")
							groupA2 = ruleGroup(user2, "namespace", "group-a", "a")
							groupB2 = ruleGroup(user2, namespaceB, "group-b", "b")
							// user-2 gets group-d in the second sync.
							// So the rules manager of user-2 also runs Update in that sync.
							groupD2 = ruleGroup(user2, "namespace", "group-d", "d")
						)

						factory := DefaultTenantManagerFactory(cfg, pusher, queryable, promRules.EngineQueryFunc(eng, queryable), fs, &NoopMultiTenantConcurrencyController{}, options.limits, nil)
						m, err := NewDefaultMultiTenantManager(cfg, factory, reg, options.logger, nil, options.limits, fs)
						require.NoError(t, err)
						t.Cleanup(m.Stop)

						syncRuleGroups := func(ownedByUser, ownedElsewhereByUser map[string]rulespb.RuleGroupList) {
							if partialSync {
								m.SyncPartialRuleGroups(ctx, ownedByUser, ownedElsewhereByUser)
							} else {
								m.SyncFullRuleGroups(ctx, ownedByUser, ownedElsewhereByUser)
							}
						}

						syncRuleGroups(map[string]rulespb.RuleGroupList{
							user1: {groupA1, groupB1},
							user2: {groupA2, groupB2},
						}, nil)
						m.Start()

						// Wait until all groups have written a sample.
						// Then all groups have series to mark stale.
						time.Sleep(3 * interval)
						synctest.Wait()
						for _, userID := range []string{user1, user2} {
							require.Positive(t, pusher.samples(userID, "a"))
							require.Positive(t, pusher.samples(userID, "b"))
						}
						loaded1 := m.GetRules(user1)
						require.Len(t, loaded1, 2)
						require.Equal(t, groupA1.Name, loaded1[0].Name())
						loaded2 := map[string]*promRules.Group{}
						for _, g := range m.GetRules(user2) {
							loaded2[g.Name()] = g
						}
						require.Len(t, loaded2, 2)

						// Another ruler always owns group-c of user-1.
						// The counter must count only the rule groups that this ruler stops.
						ownedByUser := map[string]rulespb.RuleGroupList{user2: {groupA2, groupB2, groupD2}}
						ownedElsewhereByUser := map[string]rulespb.RuleGroupList{user1: {groupC1}}
						expectedUnloaded := 0
						switch moved {
						case "none":
							ownedByUser[user1] = rulespb.RuleGroupList{groupA1}
						case "group-b":
							ownedByUser[user1] = rulespb.RuleGroupList{groupA1}
							ownedElsewhereByUser[user1] = append(ownedElsewhereByUser[user1], groupB1)
							expectedUnloaded = 1
						case "all":
							// A full sync does not list a user with no owned rule groups.
							// A partial sync lists such a user with no rule groups.
							if partialSync {
								ownedByUser[user1] = nil
							}
							ownedElsewhereByUser[user1] = append(ownedElsewhereByUser[user1], groupA1, groupB1)
						}
						syncRuleGroups(ownedByUser, ownedElsewhereByUser)

						// The manager of a removed user stops in the background.
						// After the wait, the sample counts of user-1 are final.
						synctest.Wait()
						samplesA, samplesB := pusher.samples(user1, "a"), pusher.samples(user1, "b")

						// A removed group writes its stale markers 2 intervals after it stops.
						time.Sleep(4 * interval)
						synctest.Wait()

						if moved == "none" {
							require.Positive(t, pusher.staleMarkers(user1, "b"))
						} else {
							require.Zero(t, pusher.staleMarkers(user1, "b"))
						}
						require.Zero(t, pusher.staleMarkers(user1, "a"))
						require.Equal(t, samplesB, pusher.samples(user1, "b"))

						expectedMetrics := `
							# HELP cortex_ruler_rule_groups_unloaded_total Total number of rule groups that this ruler stopped without stale markers because another ruler owns them now, while the user keeps other rule groups on this ruler.
							# TYPE cortex_ruler_rule_groups_unloaded_total counter
						`
						if moved == "all" {
							// The manager of user-1 stops without stale markers.
							// Its counter series goes away.
							require.Empty(t, m.GetRules(user1))
							require.Equal(t, samplesA, pusher.samples(user1, "a"))
						} else {
							// Only group-b leaves the rules manager.
							// The rules manager keeps the same group-a.
							// So group-a keeps its state.
							groups := m.GetRules(user1)
							require.Len(t, groups, 1)
							require.Same(t, loaded1[0], groups[0])
							expectedMetrics += fmt.Sprintf("cortex_ruler_rule_groups_unloaded_total{user=%q} %d\n", user1, expectedUnloaded)
						}

						// The rule groups of user-2 stay loaded and write no stale markers.
						groups := m.GetRules(user2)
						require.Len(t, groups, 3)
						for _, g := range groups {
							if g.Name() != groupD2.Name {
								require.Same(t, loaded2[g.Name()], g)
							}
						}
						require.Zero(t, pusher.staleMarkers(user2, "a"))
						require.Zero(t, pusher.staleMarkers(user2, "b"))
						expectedMetrics += fmt.Sprintf("cortex_ruler_rule_groups_unloaded_total{user=%q} 0\n", user2)

						require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(expectedMetrics), "cortex_ruler_rule_groups_unloaded_total"))
					})
				})
			}
		}
	}
}

// staleMarkerPusher counts the samples and the stale markers that it receives for each user and metric name.
type staleMarkerPusher struct {
	mtx         sync.Mutex
	sampleCount map[string]int
	staleCount  map[string]int
}

func newStaleMarkerPusher() *staleMarkerPusher {
	return &staleMarkerPusher{sampleCount: map[string]int{}, staleCount: map[string]int{}}
}

func (p *staleMarkerPusher) Push(ctx context.Context, req *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for _, ts := range req.Timeseries {
		key := userID + "/" + mimirpb.FromLabelAdaptersToLabels(ts.Labels).Get(model.MetricNameLabel)
		for _, s := range ts.Samples {
			if value.IsStaleNaN(s.Value) {
				p.staleCount[key]++
			} else {
				p.sampleCount[key]++
			}
		}
	}
	return &mimirpb.WriteResponse{}, nil
}

func (p *staleMarkerPusher) samples(userID, name string) int {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	return p.sampleCount[userID+"/"+name]
}

func (p *staleMarkerPusher) staleMarkers(userID, name string) int {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	return p.staleCount[userID+"/"+name]
}

func getManager(m *DefaultMultiTenantManager, user string) RulesManager {
	m.userManagerMtx.RLock()
	defer m.userManagerMtx.RUnlock()

	return m.userManagers[user]
}

func getNotifier(m *DefaultMultiTenantManager, user string) *rulerNotifier {
	m.notifiersMtx.Lock()
	defer m.notifiersMtx.Unlock()
	return m.notifiers[user]
}

func assertManagerMockRunningForUser(t *testing.T, m *DefaultMultiTenantManager, userID string) *managerMock {
	t.Helper()

	rm := getManager(m, userID)
	require.NotNil(t, rm)

	// The ruler manager start is async, so we poll it.
	test.Poll(t, 1*time.Second, true, func() interface{} {
		return rm.(*managerMock).running.Load()
	})

	return rm.(*managerMock)
}

func assertManagerMockNotRunningForUser(t *testing.T, m *DefaultMultiTenantManager, userID string) {
	t.Helper()

	rm := getManager(m, userID)
	require.Nil(t, rm)
}

func assertManagerMockStopped(t *testing.T, m *managerMock) {
	t.Helper()

	// The ruler manager stop is async, so we poll it.
	test.Poll(t, 1*time.Second, false, func() interface{} {
		return m.running.Load()
	})
}

func assertNotifierRunningForUser(t *testing.T, m *DefaultMultiTenantManager, userID string) *rulerNotifier {
	t.Helper()
	n := getNotifier(m, userID)
	require.NotNil(t, n)
	return n
}

func assertRuleGroupsMappedOnDisk(t *testing.T, m *DefaultMultiTenantManager, userID string, expectedRuleGroups rulespb.RuleGroupList) {
	t.Helper()

	// Verify that the rule groups have been mapped on disk for the given user.
	users, err := m.mapper.users()
	require.NoError(t, err)

	if len(expectedRuleGroups) > 0 {
		require.Contains(t, users, userID)
	} else {
		require.NotContains(t, users, userID)
	}

	// Verify the content of the rule groups mapped to disk.
	for namespace, expectedFormattedRuleGroups := range expectedRuleGroups.Formatted() {
		// The mapper sort groups by name in reverse order, so we apply the same sorting
		// here to expected groups.
		slices.SortFunc(expectedFormattedRuleGroups, func(a, b rulefmt.RuleGroup) int {
			return strings.Compare(b.Name, a.Name)
		})

		expectedYAML, err := yaml.Marshal(rulefmt.RuleGroups{Groups: expectedFormattedRuleGroups})
		require.NoError(t, err)

		path := filepath.Join(m.mapper.Path, userID, namespace)
		file, err := m.mapper.FS.Open(path)
		require.NoError(t, err)

		content, err := io.ReadAll(file)
		require.NoError(t, err)
		assert.Equal(t, string(expectedYAML), string(content))

		require.NoError(t, file.Close())
	}
}

func clearStructChannel(ch chan struct{}) {
	for len(ch) > 0 {
		<-ch
	}
}

func managerMockFactory(_ context.Context, _ string, n *notifier.Manager, _ log.Logger, _ prometheus.Registerer) RulesManager {
	return &managerMock{done: make(chan struct{}), notifier: n}
}

type managerMock struct {
	running  atomic.Bool
	done     chan struct{}
	notifier *notifier.Manager
	onStop   func()
}

func (m *managerMock) Run() {
	defer m.running.Store(false)
	m.running.Store(true)
	<-m.done
}

func (m *managerMock) Stop() {
	if m.onStop != nil {
		m.onStop()
	}

	close(m.done)
}

func (m *managerMock) Update(time.Duration, []string, labels.Labels, string, promRules.GroupEvalIterationFunc) error {
	return nil
}

func (m *managerMock) RuleGroups() []*promRules.Group {
	return nil
}

func (m *managerMock) UnloadGroups(...string) int {
	return 0
}
